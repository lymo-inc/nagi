import {
  type NagiFlowSnapshotGoneError,
  NagiRunStrandedError,
  NagiSignalTimeoutError,
  serializeError,
} from "./errors";
import { makeHooks } from "./exec/hooks";
import { makeMessage } from "./exec/message";
import { type AdvanceOpts, makeProgression } from "./exec/progression";
import { type FlowOf, requireCurrent } from "./flows";
import { asStepMapWithDefs, type EmitLog, getDef } from "./internal";
import {
  isTerminalRun,
  type RunCancelCause,
  type RunState,
  type TerminalPhase,
} from "./state";
import type {
  CancelArgs,
  Clock,
  Flow,
  FlowCanceledByConcurrencyFact,
  FlowHooks,
  Millis,
  ParentLink,
  Queue,
  QueueMessage,
  RetryPolicy,
  RunId,
  Store,
  StreamTransport,
} from "./types";

export interface HeartbeatConfig {
  readonly intervalMs: Millis;
  readonly leaseMs: Millis;
  readonly holdWarnMs: Millis;
}

export interface DispatchDeps {
  readonly flowOf: FlowOf;
  // By flow id, for a subflow child about to start on this code.
  readonly lookupFlow: (flowId: string) => Flow | undefined;
  readonly startChildRun: (args: {
    readonly child: Flow;
    readonly childInput: unknown;
    readonly parent: ParentLink;
    readonly generation: number;
  }) => Promise<RunId>;
  readonly store: Store;
  readonly streamTransport?: StreamTransport;
  readonly queue: Queue;
  readonly clock: Clock;
  readonly hooks?: FlowHooks;
  readonly emitLog: EmitLog;
  readonly defaultRetry?: RetryPolicy;
  readonly fireHooks: boolean;
  readonly heartbeat: HeartbeatConfig;
}

// The run being ended. "gone": its definition is unavailable (snapshot gone,
// unregistered), so only the global hooks fire, not flow.onComplete/onError.
export type EndingRun =
  | { readonly kind: "resolved"; readonly runId: RunId; readonly flow: Flow }
  | { readonly kind: "gone"; readonly runId: RunId; readonly flowId: string };

// Explicit cancels are excluded: they cascade to children, so they
// only end a run through Dispatcher.cancel.
export type RunEnd =
  | Exclude<TerminalPhase, { readonly tag: "canceled" }>
  | {
      readonly tag: "canceled";
      readonly cause: Extract<RunCancelCause, { readonly kind: "concurrency" }>;
    };

// "snapshot-gone" is a live run pinned to a flowHash this process did not
// register: the message is left unsettled for the worker's policy. A terminal
// run's gone message is acked inside dispatchMessage and reports "done".
export type DispatchResult =
  | { readonly kind: "done" }
  | {
      readonly kind: "snapshot-gone";
      readonly error: NagiFlowSnapshotGoneError;
    };

export interface Dispatcher {
  dispatchMessage(message: QueueMessage): Promise<DispatchResult>;
  advance(runId: RunId, opts?: AdvanceOpts): Promise<void>;
  terminate(run: EndingRun, end: RunEnd): Promise<void>;
  // The store already committed the flow.canceled fact (tryStartRun's
  // concurrency pass); only the post-terminal effects remain.
  settleSuperseded(
    run: EndingRun,
    fact: FlowCanceledByConcurrencyFact,
  ): Promise<void>;
  cancel(runId: RunId, args: CancelArgs): Promise<void>;
  // Fail any signal step whose timeout has elapsed, then advance the run so the
  // failure propagates to flow.failed. Returns the number of runs failed. The
  // store does the atomic fail+delete under its per-run lock; advance runs here,
  // outside that lock, exactly as wf.signal() advances after settleSignal.
  sweepTimers(now: Date): Promise<number>;
  // Re-drive every quiet run (Store.listQuietRuns, then no queued message), and
  // fail with NagiRunStrandedError a run that re-driving cannot move. Returns
  // the number of runs it moved or ended.
  sweepQuietRuns(now: Date, quietMs: Millis): Promise<number>;
}

export function makeDispatcher(deps: DispatchDeps): Dispatcher {
  const hooks = makeHooks(deps);
  const progression = makeProgression(deps, hooks);
  const message = makeMessage(deps, hooks, progression);

  async function sweepTimers(now: Date): Promise<number> {
    const timedOut = await deps.store.sweepSignalTimeouts({ now });
    for (const { runId, stepId, attempt, fireAt } of timedOut) {
      try {
        // Fire onStepError before finalizing the flow, the same ordering every
        // other failure path uses (handleStepError, markStepSettled) — so a
        // consumer's per-step error handling sees signal timeouts too.
        if (deps.hooks?.onStepError) {
          const flow = requireCurrent(await deps.flowOf(runId));
          await hooks.fireHook(
            deps.hooks.onStepError,
            {
              runId,
              flowId: flow.id,
              stepId,
              attempt,
              kind: "signal",
              error: serializeError(
                new NagiSignalTimeoutError({ runId, stepId, fireAt }),
              ),
              at: now,
            },
            "onStepError",
          );
        }
        await progression.advance(runId);
      } catch (err) {
        // The store already failed the step and dropped its timer; this run
        // will not be re-swept. Log and keep going — the rest of the batch
        // must not be stranded behind one gone snapshot or store blip.
        deps.emitLog({
          level: "error",
          msg: "nagi: signal timeout fired but the run could not be advanced — re-drive with replay({ from })",
          attrs: { runId, stepId, attempt, error: String(err) },
        });
      }
    }
    return timedOut.length;
  }

  // A quiet run's only way forward is a message for each step that was in
  // flight when its driver vanished, plus an advance for everything after.
  async function redrive(flow: Flow, state: RunState): Promise<boolean> {
    const { queue } = deps;
    const defs = asStepMapWithDefs(flow.steps);
    let deliberateWait = false;
    for (const [stepId, step] of Object.entries(state.steps)) {
      const def = defs[stepId] === undefined ? undefined : getDef(defs[stepId]);
      switch (step.tag) {
        case "awaitingSignal":
          // timeoutMs: "unbounded" is the explicit opt-in to park forever.
          if (def?.kind === "signal" && def.timeoutMs === "unbounded") {
            deliberateWait = true;
            break;
          }
          await queue.enqueue(state.runId, stepId, {
            attempt: step.attempt,
            flowId: flow.id,
          });
          break;
        case "running":
        case "awaitingChild":
        case "aborting":
          await queue.enqueue(state.runId, stepId, {
            attempt: step.attempt,
            flowId: flow.id,
          });
          break;
        case "backoff":
          await queue.enqueue(state.runId, stepId, {
            attempt: step.failedAttempt + 1,
            delayMs: Math.max(0, step.retryAt.getTime() - Date.now()),
            flowId: flow.id,
          });
          break;
        default:
          break;
      }
    }
    await progression.advance(state.runId);
    return deliberateWait;
  }

  async function driveQuietRun(runId: RunId): Promise<boolean> {
    const { store, queue } = deps;
    const inspect = queue.inspect?.bind(queue);
    if (inspect === undefined) return false;
    if ((await inspect(runId)).length > 0) return false; // backlog, not strand
    const state = await store.loadRunState(runId);
    if (isTerminalRun(state)) return false;

    const resolution = await deps.flowOf(runId);
    switch (resolution.kind) {
      case "gone-terminal":
        return false;
      case "gone-live":
        // No message will ever reach the worker's snapshot-gone policy.
        await progression.terminate(
          { kind: "gone", runId, flowId: resolution.error.flowId },
          { tag: "failed", error: serializeError(resolution.error) },
        );
        return true;
      case "current":
        break;
    }

    const deliberateWait = await redrive(resolution.flow, state);
    const after = await store.loadRunState(runId);
    if (isTerminalRun(after) || (await inspect(runId)).length > 0) return true;
    if (deliberateWait) return false;

    const open = Object.entries(after.steps)
      .filter(([, s]) => !["completed", "skipped", "failed"].includes(s.tag))
      .map(([id]) => id);
    await progression.terminate(
      { kind: "resolved", runId, flow: resolution.flow },
      {
        tag: "failed",
        error: serializeError(
          new NagiRunStrandedError({
            runId,
            flowId: resolution.flow.id,
            stepIds: open,
            reason: "re-driving it enqueued nothing",
          }),
        ),
      },
    );
    return true;
  }

  async function sweepQuietRuns(now: Date, quietMs: Millis): Promise<number> {
    const quiet = await deps.store.listQuietRuns({
      quietSince: new Date(now.getTime() - quietMs),
    });
    let moved = 0;
    for (const runId of quiet) {
      try {
        if (await driveQuietRun(runId)) {
          moved++;
          deps.emitLog({
            level: "warn",
            msg: "nagi: quiet run re-driven",
            attrs: { runId },
          });
        }
      } catch (err) {
        // The run stays quiet, so the next sweep tries again.
        deps.emitLog({
          level: "error",
          msg: "nagi: quiet run could not be re-driven",
          attrs: { runId, error: String(err) },
        });
      }
    }
    return moved;
  }

  return {
    sweepQuietRuns,
    dispatchMessage: message.dispatchMessage,
    advance: progression.advance,
    terminate: progression.terminate,
    settleSuperseded: progression.settleSuperseded,
    cancel: progression.cancel,
    sweepTimers,
  };
}
