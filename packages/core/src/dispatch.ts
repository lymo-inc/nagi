import {
  type NagiFlowSnapshotGoneError,
  NagiSignalTimeoutError,
  serializeError,
} from "./errors";
import { makeHooks } from "./exec/hooks";
import { makeMessage } from "./exec/message";
import { makeProgression } from "./exec/progression";
import { type FlowOf, requireCurrent } from "./flows";
import type { EmitLog } from "./internal";
import type { RunCancelCause, TerminalPhase } from "./state";
import type {
  CancelArgs,
  Clock,
  Flow,
  FlowCanceledByConcurrencyFact,
  FlowHooks,
  Millis,
  ParentRef,
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
    readonly parent: ParentRef;
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
  advance(runId: RunId): Promise<void>;
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

  return {
    dispatchMessage: message.dispatchMessage,
    advance: progression.advance,
    terminate: progression.terminate,
    settleSuperseded: progression.settleSuperseded,
    cancel: progression.cancel,
    sweepTimers,
  };
}
