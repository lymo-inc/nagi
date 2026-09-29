import type { Dispatcher } from "./dispatch";
import {
  NagiRuntimeError,
  NagiSnapshotDriftError,
  validationError,
} from "./errors";
import { Facts } from "./facts";
import type { FlowRegistry, FlowResolution } from "./flows";
import { compact } from "./internal";
import { currentChildRunId } from "./run-id";
import { resetFactsOf } from "./scheduler";
import { isStepTerminal, isTerminalRun, stepStateOf } from "./state";
import type {
  Clock,
  DriftPolicy,
  Flow,
  Queue,
  QueueMessage,
  ReplayOpts,
  RunId,
  RunState,
  StepId,
  Store,
} from "./types";

export interface ReplayedRun {
  readonly runId: RunId;
  readonly flow: Flow;
}

// The dispatcher one replay call drives. `run.flow` answers for run.runId
// alone, so a drift-allowed synthesis never answers for a parent the run wakes.
//   "enqueue" — shared queue, hooks on; workers carry the run on.
//   "inline"  — fireHooks: false. Hooks off, and every enqueue (the run, the
//               children it spawns, the parent it wakes) lands on `queue`, the
//               replay's own, which is the only queue the replay drains.
export type ReplayScope =
  | { readonly kind: "enqueue"; readonly run: ReplayedRun }
  | {
      readonly kind: "inline";
      readonly run: ReplayedRun;
      readonly queue: Queue;
    };

export interface ReplayDeps {
  readonly store: Store;
  readonly queue: Queue;
  readonly clock: Clock;
  readonly registry: FlowRegistry;
  readonly driftPolicy: DriftPolicy;
  readonly flowOfUnder: (
    runId: RunId,
    drift: DriftPolicy,
  ) => Promise<FlowResolution>;
  readonly dispatcherFor: (scope: ReplayScope) => Dispatcher;
}

export function makeReplay(deps: ReplayDeps): {
  replay(runId: RunId, opts?: ReplayOpts): Promise<void>;
} {
  const { registry, store, queue, clock, driftPolicy } = deps;

  async function replay(
    runId: RunId,
    opts: ReplayOpts = { mode: "continue" },
  ): Promise<void> {
    // allowDrift is the per-call override of a "freeze" runtime; under
    // "synthesize" the worker would resume this run against live handlers
    // anyway, so refusing here would only make replay stricter than dispatch.
    const drift: DriftPolicy =
      opts.allowDrift === true ? "synthesize" : driftPolicy;
    const resolution = await deps.flowOfUnder(runId, drift);
    if (resolution.kind !== "current" && resolution.live === undefined) {
      throw new NagiRuntimeError(
        `Run ${runId} references flow "${resolution.error.flowId}" which is not registered with nagi().`,
      );
    }
    const runState = await store.loadRunState(runId);
    if (runState.phase.tag === "canceled") {
      throw new NagiRuntimeError(
        `Run ${runId} was canceled. Replay is not supported for canceled runs — start a new run instead.`,
      );
    }
    if (opts.mode === "inspect") return;

    if (resolution.kind !== "current") {
      if (drift === "freeze") {
        throw new NagiSnapshotDriftError({
          runId,
          expected: resolution.error.pinnedHash,
          actual: registry.hashOf(resolution.error.flowId),
        });
      }
      throw resolution.error;
    }
    const { flow } = resolution;

    if (opts.from !== undefined && !(opts.from in flow.steps)) {
      throw validationError(
        `replay({ from }): step "${opts.from}" is not a step in flow "${flow.id}".`,
        ["from"],
      );
    }

    const run: ReplayedRun = { runId, flow };
    const inline =
      opts.fireHooks === false ? makeInlineQueue(queue) : undefined;
    const dispatcher = deps.dispatcherFor(
      inline === undefined
        ? { kind: "enqueue", run }
        : { kind: "inline", run, queue: inline },
    );
    try {
      if (opts.from !== undefined) {
        await resetFrom(flow, runState, opts.from, opts.scope, dispatcher);
      }
      await dispatcher.advance(runId);
      if (inline !== undefined) await drainInline(dispatcher, inline);
    } finally {
      await inline?.handOff();
    }
  }

  async function resetFrom(
    flow: Flow,
    runState: RunState,
    from: StepId,
    scope: ReplayOpts["scope"],
    dispatcher: Dispatcher,
  ): Promise<void> {
    const { runId } = runState;
    const origin = { runId, stepId: from, ...compact({ scope }) };
    const inFlight = resetFactsOf(flow, runState, {
      ...origin,
      at: clock.now(),
    }).map((f) => f.stepId);
    await abortInFlight(runState, inFlight);
    // Descendants can settle during the abort wait; the reset set is chosen
    // from their state.
    const current = await store.loadRunState(runId);
    const resets = resetFactsOf(flow, current, { ...origin, at: clock.now() });
    for (const fact of resets) await store.appendFact(runId, fact);

    // After the resets, so the old child's end finds its parent step no longer
    // awaitingChild and cannot settle it.
    for (const { stepId } of resets) {
      if (stepStateOf(current, stepId).tag !== "awaitingChild") continue;
      await dispatcher.cancel(await currentChildRunId(current, stepId), {
        cause: "explicit",
        reason: `superseded by replay({ from: "${from}" })`,
      });
    }
  }

  // Every running member of stepIds is aborted under one shared deadline. A
  // lease reap can start a newer attempt while we wait (the one we aborted was
  // dead), so every attempt that starts is aborted in turn: resetting under a
  // live attempt would let its pre-reset result settle the step.
  async function abortInFlight(
    state: RunState,
    stepIds: readonly StepId[],
  ): Promise<void> {
    const waiting = new Map<StepId, number>();
    for (const id of stepIds) {
      if (stepStateOf(state, id).tag === "running") waiting.set(id, 0);
    }
    const { runId } = state;
    const start = Date.now();
    for (let s = state; waiting.size > 0; s = await store.loadRunState(runId)) {
      if (isTerminalRun(s)) return;
      for (const [stepId, aborted] of waiting) {
        const step = stepStateOf(s, stepId);
        if (step.tag === "pending" || isStepTerminal(step)) {
          waiting.delete(stepId);
        } else if (step.tag === "running" && step.attempt > aborted) {
          waiting.set(stepId, step.attempt);
          await store.appendFact(
            runId,
            Facts.stepAbortRequested({
              runId,
              stepId,
              attempt: step.attempt,
              at: clock.now(),
            }),
          );
        }
      }
      const [stuck] = waiting;
      if (stuck === undefined) return;
      if (Date.now() - start >= ABORT_SETTLE_DEADLINE_MS) {
        throw new NagiRuntimeError(
          `replay({ from }): timed out after ${ABORT_SETTLE_DEADLINE_MS}ms waiting for step "${stuck[0]}" ` +
            `(attempt ${stuck[1]}) to honor abort signal. Handler may be ignoring ctx.signal.`,
        );
      }
      await new Promise((r) => setTimeout(r, ABORT_SETTLE_POLL_MS));
    }
  }

  return { replay };
}

// Wall-clock bounds (not the injected clock): the abort waits on a real
// handler that may be ignoring ctx.signal, so these must elapse in real time.
const ABORT_SETTLE_DEADLINE_MS = 30_000;
const ABORT_SETTLE_POLL_MS = 50;

const MAX_REPLAY_DISPATCHES = 4096;
async function drainInline(
  dispatcher: Dispatcher,
  inline: Queue,
): Promise<void> {
  for (let i = 0; i < MAX_REPLAY_DISPATCHES; i++) {
    const [msg] = await inline.dequeue({ count: 1 });
    if (msg === undefined) return;
    const result = await dispatcher.dispatchMessage(msg);
    if (result.kind === "snapshot-gone") throw result.error;
  }
}

interface InlineQueue extends Queue {
  // Moves every message still held here to the shared queue, so a replay that
  // throws or hits its dispatch cap strands nothing.
  handOff(): Promise<void>;
}

// Holds only what the replay's own execution enqueued, so draining it cannot
// pick up another run's message. A delayed enqueue (retry backoff) cannot be
// waited out inline and goes straight to the shared queue.
function makeInlineQueue(shared: Queue): InlineQueue {
  const ready: QueueMessage[] = [];
  const leased = new Map<string, QueueMessage>();
  let seq = 0;

  const toShared = (m: QueueMessage, delayMs?: number): Promise<void> =>
    shared.enqueue(m.runId, m.stepId, {
      attempt: m.attempt,
      payload: m.payload,
      ...compact({ flowId: m.flowId, delayMs }),
    });

  return {
    async enqueue(runId, stepId, opts) {
      if ((opts?.delayMs ?? 0) > 0) {
        await shared.enqueue(runId, stepId, opts);
        return;
      }
      ready.push({
        receipt: `inline-${seq++}`,
        runId,
        stepId,
        payload: opts?.payload ?? null,
        attempt: opts?.attempt ?? 1,
        readCount: 0,
        ...compact({ flowId: opts?.flowId }),
      });
    },
    async dequeue({ count }) {
      const out = ready
        .splice(0, count)
        .map((m) => ({ ...m, readCount: m.readCount + 1 }));
      for (const m of out) leased.set(m.receipt, m);
      return out;
    },
    async ack(receipt) {
      leased.delete(receipt);
    },
    async nack(receipt, opts) {
      const m = leased.get(receipt);
      if (m === undefined) return;
      leased.delete(receipt);
      if ((opts?.delayMs ?? 0) > 0) await toShared(m, opts?.delayMs);
      else ready.push(m);
    },
    async extend() {},
    async handOff() {
      const rest = [...leased.values(), ...ready.splice(0)];
      leased.clear();
      for (const m of rest) await toShared(m);
    },
  };
}
