import type { Dispatcher } from "./dispatch";
import {
  NagiRuntimeError,
  NagiSnapshotDriftError,
  validationError,
} from "./errors";
import { Facts } from "./facts";
import type { FlowRegistry, FlowResolution } from "./flows";
import { compact } from "./internal";
import { resetFactsOf } from "./scheduler";
import { attemptOf, isStepTerminal, isTerminalRun, stepStateOf } from "./state";
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

    if (opts.from !== undefined) {
      if (!(opts.from in flow.steps)) {
        throw validationError(
          `replay({ from }): step "${opts.from}" is not a step in flow "${flow.id}".`,
          ["from"],
        );
      }
      await abortInFlight(runState, opts.from);
      const resets = resetFactsOf(flow, {
        runId,
        stepId: opts.from,
        at: clock.now(),
        ...compact({ scope: opts.scope }),
      });
      for (const fact of resets) await store.appendFact(runId, fact);
    }

    const run: ReplayedRun = { runId, flow };
    if (opts.fireHooks !== false) {
      await deps.dispatcherFor({ kind: "enqueue", run }).advance(runId);
      return;
    }
    const inline = makeInlineQueue(queue);
    const dispatcher = deps.dispatcherFor({
      kind: "inline",
      run,
      queue: inline,
    });
    try {
      await dispatcher.advance(runId);
      await drainInline(dispatcher, inline);
    } finally {
      await inline.handOff();
    }
  }

  async function abortInFlight(state: RunState, stepId: StepId): Promise<void> {
    const step = stepStateOf(state, stepId);
    if (step.tag !== "running") return;
    const { runId } = state;
    await store.appendFact(
      runId,
      Facts.stepAbortRequested({
        runId,
        stepId,
        attempt: step.attempt,
        at: clock.now(),
      }),
    );
    const start = Date.now();
    while (Date.now() - start < ABORT_SETTLE_DEADLINE_MS) {
      const s = await store.loadRunState(runId);
      const ss = s.steps[stepId];
      if (
        ss === undefined ||
        isStepTerminal(ss) ||
        isTerminalRun(s) ||
        attemptOf(ss) > step.attempt
      ) {
        return;
      }
      await new Promise((r) => setTimeout(r, ABORT_SETTLE_POLL_MS));
    }
    throw new NagiRuntimeError(
      `replay({ from }): timed out after ${ABORT_SETTLE_DEADLINE_MS}ms waiting for step "${stepId}" ` +
        `(attempt ${step.attempt}) to honor abort signal. Handler may be ignoring ctx.signal.`,
    );
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
