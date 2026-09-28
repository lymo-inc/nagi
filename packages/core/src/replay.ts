import type { Dispatcher } from "./dispatch";
import {
  NagiRuntimeError,
  NagiSnapshotDriftError,
  validationError,
} from "./errors";
import type { FlowRegistry, FlowResolution } from "./flows";
import { compact } from "./internal";
import { resetFactsOf } from "./scheduler";
import type {
  Clock,
  DriftPolicy,
  Flow,
  Queue,
  QueueMessage,
  ReplayOpts,
  RunId,
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
        `Run ${runId} was canceled (superseded by a newer run with the same concurrency key). ` +
          `Replay is not supported for canceled runs — start a new run instead.`,
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
      if (runState.phase.tag === "running") {
        throw new NagiRuntimeError(
          `Run ${runId} is still running — replay({ from }) would race in-flight workers. ` +
            `Wait for the run to settle (completed / failed) before resetting from a step.`,
        );
      }
      if (!(opts.from in flow.steps)) {
        throw validationError(
          `replay({ from }): step "${opts.from}" is not a step in flow "${flow.id}".`,
          ["from"],
        );
      }
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

  return { replay };
}

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
