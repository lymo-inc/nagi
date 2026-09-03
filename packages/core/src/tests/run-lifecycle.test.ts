import { describe, expect, it, vi } from "vitest";
import { flow } from "../builder";
import type { Hooks } from "../exec/hooks";
import { makeFlowRegistry } from "../flow-registry";
import { InMemoryClock, InMemoryQueue, InMemoryStore } from "../memory";
import {
  makeRunLifecycle,
  type RunLifecycle,
  type StagedStart,
  type TxBoundary,
} from "../run-lifecycle";
import type {
  Flow,
  ParentRef,
  Queue,
  QueueEnqueueOpts,
  RunId,
  StepId,
  Tx,
} from "../types";
import { passthroughSchema } from "./test-helpers";

interface VideoInput {
  readonly videoId: string;
}

const plainFlow = flow({
  id: "lc-plain",
  input: passthroughSchema<VideoInput>(),
  build: (b) => ({
    analyze: b.task({ run: async ({ input }) => ({ ok: input.videoId }) }),
  }),
});

const concurrentFlow = flow({
  id: "lc-concurrent",
  input: passthroughSchema<VideoInput>(),
  concurrency: { keyFn: (i) => i.videoId, mode: "cancel-in-progress" },
  build: (b) => ({
    analyze: b.task({ run: async ({ input }) => ({ ok: input.videoId }) }),
  }),
});

const gatedFlow = flow({
  id: "lc-gated",
  input: passthroughSchema<VideoInput>(),
  build: (b) => ({
    maybe: b.task({ when: () => false, run: async () => ({}) }),
    always: b.task({ run: async () => ({}) }),
  }),
});

const FAKE_TX = { __fakeTx: true } as unknown as Tx;
const OWN: TxBoundary = { kind: "own" };
const CALLER: TxBoundary = { kind: "caller", tx: FAKE_TX };

interface Fixture {
  readonly lifecycle: RunLifecycle;
  readonly store: InMemoryStore;
  readonly queue: InMemoryQueue;
  readonly clock: InMemoryClock;
  readonly fired: string[];
  readonly txEnqueued: Array<{ tx: Tx; runId: RunId; stepId: StepId }>;
  readonly advance: ReturnType<typeof vi.fn>;
  readonly propagateToParent: ReturnType<typeof vi.fn>;
}

function makeFixture(flows: readonly Flow[]): Fixture {
  const store = new InMemoryStore();
  const queue = new InMemoryQueue();
  const clock = new InMemoryClock();
  const fired: string[] = [];
  const txEnqueued: Fixture["txEnqueued"] = [];
  const hooks: Hooks = {
    async fireHook(hook, event, name) {
      fired.push(name);
      await hook?.(event);
    },
    async fireStepLifecycle() {},
  };
  const advance = vi.fn(async () => {});
  const propagateToParent = vi.fn(async () => {});
  const lifecycle = makeRunLifecycle({
    store,
    queue,
    clock,
    registry: makeFlowRegistry(flows),
    codeVersion: "test",
    hashFor: (id) => `hash-${id}`,
    queueForTx: (tx): Queue => ({
      async enqueue(runId: RunId, stepId: StepId, opts?: QueueEnqueueOpts) {
        txEnqueued.push({ tx, runId, stepId });
        await queue.enqueue(runId, stepId, opts);
      },
      dequeue: queue.dequeue.bind(queue),
      ack: queue.ack.bind(queue),
      nack: queue.nack.bind(queue),
      extend: queue.extend.bind(queue),
    }),
    hooks,
    flowHooks: undefined,
    dispatcher: { advance, propagateToParent },
    emitLog: () => {},
  });
  return {
    lifecycle,
    store,
    queue,
    clock,
    fired,
    txEnqueued,
    advance,
    propagateToParent,
  };
}

const PARENT: ParentRef = {
  runId: "parent-run" as RunId,
  stepId: "child" as StepId,
  attempt: 1 as never,
};

function started(staged: StagedStart) {
  if (staged.kind !== "started") throw new Error(`expected started`);
  return staged.effects;
}

interface Scenario {
  readonly name: string;
  readonly flow: Flow;
  readonly boundary: TxBoundary;
  readonly parent?: ParentRef;
  // A prior run to supersede, started on its own tx and applied.
  readonly prior?: boolean;
  readonly expectDispatch: (runId: RunId) => unknown;
  readonly expectSuperseded: (prior: RunId | undefined) => unknown;
  readonly expectTxEnqueued: number;
}

const scenarios: readonly Scenario[] = [
  {
    name: "plain",
    flow: plainFlow,
    boundary: OWN,
    expectDispatch: () => ({ kind: "enqueue", steps: ["analyze"] }),
    expectSuperseded: () => [],
    expectTxEnqueued: 0,
  },
  {
    name: "concurrency supersede",
    flow: concurrentFlow,
    boundary: OWN,
    prior: true,
    expectDispatch: () => ({ kind: "enqueue", steps: ["analyze"] }),
    expectSuperseded: (prior) => [
      expect.objectContaining({
        runId: prior,
        flowId: "lc-concurrent",
        error: expect.objectContaining({
          name: "NagiCanceledError",
          cause: { canceledByRunId: expect.any(String), concurrencyKey: "v1" },
        }),
      }),
    ],
    expectTxEnqueued: 0,
  },
  {
    name: "with parent",
    flow: plainFlow,
    boundary: OWN,
    parent: PARENT,
    expectDispatch: () => ({ kind: "enqueue", steps: ["analyze"] }),
    expectSuperseded: () => [],
    expectTxEnqueued: 0,
  },
  {
    name: "staged, no supersede",
    flow: plainFlow,
    boundary: CALLER,
    expectDispatch: () => ({ kind: "enqueued", steps: ["analyze"] }),
    expectSuperseded: () => [],
    expectTxEnqueued: 1,
  },
  {
    name: "staged, concurrency supersede",
    flow: concurrentFlow,
    boundary: CALLER,
    prior: true,
    expectDispatch: () => ({ kind: "enqueued", steps: ["analyze"] }),
    expectSuperseded: (prior) => [
      expect.objectContaining({ runId: prior, flowId: "lc-concurrent" }),
    ],
    expectTxEnqueued: 1,
  },
  {
    name: "when-false root defers to advance (own)",
    flow: gatedFlow,
    boundary: OWN,
    expectDispatch: () => ({ kind: "advance" }),
    expectSuperseded: () => [],
    expectTxEnqueued: 0,
  },
  {
    name: "when-false root defers to advance (staged)",
    flow: gatedFlow,
    boundary: CALLER,
    expectDispatch: () => ({ kind: "advance" }),
    expectSuperseded: () => [],
    expectTxEnqueued: 0,
  },
];

describe("run lifecycle — start scenarios", () => {
  for (const s of scenarios) {
    it(s.name, async () => {
      const fx = makeFixture([s.flow]);
      const input = { videoId: "v1" };

      let priorRunId: RunId | undefined;
      if (s.prior === true) {
        priorRunId = "prior-run" as RunId;
        const prior = await fx.lifecycle.stage({
          flow: s.flow,
          validatedInput: input,
          runId: priorRunId,
          parent: undefined,
          boundary: OWN,
        });
        await fx.lifecycle.applyEffects(started(prior));
        await fx.queue.dequeue({ count: 10 });
        fx.fired.length = 0;
      }

      const runId = "run-1" as RunId;
      const staged = await fx.lifecycle.stage({
        flow: s.flow,
        validatedInput: input,
        runId,
        parent: s.parent,
        boundary: s.boundary,
      });
      const effects = started(staged);

      expect(effects.dispatch).toEqual(s.expectDispatch(runId));
      expect(effects.superseded).toEqual(s.expectSuperseded(priorRunId));
      const state = await fx.store.loadRunState(runId);
      expect(state.flowId).toBe(s.flow.id);
      expect(state.flowHash).toBe(`hash-${s.flow.id}`);
      expect(effects.started).toEqual({
        runId,
        flowId: s.flow.id,
        input,
        at: state.facts[0]?.at,
        ...(s.parent !== undefined ? { parent: s.parent } : {}),
      });

      // Staging writes the row (+ tx-bound enqueue) and nothing else: no hooks,
      // no parent propagation, no own-tx enqueue until the effects are applied.
      expect(fx.fired).toEqual([]);
      expect(fx.propagateToParent).not.toHaveBeenCalled();
      expect(fx.advance).not.toHaveBeenCalled();
      expect(fx.txEnqueued).toHaveLength(s.expectTxEnqueued);
      for (const e of fx.txEnqueued) expect(e.tx).toBe(FAKE_TX);
      const queuedBeforeApply = await fx.queue.dequeue({ count: 10 });
      expect(queuedBeforeApply).toHaveLength(s.expectTxEnqueued);

      if (s.parent !== undefined) {
        expect(state.parent).toEqual({
          runId: s.parent.runId,
          stepId: s.parent.stepId,
        });
        expect(await fx.store.listChildren(s.parent.runId)).toEqual([runId]);
      }

      await fx.lifecycle.applyEffects(effects);

      const supersededHooks = effects.superseded.flatMap(() => [
        "flow.onError",
        "onFlowError",
      ]);
      expect(fx.fired).toEqual([
        ...supersededHooks,
        "flow.onStart",
        "onFlowStart",
      ]);
      expect(fx.propagateToParent).toHaveBeenCalledTimes(
        effects.superseded.length,
      );
      for (const e of effects.superseded) {
        expect(fx.propagateToParent).toHaveBeenCalledWith(e.runId, {
          kind: "canceled",
          error: e.error,
        });
      }
      expect(fx.advance).toHaveBeenCalledTimes(
        effects.dispatch.kind === "advance" ? 1 : 0,
      );
      const queuedAfterApply = await fx.queue.dequeue({ count: 10 });
      expect(queuedAfterApply.map((m) => m.stepId)).toEqual(
        effects.dispatch.kind === "enqueue" ? effects.dispatch.steps : [],
      );
    });
  }

  it("re-staging an existing runId yields `exists` on either boundary", async () => {
    const fx = makeFixture([plainFlow]);
    const runId = "dup" as RunId;
    const first = await fx.lifecycle.stage({
      flow: plainFlow,
      validatedInput: { videoId: "v1" },
      runId,
      parent: undefined,
      boundary: OWN,
    });
    expect(first.kind).toBe("started");
    for (const boundary of [OWN, CALLER]) {
      const again = await fx.lifecycle.stage({
        flow: plainFlow,
        validatedInput: { videoId: "v1" },
        runId,
        parent: undefined,
        boundary,
      });
      expect(again).toEqual({ kind: "exists" });
    }
    expect(fx.txEnqueued).toHaveLength(0);
  });

  it("start() mints a runId, validates a supplied one, and validates input", async () => {
    const fx = makeFixture([plainFlow]);
    const minted = await fx.lifecycle.start({
      flowId: "lc-plain",
      input: { videoId: "v1" },
      runId: undefined,
      boundary: OWN,
    });
    expect(minted.runId).toMatch(/^run-/);
    expect(minted.staged.kind).toBe("started");

    await expect(
      fx.lifecycle.start({
        flowId: "lc-plain",
        input: { videoId: "v1" },
        runId: "" as RunId,
        boundary: OWN,
      }),
    ).rejects.toThrow("opts.runId must be a non-empty string");
  });
});
