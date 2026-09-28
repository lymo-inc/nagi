import { describe, expect, it } from "vitest";
import { flow } from "../builder";
import { NagiCanceledError } from "../errors";
import { InMemoryClock, InMemoryQueue, InMemoryStore } from "../memory";
import { nagi } from "../runtime";
import { stepStateOf } from "../state";
import type { FlowErrorEvent, RunId, SerializedError } from "../types";
import { type Harness, makeHarness, passthroughSchema } from "./test-helpers";

type Mode = "fail" | "cancel-self" | "park";

interface ChildInput {
  readonly mode: Mode;
  readonly key: string;
}

function makeFlows(flowErrors: FlowErrorEvent[]) {
  const child = flow({
    id: "term-child",
    input: passthroughSchema<ChildInput>(),
    concurrency: { keyFn: (i) => i.key, mode: "cancel-in-progress" },
    onError: (e) => {
      flowErrors.push(e);
    },
    build: (b) => {
      const work = b.task({
        retry: { maxAttempts: 1, backoff: "fixed", initialDelayMs: 0 },
        run: async ({ input, ctx }) => {
          if (input.mode === "fail") throw new Error("child blew up");
          if (input.mode === "cancel-self") {
            throw new NagiCanceledError({
              runId: ctx.runId,
              canceledByRunId: "run-winner" as RunId,
              concurrencyKey: input.key,
            });
          }
          return {};
        },
      });
      const wait = b.signal({
        needs: { work },
        timeoutMs: "unbounded" as const,
        schema: passthroughSchema<{ ok: true }>(),
      });
      return { work, wait };
    },
  });
  const parent = flow({
    id: "term-parent",
    input: passthroughSchema<ChildInput>(),
    build: (b) => ({
      sub: b.subflow(child, { input: ({ input }) => input }),
    }),
  });
  return { child, parent };
}

interface Case {
  readonly name: string;
  readonly mode: Mode;
  readonly phase: "failed" | "canceled";
  readonly end?: (h: Harness, childRunId: RunId) => Promise<void>;
  readonly message: RegExp;
}

const cases: readonly Case[] = [
  {
    name: "handler failure",
    mode: "fail",
    phase: "failed",
    message: /child blew up/,
  },
  {
    name: "handler throws NagiCanceledError (supersede observed in-handler)",
    mode: "cancel-self",
    phase: "canceled",
    message: /superseded by run run-winner/,
  },
  {
    name: "concurrency supersede committed by the store",
    mode: "park",
    phase: "canceled",
    end: async (h) => {
      const f = makeFlows([]).child;
      await h.wf.startById(f.id, { mode: "park", key: "k" });
    },
    message: /superseded by run run-/,
  },
  {
    name: "wf.cancel",
    mode: "park",
    phase: "canceled",
    end: (h, id) => h.wf.cancel(id, { reason: "stop" }),
    message: /was canceled: stop/,
  },
  {
    name: "operator abort",
    mode: "park",
    phase: "canceled",
    end: (h, id) => h.wf.operator().abort(id, { actor: "alice", note: "why" }),
    message: /was canceled by alice: why/,
  },
];

describe("run termination — every end fires the error hooks and wakes the parent with one error", () => {
  for (const c of cases) {
    it(c.name, async () => {
      const flowErrors: FlowErrorEvent[] = [];
      const globalErrors: FlowErrorEvent[] = [];
      const { child, parent } = makeFlows(flowErrors);
      const h = await makeHarness([parent, child], {
        hooks: { onFlowError: (e) => void globalErrors.push(e) },
      });

      const parentRunId = await h.wf.start(parent, { mode: c.mode, key: "k" });
      await h.drain();
      const [childRunId] = await h.store.listChildren(parentRunId);
      if (childRunId === undefined) throw new Error("no child run");
      await c.end?.(h, childRunId);
      await h.drain();

      expect((await h.store.loadRunState(childRunId)).phase.tag).toBe(c.phase);

      const childGlobal = globalErrors.filter((e) => e.runId === childRunId);
      const childFlow = flowErrors.filter((e) => e.runId === childRunId);
      expect(childGlobal).toHaveLength(1);
      expect(childFlow).toHaveLength(1);
      const error = childGlobal[0]?.error as SerializedError;
      expect(childFlow[0]?.error).toEqual(error);
      expect(error.message).toMatch(c.message);

      const parentState = await h.store.loadRunState(parentRunId);
      const sub = stepStateOf(parentState, "sub");
      expect(sub.tag).toBe("failed");
      if (sub.tag === "failed") expect(sub.error).toEqual(error);
      expect(parentState.phase.tag).toBe("failed");
    });
  }
});

describe("run termination — snapshot gone", () => {
  it("fires the global error hook and wakes the parent", async () => {
    const store = new InMemoryStore();
    const queue = new InMemoryQueue();
    const clock = new InMemoryClock();

    const a = makeFlows([]);
    const wfA = await nagi({ flows: [a.parent, a.child], store, queue, clock });
    const parentRunId = await wfA.start(a.parent, { mode: "park", key: "k" });
    // Dispatch only the parent's subflow step: the child is spawned and its
    // messages stay queued for the next deploy.
    await wfA.worker({ timerSweepIntervalMs: 0 }).runOnce({ maxSteps: 1 });
    const [childRunId] = await store.listChildren(parentRunId);
    if (childRunId === undefined) throw new Error("no child run");

    const drifted = flow({
      id: "term-child",
      input: passthroughSchema<ChildInput>(),
      build: (b) => ({ other: b.task({ run: async () => ({}) }) }),
    });
    const globalErrors: FlowErrorEvent[] = [];
    const wfB = await nagi({
      flows: [a.parent, drifted],
      store,
      queue,
      clock,
      hooks: { onFlowError: (e) => void globalErrors.push(e) },
    });
    await wfB
      .worker({
        timerSweepIntervalMs: 0,
        snapshotGonePolicy: () => ({ action: "fail" }),
      })
      .runUntilEmpty();

    expect((await store.loadRunState(childRunId)).phase.tag).toBe("failed");
    const childErrors = globalErrors.filter((e) => e.runId === childRunId);
    expect(childErrors).toHaveLength(1);
    expect(childErrors[0]).toMatchObject({
      flowId: "term-child",
      error: { name: "NagiFlowSnapshotGoneError" },
    });

    const parentState = await store.loadRunState(parentRunId);
    const sub = stepStateOf(parentState, "sub");
    expect(sub.tag).toBe("failed");
    if (sub.tag === "failed") expect(sub.error).toEqual(childErrors[0]?.error);
    expect(parentState.phase.tag).toBe("failed");
  });
});
