import { describe, expect, it } from "vitest";
import { flow } from "../builder";
import { NagiRuntimeError } from "../errors";
import { InMemoryClock, InMemoryQueue, InMemoryStore } from "../memory";
import { nagi } from "../runtime";
import type { DriftPolicy, Flow, RunId } from "../types";
import { makeHarness, passthroughSchema } from "./test-helpers";

const empty = passthroughSchema<Record<string, never>>();

function childV1() {
  return flow({
    id: "rs-child",
    input: empty,
    build: (b) => ({ s: b.task({ run: async () => ({ v: 1 }) }) }),
  });
}

function childV2() {
  return flow({
    id: "rs-child",
    input: empty,
    build: (b) => ({
      s: b.task({ run: async () => ({ v: 2 }) }),
      extra: b.task({ run: async () => ({ extra: true }) }),
    }),
  });
}

function parentOf(child: Flow<"rs-child", typeof empty>) {
  return flow({
    id: "rs-parent",
    input: empty,
    build: (b) => {
      const sub = b.subflow(child, { input: () => ({}) });
      const after = b.task({ needs: { sub }, run: async () => ({ done: 1 }) });
      return { sub, after };
    },
  });
}

describe("replay scope — the override answers for the replayed run alone", () => {
  it("a drift-allowed child replay wakes its parent on the parent's own flow", async () => {
    const store = new InMemoryStore();
    const queue = new InMemoryQueue();
    const clock = new InMemoryClock();

    const c1 = childV1();
    const wfA = await nagi({ flows: [parentOf(c1), c1], store, queue, clock });
    const parentRunId = await wfA.start(parentOf(c1), {});
    // Dispatch only the parent's subflow step: the child is spawned and the
    // parent parks, the child's step still queued.
    await wfA.worker({ concurrency: 1 }).runOnce({ maxSteps: 1 });
    const [childRunId] = await store.listChildren(parentRunId);
    if (childRunId === undefined) throw new Error("child not spawned");

    // A deploy changes only the child, and this runtime freezes drift.
    const c2 = childV2();
    const wfB = await nagi({ flows: [parentOf(c2), c2], store, queue, clock });
    await wfB.replay(childRunId, {
      mode: "continue",
      allowDrift: true,
      fireHooks: false,
    });

    const child = await store.loadRunState(childRunId);
    expect(child.phase.tag).toBe("completed");
    expect(child.steps["s"]).toMatchObject({ output: { v: 2 } });
    expect(child.steps["extra"]).toBeUndefined();

    const parent = await store.loadRunState(parentRunId);
    expect(parent.phase.tag).toBe("completed");
    expect(parent.steps["after"]).toMatchObject({ output: { done: 1 } });
    // Resolved to the child's synthesized flow, the parent would have run the
    // child's step `s` instead of its own `after`.
    expect(parent.steps["s"]).toBeUndefined();
  });

  it("fireHooks: false never dispatches another run's queued message", async () => {
    const ran: RunId[] = [];
    const completed: RunId[] = [];
    const f = flow({
      id: "rs-isolated",
      input: empty,
      build: (b) => ({
        a: b.task({
          run: async ({ ctx }) => {
            ran.push(ctx.runId);
            return {};
          },
        }),
      }),
    });
    const h = await makeHarness(f, {
      hooks: { onFlowComplete: (e) => void completed.push(e.runId) },
    });
    const replayed = await h.wf.start(f, {});
    await h.drain();
    const other = await h.wf.start(f, {});

    await h.wf.replay(replayed, { mode: "continue", fireHooks: false });

    expect(ran).toEqual([replayed]);
    expect(await h.wf.inspectQueue(other)).toHaveLength(1);
    expect((await h.store.loadRunState(other)).phase.tag).toBe("running");

    // The other run is left to the worker, hooks and all.
    await h.drain();
    expect(ran).toEqual([replayed, other]);
    expect(completed).toEqual([replayed, other]);
  });

  it("fireHooks: false keeps a child spawned by the replay quiet and inline", async () => {
    const fired: string[] = [];
    const child = flow({
      id: "rs-quiet-child",
      input: empty,
      build: (b) => ({ s: b.task({ run: async () => ({ v: 1 }) }) }),
      onStart: () => void fired.push("child.onStart"),
    });
    const parent = flow({
      id: "rs-quiet-parent",
      input: empty,
      build: (b) => ({ sub: b.subflow(child, { input: () => ({}) }) }),
    });
    const h = await makeHarness([parent, child], {
      hooks: {
        onFlowStart: (e) => void fired.push(`onFlowStart:${e.flowId}`),
        onFlowComplete: (e) => void fired.push(`onFlowComplete:${e.flowId}`),
      },
    });
    const runId = await h.wf.start(parent, {});
    await h.drain();
    fired.length = 0;

    await h.wf.replay(runId, {
      mode: "continue",
      from: "sub",
      fireHooks: false,
    });

    expect(fired).toEqual([]);
    expect((await h.store.loadRunState(runId)).phase.tag).toBe("completed");
    const children = await h.store.listChildren(runId);
    expect(children).toHaveLength(2);
    for (const c of children) {
      expect((await h.store.loadRunState(c)).phase.tag).toBe("completed");
    }
    expect(await h.drain()).toBe(0);
  });
});

describe("signals validate against the run's own flow, not the live one", () => {
  function v1() {
    return flow({
      id: "rs-signal",
      input: empty,
      build: (b) => ({
        wait: b.signal({
          timeoutMs: "unbounded",
          schema: passthroughSchema<{ ok: boolean }>(),
        }),
      }),
    });
  }
  function v2() {
    return flow({
      id: "rs-signal",
      input: empty,
      build: (b) => ({
        wait: b.signal({
          timeoutMs: "unbounded",
          schema: passthroughSchema<{ ok: boolean }>(),
        }),
        late: b.signal({
          timeoutMs: "unbounded",
          schema: passthroughSchema<{ ok: boolean }>(),
        }),
      }),
    });
  }

  for (const driftPolicy of [
    "freeze",
    "synthesize",
  ] as const satisfies readonly DriftPolicy[]) {
    it(`${driftPolicy}: a signal step only the live flow has is rejected`, async () => {
      const store = new InMemoryStore();
      const queue = new InMemoryQueue();
      const clock = new InMemoryClock();
      const wfA = await nagi({ flows: [v1()], store, queue, clock });
      const runId = await wfA.start(v1(), {});

      const wfB = await nagi({
        flows: [v2()],
        store,
        queue,
        clock,
        driftPolicy,
      });
      await expect(wfB.signal(runId, "late", { ok: true })).rejects.toThrow(
        NagiRuntimeError,
      );
      expect(
        (await store.loadRunState(runId)).facts.some(
          (f) => f.kind === "signal.buffered",
        ),
      ).toBe(false);
    });
  }

  it("synthesize: a signal to the pinned step is accepted and the run completes on the pinned shape", async () => {
    const store = new InMemoryStore();
    const queue = new InMemoryQueue();
    const clock = new InMemoryClock();
    const wfA = await nagi({ flows: [v1()], store, queue, clock });
    const runId = await wfA.start(v1(), {});

    const wfB = await nagi({
      flows: [v2()],
      store,
      queue,
      clock,
      driftPolicy: "synthesize",
    });
    await wfB.signal(runId, "wait", { ok: true });
    await wfB.worker({ timerSweepIntervalMs: 0 }).runUntilEmpty();

    const state = await store.loadRunState(runId);
    expect(state.phase.tag).toBe("completed");
    expect(state.steps["wait"]).toMatchObject({ output: { ok: true } });
    expect(state.steps["late"]).toBeUndefined();
  });
});
