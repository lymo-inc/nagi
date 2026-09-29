import { describe, expect, it } from "vitest";
import { flow } from "../builder";
import type { StepResetFact } from "../facts/step";
import { makeHarness, passthroughSchema } from "./test-helpers";

// a → b → c, each recording how many times its body ran, so an isolated rerun
// of `b` is visible as "b ran twice, c ran once".
interface Runs {
  a: number;
  b: number;
  c: number;
}

function newRuns(): Runs {
  return { a: 0, b: 0, c: 0 };
}

// a → b → c where a throws on its first attempt only. Rerunning a with scope
// "step" must rerun b and c — they hold no value, only a transitive skip —
// or the run would settle "completed" without ever producing their output.
function failOnceChain(id: string, runs: Runs) {
  return flow({
    id,
    input: passthroughSchema<Record<string, never>>(),
    build: (b) => {
      const a = b.task({
        retry: { maxAttempts: 1, backoff: "fixed" },
        run: async () => {
          runs.a += 1;
          if (runs.a === 1) throw new Error("boom");
          return { v: "a" };
        },
      });
      const bStep = b.task({
        needs: { a },
        run: async () => {
          runs.b += 1;
          return { v: "b" };
        },
      });
      const c = b.task({
        needs: { b: bStep },
        run: async () => {
          runs.c += 1;
          return { v: "c" };
        },
      });
      return { a, b: bStep, c };
    },
    output: (s) => s.c,
  });
}

function chainFlow(id: string, runs: Runs) {
  return flow({
    id,
    input: passthroughSchema<Record<string, never>>(),
    build: (b) => {
      const a = b.task({
        run: async () => {
          runs.a += 1;
          return { v: "a" };
        },
      });
      const bStep = b.task({
        needs: { a },
        run: async () => {
          runs.b += 1;
          return { v: `b${runs.b}` };
        },
      });
      const c = b.task({
        needs: { b: bStep },
        run: async ({ needs }) => {
          runs.c += 1;
          return { sawB: needs.b.v };
        },
      });
      return { a, b: bStep, c };
    },
    output: (s) => s.c,
  });
}

describe('replay({ from, scope: "step" })', () => {
  it("reruns only the named step and leaves completed descendants alone", async () => {
    const runs = newRuns();
    const f = chainFlow("isolated-rerun", runs);
    const h = await makeHarness(f);
    const runId = await h.wf.start(f, {});
    await h.drain();
    expect(runs).toEqual({ a: 1, b: 1, c: 1 });

    await h.wf.replay(runId, { mode: "continue", from: "b", scope: "step" });
    await h.drain();

    // b re-ran; c did NOT, so it still holds output derived from b's OLD value.
    expect(runs).toEqual({ a: 1, b: 2, c: 1 });
    const r = await h.result(runId);
    expect(r.output("b")).toEqual({ v: "b2" });
    expect(r.output("c")).toEqual({ sawB: "b1" });
    expect(r.status).toBe("completed");
  });

  it("defaults to cascade — descendants re-run when scope is omitted", async () => {
    const runs = newRuns();
    const f = chainFlow("default-cascade", runs);
    const h = await makeHarness(f);
    const runId = await h.wf.start(f, {});
    await h.drain();

    await h.wf.replay(runId, { mode: "continue", from: "b" });
    await h.drain();

    expect(runs).toEqual({ a: 1, b: 2, c: 2 });
    const r = await h.result(runId);
    expect(r.output("c")).toEqual({ sawB: "b2" });
  });

  it("records the caller's intent as scope on the origin reset fact", async () => {
    const runs = newRuns();
    const f = chainFlow("scope-audit", runs);
    const h = await makeHarness(f);
    const runId = await h.wf.start(f, {});
    await h.drain();

    await h.wf.replay(runId, { mode: "continue", from: "b", scope: "step" });
    await h.drain();

    const r = await h.result(runId);
    const resets = r.factsOf("step.reset") as StepResetFact[];
    expect(resets).toHaveLength(1);
    expect(resets[0]?.stepId).toBe("b");
    expect(resets[0]?.scope).toBe("step");
    expect(resets[0]?.cascadedFrom).toBeUndefined();
  });

  it("omits scope for a cascading reset, so existing facts keep their shape", async () => {
    const runs = newRuns();
    const f = chainFlow("cascade-audit", runs);
    const h = await makeHarness(f);
    const runId = await h.wf.start(f, {});
    await h.drain();

    await h.wf.replay(runId, { mode: "continue", from: "b" });
    await h.drain();

    const r = await h.result(runId);
    const resets = r.factsOf("step.reset") as StepResetFact[];
    const origin = resets.find((x) => x.stepId === "b");
    const downstream = resets.find((x) => x.stepId === "c");
    expect(origin?.scope).toBeUndefined();
    expect(downstream?.cascadedFrom).toBe("b");
  });

  it("disambiguates a leaf step — the case absence of siblings cannot", async () => {
    // `c` is a leaf: cascading and isolated reset touch the same single step,
    // so only the recorded intent tells the two apart.
    const runs = newRuns();
    const f = chainFlow("leaf-intent", runs);
    const h = await makeHarness(f);
    const runId = await h.wf.start(f, {});
    await h.drain();

    await h.wf.replay(runId, { mode: "continue", from: "c", scope: "step" });
    await h.drain();

    const r = await h.result(runId);
    const resets = r.factsOf("step.reset") as StepResetFact[];
    expect(resets).toHaveLength(1);
    expect(resets[0]?.scope).toBe("step");
  });

  it("reopens a settled run and recomputes flow output from the rerun step", async () => {
    const runs = newRuns();
    const f = chainFlow("reopen-isolated", runs);
    const h = await makeHarness(f);
    const runId = await h.wf.start(f, {});
    await h.drain();
    expect((await h.result(runId)).status).toBe("completed");

    // Isolated rerun of the LEAF, so the flow output itself changes.
    await h.wf.replay(runId, { mode: "continue", from: "c", scope: "step" });
    await h.drain();

    const r = await h.result(runId);
    expect(r.status).toBe("completed");
    expect(runs.c).toBe(2);
    expect(r.factCount("flow.completed")).toBe(2);
  });
  it("replaying a failed step with scope step reruns its transitively-skipped descendants", async () => {
    const runs = newRuns();
    const f = failOnceChain("replay-isolated-unsettled", runs);
    const h = await makeHarness(f);
    const runId = await h.wf.start(f, {});
    await h.drain();

    const before = await h.result(runId);
    expect(before.status).toBe("failed");
    expect(before.stepStatus("b")).toBe("skipped");
    expect(before.stepStatus("c")).toBe("skipped");

    await h.wf.replay(runId, { mode: "continue", from: "a", scope: "step" });
    await h.drain();

    const after = await h.result(runId);
    expect(after.status).toBe("completed");
    expect(after.stepStatus("b")).toBe("completed");
    expect(after.stepStatus("c")).toBe("completed");
    expect(runs).toEqual({ a: 2, b: 1, c: 1 });
  });
});

describe('subflow step under scope: "step"', () => {
  it("spawns a fresh child generation and leaves the consumer untouched", async () => {
    let childRuns = 0;
    let consumeRuns = 0;

    const child = flow({
      id: "scoped-child",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => ({
        work: b.task({
          run: async () => {
            childRuns += 1;
            return { gen: childRuns };
          },
        }),
      }),
      output: (s) => s.work,
    });

    const parent = flow({
      id: "scoped-parent",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => {
        const sub = b.subflow(child, { input: () => ({}) });
        const consume = b.task({
          needs: { sub },
          run: async ({ needs }) => {
            consumeRuns += 1;
            return { sawGen: needs.sub.output.gen };
          },
        });
        return { sub, consume };
      },
    });

    const h = await makeHarness([parent, child]);
    const runId = await h.wf.start(parent, {});
    await h.drain();

    const before = await h.result(runId);
    const firstChildId = (before.output("sub") as { childRunId: string })
      .childRunId;
    expect(childRuns).toBe(1);
    expect(consumeRuns).toBe(1);

    await h.wf.replay(runId, { mode: "continue", from: "sub", scope: "step" });
    await h.drain();

    const after = await h.result(runId);
    const secondChildId = (after.output("sub") as { childRunId: string })
      .childRunId;

    // A reset bumps the subflow generation, so deriveChildRunId mints a NEW
    // child rather than re-attaching to the finished one.
    expect(secondChildId).not.toBe(firstChildId);
    expect(childRuns).toBe(2);
    // ...and the consumer kept its output from the FIRST child generation.
    expect(consumeRuns).toBe(1);
    expect(after.output("consume")).toEqual({ sawGen: 1 });
  });
});
