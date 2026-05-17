// Pure unit tests for the scheduler.
// Build a Flow + a synthetic RunState (via InMemoryStore) and assert the
// decision the scheduler returns. No worker, no dispatch.

import { describe, expect, it } from "vitest";
import { flow } from "./builder";
import { InMemoryStore } from "./memory";
import {
  descendantsOf,
  extractInput,
  flowTermination,
  nextRunnable,
} from "./scheduler";
import { passthroughSchema } from "./test-helpers";
import type { Fact, Flow, RunId, RunState } from "./types";

const RUN: RunId = "run-test" as RunId;

async function projectFacts(facts: readonly Fact[]): Promise<RunState> {
  const store = new InMemoryStore();
  for (const fact of facts) await store.appendFact(RUN, fact);
  return store.loadRunState(RUN);
}

function startedFact(flowId: string, input: unknown): Fact {
  return {
    kind: "flow.started",
    runId: RUN,
    flowId,
    input: input as never,
    at: new Date(),
  };
}

function completedStepFact(stepId: string, output: unknown): Fact {
  return {
    kind: "step.completed",
    runId: RUN,
    stepId,
    attempt: 1,
    output: output as never,
    at: new Date(),
  };
}

function failedStepFact(stepId: string): Fact {
  return {
    kind: "step.failed",
    runId: RUN,
    stepId,
    attempt: 1,
    error: { name: "Error", message: "boom" },
    at: new Date(),
  };
}

function skippedStepFact(
  stepId: string,
  reason: "when-false" | "transitive",
): Fact {
  return { kind: "step.skipped", runId: RUN, stepId, reason, at: new Date() };
}

function linearFlow(): Flow {
  return flow({
    id: "linear",
    input: passthroughSchema<{ n: number }>(),
    build: (b) => {
      const a = b.task({
        run: async ({ input }) => ({ doubled: input.n * 2 }),
      });
      const c = b.task({
        needs: { a },
        run: async ({ needs }) => ({ tripled: needs.a.doubled * 3 }),
      });
      return { a, c };
    },
  });
}

function gatedFlow(): Flow {
  return flow({
    id: "gated",
    input: passthroughSchema<{ enable: boolean }>(),
    build: (b) => {
      const gate = b.task({
        run: async ({ input }) => ({ enabled: input.enable }),
      });
      const branch = b.task({
        needs: { gate },
        when: ({ needs }) => needs.gate.enabled,
        run: async () => ({ ran: true }),
      });
      return { gate, branch };
    },
  });
}

describe("nextRunnable", () => {
  it("returns initial steps (no needs) on a fresh run", async () => {
    const f = linearFlow();
    const state = await projectFacts([startedFact(f.id, { n: 1 })]);
    expect(nextRunnable({ flow: f, runState: state, input: { n: 1 } })).toEqual(
      {
        runnable: ["a"],
        skip: [],
      },
    );
  });

  it("returns downstream once upstream completes", async () => {
    const f = linearFlow();
    const state = await projectFacts([
      startedFact(f.id, { n: 1 }),
      completedStepFact("a", { doubled: 2 }),
    ]);
    expect(nextRunnable({ flow: f, runState: state, input: { n: 1 } })).toEqual(
      {
        runnable: ["c"],
        skip: [],
      },
    );
  });

  it("blocks downstream while upstream is pending", async () => {
    const f = linearFlow();
    const state = await projectFacts([startedFact(f.id, { n: 1 })]);
    const decision = nextRunnable({
      flow: f,
      runState: state,
      input: { n: 1 },
    });
    expect(decision.runnable).not.toContain("c");
  });

  it("marks step as skip with reason when-false", async () => {
    const f = gatedFlow();
    const state = await projectFacts([
      startedFact(f.id, { enable: false }),
      completedStepFact("gate", { enabled: false }),
    ]);
    expect(
      nextRunnable({ flow: f, runState: state, input: { enable: false } }),
    ).toEqual({
      runnable: [],
      skip: [{ stepId: "branch", reason: "when-false" }],
    });
  });

  it("marks step as skip transitive when upstream was skipped", async () => {
    const f = linearFlow();
    const state = await projectFacts([
      startedFact(f.id, { n: 1 }),
      skippedStepFact("a", "when-false"),
    ]);
    expect(nextRunnable({ flow: f, runState: state, input: { n: 1 } })).toEqual(
      {
        runnable: [],
        skip: [{ stepId: "c", reason: "transitive" }],
      },
    );
  });

  it("marks step as skip transitive when upstream failed", async () => {
    const f = linearFlow();
    const state = await projectFacts([
      startedFact(f.id, { n: 1 }),
      failedStepFact("a"),
    ]);
    expect(nextRunnable({ flow: f, runState: state, input: { n: 1 } })).toEqual(
      {
        runnable: [],
        skip: [{ stepId: "c", reason: "transitive" }],
      },
    );
  });

  it("does not re-emit an already-running step", async () => {
    const f = linearFlow();
    const state = await projectFacts([
      startedFact(f.id, { n: 1 }),
      {
        kind: "step.started",
        runId: RUN,
        stepId: "a",
        attempt: 1,
        at: new Date(),
      },
    ]);
    expect(nextRunnable({ flow: f, runState: state, input: { n: 1 } })).toEqual(
      {
        runnable: [],
        skip: [],
      },
    );
  });
});

describe("flowTermination", () => {
  it("done=false while any step is pending", async () => {
    const f = linearFlow();
    const state = await projectFacts([startedFact(f.id, { n: 1 })]);
    expect(flowTermination(f, state)).toEqual({ done: false, failed: false });
  });

  it("done=true and failed=false when all completed", async () => {
    const f = linearFlow();
    const state = await projectFacts([
      startedFact(f.id, { n: 1 }),
      completedStepFact("a", {}),
      completedStepFact("c", {}),
    ]);
    expect(flowTermination(f, state)).toEqual({ done: true, failed: false });
  });

  it("done=true and failed=true when any step failed terminally", async () => {
    const f = linearFlow();
    const state = await projectFacts([
      startedFact(f.id, { n: 1 }),
      failedStepFact("a"),
      skippedStepFact("c", "transitive"),
    ]);
    expect(flowTermination(f, state)).toEqual({ done: true, failed: true });
  });

  it("done=true with mixed completed + skipped (none failed) is not failed", async () => {
    const f = gatedFlow();
    const state = await projectFacts([
      startedFact(f.id, { enable: false }),
      completedStepFact("gate", { enabled: false }),
      skippedStepFact("branch", "when-false"),
    ]);
    expect(flowTermination(f, state)).toEqual({ done: true, failed: false });
  });
});

describe("extractInput", () => {
  it("returns the input from the flow.started fact", async () => {
    const state = await projectFacts([startedFact("any", { hello: "world" })]);
    expect(extractInput(state)).toEqual({ hello: "world" });
  });

  it("throws when no flow.started fact exists", async () => {
    const state = await projectFacts([]);
    expect(() => extractInput(state)).toThrow(/No flow.started/);
  });
});

describe("descendantsOf", () => {
  it("linear chain — reset bubbles down only", () => {
    const f = flow({
      id: "linear",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => {
        const a = b.task({ run: async () => null });
        const bStep = b.task({ needs: { a }, run: async () => null });
        const c = b.task({ needs: { b: bStep }, run: async () => null });
        const d = b.task({ needs: { c }, run: async () => null });
        return { a, b: bStep, c, d };
      },
    });
    expect(descendantsOf(f, "b")).toEqual(["b", "c", "d"]);
    expect(descendantsOf(f, "a")).toEqual(["a", "b", "c", "d"]);
    expect(descendantsOf(f, "d")).toEqual(["d"]);
  });

  it("diamond — a → {b, c} → d, reset of b cascades to d only", () => {
    const f = flow({
      id: "diamond",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => {
        const a = b.task({ run: async () => null });
        const bStep = b.task({ needs: { a }, run: async () => null });
        const c = b.task({ needs: { a }, run: async () => null });
        const d = b.task({
          needs: { b: bStep, c },
          run: async () => null,
        });
        return { a, b: bStep, c, d };
      },
    });
    expect([...descendantsOf(f, "b")].sort()).toEqual(["b", "d"]);
    expect([...descendantsOf(f, "c")].sort()).toEqual(["c", "d"]);
    expect([...descendantsOf(f, "a")].sort()).toEqual(["a", "b", "c", "d"]);
  });

  it("match — resetting the match step cascades into all arm steps", () => {
    const f = flow({
      id: "match-cascade",
      input: passthroughSchema<{ kind: "a" | "b" }>(),
      build: (b) =>
        ({
          m: b.match({
            on: ({ input }) => input.kind,
            cases: {
              a: (b1) => ({ x: b1.task({ run: async () => null }) }),
              b: (b1) => ({ y: b1.task({ run: async () => null }) }),
            },
          }),
        }) as never,
    });
    const desc = descendantsOf(f, "m");
    expect(desc[0]).toBe("m");
    expect([...desc].slice(1).sort()).toEqual(["m.a.x", "m.b.y"]);
  });

  it("arm step — resetting an arm step does NOT cascade to siblings or parent", () => {
    const f = flow({
      id: "match-arm-reset",
      input: passthroughSchema<{ kind: "a" }>(),
      build: (b) =>
        ({
          m: b.match({
            on: ({ input }) => input.kind,
            cases: {
              a: (b1) => ({
                x: b1.task({ run: async () => null }),
                y: b1.task({ run: async () => null }),
              }),
            },
          }),
        }) as never,
    });
    expect(descendantsOf(f, "m.a.x")).toEqual(["m.a.x"]);
  });
});
