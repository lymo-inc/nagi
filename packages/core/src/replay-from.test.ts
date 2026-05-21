import { describe, expect, it } from "vitest";
import { flow } from "./builder";
import { NagiRuntimeError, NagiValidationError } from "./runtime";
import { makeHarness, passthroughSchema } from "./test-helpers";
import type { StepResetFact } from "./types";

describe("wf.replay({ from }) — step-scoped replay", () => {
  it("re-runs `from` and downstream on a completed run; preserves upstream", async () => {
    let aRuns = 0;
    let bRuns = 0;
    let cRuns = 0;
    const f = flow({
      id: "from-completed",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => {
        const a = b.task({
          run: async () => {
            aRuns += 1;
            return { v: 1 };
          },
        });
        const bStep = b.task({
          needs: { a },
          run: async () => {
            bRuns += 1;
            return { v: 2 };
          },
        });
        const c = b.task({
          needs: { b: bStep },
          run: async () => {
            cRuns += 1;
            return { v: 3 };
          },
        });
        return { a, b: bStep, c };
      },
    });
    const h = await makeHarness(f);
    const runId = await h.wf.start(f, {});
    await h.drain();
    expect([aRuns, bRuns, cRuns]).toEqual([1, 1, 1]);

    await h.wf.replay(runId, { mode: "continue", from: "b" });
    await h.drain();

    expect([aRuns, bRuns, cRuns]).toEqual([1, 2, 2]);

    const state = await h.store.loadRunState(runId);
    const resets = state.facts.filter(
      (f): f is StepResetFact => f.kind === "step.reset",
    );
    expect(resets.map((r) => r.stepId).sort()).toEqual(["b", "c"]);
    const named = resets.find((r) => r.stepId === "b");
    const cascaded = resets.find((r) => r.stepId === "c");
    expect(named?.cascadedFrom).toBeUndefined();
    expect(cascaded?.cascadedFrom).toBe("b");
  });

  it("throws NagiValidationError when `from` is not a step in the flow", async () => {
    const f = flow({
      id: "from-unknown",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => ({ a: b.task({ run: async () => null }) }),
    });
    const h = await makeHarness(f);
    const runId = await h.wf.start(f, {});
    await h.drain();

    await expect(
      h.wf.replay(runId, { mode: "continue", from: "nope" }),
    ).rejects.toBeInstanceOf(NagiValidationError);
  });

  it("throws NagiRuntimeError when the run is still running", async () => {
    const f = flow({
      id: "from-running",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => ({
        wait: b.signal({ schema: passthroughSchema<Record<string, never>>() }),
      }),
    });
    const h = await makeHarness(f);
    const runId = await h.wf.start(f, {});
    await h.drain();
    const state = await h.store.loadRunState(runId);
    expect(state.status).toBe("running");

    await expect(
      h.wf.replay(runId, { mode: "continue", from: "wait" }),
    ).rejects.toBeInstanceOf(NagiRuntimeError);
  });

  it("`from` overrides the default 'first incomplete' behavior on a failed run", async () => {
    let aRuns = 0;
    let bRuns = 0;
    let bShouldFail = true;
    const f = flow({
      id: "from-failed",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => {
        const a = b.task({
          run: async () => {
            aRuns += 1;
            return { v: aRuns };
          },
        });
        const bStep = b.task({
          needs: { a },
          retry: { maxAttempts: 1, backoff: "fixed" },
          run: async () => {
            bRuns += 1;
            if (bShouldFail) throw new Error("boom");
            return { v: bRuns };
          },
        });
        return { a, b: bStep };
      },
    });
    const h = await makeHarness(f);
    const runId = await h.wf.start(f, {});
    await h.drain();
    expect(aRuns).toBe(1);
    expect(bRuns).toBe(1);
    const failed = await h.store.loadRunState(runId);
    expect(failed.status).toBe("failed");

    bShouldFail = false;
    await h.wf.replay(runId, { mode: "continue", from: "a" });
    await h.drain();
    expect(aRuns).toBe(2);
    expect(bRuns).toBe(2);
  });

  it("`from` on a match step re-selects the arm and re-runs arm steps", async () => {
    let pickerCalls = 0;
    let armCalls = 0;
    const picks: Array<"a" | "b"> = ["a", "b"];
    const f = flow({
      id: "from-match",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) =>
        ({
          m: b.match({
            arms: [
              {
                when: () => {
                  pickerCalls += 1;
                  return (picks[pickerCalls - 1] ?? "a") === "a";
                },
                build: (b1) => ({
                  x: b1.task({
                    run: async () => {
                      armCalls += 1;
                      return { arm: "a" };
                    },
                  }),
                }),
              },
              {
                otherwise: true,
                build: (b1) => ({
                  x: b1.task({
                    run: async () => {
                      armCalls += 1;
                      return { arm: "b" };
                    },
                  }),
                }),
              },
            ],
          }),
        }) as never,
    });
    const h = await makeHarness(f);
    const runId = await h.wf.start(f, {});
    await h.drain();
    expect(pickerCalls).toBe(1);
    expect(armCalls).toBe(1);

    await h.wf.replay(runId, { mode: "continue", from: "m" });
    await h.drain();

    expect(pickerCalls).toBe(2);
    expect(armCalls).toBe(2);
  });

  it("`from` + `fireHooks: false` still suppresses hooks", async () => {
    const fires: string[] = [];
    const f = flow({
      id: "from-hooks",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => ({
        a: b.task({
          run: async () => ({ ok: true }),
          onComplete: () => {
            fires.push("step.onComplete");
          },
        }),
      }),
      onComplete: () => {
        fires.push("flow.onComplete");
      },
    });
    const h = await makeHarness(f, {
      hooks: {
        onStepComplete: () => {
          fires.push("onStepComplete");
        },
        onFlowComplete: () => {
          fires.push("onFlowComplete");
        },
      },
    });
    const runId = await h.wf.start(f, {});
    await h.drain();
    const baseline = fires.length;
    expect(baseline).toBeGreaterThan(0);

    await h.wf.replay(runId, {
      mode: "continue",
      from: "a",
      fireHooks: false,
    });
    await h.drain();

    expect(fires.length).toBe(baseline);
  });
});
