import { describe, expect, it } from "vitest";
import { flow } from "../builder";
import { NagiRuntimeError, NagiValidationError } from "../errors";
import { Facts } from "../facts";
import type { AttemptNumber, StepResetFact } from "../types";
import { makeHarness, passthroughSchema } from "./test-helpers";

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

  it("resets a parked step on a live run", async () => {
    const f = flow({
      id: "from-running",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => ({
        wait: b.signal({
          timeoutMs: "unbounded" as const,
          schema: passthroughSchema<Record<string, never>>(),
        }),
      }),
    });
    const h = await makeHarness(f);
    const runId = await h.wf.start(f, {});
    await h.drain();
    expect((await h.store.loadRunState(runId)).phase.tag).toBe("running");

    await h.wf.replay(runId, { mode: "continue", from: "wait" });
    await h.drain();

    const r = await h.result(runId);
    expect(r.factCount("step.reset")).toBe(1);
    expect(r.factCount("step.abort-requested")).toBe(0);
    expect((await h.store.loadRunState(runId)).steps["wait"]?.tag).toBe(
      "awaitingSignal",
    );
  });

  it("aborts an in-flight `from` step before resetting it", async () => {
    let abortObserved = false;
    let aAttempts = 0;
    const f = flow({
      id: "from-in-flight",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => ({
        a: b.task({
          retry: { maxAttempts: 1, backoff: "fixed" },
          run: async ({ ctx }) => {
            aAttempts += 1;
            if (aAttempts === 1) {
              for (let i = 0; i < 200; i++) {
                if (ctx.signal.aborted) {
                  abortObserved = true;
                  throw new Error("aborted");
                }
                await new Promise((r) => setTimeout(r, 5));
              }
              return { ran: 1 };
            }
            return { ran: 2 };
          },
        }),
      }),
    });
    const h = await makeHarness(f);
    const runId = await h.wf.start(f, {});
    const worker = h.startWorker({ pollIntervalMs: 5 });
    try {
      await h.waitForStep(runId, "a", "running", 2_000);
      await h.wf.replay(runId, { mode: "continue", from: "a" });
      await h.waitForStep(runId, "a", "completed", 3_000);
    } finally {
      await worker.stop();
    }
    expect(abortObserved).toBe(true);
    expect(aAttempts).toBe(2);
    const r = await h.result(runId);
    expect(r.factCount("step.abort-requested")).toBe(1);
    expect(r.factCount("step.canceled")).toBe(1);
    expect(r.status).toBe("completed");
  });

  it("recovers a canceled step that holds a live run open", async () => {
    let attempts = 0;
    const f = flow({
      id: "from-canceled-step",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => ({
        a: b.task({
          run: async ({ ctx }) => {
            attempts += 1;
            if (attempts === 1) {
              await new Promise<void>((_, reject) =>
                ctx.signal.addEventListener("abort", () =>
                  reject(ctx.signal.reason),
                ),
              );
            }
            return { ok: true };
          },
        }),
      }),
    });
    const h = await makeHarness(f);
    const runId = await h.wf.start(f, {});
    const worker = h.startWorker({ pollIntervalMs: 5 });
    try {
      await h.waitForStep(runId, "a", "running", 2_000);
      // The state a replay that crashed between its abort and its reset leaves.
      await h.store.appendFact(
        runId,
        Facts.stepAbortRequested({
          runId,
          stepId: "a",
          attempt: 1 as AttemptNumber,
          at: new Date(),
        }),
      );
      await h.waitForStep(runId, "a", "canceled", 3_000);
      expect((await h.store.loadRunState(runId)).phase.tag).toBe("running");

      await h.wf.replay(runId, { mode: "continue", from: "a" });
      await h.waitForStep(runId, "a", "completed", 3_000);
    } finally {
      await worker.stop();
    }
    expect(attempts).toBe(2);
    expect((await h.result(runId)).status).toBe("completed");
  });

  it("throws NagiRuntimeError on a canceled run", async () => {
    const f = flow({
      id: "from-canceled-run",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => ({
        a: b.signal({
          timeoutMs: "unbounded" as const,
          schema: passthroughSchema<Record<string, never>>(),
        }),
      }),
    });
    const h = await makeHarness(f);
    const runId = await h.wf.start(f, {});
    await h.drain();
    await h.wf.cancel(runId);
    await expect(
      h.wf.replay(runId, { mode: "continue", from: "a" }),
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
    expect(failed.phase.tag).toBe("failed");

    bShouldFail = false;
    await h.wf.replay(runId, { mode: "continue", from: "a" });
    await h.drain();
    expect(aRuns).toBe(2);
    expect(bRuns).toBe(2);
    const result = await h.result(runId);
    expect(result.status).toBe("completed");
    expect(result.factCount("flow.failed")).toBe(1);
    expect(result.factCount("flow.completed")).toBe(1);
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
