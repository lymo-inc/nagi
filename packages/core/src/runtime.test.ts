// End-to-end smoke tests — real worker, real timers, full lifecycle.
//
// The deterministic / fast-feedback layer lives in:
//   - scheduler.test.ts   pure decisions
//   - dispatch.test.ts    backoff + driver-style dispatch
//   - builder.test.ts     id assignment + needs rewriting
//   - idempotency.test.ts ctx.once + ctx.idempotencyKey
//
// This file is the assembly check: do the pieces compose into a working
// worker loop? One test per scenario; helpers from `test-helpers.ts` hide
// the AbortController / worker lifecycle / waitForFlowEnd plumbing.

import { describe, expect, it } from "vitest";
import { flow } from "./builder";
import { emptySchema, makeHarness, passthroughSchema, runFlow } from "./test-helpers";

describe("e2e: linear task chain", () => {
  it("runs both steps and threads upstream output through needs", async () => {
    const f = flow({
      id: "linear-e2e",
      input: passthroughSchema<{ start: number }>(),
      build: (b) => {
        const a = b.task({ run: async ({ input }) => ({ doubled: input.start * 2 }) });
        const c = b.task({
          needs: { a },
          run: async ({ needs }) => ({ plusTen: needs.a.doubled + 10 }),
        });
        return { a, c };
      },
    });

    const result = await runFlow(f, { start: 5 });
    expect(result.status).toBe("completed");
    expect(result.output("a")).toEqual({ doubled: 10 });
    expect(result.output("c")).toEqual({ plusTen: 20 });
  });
});

describe("e2e: when:false skip cascade", () => {
  it("skips a gated step and transitively skips its downstream", async () => {
    const f = flow({
      id: "skip-e2e",
      input: passthroughSchema<{ enable: boolean }>(),
      build: (b) => {
        const gate = b.task({ run: async ({ input }) => ({ enabled: input.enable }) });
        const branch = b.task({
          needs: { gate },
          when: ({ needs }) => needs.gate.enabled,
          run: async () => ({ ran: true }),
        });
        const after = b.task({
          needs: { branch },
          run: async () => ({ ran: true }),
        });
        return { gate, branch, after };
      },
    });

    const result = await runFlow(f, { enable: false });
    expect(result.stepStatus("gate")).toBe("completed");
    expect(result.stepStatus("branch")).toBe("skipped");
    expect(result.stepStatus("after")).toBe("skipped");

    const skips = result.factsOf("step.skipped");
    expect(skips.find((s) => s.stepId === "branch")?.reason).toBe("when-false");
    expect(skips.find((s) => s.stepId === "after")?.reason).toBe("transitive");
  });
});

describe("e2e: retry with real backoff timers", () => {
  it("retries up to maxAttempts and succeeds on the final attempt", async () => {
    let attempts = 0;
    const f = flow({
      id: "retry-e2e",
      input: emptySchema(),
      build: (b) => ({
        flaky: b.task({
          retry: { maxAttempts: 3, backoff: "exponential", initialDelayMs: 10, maxDelayMs: 50 },
          run: async () => {
            attempts++;
            if (attempts < 3) throw new Error(`fail ${attempts}`);
            return { attempts };
          },
        }),
      }),
    });

    const result = await runFlow(f, {});
    expect(attempts).toBe(3);
    expect(result.output("flaky")).toEqual({ attempts: 3 });
    expect(result.factCount("step.retried")).toBe(2);
  });

  it("finalizes flow as failed when retries are exhausted", async () => {
    const f = flow({
      id: "doomed-e2e",
      input: emptySchema(),
      build: (b) => ({
        doomed: b.task({
          retry: { maxAttempts: 2, backoff: "fixed", initialDelayMs: 5 },
          run: async () => {
            throw new Error("permanent");
          },
        }),
      }),
    });

    const result = await runFlow(f, {});
    expect(result.status).toBe("failed");
    expect(result.error("doomed").message).toBe("permanent");
  });
});

describe("e2e: signal full loop", () => {
  it("waits for an external signal and resumes downstream", async () => {
    const f = flow({
      id: "signal-e2e",
      input: passthroughSchema<{ subject: string }>(),
      build: (b) => {
        const prep = b.task({ run: async ({ input }) => ({ subject: input.subject }) });
        const review = b.signal({
          needs: { prep },
          schema: passthroughSchema<{ approved: boolean }>(),
        });
        const send = b.task({
          needs: { review, prep },
          run: async ({ needs }) => ({
            sent: needs.review.approved,
            subject: needs.prep.subject,
          }),
        });
        return { prep, review, send };
      },
    });

    const h = makeHarness(f);
    const w = h.startWorker();
    try {
      const runId = await h.wf.start(f, { subject: "hi" });
      await h.waitForStep(runId, "review", "running");
      await h.wf.signal(runId, "review", { approved: true });

      const result = await h.waitForEnd(runId);
      expect(result.output("review")).toEqual({ approved: true });
      expect(result.output("send")).toEqual({ sent: true, subject: "hi" });
    } finally {
      await w.stop();
    }
  });
});

describe("e2e: ctx primitives plumbed through dispatch", () => {
  it("ctx.once is wired and memoizes inside a real handler", async () => {
    let calls = 0;
    const f = flow({
      id: "ctx-once-e2e",
      input: emptySchema(),
      build: (b) => ({
        t: b.task({
          run: async ({ ctx }) => {
            const first = await ctx.once("compute", async () => {
              calls++;
              return { v: 42 };
            });
            const second = await ctx.once("compute", async () => {
              calls++;
              return { v: 999 };
            });
            return { first: first.v, second: second.v };
          },
        }),
      }),
    });

    const result = await runFlow(f, {});
    expect(calls).toBe(1);
    expect(result.output("t")).toEqual({ first: 42, second: 42 });
  });

  it("ctx.idempotencyKey is stable across retries of the same step", async () => {
    const seen: string[] = [];
    const f = flow({
      id: "idem-e2e",
      input: emptySchema(),
      build: (b) => ({
        t: b.task({
          retry: { maxAttempts: 3, backoff: "fixed", initialDelayMs: 5 },
          run: async ({ ctx }) => {
            seen.push(ctx.idempotencyKey("upload"));
            if (seen.length < 3) throw new Error("retry");
            return { ok: true };
          },
        }),
      }),
    });

    await runFlow(f, {});
    expect(seen).toHaveLength(3);
    expect(new Set(seen).size).toBe(1);
    expect(seen[0]).toMatch(/^nagi:run-/);
    expect(seen[0]).toMatch(/:t:upload$/);
  });
});
