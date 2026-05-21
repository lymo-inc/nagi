import { describe, expect, it } from "vitest";
import { flow } from "../builder";
import { NagiRuntimeError, NagiValidationError } from "../runtime";
import type { RunId, StandardSchemaV1 } from "../types";
import {
  emptySchema,
  makeHarness,
  passthroughSchema,
  runFlow,
} from "./test-helpers";

describe("e2e: linear task chain", () => {
  it("runs both steps and threads upstream output through needs", async () => {
    const f = flow({
      id: "linear-e2e",
      input: passthroughSchema<{ start: number }>(),
      build: (b) => {
        const a = b.task({
          run: async ({ input }) => ({ doubled: input.start * 2 }),
        });
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
        const gate = b.task({
          run: async ({ input }) => ({ enabled: input.enable }),
        });
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
          retry: {
            maxAttempts: 3,
            backoff: "exponential",
            initialDelayMs: 10,
            maxDelayMs: 50,
          },
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
        const prep = b.task({
          run: async ({ input }) => ({ subject: input.subject }),
        });
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

    const h = await makeHarness(f);
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

describe("start: caller-supplied runId", () => {
  const trivialFlow = () =>
    flow({
      id: "supplied-runid",
      input: passthroughSchema<{ x: number }>(),
      build: (b) => ({
        only: b.task({ run: async ({ input }) => ({ doubled: input.x * 2 }) }),
      }),
    });

  it("uses the supplied runId when no run exists yet", async () => {
    const f = trivialFlow();
    const h = await makeHarness(f);
    const w = h.startWorker();
    try {
      const supplied = "run-deterministic-abc" as RunId;
      const runId = await h.wf.start(f, { x: 3 }, { runId: supplied });
      expect(runId).toBe(supplied);
      const result = await h.waitForEnd(runId);
      expect(result.output("only")).toEqual({ doubled: 6 });
    } finally {
      await w.stop();
    }
  });

  it("mints a fresh runId when opts is omitted (back-compat)", async () => {
    const f = trivialFlow();
    const h = await makeHarness(f);
    const w = h.startWorker();
    try {
      const runId = await h.wf.start(f, { x: 1 });
      expect(runId).toMatch(/^run-/);
      await h.waitForEnd(runId);
    } finally {
      await w.stop();
    }
  });

  it("is an idempotent no-op when the same runId is supplied twice", async () => {
    const f = trivialFlow();
    const h = await makeHarness(f);

    const supplied = "run-idem-xyz" as RunId;
    const first = await h.wf.start(f, { x: 7 }, { runId: supplied });
    const second = await h.wf.start(f, { x: 999 }, { runId: supplied });

    expect(first).toBe(supplied);
    expect(second).toBe(supplied);

    const state = await h.store.loadRunState(supplied);
    expect(state.facts.filter((f) => f.kind === "flow.started")).toHaveLength(
      1,
    );
    const started = state.facts.find((f) => f.kind === "flow.started");
    expect(
      started && started.kind === "flow.started" ? started.input : null,
    ).toEqual({ x: 7 });

    await h.drain();
    const final = await h.result(supplied);
    expect(final.output("only")).toEqual({ doubled: 14 });
  });

  it("rejects an empty-string runId with NagiValidationError", async () => {
    const f = trivialFlow();
    const h = await makeHarness(f);
    await expect(
      h.wf.start(f, { x: 1 }, { runId: "" as RunId }),
    ).rejects.toBeInstanceOf(NagiValidationError);
  });

  it("rejects a non-string runId with NagiValidationError", async () => {
    const f = trivialFlow();
    const h = await makeHarness(f);
    await expect(
      h.wf.start(f, { x: 1 }, { runId: 123 as unknown as RunId }),
    ).rejects.toBeInstanceOf(NagiValidationError);
  });
});

describe("startById: registry-aware dispatch", () => {
  // Schema that actually rejects bad input — passthroughSchema accepts
  // anything, which would let an invalid payload sneak through and mask
  // the validation behavior we want to assert on.
  const strictNumberXSchema: StandardSchemaV1<{ x: number }, { x: number }> = {
    "~standard": {
      version: 1,
      vendor: "nagi-test",
      validate: (value: unknown) => {
        if (
          typeof value !== "object" ||
          value === null ||
          typeof (value as { x: unknown }).x !== "number"
        ) {
          return {
            issues: [{ message: "x must be a number", path: ["x"] }],
          };
        }
        return { value: value as { x: number } };
      },
    },
  };

  const strictFlow = () =>
    flow({
      id: "start-by-id",
      input: strictNumberXSchema,
      build: (b) => ({
        only: b.task({ run: async ({ input }) => ({ tripled: input.x * 3 }) }),
      }),
    });

  it("starts the flow when the id is registered and input validates", async () => {
    const f = strictFlow();
    const h = await makeHarness(f);
    const w = h.startWorker();
    try {
      const runId = await h.wf.startById("start-by-id", { x: 4 });
      const result = await h.waitForEnd(runId);
      expect(result.output("only")).toEqual({ tripled: 12 });
    } finally {
      await w.stop();
    }
  });

  it("honors caller-supplied runId", async () => {
    const f = strictFlow();
    const h = await makeHarness(f);
    const supplied = "run-by-id-xyz" as RunId;
    const runId = await h.wf.startById(
      "start-by-id",
      { x: 2 },
      { runId: supplied },
    );
    expect(runId).toBe(supplied);
  });

  it("throws NagiRuntimeError when the flowId is not registered", async () => {
    const f = strictFlow();
    const h = await makeHarness(f);
    await expect(
      h.wf.startById("not-a-real-flow", { x: 1 }),
    ).rejects.toBeInstanceOf(NagiRuntimeError);
  });

  it("throws NagiValidationError when input fails the flow's schema", async () => {
    const f = strictFlow();
    const h = await makeHarness(f);
    await expect(
      h.wf.startById("start-by-id", { x: "not-a-number" }),
    ).rejects.toBeInstanceOf(NagiValidationError);
  });

  it("start(flow, input, opts) is observably equivalent to startById(flow.id, ...)", async () => {
    const f = strictFlow();
    const h = await makeHarness(f);
    const w = h.startWorker();
    try {
      const viaStart = await h.wf.start(f, { x: 5 });
      const viaById = await h.wf.startById("start-by-id", { x: 6 });
      const r1 = await h.waitForEnd(viaStart);
      const r2 = await h.waitForEnd(viaById);
      expect(r1.output("only")).toEqual({ tripled: 15 });
      expect(r2.output("only")).toEqual({ tripled: 18 });
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
