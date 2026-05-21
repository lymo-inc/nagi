import { describe, expect, it } from "vitest";
import { flow } from "../builder";
import { NagiRuntimeError, NagiValidationError } from "../runtime";
import type {
  FlowCanceledFact,
  StepAbortRequestedFact,
  StepResetFact,
  StepSkippedFact,
} from "../types";
import { makeHarness, passthroughSchema } from "./test-helpers";

describe("wf.operator().skip()", () => {
  it("manual skip with cascade=skip transitively skips downstream", async () => {
    let bRan = 0;
    const f = flow({
      id: "skip-cascade-skip",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => {
        const a = b.signal({
          schema: passthroughSchema<Record<string, never>>(),
        });
        const bStep = b.task({
          needs: { a },
          run: async () => {
            bRan += 1;
            return { v: 1 };
          },
        });
        return { a, b: bStep };
      },
    });
    const h = await makeHarness(f);
    const runId = await h.wf.start(f, {});
    await h.drain();

    await h.wf
      .operator()
      .skip(runId, "a", { actor: "ops@nagi", note: "vendor down" });
    await h.drain();

    const result = await h.result(runId);
    expect(result.stepStatus("a")).toBe("skipped");
    expect(result.stepStatus("b")).toBe("skipped");
    expect(bRan).toBe(0);
    const skipFacts = result.factsOf("step.skipped");
    const manual = skipFacts.find((s) => s.stepId === "a") as
      | StepSkippedFact
      | undefined;
    expect(manual?.reason).toBe("manual");
    expect(manual?.actor).toBe("ops@nagi");
    expect(manual?.note).toBe("vendor down");
    expect(manual?.cascade).toBe("skip");
  });

  it("manual skip with cascade=continue lets downstream run with needs.x resolved as skipped", async () => {
    let observed: unknown = "untouched";
    const f = flow({
      id: "skip-cascade-continue",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => {
        const a = b.signal({
          schema: passthroughSchema<{ v: number }>(),
        });
        const bStep = b.task({
          needs: { a },
          run: async ({ needs }) => {
            observed = needs.a;
            return { ran: true };
          },
        });
        return { a, b: bStep };
      },
    });
    const h = await makeHarness(f);
    const runId = await h.wf.start(f, {});
    await h.drain();

    await h.wf.operator().skip(runId, "a", {
      actor: "ops@nagi",
      cascade: "continue",
    });
    await h.drain();

    const result = await h.result(runId);
    expect(result.stepStatus("a")).toBe("skipped");
    expect(result.stepStatus("b")).toBe("completed");
    expect(observed).toEqual({ tag: "skipped" });
  });

  it("rejects skip with empty actor", async () => {
    const f = flow({
      id: "skip-no-actor",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => ({
        a: b.signal({ schema: passthroughSchema<Record<string, never>>() }),
      }),
    });
    const h = await makeHarness(f);
    const runId = await h.wf.start(f, {});
    await h.drain();

    await expect(
      h.wf.operator().skip(runId, "a", { actor: "" }),
    ).rejects.toBeInstanceOf(NagiValidationError);
  });

  it("rejects skip on unknown step", async () => {
    const f = flow({
      id: "skip-unknown-step",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => ({
        a: b.signal({ schema: passthroughSchema<Record<string, never>>() }),
      }),
    });
    const h = await makeHarness(f);
    const runId = await h.wf.start(f, {});
    await h.drain();

    await expect(
      h.wf.operator().skip(runId, "nope", { actor: "ops" }),
    ).rejects.toBeInstanceOf(NagiValidationError);
  });

  it("is a no-op on an already-terminal step", async () => {
    const f = flow({
      id: "skip-already-terminal",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => ({
        a: b.task({ run: async () => ({ v: 1 }) }),
      }),
    });
    const h = await makeHarness(f);
    const runId = await h.wf.start(f, {});
    await h.drain();
    await h.wf.operator().skip(runId, "a", { actor: "ops" });
    const result = await h.result(runId);
    expect(result.stepStatus("a")).toBe("completed");
    expect(result.factsOf("step.skipped")).toHaveLength(0);
  });
});

describe("wf.operator().retry()", () => {
  it("retries a terminal failed step and stamps audit fields on the reset", async () => {
    let bAttempts = 0;
    let shouldFail = true;
    const f = flow({
      id: "retry-terminal",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => {
        const a = b.task({ run: async () => ({ v: 1 }) });
        const bStep = b.task({
          needs: { a },
          retry: { maxAttempts: 1, backoff: "fixed" },
          run: async () => {
            bAttempts += 1;
            if (shouldFail) throw new Error("boom");
            return { v: 2 };
          },
        });
        return { a, b: bStep };
      },
    });
    const h = await makeHarness(f);
    const runId = await h.wf.start(f, {});
    await h.drain();
    expect(bAttempts).toBe(1);

    shouldFail = false;
    await h.wf
      .operator()
      .retry(runId, "b", { actor: "ops@nagi", note: "vendor recovered" });
    await h.drain();

    expect(bAttempts).toBe(2);
    const result = await h.result(runId);
    expect(result.stepStatus("b")).toBe("completed");
    const resets = result.factsOf("step.reset") as StepResetFact[];
    const named = resets.find((r) => r.stepId === "b");
    expect(named?.actor).toBe("ops@nagi");
    expect(named?.note).toBe("vendor recovered");
  });

  it("retries a running step by aborting the in-flight handler", async () => {
    let abortObserved = false;
    let aAttempts = 0;
    const f = flow({
      id: "retry-running",
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
    (h.deps as { cancelPollIntervalMs?: number }).cancelPollIntervalMs = 20;
    const runId = await h.wf.start(f, {});
    const worker = h.startWorker({ pollIntervalMs: 5 });
    try {
      await h.waitForStep(runId, "a", "running", 2_000);
      await h.wf.operator().retry(runId, "a", { actor: "ops@nagi" });
      await h.waitForStep(runId, "a", "completed", 3_000);
    } finally {
      await worker.stop();
    }
    expect(abortObserved).toBe(true);
    expect(aAttempts).toBe(2);
    const state = await h.store.loadRunState(runId);
    expect(
      state.facts.some(
        (f) =>
          f.kind === "step.abort-requested" &&
          (f as StepAbortRequestedFact).stepId === "a",
      ),
    ).toBe(true);
    expect(
      state.facts.some((f) => f.kind === "step.canceled" && f.stepId === "a"),
    ).toBe(true);
  });

  it("rejects retry on a canceled run", async () => {
    const f = flow({
      id: "retry-canceled-run",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => ({
        a: b.signal({ schema: passthroughSchema<Record<string, never>>() }),
      }),
    });
    const h = await makeHarness(f);
    const runId = await h.wf.start(f, {});
    await h.drain();
    await h.wf.cancel(runId);
    await expect(
      h.wf.operator().retry(runId, "a", { actor: "ops" }),
    ).rejects.toBeInstanceOf(NagiRuntimeError);
  });
});

describe("wf.operator().abort()", () => {
  it("writes flow.canceled with cause=operator and audit fields", async () => {
    const f = flow({
      id: "abort-audit",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => ({
        a: b.signal({ schema: passthroughSchema<Record<string, never>>() }),
      }),
    });
    const h = await makeHarness(f);
    const runId = await h.wf.start(f, {});
    await h.drain();

    await h.wf.operator().abort(runId, { actor: "ops@nagi", note: "stuck" });

    const state = await h.store.loadRunState(runId);
    expect(state.phase.tag).toBe("canceled");
    const canceled = state.facts.find(
      (f): f is FlowCanceledFact => f.kind === "flow.canceled",
    );
    expect(canceled?.cause).toBe("operator");
    if (canceled?.cause === "operator") {
      expect(canceled.actor).toBe("ops@nagi");
      expect(canceled.note).toBe("stuck");
      expect(canceled.reason).toBe("stuck");
    }
  });

  it("is a logged no-op on an already-terminal run", async () => {
    const f = flow({
      id: "abort-noop",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => ({ a: b.task({ run: async () => null }) }),
    });
    const h = await makeHarness(f);
    const runId = await h.wf.start(f, {});
    await h.drain();
    const before = (await h.store.loadRunState(runId)).facts.length;
    await h.wf.operator().abort(runId, { actor: "ops" });
    const after = (await h.store.loadRunState(runId)).facts.length;
    expect(after).toBe(before);
  });

  it("wf.cancel writes flow.canceled with cause=explicit and reason", async () => {
    const f = flow({
      id: "cancel-explicit",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => ({
        a: b.signal({ schema: passthroughSchema<Record<string, never>>() }),
      }),
    });
    const h = await makeHarness(f);
    const runId = await h.wf.start(f, {});
    await h.drain();
    await h.wf.cancel(runId, { reason: "manual via wf.cancel" });
    const state = await h.store.loadRunState(runId);
    const canceled = state.facts.find(
      (f): f is FlowCanceledFact => f.kind === "flow.canceled",
    );
    expect(canceled?.cause).toBe("explicit");
    if (canceled?.cause === "explicit") {
      expect(canceled.reason).toBe("manual via wf.cancel");
    }
  });
});
