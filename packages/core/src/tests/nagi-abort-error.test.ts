import { describe, expect, it } from "vitest";
import { flow } from "../builder";
import { type CancelArgs, NagiAbortError, type RunId } from "../index";
import { makeHarness, passthroughSchema } from "./test-helpers";

describe("NagiAbortError shape (N12, D10=A)", () => {
  it('has name="AbortError" for WHATWG conformance', () => {
    const err = new NagiAbortError("run-x" as RunId, "run");
    expect(err.name).toBe("AbortError");
  });

  it("is detectable via instanceof (rename does not break the discriminator)", () => {
    const err: unknown = new NagiAbortError("run-x" as RunId, "step");
    expect(err instanceof NagiAbortError).toBe(true);
    expect(err instanceof Error).toBe(true);
  });

  it("carries a required kind field for run-watchdog vs step-timeout aborts", () => {
    const runErr = new NagiAbortError("r" as RunId, "run");
    const stepErr = new NagiAbortError("r" as RunId, "step");
    expect(runErr.kind).toBe("run");
    expect(stepErr.kind).toBe("step");
  });

  it("is exported from @nagi-js/core for typed consumer-side translation", async () => {
    // Loads the public entry; if the export is missing this throws at import time.
    const mod = await import("../index");
    expect(typeof mod.NagiAbortError).toBe("function");
    const err = new mod.NagiAbortError("r" as RunId, "run");
    expect(err).toBeInstanceOf(Error);
  });

  it('end-to-end: watchdog-fired abort on a canceled run surfaces as name="AbortError" with kind="run"', async () => {
    let observed: unknown;
    const f = flow({
      id: "watchdog-abort-e2e",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => ({
        a: b.task({
          retry: { maxAttempts: 1, backoff: "fixed" },
          run: async ({ ctx }) => {
            try {
              for (let i = 0; i < 400; i++) {
                if (ctx.signal.aborted) {
                  observed = ctx.signal.reason;
                  throw ctx.signal.reason instanceof Error
                    ? ctx.signal.reason
                    : new Error("aborted");
                }
                await new Promise((r) => setTimeout(r, 5));
              }
              return {};
            } catch (err) {
              if (observed === undefined) observed = err;
              throw err;
            }
          },
        }),
      }),
    });
    const h = await makeHarness(f);
    const runId = await h.wf.start(f, {});
    const worker = h.startWorker({ pollIntervalMs: 5 });
    try {
      await h.waitForStep(runId, "a", "running", 2_000);
      await h.wf.cancel(runId, {
        reason: "watchdog test",
      } as CancelArgs);
      await h.waitForEnd(runId, 3_000);
    } finally {
      await worker.stop();
    }

    expect(observed).toBeInstanceOf(NagiAbortError);
    const abort = observed as NagiAbortError;
    expect(abort.name).toBe("AbortError");
    expect(abort.kind).toBe("run");
    expect(abort.runId).toBe(runId);
  });

  it("classifyFailure still treats a thrown NagiAbortError as a cancel (instanceof check, not name)", async () => {
    // The instanceof check inside classifyFailure must survive the rename: a
    // handler-thrown NagiAbortError on a canceled run produces step.canceled
    // with the error captured (the includeError branch fires).
    let release = (): void => undefined;
    const barrier = new Promise<void>((r) => {
      release = r;
    });
    const f = flow({
      id: "classify-throws-nagi-abort",
      input: passthroughSchema<{ readonly key: string }>(),
      concurrency: {
        keyFn: (input) => input.key,
        mode: "cancel-in-progress",
      },
      build: (b) => ({
        a: b.task({
          run: async ({ ctx }) => {
            await barrier;
            throw new NagiAbortError(ctx.runId, "run");
          },
        }),
      }),
    });
    const h = await makeHarness(f);
    const firstRunId = await h.wf.start(f, { key: "k1" });
    const dispatching = h.drainOnce(1);
    // Wait until a is observed running before superseding it.
    for (let i = 0; i < 200; i++) {
      const s = await h.store.loadRunState(firstRunId);
      if (s.steps["a"]?.tag === "running") break;
      await new Promise((r) => setTimeout(r, 5));
    }
    await h.wf.start(f, { key: "k1" });
    release();
    await dispatching;

    const result = await h.result(firstRunId);
    const canceled = result.factsOf("step.canceled")[0];
    expect(canceled).toBeDefined();
    expect(canceled?.error?.name).toBe("AbortError");
  });
});
