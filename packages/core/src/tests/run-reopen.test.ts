import { describe, expect, it } from "vitest";
import { flow } from "../builder";
import { NagiConcurrencyConflictError } from "../errors";
import { Facts } from "../facts";
import { foldRun } from "../state";
import type { AttemptNumber, RunId, SerializedError } from "../types";
import { makeHarness, passthroughSchema } from "./test-helpers";

const RUN = "run-reopen" as RunId;
const AT = new Date("2026-01-01T00:00:00Z");
const ERR: SerializedError = { name: "Error", message: "boom" };

function failedRunFacts() {
  return [
    Facts.flowStarted({ runId: RUN, flowId: "f", input: {}, at: AT }),
    Facts.stepStarted(RUN, "a", 1 as AttemptNumber, "task", AT),
    Facts.stepFailed(RUN, "a", 1 as AttemptNumber, ERR, AT),
    Facts.flowFailed(RUN, ERR, AT),
  ];
}

describe("step.reset reopens a settled run", () => {
  it("fold: reset after flow.failed moves the phase back to running", () => {
    const state = foldRun(RUN, [
      ...failedRunFacts(),
      Facts.stepReset({ runId: RUN, stepId: "a", at: AT }),
    ]);
    expect(state.phase.tag).toBe("running");
    expect(state.steps["a"]?.tag).toBe("pending");
  });

  it("fold: reset after flow.canceled leaves the run canceled", () => {
    const state = foldRun(RUN, [
      Facts.flowStarted({ runId: RUN, flowId: "f", input: {}, at: AT }),
      Facts.stepStarted(RUN, "a", 1 as AttemptNumber, "task", AT),
      Facts.flowCanceled(RUN, { cause: "explicit", reason: "stop" }, AT),
      Facts.stepReset({ runId: RUN, stepId: "a", at: AT }),
    ]);
    expect(state.phase.tag).toBe("canceled");
  });

  it("cancel watcher does not abort a handler re-run via operator.retry", async () => {
    let attempts = 0;
    const f = flow({
      id: "reopen-watcher",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => {
        const a = b.task({ run: async () => ({ v: 1 }) });
        const bStep = b.task({
          needs: { a },
          retry: { maxAttempts: 1, backoff: "fixed" },
          run: async ({ ctx }) => {
            attempts += 1;
            if (attempts === 1) throw new Error("boom");
            // Outlive the 250 ms cancel-watcher tick while honoring ctx.signal,
            // the way a real fetch/LLM call would.
            await new Promise<void>((resolve, reject) => {
              const t = setTimeout(resolve, 600);
              ctx.signal.addEventListener(
                "abort",
                () => {
                  clearTimeout(t);
                  reject(ctx.signal.reason);
                },
                { once: true },
              );
            });
            return { ok: true };
          },
        });
        return { a, b: bStep };
      },
    });
    const h = await makeHarness(f);
    const runId = await h.wf.start(f, {});
    await h.drain();
    expect((await h.result(runId)).status).toBe("failed");

    await h.wf.operator().retry(runId, "b", { actor: "ops" });
    await h.drain();

    expect(attempts).toBe(2);
    const result = await h.result(runId);
    expect(result.stepStatus("b")).toBe("completed");
    expect(result.status).toBe("completed");
  }, 5_000);

  it("reopen refuses to steal a concurrency key another active run holds", async () => {
    let shouldFail = true;
    const f = flow({
      id: "reopen-conflict",
      input: passthroughSchema<Record<string, never>>(),
      concurrency: { keyFn: () => "k", mode: "cancel-in-progress" },
      build: (b) => {
        const s = b.task({
          retry: { maxAttempts: 1, backoff: "fixed" },
          run: async () => {
            if (shouldFail) throw new Error("boom");
            return { ok: true };
          },
        });
        const wait = b.signal({
          needs: { s },
          timeoutMs: "unbounded" as const,
          names: ["go"],
          schema: passthroughSchema<{ ok: boolean }>(),
        });
        return { s, wait };
      },
    });
    const h = await makeHarness(f);
    const run1 = await h.wf.start(f, {});
    await h.drain();
    expect((await h.result(run1)).status).toBe("failed");

    // run2 takes the freed key and parks on the signal, so it stays running.
    shouldFail = false;
    const run2 = await h.wf.start(f, {});
    await h.drain();
    expect((await h.result(run2)).status).toBe("running");

    await expect(
      h.wf.operator().retry(run1, "s", { actor: "ops" }),
    ).rejects.toBeInstanceOf(NagiConcurrencyConflictError);

    const result = await h.result(run1);
    expect(result.status).toBe("failed");
    expect(result.factCount("step.reset")).toBe(0);
    expect((await h.result(run2)).status).toBe("running");
  });
});
