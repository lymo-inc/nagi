import { describe, expect, it } from "vitest";
import { flow } from "../builder";
import { canonicalize, sha256Canonical } from "../canonicalize";
import type { StepId, StreamEvent } from "../types";
import { emptySchema, makeHarness, runFlow } from "./test-helpers";

// Deadlines run on the real clock (setTimeout), not the InMemoryClock, so these
// use short real durations. SLOW must stay comfortably above DEADLINE or a
// loaded CI box can settle the step before the deadline fires.
const DEADLINE = 25;
const SLOW = 400;

function sleep(ms: number, signal?: AbortSignal): Promise<void> {
  return new Promise((resolve, reject) => {
    const t = setTimeout(resolve, ms);
    signal?.addEventListener("abort", () => {
      clearTimeout(t);
      reject(signal.reason);
    });
  });
}

describe("handler-step timeoutMs", () => {
  it("aborts ctx.signal at the deadline and settles the step as failed", async () => {
    const f = flow({
      id: "task-deadline",
      input: emptySchema(),
      build: (b) => ({
        slow: b.task({
          timeoutMs: DEADLINE,
          retry: { maxAttempts: 1, backoff: "fixed" },
          run: async ({ ctx }) => {
            await sleep(SLOW, ctx.signal);
            return { reached: true };
          },
        }),
      }),
    });

    const r = await runFlow(f, {});
    expect(r.status).toBe("failed");
    expect(r.stepStatus("slow")).toBe("failed");
    expect(r.error("slow").name).toBe("NagiStepTimeoutError");
  });

  it("is NOT a cancellation — the step fails rather than settling as canceled", async () => {
    const f = flow({
      id: "deadline-not-cancel",
      input: emptySchema(),
      build: (b) => ({
        slow: b.task({
          timeoutMs: DEADLINE,
          retry: { maxAttempts: 1, backoff: "fixed" },
          run: async ({ ctx }) => {
            await sleep(SLOW, ctx.signal);
            return {};
          },
        }),
      }),
    });

    const r = await runFlow(f, {});
    expect(r.stepStatus("slow")).toBe("failed");
    expect(r.factCount("step.canceled")).toBe(0);
    expect(r.factCount("step.failed")).toBe(1);
  });

  it("retries under the step's policy — a transient slow attempt recovers", async () => {
    let attempts = 0;
    const f = flow({
      id: "deadline-retry",
      input: emptySchema(),
      build: (b) => ({
        flaky: b.task({
          timeoutMs: DEADLINE,
          retry: { maxAttempts: 3, backoff: "fixed", initialDelayMs: 1 },
          run: async ({ ctx }) => {
            attempts += 1;
            // Only the first attempt overruns; the retry returns immediately.
            if (attempts === 1) await sleep(SLOW, ctx.signal);
            return { attempts };
          },
        }),
      }),
    });

    const r = await runFlow(f, {}, { timeoutMs: 10_000 });
    expect(r.status).toBe("completed");
    expect(r.output("flaky")).toEqual({ attempts: 2 });
    expect(r.factCount("step.retried")).toBe(1);
  });

  it("exhausts maxAttempts when the step is genuinely stuck", async () => {
    const f = flow({
      id: "deadline-exhaust",
      input: emptySchema(),
      build: (b) => ({
        stuck: b.task({
          timeoutMs: DEADLINE,
          retry: { maxAttempts: 2, backoff: "fixed", initialDelayMs: 1 },
          run: async ({ ctx }) => {
            await sleep(SLOW, ctx.signal);
            return {};
          },
        }),
      }),
    });

    const r = await runFlow(f, {}, { timeoutMs: 10_000 });
    expect(r.status).toBe("failed");
    expect(r.error("stuck").name).toBe("NagiStepTimeoutError");
    expect(r.factCount("step.retried")).toBe(1);
  });

  it("reports the deadline even when the handler throws its own error on abort", async () => {
    const f = flow({
      id: "deadline-own-error",
      input: emptySchema(),
      build: (b) => ({
        masked: b.task({
          timeoutMs: DEADLINE,
          retry: { maxAttempts: 1, backoff: "fixed" },
          run: async ({ ctx }) => {
            try {
              await sleep(SLOW, ctx.signal);
            } catch {
              // A real client (fetch, an SDK) throws its OWN abort-shaped error
              // here. The signal's reason must still win.
              throw new Error("upstream connection reset");
            }
            return {};
          },
        }),
      }),
    });

    const r = await runFlow(f, {});
    expect(r.error("masked").name).toBe("NagiStepTimeoutError");
    expect(r.error("masked").message).not.toContain("connection reset");
  });

  it("applies to activity steps, whose body runs outside the tx", async () => {
    const f = flow({
      id: "activity-deadline",
      input: emptySchema(),
      build: (b) => ({
        slow: b.activity({
          timeoutMs: DEADLINE,
          retry: { maxAttempts: 1, backoff: "fixed" },
          run: async ({ ctx }) => {
            await sleep(SLOW, ctx.signal);
            return {};
          },
        }),
      }),
    });

    const r = await runFlow(f, {});
    expect(r.status).toBe("failed");
    expect(r.error("slow").name).toBe("NagiStepTimeoutError");
  });

  it("applies to streaming steps, and closes the subscriber rather than hanging it", async () => {
    const f = flow({
      id: "streaming-deadline",
      input: emptySchema(),
      build: (b) => ({
        slow: b.streamingTask<
          Record<string, never>,
          Record<string, never>,
          string
        >({
          timeoutMs: DEADLINE,
          retry: { maxAttempts: 1, backoff: "fixed" },
          run: async ({ ctx }) => {
            await ctx.emit("first");
            await sleep(SLOW, ctx.signal);
            return {};
          },
        }),
      }),
    });

    const h = await makeHarness(f);
    const runId = await h.wf.start(f, {});
    const events: StreamEvent<string>[] = [];
    // A stalled streaming step must not strand its subscribers: the deadline
    // has to terminate the iterator, not just fail the run.
    const collected = (async () => {
      for await (const ev of h.wf.subscribe<string>(runId, "slow" as StepId))
        events.push(ev);
    })();
    await h.drain();
    await collected;

    const r = await h.result(runId);
    expect(r.status).toBe("failed");
    expect(r.error("slow").name).toBe("NagiStepTimeoutError");
    expect(events.at(0)).toEqual({ kind: "chunk", chunk: "first" });
    expect(events.at(-1)).toMatchObject({
      kind: "error",
      error: { name: "NagiStepTimeoutError" },
    });
  });

  it("omitting timeoutMs arms no deadline — a slow step still completes", async () => {
    const f = flow({
      id: "no-deadline",
      input: emptySchema(),
      build: (b) => ({
        slow: b.task({
          run: async ({ ctx }) => {
            await sleep(DEADLINE * 3, ctx.signal);
            return { ok: true };
          },
        }),
      }),
    });

    const r = await runFlow(f, {}, { timeoutMs: 10_000 });
    expect(r.status).toBe("completed");
    expect(r.output("slow")).toEqual({ ok: true });
  });

  it("does not fire once the step has completed inside its deadline", async () => {
    const h = await makeHarness(
      flow({
        id: "fast-under-deadline",
        input: emptySchema(),
        build: (b) => ({
          fast: b.task({ timeoutMs: 5_000, run: async () => ({ ok: true }) }),
        }),
      }),
    );
    const runId = await h.wf.startById("fast-under-deadline", {});
    await h.drain();
    const r = await h.result(runId);
    expect(r.status).toBe("completed");
    expect(r.factCount("step.failed")).toBe(0);
  });

  it("participates in the flow hash — a changed deadline is a changed flow", async () => {
    const mk = (timeoutMs?: number) =>
      flow({
        id: "hashed",
        input: emptySchema(),
        build: (b) => ({
          s: b.task({
            ...(timeoutMs === undefined ? {} : { timeoutMs }),
            run: async () => ({}),
          }),
        }),
      });

    const hash = async (f: ReturnType<typeof mk>) =>
      sha256Canonical(await canonicalize(f));
    const [none, short, long] = await Promise.all([
      hash(mk()),
      hash(mk(1_000)),
      hash(mk(2_000)),
    ]);
    expect(short).not.toBe(none);
    expect(long).not.toBe(short);
  });
});
