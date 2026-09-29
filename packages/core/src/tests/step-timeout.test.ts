import { describe, expect, it } from "vitest";
import { flow } from "../builder";
import { canonicalize, sha256Canonical } from "../canonicalize";
import { NagiValidationError } from "../errors";
import { InMemoryClock, InMemoryQueue, InMemoryStore } from "../memory";
import { nagi } from "../runtime";
import { errorOf } from "../state";
import type {
  AttemptNumber,
  Json,
  RunId,
  StepCanceledFact,
  StepCompletedFact,
  StepFailedFact,
  StepId,
  StreamEvent,
  Tx,
} from "../types";
import {
  emptySchema,
  makeHarness,
  passthroughSchema,
  runFlow,
} from "./test-helpers";

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
    expect(events.at(-1)).toEqual({ kind: "error" });
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

  it("a body that swallows the abort and returns still fails the step", async () => {
    const f = flow({
      id: "swallowed-abort-task",
      input: emptySchema(),
      build: (b) => ({
        slow: b.task({
          timeoutMs: DEADLINE,
          retry: { maxAttempts: 1, backoff: "fixed" },
          run: async ({ ctx }) => {
            try {
              await sleep(SLOW, ctx.signal);
            } catch {
              return { partial: true };
            }
            return { full: true };
          },
        }),
      }),
    });

    const r = await runFlow(f, {});
    expect(r.status).toBe("failed");
    expect(r.error("slow").name).toBe("NagiStepTimeoutError");
  });

  it("a body that swallows the abort and returns still fails the step (activity)", async () => {
    const f = flow({
      id: "swallowed-abort-activity",
      input: emptySchema(),
      build: (b) => ({
        slow: b.activity({
          timeoutMs: DEADLINE,
          retry: { maxAttempts: 1, backoff: "fixed" },
          run: async ({ ctx }) => {
            try {
              await sleep(SLOW, ctx.signal);
            } catch {
              return { partial: true };
            }
            return { full: true };
          },
        }),
      }),
    });

    const r = await runFlow(f, {});
    expect(r.status).toBe("failed");
    expect(r.error("slow").name).toBe("NagiStepTimeoutError");
  });

  it("rejects a timeoutMs Node cannot represent", () => {
    const build = (timeoutMs: number) =>
      flow({
        id: "invalid-handler-timeout",
        input: emptySchema(),
        build: (b) => ({
          s: b.task({ timeoutMs, run: async () => ({}) }),
        }),
      });

    for (const v of [
      0,
      -1,
      1.5,
      Number.NaN,
      Number.POSITIVE_INFINITY,
      2 ** 31,
    ]) {
      expect(() => build(v)).toThrow(NagiValidationError);
    }
    expect(() => build(2 ** 31 - 1)).not.toThrow();
    expect(() => build(1)).not.toThrow();
  });

  it("rejects a non-finite signal timeoutMs", () => {
    const build = (timeoutMs: number | "unbounded") =>
      flow({
        id: "invalid-signal-timeout",
        input: emptySchema(),
        build: (b) => ({
          wait: b.signal({
            timeoutMs,
            schema: passthroughSchema<{ ok: true }>(),
          }),
        }),
      });

    expect(() => build(Number.NaN)).toThrow(NagiValidationError);
    expect(() => build(Number.POSITIVE_INFINITY)).toThrow(NagiValidationError);
    expect(() => build("unbounded")).not.toThrow();
    expect(() => build(7 * 24 * 3_600_000)).not.toThrow();
  });

  it("a commit error after the deadline is not relabelled as a timeout", async () => {
    // The handler body returns immediately; the COMMIT overruns the deadline
    // and fails with a real error. That error must survive, not be relabelled
    // NagiStepTimeoutError.
    class SlowCommitStore extends InMemoryStore {
      override async runStep<T extends Json>(
        _runId: RunId,
        _stepId: StepId,
        _attempt: AttemptNumber,
        _body: (tx: Tx) => Promise<{
          readonly output: T;
          readonly fact: StepCompletedFact | StepFailedFact | StepCanceledFact;
        }>,
      ): Promise<T> {
        await new Promise((resolve) => setTimeout(resolve, SLOW));
        throw new Error("commit failed");
      }
    }

    const store = new SlowCommitStore();
    const queue = new InMemoryQueue();
    const clock = new InMemoryClock();

    const f = flow({
      id: "commit-error-after-deadline",
      input: emptySchema(),
      build: (b) => ({
        fast: b.task({
          timeoutMs: DEADLINE,
          retry: { maxAttempts: 1, backoff: "fixed" },
          run: async () => ({ ok: true }),
        }),
      }),
    });

    const wf = await nagi({ flows: [f], store, queue, clock });
    const runId = await wf.start(f, {});
    await wf.worker({ pollIntervalMs: 1 }).runOnce({ maxSteps: 10 });

    const state = await store.loadRunState(runId);
    const step = state.steps["fast"];
    if (!step) throw new Error("step 'fast' missing from run state");
    const err = errorOf(step);
    expect(err?.name).toBe("Error");
    expect(err?.message).toBe("commit failed");
  });
});
