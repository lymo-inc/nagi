import { describe, expect, it, vi } from "vitest";
import { flow } from "../builder";
import { InMemoryClock, InMemoryQueue, InMemoryStore } from "../memory";
import { nagi } from "../runtime";
import { isTerminalRun } from "../state";
import type { RunId, WorkerConfig } from "../types";
import { passthroughSchema } from "./test-helpers";

// A live run with no lease, no timer, no queued message and no live child has
// nothing left that will ever move it. Lymo hit this on staging: run 7e9edbb5
// (videoAnalysis, 2026-06-02) failed its awaitAudio signal by timeout, the
// advance after it never landed, and the run sat `running` for 176 days.
// The worker now finds such quiet runs and drives them to an end by itself.

const QUIET: WorkerConfig = {
  pollIntervalMs: 5,
  quietRunSweepIntervalMs: 10,
  quietRunMs: 30,
};

async function setup<F extends ReturnType<typeof flow>>(f: F) {
  const store = new InMemoryStore();
  const queue = new InMemoryQueue();
  const wf = await nagi({
    flows: [f],
    store,
    queue,
    clock: new InMemoryClock(),
  });
  const run = (config: WorkerConfig = QUIET) => {
    const ac = new AbortController();
    const done = wf.worker({ ...config, signal: ac.signal }).run();
    return async () => {
      ac.abort();
      await done;
    };
  };
  const ended = (runId: RunId) =>
    vi.waitFor(
      async () => {
        const state = await store.loadRunState(runId);
        expect(isTerminalRun(state)).toBe(true);
        return state;
      },
      { timeout: 2_000 },
    );
  return { store, queue, wf, run, ended };
}

const signalFlow = flow({
  id: "quiet-signal",
  input: passthroughSchema<Record<string, never>>(),
  build: (b) => {
    const wait = b.signal({
      timeoutMs: 20,
      schema: passthroughSchema<Record<string, never>>(),
    });
    const after = b.task({ needs: { wait }, run: async () => ({ ok: true }) });
    return { wait, after };
  },
});

const chain = flow({
  id: "quiet-chain",
  input: passthroughSchema<Record<string, never>>(),
  build: (b) => {
    const a = b.task({ run: async () => ({ a: 1 }) });
    const c = b.task({ needs: { a }, run: async () => ({ c: 2 }) });
    return { a, c };
  },
});

describe("quiet-run sweep", () => {
  it("fails a run whose signal timed out but whose advance was lost (staging 7e9edbb5)", async () => {
    const { store, wf, run, ended } = await setup(signalFlow);
    const runId = await wf.start(signalFlow, {});

    // The first advance after the timeout throws (deploy, pool blip): the
    // store has already failed the step and dropped its timer.
    const real = store.sweepSignalTimeouts.bind(store);
    vi.spyOn(store, "sweepSignalTimeouts").mockImplementation(async (args) => {
      const out = await real(args);
      if (out.length > 0) {
        vi.spyOn(store, "loadRunState").mockImplementationOnce(async () => {
          throw new Error("connection terminated");
        });
      }
      return out;
    });

    const stop = run({ ...QUIET, timerSweepIntervalMs: 10 });
    try {
      const state = await ended(runId);
      expect(state.phase.tag).toBe("failed");
      if (state.phase.tag === "failed")
        expect(state.phase.error.name).toBe("NagiSignalTimeoutError");
    } finally {
      await stop();
    }
  });

  it("drives a run whose settled step's advance was lost", async () => {
    const { store, queue, wf, run, ended } = await setup(chain);
    const runId = await wf.start(chain, {});

    // Crash between settle and advance: the step's fact is durable, its
    // message is gone, nothing enqueued its successor.
    const [msg] = await queue.dequeue({ count: 1 });
    if (msg === undefined) throw new Error("expected a message");
    const { Facts } = await import("../facts");
    const at = new Date();
    await store.appendFact(runId, Facts.stepStarted(runId, "a", 1, "task", at));
    await store.appendFact(
      runId,
      Facts.stepCompleted(runId, "a", 1, { a: 1 }, at),
    );
    await queue.ack(msg.receipt);

    const stop = run();
    try {
      const state = await ended(runId);
      expect(state.phase.tag).toBe("completed");
    } finally {
      await stop();
    }
  });

  it("re-dispatches a parked step whose lease and message both vanished", async () => {
    const { store, queue, wf, run, ended } = await setup(chain);
    const runId = await wf.start(chain, {});
    const [msg] = await queue.dequeue({ count: 1 });
    if (msg === undefined) throw new Error("expected a message");
    const { Facts } = await import("../facts");
    await store.appendFact(
      runId,
      Facts.stepStarted(runId, "a", 1, "task", new Date()),
    );
    await queue.ack(msg.receipt);

    const stop = run();
    try {
      const state = await ended(runId);
      expect(state.phase.tag).toBe("completed");
    } finally {
      await stop();
    }
  });

  it("leaves an explicitly unbounded signal wait alone", async () => {
    const unbounded = flow({
      id: "quiet-unbounded",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => ({
        wait: b.signal({
          timeoutMs: "unbounded" as const,
          schema: passthroughSchema<Record<string, never>>(),
        }),
      }),
    });
    const { store, wf, run } = await setup(unbounded);
    const runId = await wf.start(unbounded, {});
    const stop = run();
    await new Promise((r) => setTimeout(r, 150));
    await stop();
    const state = await store.loadRunState(runId);
    expect(state.phase.tag).toBe("running");
    expect(state.facts.at(-1)?.kind).toBe("step.started");
  });

  it("does not touch a run whose next attempt is queued behind a retry delay", async () => {
    const backoff = flow({
      id: "quiet-backoff",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => ({
        a: b.task({
          retry: { maxAttempts: 2, backoff: "fixed", initialDelayMs: 10_000 },
          run: async () => {
            throw new Error("flaky");
          },
        }),
      }),
    });
    const { store, queue, wf, run } = await setup(backoff);
    const runId = await wf.start(backoff, {});
    const stop = run();
    await new Promise((r) => setTimeout(r, 150));
    await stop();
    const state = await store.loadRunState(runId);
    expect(state.phase.tag).toBe("running");
    expect(state.facts.filter((f) => f.kind === "step.started")).toHaveLength(
      1,
    );
    expect(await queue.inspect(runId)).toHaveLength(1);
  });

  it("quietRunSweepIntervalMs: 0 disables the sweep", async () => {
    const { store, run } = await setup(chain);
    const sweep = vi.spyOn(store, "listQuietRuns");
    const stop = run({ ...QUIET, quietRunSweepIntervalMs: 0 });
    await new Promise((r) => setTimeout(r, 60));
    await stop();
    expect(sweep).not.toHaveBeenCalled();
  });
});
