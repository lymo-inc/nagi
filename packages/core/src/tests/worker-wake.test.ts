import { describe, expect, it, vi } from "vitest";
import { flow } from "../builder";
import { InMemoryClock, InMemoryQueue, InMemoryStore } from "../memory";
import { nagi } from "../runtime";
import { isTerminalRun } from "../state";
import type { Flow, RunId, Tx, WorkerConfig } from "../types";
import { passthroughSchema } from "./test-helpers";

// Prod (rc.25, 2026-11-12 → 11-24): a step whose needs had all settled waited
// 0.87 s at p50 before it started even with no step in flight, and a 10-step
// chain on Postgres + pgmq took 1.0–1.2 s per step: the worker slept out its
// 1 s poll after the very enqueue it had just made. And a message over
// maxConcurrencyPerFlow was nacked and re-read about once a second until a
// slot freed (536,356 pgmq.set_vt on prod vs 38,913 lease heartbeats).

const POLL = 1_000; // the production default; every hop here must beat it

async function setup(f: Flow, worker: WorkerConfig = {}) {
  const store = new InMemoryStore();
  const queue = new InMemoryQueue();
  const wf = await nagi({
    flows: [f],
    store,
    queue,
    clock: new InMemoryClock(),
  });
  const ac = new AbortController();
  const done = wf
    .worker({
      pollIntervalMs: POLL,
      timerSweepIntervalMs: 0,
      reaperIntervalMs: 0,
      ...worker,
      signal: ac.signal,
    })
    .run();
  // Let the worker reach its first idle sleep.
  await new Promise((r) => setTimeout(r, 30));
  const stop = async () => {
    ac.abort();
    await done;
  };
  const ended = (runId: RunId) =>
    vi.waitFor(
      async () => {
        const s = await store.loadRunState(runId);
        expect(isTerminalRun(s)).toBe(true);
        return s;
      },
      { timeout: 5_000, interval: 5 },
    );
  return { store, queue, wf, stop, ended };
}

const chain = flow({
  id: "wake-chain",
  input: passthroughSchema<Record<string, never>>(),
  build: (b) => {
    const a = b.task({ run: async () => 1 });
    const c = b.task({ needs: { a }, run: async () => 2 });
    const d = b.task({ needs: { c }, run: async () => 3 });
    const e = b.task({ needs: { d }, run: async () => 4 });
    return { a, c, d, e };
  },
});

describe("worker wakes on work instead of sleeping out its poll", () => {
  it("a 4-step chain hands each step to the next without a poll wait", async () => {
    const { wf, stop, ended } = await setup(chain);
    try {
      const t0 = Date.now();
      const runId = await wf.start(chain, {});
      expect((await ended(runId)).phase.tag).toBe("completed");
      expect(Date.now() - t0).toBeLessThan(POLL / 2);
    } finally {
      await stop();
    }
  });

  it("a run started while the worker sleeps starts at once", async () => {
    const one = flow({
      id: "wake-one",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => ({ a: b.task({ run: async () => 1 }) }),
    });
    const { wf, stop, ended } = await setup(one);
    try {
      const t0 = Date.now();
      const runId = await wf.start(one, {});
      await ended(runId);
      expect(Date.now() - t0).toBeLessThan(POLL / 2);
    } finally {
      await stop();
    }
  });

  it("a staged start wakes the worker when its effects are applied", async () => {
    const one = flow({
      id: "wake-staged",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => ({ a: b.task({ run: async () => 1 }) }),
    });
    const { wf, stop, ended } = await setup(one);
    try {
      const t0 = Date.now();
      const res = await wf.startStaged(
        one,
        {},
        {
          tx: { fake: true } as unknown as Tx,
        },
      );
      await res.applyOnCommit();
      await ended(res.runId);
      expect(Date.now() - t0).toBeLessThan(POLL / 2);
    } finally {
      await stop();
    }
  });
});

describe("maxConcurrencyPerFlow holds an over-cap message instead of re-reading it", () => {
  const fan = flow({
    id: "hold-fan",
    input: passthroughSchema<Record<string, never>>(),
    build: (b) => {
      const root = b.task({ run: async () => 0 });
      const slow = b.task({
        needs: { root },
        run: async () => {
          await new Promise((r) => setTimeout(r, 100));
          return 1;
        },
      });
      const next = b.task({ needs: { root }, run: async () => 2 });
      return { root, slow, next };
    },
  });

  it("runs the held step as soon as the slot frees, with no nack and one read", async () => {
    const { queue, wf, stop, ended, store } = await setup(fan, {
      concurrency: 4,
      maxConcurrencyPerFlow: 1,
    });
    const nack = vi.spyOn(queue, "nack");
    try {
      const t0 = Date.now();
      const runId = await wf.start(fan, {});
      expect((await ended(runId)).phase.tag).toBe("completed");
      // slow (100 ms) and next run one after the other, never a poll apart.
      expect(Date.now() - t0).toBeLessThan(POLL / 2);
      expect(nack).not.toHaveBeenCalled();
      const started = (await store.loadRunState(runId)).facts.filter(
        (f) => f.kind === "step.started",
      );
      expect(started).toHaveLength(3);
    } finally {
      await stop();
    }
  });

  it("hands held messages back on shutdown so no peer waits out the visibility timeout", async () => {
    const block = flow({
      id: "hold-shutdown",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => {
        const root = b.task({ run: async () => 0 });
        const slow = b.task({
          needs: { root },
          run: async () => {
            await new Promise((r) => setTimeout(r, 150));
            return 1;
          },
        });
        const next = b.task({ needs: { root }, run: async () => 2 });
        return { root, slow, next };
      },
    });
    const { queue, wf, stop, store } = await setup(block, {
      concurrency: 4,
      maxConcurrencyPerFlow: 1,
    });
    const runId = await wf.start(block, {});
    await vi.waitFor(
      async () => {
        const s = await store.loadRunState(runId);
        expect(
          s.facts.some((f) => f.kind === "step.started" && f.stepId !== "root"),
        ).toBe(true);
      },
      { timeout: 2_000, interval: 5 },
    );
    await stop();
    const msgs = await queue.inspect(runId);
    // Whatever is still queued is visible now, not parked behind a lease.
    for (const m of msgs)
      expect(m.visibleAt.getTime()).toBeLessThanOrEqual(Date.now() + 5);
  });
});
