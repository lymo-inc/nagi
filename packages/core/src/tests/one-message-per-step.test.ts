import { describe, expect, it, vi } from "vitest";
import { flow } from "../builder";
import { InMemoryClock, InMemoryQueue, InMemoryStore } from "../memory";
import { nagi } from "../runtime";
import { isTerminalRun } from "../state";
import type { Flow, RunId } from "../types";
import { passthroughSchema } from "./test-helpers";

// Prod (2026-11): 163,850 nagi acks for 79,598 step.started facts, 2.06
// messages per started step. advance() enqueued every pending runnable step,
// so each sibling settle re-queued the siblings still waiting for a slot, and
// every extra copy cost a read, a fold and an ack.

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

async function run(f: Flow) {
  const store = new InMemoryStore();
  const queue = new InMemoryQueue();
  const wf = await nagi({
    flows: [f],
    store,
    queue,
    clock: new InMemoryClock(),
  });
  const enqueue = vi.spyOn(queue, "enqueue");
  const ac = new AbortController();
  const done = wf
    .worker({
      concurrency: 1,
      pollIntervalMs: 5,
      timerSweepIntervalMs: 0,
      reaperIntervalMs: 0,
      signal: ac.signal,
    })
    .run();
  try {
    const runId = await wf.start(f, {});
    const state = await vi.waitFor(
      async () => {
        const s = await store.loadRunState(runId as RunId);
        expect(isTerminalRun(s)).toBe(true);
        return s;
      },
      { timeout: 5_000, interval: 5 },
    );
    return { state, enqueue, queue, wf, store, runId };
  } finally {
    ac.abort();
    await done;
  }
}

describe("advance() queues a step once", () => {
  it("a fan-out of four behind one slot sends one message per step", async () => {
    const fan = flow({
      id: "one-msg-fan",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => {
        const root = b.task({ run: async () => 0 });
        const leaf = () =>
          b.task({
            needs: { root },
            run: async () => {
              await sleep(10);
              return 1;
            },
          });
        const a = leaf();
        const c = leaf();
        const d = leaf();
        const e = leaf();
        const join = b.task({ needs: { a, c, d, e }, run: async () => 2 });
        return { root, a, c, d, e, join };
      },
    });
    const { state, enqueue } = await run(fan);
    expect(state.phase.tag).toBe("completed");
    const perStep = new Map<string, number>();
    for (const [, stepId] of enqueue.mock.calls)
      perStep.set(stepId, (perStep.get(stepId) ?? 0) + 1);
    expect(Object.fromEntries(perStep)).toEqual({
      root: 1,
      a: 1,
      c: 1,
      d: 1,
      e: 1,
      join: 1,
    });
  });

  it("a signal settling does not re-queue the siblings still waiting", async () => {
    const withSignal = flow({
      id: "one-msg-signal",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => {
        const wait = b.signal({
          timeoutMs: 5_000,
          schema: passthroughSchema<{ ok: boolean }>(),
        });
        const slow = b.task({
          run: async () => {
            await sleep(30);
            return 1;
          },
        });
        const queued = b.task({ run: async () => 3 });
        const after = b.task({
          needs: { wait, slow, queued },
          run: async () => 2,
        });
        return { wait, slow, queued, after };
      },
    });
    const store = new InMemoryStore();
    const queue = new InMemoryQueue();
    const wf = await nagi({
      flows: [withSignal],
      store,
      queue,
      clock: new InMemoryClock(),
    });
    const enqueue = vi.spyOn(queue, "enqueue");
    const ac = new AbortController();
    const done = wf
      .worker({
        concurrency: 1,
        pollIntervalMs: 5,
        timerSweepIntervalMs: 0,
        reaperIntervalMs: 0,
        signal: ac.signal,
      })
      .run();
    try {
      const runId = await wf.start(withSignal, {});
      // wait parks, slow holds the one slot, queued is still in the queue.
      await sleep(10);
      await wf.signal(runId, "wait", { ok: true });
      await vi.waitFor(
        async () =>
          expect(isTerminalRun(await store.loadRunState(runId))).toBe(true),
        { timeout: 5_000, interval: 5 },
      );
    } finally {
      ac.abort();
      await done;
    }
    const counts = new Map<string, number>();
    for (const [, stepId] of enqueue.mock.calls)
      counts.set(stepId, (counts.get(stepId) ?? 0) + 1);
    expect(Object.fromEntries(counts)).toEqual({
      wait: 1,
      slow: 1,
      queued: 1,
      after: 1,
    });
  });

  it("re-queues a runnable step whose message is gone", async () => {
    const chain = flow({
      id: "one-msg-lost",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => {
        const a = b.task({ run: async () => 1 });
        const c = b.task({ run: async () => 2 });
        return { a, c };
      },
    });
    const store = new InMemoryStore();
    const queue = new InMemoryQueue();
    const wf = await nagi({
      flows: [chain],
      store,
      queue,
      clock: new InMemoryClock(),
    });
    const runId = await wf.start(chain, {});
    // Lose c's message: the store still says c is pending and runnable.
    const msgs = await queue.dequeue({ count: 10 });
    const lost = msgs.find((m) => m.stepId === "c");
    if (lost === undefined) throw new Error("expected c's message");
    await queue.ack(lost.receipt);
    for (const m of msgs)
      if (m.stepId !== "c") await queue.nack(m.receipt, { delayMs: 0 });

    const ac = new AbortController();
    const done = wf
      .worker({
        concurrency: 1,
        pollIntervalMs: 5,
        timerSweepIntervalMs: 0,
        reaperIntervalMs: 0,
        signal: ac.signal,
      })
      .run();
    try {
      // a settles, its advance sees c runnable with nothing queued for it.
      const state = await vi.waitFor(
        async () => {
          const s = await store.loadRunState(runId);
          expect(isTerminalRun(s)).toBe(true);
          return s;
        },
        { timeout: 5_000, interval: 5 },
      );
      expect(state.phase.tag).toBe("completed");
    } finally {
      ac.abort();
      await done;
    }
  });
});
