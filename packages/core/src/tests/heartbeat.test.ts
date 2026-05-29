import { describe, expect, it, vi } from "vitest";
import { flow } from "../builder";
import { InMemoryClock, InMemoryQueue, InMemoryStore } from "../memory";
import { nagi } from "../runtime";
import { isTerminalRun } from "../state";
import { startHeartbeat } from "../step-exec";
import type { Queue, RunId, Store } from "../types";
import { passthroughSchema } from "./test-helpers";

function makeStore(extendLease = vi.fn().mockResolvedValue(undefined)): {
  store: Store;
  extendLease: ReturnType<typeof vi.fn>;
} {
  const store = { extendLease } as unknown as Store;
  return { store, extendLease };
}

describe("startHeartbeat", () => {
  it("extends the lease every interval until stopped", async () => {
    vi.useFakeTimers();
    try {
      const extend = vi.fn().mockResolvedValue(undefined);
      const queue = { extend } as unknown as Queue;
      const { store, extendLease } = makeStore();
      const emitLog = vi.fn();

      const hb = startHeartbeat({
        queue,
        store,
        runId: "r" as RunId,
        stepId: "s",
        attempt: 1,
        receipt: "42",
        intervalMs: 100,
        leaseMs: 500,
        emitLog,
      });

      await vi.advanceTimersByTimeAsync(350);
      expect(extend).toHaveBeenCalledTimes(3);
      expect(extend).toHaveBeenCalledWith("42", 500);
      expect(extendLease).toHaveBeenCalledTimes(3);
      expect(extendLease).toHaveBeenCalledWith("r", "s", 1, 500);

      hb.stop();
      await vi.advanceTimersByTimeAsync(500);
      expect(extend).toHaveBeenCalledTimes(3);
    } finally {
      vi.useRealTimers();
    }
  });

  it("keeps beating and logs a warning when an extension fails", async () => {
    vi.useFakeTimers();
    try {
      const extend = vi
        .fn()
        .mockRejectedValueOnce(new Error("boom"))
        .mockResolvedValue(undefined);
      const queue = { extend } as unknown as Queue;
      const { store } = makeStore();
      const emitLog = vi.fn();

      const hb = startHeartbeat({
        queue,
        store,
        runId: "r" as RunId,
        stepId: "s",
        attempt: 1,
        receipt: "7",
        intervalMs: 50,
        leaseMs: 200,
        emitLog,
      });

      await vi.advanceTimersByTimeAsync(120);
      expect(extend).toHaveBeenCalledTimes(2);
      expect(emitLog).toHaveBeenCalledWith(
        expect.objectContaining({ level: "warn" }),
      );

      hb.stop();
    } finally {
      vi.useRealTimers();
    }
  });

  it("logs a warning but keeps beating when store.extendLease fails", async () => {
    vi.useFakeTimers();
    try {
      const extend = vi.fn().mockResolvedValue(undefined);
      const queue = { extend } as unknown as Queue;
      const extendLease = vi
        .fn()
        .mockRejectedValueOnce(new Error("store boom"))
        .mockResolvedValue(undefined);
      const { store } = makeStore(extendLease);
      const emitLog = vi.fn();

      const hb = startHeartbeat({
        queue,
        store,
        runId: "r" as RunId,
        stepId: "s",
        attempt: 1,
        receipt: "9",
        intervalMs: 50,
        leaseMs: 200,
        emitLog,
      });

      await vi.advanceTimersByTimeAsync(120);
      expect(extendLease).toHaveBeenCalledTimes(2);
      expect(emitLog).toHaveBeenCalledWith(
        expect.objectContaining({
          level: "warn",
          msg: expect.stringContaining("store lease"),
        }),
      );

      hb.stop();
    } finally {
      vi.useRealTimers();
    }
  });
});

describe("worker visibility heartbeat", () => {
  it("keeps a slow step's message leased so its body runs exactly once", async () => {
    let runs = 0;
    const f = flow({
      id: "hb-slow-step",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => {
        // Handler outlives the queue's 30ms initial lease — without the
        // heartbeat the message would become visible again and be redelivered.
        const slow = b.task({
          run: async () => {
            runs += 1;
            await new Promise((r) => setTimeout(r, 120));
            return { ok: true };
          },
        });
        return { slow };
      },
    });

    const store = new InMemoryStore();
    const queue = new InMemoryQueue({ leaseMs: 30 });
    const extendSpy = vi.spyOn(queue, "extend");
    const clock = new InMemoryClock();

    const wf = await nagi({
      flows: [f],
      store,
      queue,
      clock,
      heartbeatIntervalMs: 10,
      heartbeatLeaseMs: 50,
    });

    const ac = new AbortController();
    const worker = wf.worker({ pollIntervalMs: 5, signal: ac.signal });
    const done = worker.run();

    try {
      const runId = await wf.start(f, {});

      const start = Date.now();
      while (Date.now() - start < 3_000) {
        if (isTerminalRun(await store.loadRunState(runId))) break;
        await new Promise((r) => setTimeout(r, 5));
      }

      const state = await store.loadRunState(runId);
      expect(isTerminalRun(state)).toBe(true);
      expect(runs).toBe(1);
      expect(extendSpy).toHaveBeenCalled();
      for (const call of extendSpy.mock.calls) {
        expect(call[1]).toBe(50);
      }
    } finally {
      ac.abort();
      await done;
    }
  });
});
