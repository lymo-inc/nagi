import { describe, expect, it, vi } from "vitest";
import { flow } from "../builder";
import { InMemoryStore } from "../memory";
import { startCancelWatcher } from "../step-exec";
import type { Millis, RunId } from "../types";
import { makeHarness, passthroughSchema } from "./test-helpers";

describe("startCancelWatcher", () => {
  it("stop() clears the pending tick and ends the poll loop", async () => {
    vi.useFakeTimers();
    try {
      const store = new InMemoryStore();
      const load = vi.spyOn(store, "loadRunState");
      const watcher = startCancelWatcher({
        store,
        runId: "r" as RunId,
        stepId: "s",
        attempt: 1,
        ac: new AbortController(),
        intervalMs: 100 as Millis,
      });
      expect(vi.getTimerCount()).toBe(1);

      await vi.advanceTimersByTimeAsync(100);
      expect(load).toHaveBeenCalledTimes(1);
      expect(vi.getTimerCount()).toBe(1);

      watcher.stop();
      expect(vi.getTimerCount()).toBe(0);
      await vi.advanceTimersByTimeAsync(1_000);
      expect(load).toHaveBeenCalledTimes(1);
      expect(vi.getTimerCount()).toBe(0);
    } finally {
      vi.useRealTimers();
    }
  });

  it("a settled step leaves no watcher timer on the event loop", async () => {
    vi.useFakeTimers();
    try {
      const f = flow({
        id: "cancel-watcher-settled",
        input: passthroughSchema<Record<string, never>>(),
        build: (b) => ({
          task: b.task({ run: async () => 1 }),
          activity: b.activity({ run: async () => 2 }),
        }),
      });
      const h = await makeHarness(f);
      const runId = await h.wf.start(f, {});
      await h.drain();
      expect((await h.result(runId)).status).toBe("completed");
      expect(vi.getTimerCount()).toBe(0);
    } finally {
      vi.useRealTimers();
    }
  });
});
