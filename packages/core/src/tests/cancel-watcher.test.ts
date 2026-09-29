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

      await watcher.stop();
      expect(vi.getTimerCount()).toBe(0);
      await vi.advanceTimersByTimeAsync(1_000);
      expect(load).toHaveBeenCalledTimes(1);
      expect(vi.getTimerCount()).toBe(0);
    } finally {
      vi.useRealTimers();
    }
  });

  it("stop() waits for an in-flight state read; no tick starts after it resolves", async () => {
    vi.useFakeTimers();
    try {
      const store = new InMemoryStore();
      const runId = "r" as RunId;
      const live = await store.loadRunState(runId);
      let release: () => void = () => {};
      const load = vi.spyOn(store, "loadRunState").mockImplementationOnce(
        () =>
          new Promise((r) => {
            release = () => r(live);
          }),
      );
      const watcher = startCancelWatcher({
        store,
        runId,
        stepId: "s",
        attempt: 1,
        ac: new AbortController(),
        intervalMs: 100 as Millis,
      });
      await vi.advanceTimersByTimeAsync(100);
      expect(load).toHaveBeenCalledTimes(1);

      let stopped = false;
      const stopping = (async () => {
        await watcher.stop();
        stopped = true;
      })();
      const again = watcher.stop();
      await vi.advanceTimersByTimeAsync(1_000);
      expect(stopped).toBe(false);

      release();
      await stopping;
      await again;
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
