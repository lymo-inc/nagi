import { describe, expect, it, vi } from "vitest";
import { flow } from "./builder";
import { InMemoryQueue, InMemoryStore } from "./memory";
import { nagi } from "./runtime";
import { passthroughSchema } from "./test-helpers";
import type { Logger, QueueDequeueOpts, QueueMessage } from "./types";

const echo = flow({
  id: "echo",
  input: passthroughSchema<{ x: number }>(),
  build: (b) => ({
    step: b.task({ run: async ({ input }) => ({ y: input.x }) }),
  }),
});

function spyLogger(): Logger {
  return { debug: vi.fn(), info: vi.fn(), warn: vi.fn(), error: vi.fn() };
}

// A queue whose dequeue always throws — the one path that rejects worker.run()
// (per-message errors are swallowed by the worker). Models a lost DB connection.
class CrashingQueue extends InMemoryQueue {
  override async dequeue(
    _opts: QueueDequeueOpts,
  ): Promise<readonly QueueMessage[]> {
    throw new Error("queue connection lost");
  }
}

describe("nagi.run — shape & worker", () => {
  it("returns { wf, stop } from a single await", async () => {
    const handle = await nagi.run({
      flows: [echo],
      store: new InMemoryStore(),
      queue: new InMemoryQueue(),
    });
    expect(typeof handle.stop).toBe("function");
    expect(typeof handle.wf.start).toBe("function");
    await handle.stop();
  });

  it("the internal worker processes a started flow with no manual drain", async () => {
    const store = new InMemoryStore();
    const handle = await nagi.run({
      flows: [echo],
      store,
      queue: new InMemoryQueue(),
      worker: { pollIntervalMs: 5 },
    });
    const runId = await handle.wf.start(echo, { x: 7 });
    await vi.waitFor(async () => {
      expect((await store.loadRunState(runId)).status).toBe("completed");
    });
    await handle.stop();
  });
});

describe("nagi.run — stop() lifecycle", () => {
  it("stop() awaits an in-flight handler before resolving", async () => {
    let started = false;
    let finished = false;
    let release: () => void = () => {};
    const gate = new Promise<void>((r) => {
      release = r;
    });
    const gated = flow({
      id: "gated",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => ({
        step: b.task({
          run: async () => {
            started = true;
            await gate;
            finished = true;
            return {};
          },
        }),
      }),
    });

    const handle = await nagi.run({
      flows: [gated],
      store: new InMemoryStore(),
      queue: new InMemoryQueue(),
      worker: { pollIntervalMs: 5 },
    });
    await handle.wf.start(gated, {});
    await vi.waitFor(() => expect(started).toBe(true));

    const stopP = handle.stop();
    let stopResolved = false;
    void stopP.then(() => {
      stopResolved = true;
    });
    await new Promise((r) => setTimeout(r, 30));
    expect(stopResolved).toBe(false); // parked on the in-flight handler
    expect(finished).toBe(false);

    release();
    await stopP;
    expect(finished).toBe(true); // handler ran to completion before stop resolved
  });

  it("stop() is idempotent — twice and concurrently — and never throws", async () => {
    const handle = await nagi.run({
      flows: [echo],
      store: new InMemoryStore(),
      queue: new InMemoryQueue(),
      worker: { pollIntervalMs: 5 },
    });
    const a = handle.stop();
    const b = handle.stop();
    expect(a).toBe(b); // memoized: one shutdown, shared promise
    await expect(Promise.all([a, b, handle.stop()])).resolves.toBeDefined();
  });
});

describe("nagi.run — graceful vs crash", () => {
  it("graceful stop() never calls logger.error", async () => {
    const logger = spyLogger();
    const store = new InMemoryStore();
    const handle = await nagi.run({
      flows: [echo],
      store,
      queue: new InMemoryQueue(),
      worker: { pollIntervalMs: 5 },
      logger,
    });
    const runId = await handle.wf.start(echo, { x: 5 });
    await vi.waitFor(async () => {
      expect((await store.loadRunState(runId)).status).toBe("completed");
    });
    await handle.stop();
    expect(logger.error).not.toHaveBeenCalled();
  });

  it("a true loop crash is logged once via logger.error; stop() still resolves", async () => {
    const logger = spyLogger();
    const handle = await nagi.run({
      flows: [echo],
      store: new InMemoryStore(),
      queue: new CrashingQueue(),
      worker: { pollIntervalMs: 5 },
      logger,
    });
    await vi.waitFor(() => expect(logger.error).toHaveBeenCalledTimes(1));
    expect(logger.error).toHaveBeenCalledWith(
      "nagi.run: worker exited unexpectedly",
      expect.objectContaining({
        error: expect.stringContaining("queue connection lost"),
      }),
    );
    await expect(handle.stop()).resolves.toBeUndefined();
  });
});

describe("nagi.run — external signal", () => {
  it("aborting an external signal triggers graceful shutdown (no logger.error)", async () => {
    const logger = spyLogger();
    const ac = new AbortController();
    const handle = await nagi.run({
      flows: [echo],
      store: new InMemoryStore(),
      queue: new InMemoryQueue(),
      worker: { pollIntervalMs: 5 },
      signal: ac.signal,
      logger,
    });
    ac.abort();
    await handle.stop();
    expect(logger.error).not.toHaveBeenCalled();
  });

  it("an already-aborted external signal yields an immediately-stopped runtime", async () => {
    const logger = spyLogger();
    const ac = new AbortController();
    ac.abort();
    const handle = await nagi.run({
      flows: [echo],
      store: new InMemoryStore(),
      queue: new InMemoryQueue(),
      signal: ac.signal,
      logger,
    });
    await handle.stop();
    expect(logger.error).not.toHaveBeenCalled();
  });
});

describe("nagi.run — back-compat", () => {
  it("legacy nagi() + wf.worker({ signal }) + worker.run() still drives a flow", async () => {
    const store = new InMemoryStore();
    const wf = await nagi({
      flows: [echo],
      store,
      queue: new InMemoryQueue(),
    });
    const ac = new AbortController();
    const worker = wf.worker({ signal: ac.signal, pollIntervalMs: 5 });
    const loop = worker.run();
    const runId = await wf.start(echo, { x: 9 });
    await vi.waitFor(async () => {
      expect((await store.loadRunState(runId)).status).toBe("completed");
    });
    ac.abort();
    await loop;
  });
});
