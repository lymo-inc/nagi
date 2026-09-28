import { describe, expect, it, vi } from "vitest";
import { flow } from "../builder";
import { InMemoryClock, InMemoryQueue, InMemoryStore } from "../memory";
import { nagi } from "../runtime";
import type { WorkerConfig } from "../types";
import { passthroughSchema } from "./test-helpers";

const f = flow({
  id: "worker-reaper",
  input: passthroughSchema<Record<string, never>>(),
  build: (b) => ({ s1: b.task({ run: async () => ({ ok: true }) }) }),
});

// A worker built straight from wf.worker() (no nagi.run) must reap a lease
// stranded by a crashed peer, or that step is stuck forever.
async function strandedLease(opts?: { nagiReaperIntervalMs?: number }) {
  const store = new InMemoryStore({ leaseMs: 5 });
  const queue = new InMemoryQueue();
  const wf = await nagi({
    flows: [f],
    store,
    queue,
    clock: new InMemoryClock(),
    ...(opts?.nagiReaperIntervalMs !== undefined
      ? { reaperIntervalMs: opts.nagiReaperIntervalMs }
      : {}),
  });
  const runId = await wf.start(f, {});
  const [msg] = await queue.dequeue({ count: 1 });
  if (msg === undefined) throw new Error("expected dispatch");
  expect(await store.claimStep(runId, msg.stepId, msg.attempt)).not.toBeNull();

  const run = async (config: WorkerConfig) => {
    const ac = new AbortController();
    const done = wf
      .worker({ pollIntervalMs: 5, ...config, signal: ac.signal })
      .run();
    return async () => {
      ac.abort();
      await done;
    };
  };
  return { store, runId, run };
}

describe("worker-owned lease reaping", () => {
  it("a worker created without nagi.run reaps an expired lease and finishes the run", async () => {
    const { store, runId, run } = await strandedLease();
    const stop = await run({ reaperIntervalMs: 10 });
    try {
      await vi.waitFor(
        async () => {
          const state = await store.loadRunState(runId);
          expect(state.phase.tag).toBe("completed");
          expect(state.facts.some((x) => x.kind === "lease.reaped")).toBe(true);
        },
        { timeout: 2_000 },
      );
    } finally {
      await stop();
    }
  });

  it("reaperIntervalMs: 0 disables the worker's reaper", async () => {
    const { store, run } = await strandedLease();
    const sweep = vi.spyOn(store, "sweepLeases");
    const stop = await run({ reaperIntervalMs: 0 });
    await new Promise((r) => setTimeout(r, 60));
    await stop();
    expect(sweep).not.toHaveBeenCalled();
  });

  it("NagiConfig.reaperIntervalMs is the fallback for wf.worker()", async () => {
    const { store, run } = await strandedLease({ nagiReaperIntervalMs: 0 });
    const sweep = vi.spyOn(store, "sweepLeases");
    const stop = await run({});
    await new Promise((r) => setTimeout(r, 60));
    await stop();
    expect(sweep).not.toHaveBeenCalled();
  });

  it("a failing sweep is logged, never kills the worker", async () => {
    const store = new InMemoryStore();
    vi.spyOn(store, "sweepLeases").mockRejectedValue(new Error("db down"));
    const logs: string[] = [];
    const wf = await nagi({
      flows: [f],
      store,
      queue: new InMemoryQueue(),
      onLog: (e) => logs.push(e.msg),
    });
    const ac = new AbortController();
    const done = wf
      .worker({ pollIntervalMs: 5, reaperIntervalMs: 5, signal: ac.signal })
      .run();
    await vi.waitFor(() =>
      expect(logs).toContain("worker: lease-reaper sweep failed"),
    );
    const runId = await wf.start(f, {});
    await vi.waitFor(async () =>
      expect((await store.loadRunState(runId)).phase.tag).toBe("completed"),
    );
    ac.abort();
    await done;
  });
});
