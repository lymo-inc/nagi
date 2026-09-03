import { describe, expect, it } from "vitest";
import { flow } from "../builder";
import { InMemoryClock, InMemoryQueue, InMemoryStore } from "../memory";
import { nagi } from "../runtime";
import type { RunId } from "../types";
import { passthroughSchema } from "./test-helpers";

// The outage shape this bounds: N steps of ONE flow wedge in their handlers
// and hold every worker slot, so no other flow's runs ever get scheduled.
// With maxConcurrencyPerFlow, the wedged flow saturates its cap and the
// remaining slots keep serving everyone else.
describe("worker maxConcurrencyPerFlow", () => {
  it("a slot-hogging flow cannot starve another flow", async () => {
    const store = new InMemoryStore();
    const queue = new InMemoryQueue();
    const clock = new InMemoryClock();

    let concurrentHogs = 0;
    let maxConcurrentHogs = 0;
    let releaseHogs: () => void = () => {};
    const hogGate = new Promise<void>((resolve) => {
      releaseHogs = resolve;
    });

    const hog = flow({
      id: "hog",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => ({
        wedge: b.task({
          run: async () => {
            concurrentHogs++;
            maxConcurrentHogs = Math.max(maxConcurrentHogs, concurrentHogs);
            await hogGate;
            concurrentHogs--;
            return null;
          },
        }),
      }),
    });
    const bystander = flow({
      id: "bystander",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => ({
        s: b.task({ run: async () => ({ ok: true }) }),
      }),
    });

    const wf = await nagi({ flows: [hog, bystander], store, queue, clock });

    // 4 hog runs first, so they reach the front of the queue, then 1 bystander.
    const hogRuns: RunId[] = [];
    for (let i = 0; i < 4; i++) hogRuns.push(await wf.start(hog, {}));
    const bystanderRun = await wf.start(bystander, {});

    const ac = new AbortController();
    const worker = wf.worker({
      concurrency: 4,
      maxConcurrencyPerFlow: 3,
      pollIntervalMs: 5,
      timerSweepIntervalMs: 0,
      signal: ac.signal,
    });
    const loop = worker.run();

    // The bystander completes WHILE the hogs are still wedged — the cap held
    // a slot open for it.
    try {
      const deadline = Date.now() + 5_000;
      for (;;) {
        const s = await store.loadRunState(bystanderRun);
        if (s.phase.tag === "completed") break;
        if (Date.now() > deadline)
          throw new Error("bystander starved: cap did not hold a slot open");
        await new Promise((r) => setTimeout(r, 10));
      }
      expect(maxConcurrentHogs).toBeLessThanOrEqual(3);
    } finally {
      releaseHogs();
      ac.abort();
      await loop;
    }

    // Once released, the wedged flow itself still drains to completion —
    // deferral delays, it never drops.
    await wf.worker({ timerSweepIntervalMs: 0 }).runUntilEmpty();
    for (const runId of hogRuns) {
      expect((await store.loadRunState(runId)).phase.tag).toBe("completed");
    }
  });

  it("messages without flowId are exempt (pre-upgrade messages still dispatch)", async () => {
    const store = new InMemoryStore();
    const queue = new InMemoryQueue();
    const clock = new InMemoryClock();

    const f = flow({
      id: "legacy",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => ({
        s: b.task({ run: async () => ({ ok: true }) }),
      }),
    });
    const wf = await nagi({ flows: [f], store, queue, clock });
    const runId = await wf.start(f, {});

    // Strip flowId from the queued message, simulating a pre-upgrade envelope.
    const pending = (
      queue as unknown as { pending: Array<Record<string, unknown>> }
    ).pending;
    for (const item of pending) delete item["flowId"];

    await wf
      .worker({ maxConcurrencyPerFlow: 1, timerSweepIntervalMs: 0 })
      .runUntilEmpty();
    expect((await store.loadRunState(runId)).phase.tag).toBe("completed");
  });
});
