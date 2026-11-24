import { describe, expect, it } from "vitest";
import { flow } from "../builder";
import { InMemoryClock, InMemoryQueue, InMemoryStore } from "../memory";
import { nagi } from "../runtime";
import { isTerminalRun } from "../state";
import type { Queue, RunId, Store } from "../types";
import { passthroughSchema } from "./test-helpers";

// Prod 2026-08-27 (LYMO-1BF): one `pgmq.read` connection timeout rejected
// Worker.run() and every flow stopped for 37 h. This fails the database at
// every await the worker can reach — each store and queue call, at random —
// and asserts the loop never exits and every run still ends once the
// database is back.

function flaky<T extends object>(target: T, isDown: () => boolean): T {
  return new Proxy(target, {
    get(obj, key, receiver) {
      const value = Reflect.get(obj, key, receiver);
      if (typeof value !== "function") return value;
      return (...args: unknown[]) => {
        if (isDown()) return Promise.reject(new Error("connection terminated"));
        return (value as (...a: unknown[]) => unknown).apply(obj, args);
      };
    },
  });
}

const f = flow({
  id: "chaos",
  input: passthroughSchema<{ n: number }>(),
  build: (b) => {
    const a = b.task({ run: async ({ input }) => input.n });
    const c = b.activity({ needs: { a }, run: async () => 2 });
    const d = b.task({ needs: { a, c }, run: async () => 3 });
    return { a, c, d };
  },
});

describe("the worker loop survives the database failing at any await", () => {
  it("never exits while the store and queue fail at random, and every run ends after", async () => {
    let down = false;
    // Short leases: a step whose settle failed is reaped in 100 ms, not 60 s.
    const store = new InMemoryStore({ leaseMs: 100 });
    const queue = new InMemoryQueue({ leaseMs: 100 });
    // Start runs on the healthy pair; only the worker sees the outage.
    const healthy = await nagi({
      flows: [f],
      store,
      queue,
      clock: new InMemoryClock(),
    });
    const runIds: RunId[] = [];
    for (let n = 0; n < 6; n++) runIds.push(await healthy.start(f, { n }));

    const wf = await nagi({
      flows: [f],
      store: flaky<Store>(store, () => down && Math.random() < 0.5),
      queue: flaky<Queue>(queue, () => down && Math.random() < 0.5),
      clock: new InMemoryClock(),
    });
    const ac = new AbortController();
    let exited: unknown = "running";
    const loop = wf
      .worker({
        pollIntervalMs: 5,
        concurrency: 3,
        maxConcurrencyPerFlow: 2,
        timerSweepIntervalMs: 5,
        reaperIntervalMs: 5,
        // A retry whose enqueue failed after step.retried was written leaves
        // the step in backoff with no message; the quiet-run sweep re-queues
        // it. Prod's default is 2 min, so give it room inside this window.
        quietRunSweepIntervalMs: 50,
        quietRunMs: 300,
        signal: ac.signal,
      })
      .run()
      .then(
        () => {
          exited = "resolved";
        },
        (err) => {
          exited = err;
        },
      );

    down = true;
    await new Promise((r) => setTimeout(r, 400));
    expect(exited).toBe("running");
    down = false;

    const deadline = Date.now() + 8_000;
    for (;;) {
      const states = await Promise.all(
        runIds.map((r) => store.loadRunState(r)),
      );
      if (states.every(isTerminalRun)) break;
      if (Date.now() > deadline) {
        throw new Error(
          `runs did not end: ${states.map((s) => s.phase.tag).join(",")}`,
        );
      }
      await new Promise((r) => setTimeout(r, 20));
    }
    expect(exited).toBe("running");
    ac.abort();
    await loop;
    expect(exited).toBe("resolved");
  }, 20_000);
});
