import { describe, expect, it, vi } from "vitest";
import { flow } from "../builder";
import { Facts } from "../facts";
import { InMemoryClock, InMemoryQueue, InMemoryStore } from "../memory";
import { nagi } from "../runtime";
import { makeHarness, passthroughSchema } from "./test-helpers";

function counterFlow(onRun: () => void) {
  return flow({
    id: "attempt-fence",
    input: passthroughSchema<Record<string, never>>(),
    build: (b) => ({
      t: b.task({
        run: async () => {
          onRun();
          return { ok: true };
        },
      }),
    }),
  });
}

describe("attempt fence", () => {
  it("a redelivered message for a failed attempt does not run while the step backs off", async () => {
    let calls = 0;
    const f = counterFlow(() => {
      calls++;
    });
    const h = await makeHarness(f);
    const runId = await h.wf.start(f, {});
    const [dispatch] = await h.queue.dequeue({ count: 1 });
    if (dispatch) await h.queue.ack(dispatch.receipt);

    const at = h.clock.now();
    await h.store.appendFact(
      runId,
      Facts.stepStarted(runId, "t", 1, "task", at),
    );
    await h.store.appendFact(
      runId,
      Facts.stepRetried(
        runId,
        "t",
        1,
        new Date(at.getTime() + 3_600_000),
        { name: "Error", message: "boom" },
        at,
      ),
    );
    await h.queue.enqueue(runId, "t", { attempt: 1, flowId: f.id });

    await h.drainOnce();

    expect(calls).toBe(0);
  });

  it("a stale message for an older attempt does not run next to the current one", async () => {
    let calls = 0;
    const f = counterFlow(() => {
      calls++;
    });
    const h = await makeHarness(f);
    const runId = await h.wf.start(f, {});
    const [dispatch] = await h.queue.dequeue({ count: 1 });
    if (dispatch) await h.queue.ack(dispatch.receipt);

    const at = h.clock.now();
    await h.store.appendFact(
      runId,
      Facts.stepStarted(runId, "t", 1, "task", at),
    );
    await h.store.appendFact(
      runId,
      Facts.stepRetried(
        runId,
        "t",
        1,
        new Date(at.getTime() + 3_600_000),
        { name: "Error", message: "boom" },
        at,
      ),
    );
    await h.store.appendFact(
      runId,
      Facts.stepStarted(runId, "t", 2, "task", at),
    );
    await h.queue.enqueue(runId, "t", { attempt: 1, flowId: f.id });

    await h.drainOnce();

    expect(calls).toBe(0);
  });

  it("the retry message itself is admitted", async () => {
    let calls = 0;
    const f = counterFlow(() => {
      calls++;
    });
    const h = await makeHarness(f);
    const runId = await h.wf.start(f, {});
    const [dispatch] = await h.queue.dequeue({ count: 1 });
    if (dispatch) await h.queue.ack(dispatch.receipt);

    const at = h.clock.now();
    await h.store.appendFact(
      runId,
      Facts.stepStarted(runId, "t", 1, "task", at),
    );
    await h.store.appendFact(
      runId,
      Facts.stepRetried(
        runId,
        "t",
        1,
        new Date(at.getTime() + 3_600_000),
        { name: "Error", message: "boom" },
        at,
      ),
    );
    await h.queue.enqueue(runId, "t", { attempt: 2, flowId: f.id });

    await h.drainOnce();

    expect(calls).toBe(1);
  });

  it("retry backoff longer than the lease is honoured", async () => {
    const store = new InMemoryStore({ leaseMs: 20 });
    const queue = new InMemoryQueue();
    const calls: number[] = [];
    const f = flow({
      id: "attempt-fence-backoff",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => ({
        t: b.task({
          retry: { maxAttempts: 2, backoff: "fixed", initialDelayMs: 400 },
          run: async () => {
            calls.push(Date.now());
            if (calls.length === 1) throw new Error("boom");
            return { ok: true };
          },
        }),
      }),
    });
    const wf = await nagi({
      flows: [f],
      store,
      queue,
      clock: new InMemoryClock(),
    });
    const runId = await wf.start(f, {});
    const ac = new AbortController();
    const done = wf
      .worker({ pollIntervalMs: 5, reaperIntervalMs: 10, signal: ac.signal })
      .run();
    try {
      await vi.waitFor(
        () => {
          expect(calls.length).toBe(2);
        },
        { timeout: 2_000 },
      );
    } finally {
      ac.abort();
      await done;
    }

    expect(calls[1]! - calls[0]!).toBeGreaterThanOrEqual(350);
    const state = await store.loadRunState(runId);
    expect(state.facts.some((fct) => fct.kind === "lease.reaped")).toBe(false);
  });
});
