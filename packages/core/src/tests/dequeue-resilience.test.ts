import { describe, expect, it } from "vitest";
import { flow } from "../builder";
import { InMemoryClock, InMemoryQueue, InMemoryStore } from "../memory";
import { nagi } from "../runtime";
import type {
  LogEntry,
  Millis,
  Queue,
  QueueDequeueOpts,
  QueueEnqueueOpts,
  QueueMessage,
  RunId,
  StepId,
} from "../types";
import { emptySchema } from "./test-helpers";

// Rejects the first `failures` dequeue calls, then delegates. Models the prod
// trigger: a transient pg connection timeout inside pgmq.read.
class FlakyDequeueQueue implements Queue {
  dequeueCalls = 0;
  constructor(
    private readonly inner: InMemoryQueue,
    private readonly failures: number,
  ) {}
  async enqueue(
    runId: RunId,
    stepId: StepId,
    opts?: QueueEnqueueOpts,
  ): Promise<void> {
    return this.inner.enqueue(runId, stepId, opts);
  }
  async dequeue(opts: QueueDequeueOpts): Promise<readonly QueueMessage[]> {
    this.dequeueCalls++;
    if (this.dequeueCalls <= this.failures) {
      throw new Error("Connection terminated due to connection timeout");
    }
    return this.inner.dequeue(opts);
  }
  async ack(receipt: string): Promise<void> {
    return this.inner.ack(receipt);
  }
  async nack(receipt: string, opts?: { delayMs?: Millis }): Promise<void> {
    return this.inner.nack(receipt, opts);
  }
  async extend(receipt: string, leaseMs: Millis): Promise<void> {
    return this.inner.extend(receipt, leaseMs);
  }
}

const noop = flow({
  id: "noop",
  input: emptySchema(),
  build: (b) => ({ s: b.task({ run: async () => ({ ok: true }) }) }),
});

async function waitFor(
  predicate: () => Promise<boolean>,
  message: string,
): Promise<void> {
  const deadline = Date.now() + 5_000;
  for (;;) {
    if (await predicate()) return;
    if (Date.now() > deadline) throw new Error(message);
    await new Promise((r) => setTimeout(r, 10));
  }
}

// The outage this guards: one rejected pgmq.read terminated run(), every flow
// stopped being scheduled, and the process stayed healthy enough that nothing
// noticed for 37h.
describe("worker.run dequeue resilience", () => {
  it("survives a failing dequeue and keeps dispatching", async () => {
    const store = new InMemoryStore();
    const queue = new FlakyDequeueQueue(new InMemoryQueue(), 1);
    const logs: LogEntry[] = [];

    const wf = await nagi({
      flows: [noop],
      store,
      queue,
      clock: new InMemoryClock(),
      onLog: (e) => logs.push(e),
    });
    const runId = await wf.start(noop, {});

    const ac = new AbortController();
    const worker = wf.worker({
      pollIntervalMs: 5,
      timerSweepIntervalMs: 0,
      signal: ac.signal,
    });
    const loop = worker.run();

    try {
      await waitFor(
        async () => (await store.loadRunState(runId)).phase.tag === "completed",
        "run never completed: the loop died on the dequeue failure",
      );
    } finally {
      ac.abort();
      await loop;
    }

    expect(
      logs.filter((l) => l.msg === "worker.dequeue failed; backing off"),
    ).toHaveLength(1);
  });

  it("run() settles only via abort, even while dequeue never succeeds", async () => {
    const store = new InMemoryStore();
    const queue = new FlakyDequeueQueue(
      new InMemoryQueue(),
      Number.POSITIVE_INFINITY,
    );
    const logs: LogEntry[] = [];

    const wf = await nagi({
      flows: [noop],
      store,
      queue,
      clock: new InMemoryClock(),
      onLog: (e) => logs.push(e),
    });

    const ac = new AbortController();
    const worker = wf.worker({
      pollIntervalMs: 5,
      timerSweepIntervalMs: 0,
      signal: ac.signal,
    });
    let settled = false;
    const loop = worker.run().finally(() => {
      settled = true;
    });

    await waitFor(
      async () => queue.dequeueCalls >= 3,
      "loop stopped polling during a sustained outage",
    );
    expect(settled).toBe(false);

    ac.abort();
    await loop;
    expect(settled).toBe(true);

    // Backoff grows and is capped: base = max(pollIntervalMs, 50) = 50.
    const backoffs = logs
      .filter((l) => l.msg === "worker.dequeue failed; backing off")
      .map((l) => l.attrs?.["backoffMs"]);
    expect(backoffs.slice(0, 3)).toEqual([50, 100, 200]);
    expect(backoffs.every((b) => (b as number) <= 30_000)).toBe(true);
  });

  it("runOnce still rejects — its caller is awaiting a result", async () => {
    const store = new InMemoryStore();
    const queue = new FlakyDequeueQueue(new InMemoryQueue(), 1);
    const wf = await nagi({
      flows: [noop],
      store,
      queue,
      clock: new InMemoryClock(),
    });
    await wf.start(noop, {});

    await expect(wf.worker().runOnce()).rejects.toThrow(
      "Connection terminated",
    );
  });

  it("runUntilEmpty rejects the same way", async () => {
    const store = new InMemoryStore();
    const queue = new FlakyDequeueQueue(new InMemoryQueue(), 1);
    const wf = await nagi({
      flows: [noop],
      store,
      queue,
      clock: new InMemoryClock(),
    });
    await wf.start(noop, {});

    await expect(wf.worker().runUntilEmpty()).rejects.toThrow(
      "Connection terminated",
    );
  });
});
