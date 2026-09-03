import { describe, expect, it } from "vitest";
import { flow } from "../builder";
import { NagiRuntimeError } from "../errors";
import { InMemoryClock, InMemoryQueue, InMemoryStore } from "../memory";
import { nagi } from "../runtime";
import type { Queue } from "../types";
import { passthroughSchema } from "./test-helpers";

function simpleFlow() {
  return flow({
    id: "inspectable",
    input: passthroughSchema<Record<string, never>>(),
    build: (b) => ({
      s: b.task({ run: async () => ({ ok: true }) }),
    }),
  });
}

describe("wf.inspectQueue", () => {
  it("shows a never-scheduled run's pending message, then empties once dispatched", async () => {
    const store = new InMemoryStore();
    const queue = new InMemoryQueue();
    const clock = new InMemoryClock();
    const f = simpleFlow();
    const wf = await nagi({ flows: [f], store, queue, clock });

    // No worker yet: the initial step's message sits undelivered — the
    // "starved, never scheduled" triage shape.
    const runId = await wf.start(f, {});
    const before = await wf.inspectQueue(runId);
    expect(before).toHaveLength(1);
    expect(before[0]).toMatchObject({ stepId: "s", attempt: 1, readCount: 0 });

    await wf.worker({ timerSweepIntervalMs: 0 }).runUntilEmpty();
    expect(await wf.inspectQueue(runId)).toHaveLength(0);
  });

  it("throws NagiRuntimeError when the adapter has no inspect()", async () => {
    const store = new InMemoryStore();
    const inner = new InMemoryQueue();
    // Shape-compatible adapter without the optional inspect capability.
    const queue: Queue = {
      enqueue: inner.enqueue.bind(inner),
      dequeue: inner.dequeue.bind(inner),
      ack: inner.ack.bind(inner),
      nack: inner.nack.bind(inner),
      extend: inner.extend.bind(inner),
    };
    const clock = new InMemoryClock();
    const f = simpleFlow();
    const wf = await nagi({ flows: [f], store, queue, clock });
    const runId = await wf.start(f, {});
    await expect(wf.inspectQueue(runId)).rejects.toThrowError(NagiRuntimeError);
  });
});
