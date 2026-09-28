import { describe, expect, it } from "vitest";
import { flow } from "../builder";
import { isRunEnd } from "../facts";
import { InMemoryClock, InMemoryQueue, InMemoryStore } from "../memory";
import { nagi } from "../runtime";
import type { FlowErrorEvent } from "../types";
import { passthroughSchema } from "./test-helpers";

function makeFlow(variant: "a" | "b") {
  return flow({
    id: "ends-once",
    input: passthroughSchema<Record<string, never>>(),
    build: (b) =>
      variant === "a"
        ? { s: b.task({ run: async () => ({ ok: true }) }) }
        : {
            s: b.task({ run: async () => ({ ok: true }) }),
            added: b.task({ run: async () => ({ added: true }) }),
          },
  });
}

describe("a run ends once", () => {
  it("two concurrent cancels write one end and fire the error hooks once", async () => {
    const store = new InMemoryStore();
    const flowErrors: FlowErrorEvent[] = [];
    const f = makeFlow("a");
    const wf = await nagi({
      flows: [f],
      store,
      queue: new InMemoryQueue(),
      clock: new InMemoryClock(),
      hooks: { onFlowError: (e) => void flowErrors.push(e) },
    });
    const runId = await wf.start(f, {});

    await Promise.all([
      wf.cancel(runId, { reason: "first" }),
      wf.cancel(runId, { reason: "second" }),
    ]);

    const { facts } = await store.loadRunState(runId);
    expect(facts.filter(isRunEnd)).toHaveLength(1);
    expect(flowErrors).toHaveLength(1);
  });

  it("two snapshot-gone messages for one run, dispatched concurrently, fail it once", async () => {
    const store = new InMemoryStore();
    const queue = new InMemoryQueue();
    const clock = new InMemoryClock();
    const flowErrors: FlowErrorEvent[] = [];

    const fA = makeFlow("a");
    const wfA = await nagi({ flows: [fA], store, queue, clock });
    const runId = await wfA.start(fA, {});
    await queue.enqueue(runId, "s", { flowId: fA.id });

    const fB = makeFlow("b");
    const wfB = await nagi({
      flows: [fB],
      store,
      queue,
      clock,
      hooks: { onFlowError: (e) => void flowErrors.push(e) },
    });
    const { processed } = await wfB
      .worker({
        concurrency: 2,
        timerSweepIntervalMs: 0,
        snapshotGonePolicy: () => ({ action: "fail" }),
      })
      .runUntilEmpty();

    expect(processed).toBe(2);
    const { facts, phase } = await store.loadRunState(runId);
    expect(phase.tag).toBe("failed");
    expect(facts.filter(isRunEnd)).toHaveLength(1);
    expect(flowErrors).toHaveLength(1);
    expect(await queue.dequeue({ count: 10 })).toHaveLength(0);
  });
});
