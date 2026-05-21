import { describe, expect, it } from "vitest";
import { flow } from "../builder";
import { type DispatchDeps, dispatchMessage } from "../dispatch";
import { InMemoryQueue, InMemoryStore } from "../memory";
import { nagi } from "../runtime";
import { passthroughSchema, runFlow } from "./test-helpers";

const echo = flow({
  id: "echo",
  input: passthroughSchema<{ x: number }>(),
  build: (b) => ({
    step: b.task({ run: async ({ input }) => ({ y: input.x }) }),
  }),
});

class SpyQueue extends InMemoryQueue {
  ensureSchemaCalls = 0;
  async ensureSchema(): Promise<void> {
    this.ensureSchemaCalls++;
  }
}

class BoomQueue extends InMemoryQueue {
  async ensureSchema(): Promise<void> {
    throw new Error("pgmq: permission denied to CREATE EXTENSION");
  }
}

describe("nagi() — auto queue-schema bootstrap", () => {
  it("invokes queue.ensureSchema exactly once during construction", async () => {
    const queue = new SpyQueue();
    await nagi({ flows: [echo], store: new InMemoryStore(), queue });
    expect(queue.ensureSchemaCalls).toBe(1);
  });

  it("does not call ensureSchema again on wf.start, wf.worker, or dispatch", async () => {
    const queue = new SpyQueue();
    const store = new InMemoryStore();
    const wf = await nagi({ flows: [echo], store, queue });
    expect(queue.ensureSchemaCalls).toBe(1);

    const deps = (wf as unknown as { __dispatchDeps: DispatchDeps })
      .__dispatchDeps;
    const runId = await wf.start(echo, { x: 1 });
    wf.worker({ pollIntervalMs: 5 });
    for (const msg of await queue.dequeue({ count: 32 })) {
      await dispatchMessage(deps, msg);
    }
    expect((await store.loadRunState(runId)).status).toBe("completed");
    expect(queue.ensureSchemaCalls).toBe(1);
  });

  it("constructs and runs end-to-end with a queue that has no ensureSchema hook", async () => {
    // InMemoryQueue implements Queue without ensureSchema; nagi()'s optional
    // call is a no-op and the flow still completes.
    const result = await runFlow(echo, { x: 2 });
    expect(result.status).toBe("completed");
    expect(result.output("step")).toEqual({ y: 2 });
  });

  it("fails nagi() at construction when ensureSchema rejects (fail-fast, error preserved)", async () => {
    await expect(
      nagi({
        flows: [echo],
        store: new InMemoryStore(),
        queue: new BoomQueue(),
      }),
    ).rejects.toThrow(/permission denied to CREATE EXTENSION/);
  });

  it("a rejecting ensureSchema yields no usable runtime — enqueue-before-schema is unreachable", async () => {
    // Because nagi() rejects, the caller never obtains a Wf, so no run can be
    // started and nothing can enqueue against the unprovisioned queue.
    let wf: unknown;
    try {
      wf = await nagi({
        flows: [echo],
        store: new InMemoryStore(),
        queue: new BoomQueue(),
      });
    } catch {
      wf = undefined;
    }
    expect(wf).toBeUndefined();
  });
});
