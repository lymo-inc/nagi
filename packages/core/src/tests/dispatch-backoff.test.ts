import { describe, expect, it } from "vitest";
import { flow } from "../builder";
import type { RunId } from "../types";
import { makeHarness, passthroughSchema, spyOnLog } from "./test-helpers";

const echo = flow({
  id: "echo",
  input: passthroughSchema<{ x: number }>(),
  build: (b) => ({
    step: b.task({ run: async ({ input }) => ({ y: input.x }) }),
  }),
});

const MSG = "worker.dispatch threw uncaught";
const QUIET = { timerSweepIntervalMs: 0, reaperIntervalMs: 0 } as const;

async function setup() {
  const { onLog, entries } = spyOnLog();
  const h = await makeHarness(echo, { onLog });
  const runId = "run-ghost-unregistered" as RunId;
  await h.store.appendFact(runId, {
    kind: "flow.started",
    runId,
    flowId: "not-registered-anywhere",
    input: null,
    at: new Date(),
  });
  await h.queue.enqueue(runId, "whatever");
  const uncaught = () => entries.filter((e) => e.msg === MSG);
  return { h, runId, uncaught };
}

describe("dispatch that throws backs off", () => {
  it("a dispatch that throws is nacked with a delay from readCount", async () => {
    const { h, runId, uncaught } = await setup();
    const worker = h.wf.worker({
      concurrency: 1,
      pollIntervalMs: 200,
      ...QUIET,
    });
    await worker.runOnce({ maxSteps: 1 });

    const msg = (await h.queue.inspect(runId))[0];
    expect(msg?.readCount).toBe(1);
    const wait = (msg?.visibleAt.getTime() ?? 0) - Date.now();
    expect(wait).toBeGreaterThan(100);
    expect(wait).toBeLessThanOrEqual(250);

    expect(uncaught()).toHaveLength(1);
    expect(uncaught()[0]?.attrs).toMatchObject({
      runId,
      stepId: "whatever",
      readCount: 1,
      delayMs: 200,
      error: expect.any(String),
    });
  });

  it("the delay doubles per delivery", async () => {
    const { h, runId, uncaught } = await setup();
    const worker = h.wf.worker({
      concurrency: 1,
      pollIntervalMs: 200,
      ...QUIET,
    });
    await worker.runOnce({ maxSteps: 1 });

    const deadline = Date.now() + 2000;
    for (;;) {
      const m = (await h.queue.inspect(runId))[0];
      if (m && m.visibleAt.getTime() <= Date.now()) break;
      if (Date.now() > deadline) throw new Error("never became visible");
      await new Promise((r) => setTimeout(r, 10));
    }

    await worker.runOnce({ maxSteps: 1 });
    expect(uncaught()).toHaveLength(2);
    expect(uncaught()[1]?.attrs).toMatchObject({ readCount: 2, delayMs: 400 });
  });

  it("a persistent throw does not hot-loop the worker", async () => {
    const { h, uncaught } = await setup();
    const worker = h.startWorker({ pollIntervalMs: 5, ...QUIET });
    await new Promise((r) => setTimeout(r, 400));
    await worker.stop();
    expect(uncaught().length).toBeLessThanOrEqual(5);
  });
});
