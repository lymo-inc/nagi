import { describe, expect, it } from "vitest";
import { flow } from "../builder";
import { Facts } from "../facts";
import type { AttemptNumber, QueueMessage, RunId } from "../types";
import { emptySchema, makeHarness } from "./test-helpers";

function twoStep(id: string) {
  return flow({
    id,
    input: emptySchema(),
    build: (b) => {
      const a = b.task({ run: async () => ({ v: 1 }) });
      const bStep = b.task({ needs: { a }, run: async () => ({ v: 2 }) });
      return { a, b: bStep };
    },
  });
}

describe("durable advance after settle", () => {
  it("redelivery of a settled step re-drives the run", async () => {
    const f = twoStep("durable-advance-redeliver");
    const h = await makeHarness(f);
    const runId = await h.wf.start(f, {});

    const [msg] = await h.queue.dequeue({ count: 1 });
    expect(msg?.stepId).toBe("a");

    // Simulate a crash between the step settling and advance() enqueuing the
    // next step: the fact commits, but nothing ever runs advance.
    await h.store.settleStep(
      runId,
      "a",
      Facts.stepCompleted(runId, "a", 1 as AttemptNumber, { v: 1 }, new Date()),
    );

    const stuck = await h.result(runId);
    expect(stuck.status).toBe("running");
    expect(stuck.stepStatus("b")).toBe("pending");

    // Redelivery of the (never acked) message for "a" must re-drive the run.
    await h.queue.nack((msg as QueueMessage).receipt);
    await h.drain();

    const result = await h.result(runId);
    expect(result.status).toBe("completed");
    expect(result.stepStatus("b")).toBe("completed");
    expect(result.factCount("step.completed")).toBe(2);
    expect(result.factCount("flow.completed")).toBe(1);
  });

  it("redelivery on a terminal run is acked without advancing", async () => {
    const f = twoStep("durable-advance-terminal-run");
    const h = await makeHarness(f);
    const runId = await h.wf.start(f, {});

    const [msg] = await h.queue.dequeue({ count: 1 });
    expect(msg?.stepId).toBe("a");

    await h.wf.cancel(runId);

    await h.queue.nack((msg as QueueMessage).receipt);
    await h.drain();

    const result = await h.result(runId);
    expect(result.status).toBe("canceled");
    expect(result.stepStatus("a")).toBe("pending");
    expect((await h.queue.inspect(runId)).length).toBe(0);
  });

  it("ack happens after advance: a throwing advance leaves the message redeliverable", async () => {
    const f = twoStep("durable-advance-ack-after-advance");
    const h = await makeHarness(f);
    const runId = await h.wf.start(f, {});

    // The first state read that sees `a` completed rejects — i.e. interpret()'s
    // advance() throws after the settle fact is written but before the ack.
    let fired = false;
    const loadRunState = h.store.loadRunState.bind(h.store);
    h.store.loadRunState = async (id: RunId) => {
      const s = await loadRunState(id);
      if (!fired && s.steps["a"]?.tag === "completed") {
        fired = true;
        throw new Error("flaky");
      }
      return s;
    };

    // One step through the real worker: the dispatch throws, the worker nacks.
    expect(await h.drainOnce(1)).toBe(1);
    expect(fired).toBe(true);

    // Not acked: the message is back in the queue, redeliverable.
    expect((await h.queue.inspect(runId)).length).toBe(1);

    // Redelivery takes the recover path and re-drives the run to completion.
    await h.drain();

    expect((await h.result(runId)).status).toBe("completed");
  });
});
