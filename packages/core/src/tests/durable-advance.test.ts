import { describe, expect, it } from "vitest";
import { flow } from "../builder";
import { makeDispatcher } from "../dispatch";
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

    let fired = false;
    const flakyStore = new Proxy(h.store, {
      get(target, prop, recv) {
        if (prop === "loadRunState") {
          return async (id: RunId) => {
            const s = await target.loadRunState(id);
            if (!fired && s.steps["a"]?.tag === "completed") {
              fired = true;
              throw new Error("flaky");
            }
            return s;
          };
        }
        const v = Reflect.get(target, prop, recv);
        return typeof v === "function" ? v.bind(target) : v;
      },
    });
    const dispatcher = makeDispatcher({ ...h.deps, store: flakyStore });

    const [msg] = await h.queue.dequeue({ count: 1 });
    expect(msg?.stepId).toBe("a");
    await expect(
      dispatcher.dispatchMessage(msg as QueueMessage),
    ).rejects.toThrow("flaky");

    // Not acked: interpret()'s advance() threw before the ack ran.
    expect((await h.queue.inspect(runId)).length).toBe(1);

    // Redeliver and let the harness's own (non-flaky) dispatcher drain — the
    // recover path re-drives the run to completion.
    await h.queue.nack((msg as QueueMessage).receipt);
    await h.drain();

    expect((await h.result(runId)).status).toBe("completed");
  });
});
