import { describe, expect, it } from "vitest";
import { flow } from "../builder";
import { Facts } from "../facts";
import type { AttemptNumber, QueueMessage, RunId } from "../types";
import { emptySchema, makeHarness } from "./test-helpers";

// root fans out to four branches that join again: while one branch runs, the
// others sit pending and already queued, so every advance re-enqueues them.
function fanOut(id: string) {
  return flow({
    id,
    input: emptySchema(),
    build: (b) => {
      const root = b.task({ run: async () => ({ v: 0 }) });
      const b1 = b.task({ needs: { root }, run: async () => ({ v: 1 }) });
      const b2 = b.task({ needs: { root }, run: async () => ({ v: 2 }) });
      const b3 = b.task({ needs: { root }, run: async () => ({ v: 3 }) });
      const b4 = b.task({ needs: { root }, run: async () => ({ v: 4 }) });
      const join = b.task({
        needs: { b1, b2, b3, b4 },
        run: async () => ({ v: 5 }),
      });
      return { root, b1, b2, b3, b4, join };
    },
  });
}

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
    await h.store.appendFact(
      runId,
      Facts.stepStarted(runId, "a", 1 as AttemptNumber, "task", new Date()),
    );
    await h.store.appendFact(
      runId,
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

  it("a first delivery of an already-settled step is acked without re-driving", async () => {
    const f = twoStep("durable-advance-duplicate");
    const h = await makeHarness(f);
    const runId = await h.wf.start(f, {});

    const [msg] = await h.queue.dequeue({ count: 1 });
    expect(msg?.stepId).toBe("a");
    await h.store.appendFact(
      runId,
      Facts.stepStarted(runId, "a", 1 as AttemptNumber, "task", new Date()),
    );
    await h.store.appendFact(
      runId,
      Facts.stepCompleted(runId, "a", 1 as AttemptNumber, { v: 1 }, new Date()),
    );

    // A duplicate — what advance() enqueues for a step that is already queued
    // — is not a redelivery: only the original message's redelivery re-drives.
    await h.queue.enqueue(runId, "a", { flowId: f.id });
    expect(await h.drain()).toBe(1);
    expect((await h.result(runId)).stepStatus("b")).toBe("pending");
    expect((await h.queue.inspect(runId)).length).toBe(1);

    await h.queue.nack((msg as QueueMessage).receipt);
    await h.drain();
    expect((await h.result(runId)).status).toBe("completed");
  });

  it("duplicates do not multiply on a fan-out drained one message at a time", async () => {
    const f = fanOut("durable-advance-fan-out");
    const h = await makeHarness(f);
    const runId = await h.wf.start(f, {});

    const processed = await h.drain();

    expect((await h.result(runId)).status).toBe("completed");
    // 6 steps, plus one duplicate per branch still queued when an earlier one
    // settles (3 + 2 + 1). A duplicate of a settled step used to re-drive the
    // run, which enqueued every still-queued step again.
    expect(processed).toBe(12);
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
    const worker = h.wf.worker({ concurrency: 1, pollIntervalMs: 5 });
    expect((await worker.runOnce({ maxSteps: 1 })).processed).toBe(1);
    expect(fired).toBe(true);

    // Not acked: the message is back in the queue, redeliverable.
    expect((await h.queue.inspect(runId)).length).toBe(1);

    const deadline = Date.now() + 2000;
    for (;;) {
      const m = (await h.queue.inspect(runId))[0];
      if (m && m.visibleAt.getTime() <= Date.now()) break;
      if (Date.now() > deadline) throw new Error("never became visible");
      await new Promise((r) => setTimeout(r, 10));
    }

    // The redelivery comes after the backoff, takes the recover path and
    // re-drives the run to completion.
    await h.drain();

    expect((await h.result(runId)).status).toBe("completed");
  });
});
