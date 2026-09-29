import { describe, expect, it } from "vitest";
import { flow } from "../builder";
import type { AttemptNumber, RunId } from "../types";
import { makeFixture } from "./run-lifecycle.test";
import { makeHarness, passthroughSchema } from "./test-helpers";

interface VideoInput {
  readonly videoId: string;
}

const seedFlow = flow({
  id: "seed-plain",
  input: passthroughSchema<VideoInput>(),
  build: (b) => ({
    analyze: b.task({ run: async ({ input }) => ({ ok: input.videoId }) }),
  }),
});

describe("wf.start — its first steps are queued inside the start transaction", () => {
  it("a start whose post-commit effects never run still has its first step queued", async () => {
    const fx = await makeFixture([seedFlow]);
    const runId = "run-1" as RunId;

    // Only stage: applyEffects is deliberately never called, simulating a
    // crash/deploy between the start commit and its post-commit effects.
    const staged = await fx.lifecycle.stage({
      flow: seedFlow,
      validatedInput: { videoId: "v1" },
      runId,
      parent: undefined,
      boundary: { kind: "own" },
    });
    expect(staged.kind).toBe("started");

    const queued = await fx.queue.dequeue({ count: 10 });
    expect(queued.map((m) => m.stepId)).toEqual(["analyze"]);
  });

  it("a subflow re-attach re-seeds a child whose first message was lost", async () => {
    const child = flow({
      id: "seed-child",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => ({
        work: b.task({ run: async () => ({ ok: true }) }),
      }),
    });
    const parent = flow({
      id: "seed-parent",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => ({
        sub: b.subflow(child, { input: () => ({}) }),
      }),
    });

    const h = await makeHarness([parent, child]);
    const parentRunId = await h.wf.start(parent, {});

    // Drain only the parent's first message: "sub" starts the child on its
    // own transaction, seeding the child's root step atomically.
    await h.drainOnce();

    const children = await h.store.listChildren(parentRunId);
    expect(children).toHaveLength(1);
    const childRunId = children[0];
    if (childRunId === undefined) throw new Error("unreachable");

    // Dequeue and ack the child's message without processing it — the loss.
    const lost = await h.queue.dequeue({ count: 10 });
    expect(lost.map((m) => m.stepId)).toEqual(["work"]);
    const msg = lost[0];
    if (msg === undefined) throw new Error("unreachable");
    await h.queue.ack(msg.receipt);

    // Re-dispatch the parent's subflow step, as a redelivery would.
    await h.queue.enqueue(parentRunId, "sub", {
      attempt: 2 as AttemptNumber,
      flowId: parent.id,
    });

    await h.drain();

    const result = await h.result(parentRunId);
    expect(result.status).toBe("completed");
  });
});
