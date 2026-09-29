import { describe, expect, it } from "vitest";
import { flow } from "../builder";
import { streamEndOf } from "../stream-hub";
import type { StepId } from "../types";
import { makeHarness, passthroughSchema } from "./test-helpers";

const gen = "gen" as StepId;

function gatedFlow(id: string, outcome: "return" | "throw") {
  let release: (() => void) | undefined;
  const gate = new Promise<void>((r) => {
    release = r;
  });
  const f = flow({
    id,
    input: passthroughSchema<Record<string, never>>(),
    build: (b) => ({
      gen: b.streamingTask({
        retry: { maxAttempts: 1, backoff: "fixed" },
        run: async ({ ctx }) => {
          await ctx.emit("a");
          await gate;
          if (outcome === "throw") throw new Error("boom");
          return { done: true };
        },
      }),
    }),
  });
  return { f, release: () => release?.() };
}

describe("streamEndOf", () => {
  it("is open while the step runs, then ok once it completes", async () => {
    const { f, release } = gatedFlow("stream-end-ok", "return");
    const h = await makeHarness(f);
    const worker = h.startWorker();
    try {
      const runId = await h.wf.start(f, {});
      await h.waitForStep(runId, "gen", "running");
      expect(streamEndOf(await h.store.loadRunState(runId), gen)).toBe("open");

      release();
      await h.waitForEnd(runId);
      expect(streamEndOf(await h.store.loadRunState(runId), gen)).toBe("ok");
    } finally {
      await worker.stop();
    }
  });

  it("is error once the step has failed for good", async () => {
    const { f, release } = gatedFlow("stream-end-failed", "throw");
    const h = await makeHarness(f);
    const worker = h.startWorker();
    try {
      const runId = await h.wf.start(f, {});
      await h.waitForStep(runId, "gen", "running");
      release();
      const result = await h.waitForEnd(runId);
      expect(result.status).toBe("failed");
      expect(streamEndOf(await h.store.loadRunState(runId), gen)).toBe("error");
    } finally {
      await worker.stop();
    }
  });

  it("is ok when the run is canceled while the step waits", async () => {
    const { f, release } = gatedFlow("stream-end-canceled", "return");
    const h = await makeHarness(f);
    const worker = h.startWorker();
    try {
      const runId = await h.wf.start(f, {});
      await h.waitForStep(runId, "gen", "running");
      await h.wf.cancel(runId);
      expect(streamEndOf(await h.store.loadRunState(runId), gen)).toBe("ok");
      release();
    } finally {
      await worker.stop();
    }
  });
});
