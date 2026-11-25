import { describe, expect, it, vi } from "vitest";
import { flow } from "../builder";
import { InMemoryClock, InMemoryQueue, InMemoryStore } from "../memory";
import { nagi } from "../runtime";
import { isTerminalRun } from "../state";
import type { FlowErrorEvent, RunId } from "../types";
import { passthroughSchema } from "./test-helpers";

// Lymo (2026-11): both flows with an onError start with
// `if (error.name === "NagiCanceledError") return;`, the runtime-wide
// onFlowError and analysis-run.ts compare the same string, and two handlers
// call wf.describe() and re-parse the input because the event carries only
// the runId.

async function setup(onError: (e: FlowErrorEvent) => void) {
  const flowErrors: FlowErrorEvent[] = [];
  const f = flow({
    id: "flow-error-event",
    input: passthroughSchema<{ proposalId: string; fail: boolean }>(),
    onError: async (e) => {
      // Typed by the flow's input schema.
      const id: string = e.input.proposalId;
      expect(id).toBe("p1");
      onError(e);
    },
    build: (b) => ({
      a: b.task({
        retry: { maxAttempts: 1, backoff: "fixed" },
        run: async ({ input, ctx }) => {
          if (input.fail) throw new Error("boom");
          while (!ctx.signal.aborted)
            await new Promise((r) => setTimeout(r, 5));
          return 1;
        },
      }),
    }),
  });
  const store = new InMemoryStore();
  const wf = await nagi({
    flows: [f],
    store,
    queue: new InMemoryQueue(),
    clock: new InMemoryClock(),
    hooks: { onFlowError: (e) => void flowErrors.push(e) },
  });
  const ac = new AbortController();
  const done = wf.worker({ pollIntervalMs: 5, signal: ac.signal }).run();
  const ended = (runId: RunId) =>
    vi.waitFor(
      async () =>
        expect(isTerminalRun(await store.loadRunState(runId))).toBe(true),
      { timeout: 3_000, interval: 5 },
    );
  const stop = async () => {
    ac.abort();
    await done;
  };
  return { f, wf, store, ended, stop, flowErrors };
}

describe("FlowErrorEvent carries status and input", () => {
  it("a failed run reports status failed and its input", async () => {
    const seen: FlowErrorEvent[] = [];
    const { f, wf, ended, stop, flowErrors } = await setup((e) => seen.push(e));
    try {
      const runId = await wf.start(f, { proposalId: "p1", fail: true });
      await ended(runId);
      await vi.waitFor(() => expect(flowErrors).toHaveLength(1));
    } finally {
      await stop();
    }
    for (const e of [seen[0], flowErrors[0]]) {
      expect(e?.status).toBe("failed");
      expect(e?.input).toEqual({ proposalId: "p1", fail: true });
    }
  });

  it("a canceled run reports status canceled and its input", async () => {
    const seen: FlowErrorEvent[] = [];
    const { f, wf, store, ended, stop, flowErrors } = await setup((e) =>
      seen.push(e),
    );
    try {
      const runId = await wf.start(f, { proposalId: "p1", fail: false });
      await vi.waitFor(async () =>
        expect((await store.loadRunState(runId)).steps["a"]?.tag).toBe(
          "running",
        ),
      );
      await wf.cancel(runId, { reason: "user" });
      await ended(runId);
      await vi.waitFor(() => expect(flowErrors).toHaveLength(1));
    } finally {
      await stop();
    }
    for (const e of [seen[0], flowErrors[0]]) {
      expect(e?.status).toBe("canceled");
      expect(e?.error.name).toBe("NagiCanceledError");
      expect(e?.input).toEqual({ proposalId: "p1", fail: false });
    }
  });
});
