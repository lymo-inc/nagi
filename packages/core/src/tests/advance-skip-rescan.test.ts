import { describe, expect, it } from "vitest";
import { flow, optional } from "../builder";
import type { Tx } from "../types";
import { makeHarness, passthroughSchema } from "./test-helpers";

// Fake Tx marker — InMemoryStore ignores it (no real tx), but threads it
// through so the queue.withTx path receives the same reference.
const FAKE_TX = { __fakeTx: true } as unknown as Tx;

function gatedFlow() {
  return flow({
    id: "advance-skip-rescan",
    input: passthroughSchema<Record<string, never>>(),
    build: (b) => {
      const slow = b.signal({
        timeoutMs: "unbounded" as const,
        schema: passthroughSchema<Record<string, never>>(),
      });
      const gate = b.task({ when: () => false, run: async () => ({ v: 1 }) });
      const after = b.task({
        needs: { g: optional(gate) },
        run: async ({ needs }) => ({ saw: needs.g }),
      });
      return { slow, gate, after };
    },
  });
}

describe("advance re-scans after recording skips", () => {
  it("a skip unblocks an optional() dependent without waiting for a sibling", async () => {
    const f = gatedFlow();
    const h = await makeHarness(f);
    const runId = await h.wf.start(f, {});
    await h.drain();

    const result = await h.result(runId);
    expect(result.stepStatus("slow")).toBe("running");
    expect(result.stepStatus("gate")).toBe("skipped");
    expect(result.stepStatus("after")).toBe("completed");
    expect(result.output("after")).toEqual({ saw: { tag: "skipped" } });
  });

  it("staged start: a post-commit skip unblocks an optional() dependent", async () => {
    const f = gatedFlow();
    const h = await makeHarness(f);
    const res = await h.wf.startStaged(f, {}, { tx: FAKE_TX });
    await res.applyOnCommit();
    await h.drain();

    const result = await h.result(res.runId);
    expect(result.stepStatus("slow")).toBe("running");
    expect(result.stepStatus("gate")).toBe("skipped");
    expect(result.stepStatus("after")).toBe("completed");
    expect(result.output("after")).toEqual({ saw: { tag: "skipped" } });
  });
});
