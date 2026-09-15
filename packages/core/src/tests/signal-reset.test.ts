import { describe, expect, it } from "vitest";
import { flow } from "../builder";
import { Facts } from "../facts";
import { foldRun } from "../state";
import type { AttemptNumber, RunId } from "../types";
import { emptySchema, makeHarness, passthroughSchema } from "./test-helpers";

const runId = "run-signal-reset" as RunId;
const at = new Date("2026-01-01T00:00:00Z");
const attempt = 1 as AttemptNumber;

describe("foldRun — buffered signals", () => {
  it("drops the buffer once the signal is received", () => {
    const state = foldRun(runId, [
      Facts.flowStarted({ runId, flowId: "f", input: {}, at }),
      Facts.signalBuffered({ runId, stepId: "s", payload: { n: 1 }, at }),
      Facts.stepStarted(runId, "s", attempt, "signal", at),
      Facts.signalReceived({ runId, stepId: "s", payload: { n: 1 }, at }),
      Facts.stepCompleted(runId, "s", attempt, { n: 1 }, at),
    ]);
    expect(state.bufferedSignals["s"]).toBeUndefined();
  });

  it("drops the buffer on step.reset", () => {
    const state = foldRun(runId, [
      Facts.flowStarted({ runId, flowId: "f", input: {}, at }),
      Facts.signalBuffered({ runId, stepId: "s", payload: { n: 1 }, at }),
      Facts.stepReset({ runId, stepId: "s", at }),
    ]);
    expect(state.bufferedSignals["s"]).toBeUndefined();
    expect(state.steps["s"]?.tag ?? "pending").toBe("pending");
  });
});

function gate() {
  return flow({
    id: "signal-reset-gate",
    input: emptySchema(),
    build: (b) => ({
      answer: b.signal({
        timeoutMs: "unbounded" as const,
        schema: passthroughSchema<{ text: string }>(),
      }),
    }),
    output: (s) => s.answer,
  });
}

describe("signal step reset", () => {
  it("re-parks after operator.retry instead of replaying the old payload, and accepts a new signal", async () => {
    const f = gate();
    const h = await makeHarness(f);
    const runId = await h.wf.start(f, {});
    // Early signal: buffered, then delivered when the step starts awaiting.
    await h.wf.signal(runId, "answer", { text: "first" });
    await h.drain();
    expect((await h.result(runId)).output("answer")).toEqual({ text: "first" });

    await h.wf.operator().retry(runId, "answer", { actor: "ops" });
    await h.drain();

    const parked = await h.store.loadRunState(runId);
    expect(parked.steps["answer"]?.tag).toBe("awaitingSignal"); // not completed
    expect(parked.bufferedSignals["answer"]).toBeUndefined();

    await h.wf.signal(runId, "answer", { text: "second" });
    await h.drain();
    expect((await h.result(runId)).output("answer")).toEqual({
      text: "second",
    });
  });
});
