import { describe, expect, it } from "vitest";
import { Facts } from "../facts";
import { decideSignal } from "../signals";
import { foldRun } from "../state";
import type {
  Fact,
  RunId,
  SignalBufferedFact,
  StepId,
  StepStartedFact,
} from "../types";

// Audit-pins for decideSignal's pre-awaiting buffering behavior.
// RFC 0014 D2=B: code is already correct; these tests pin regression.

function startedFacts(runId: RunId, stepId: StepId): Fact[] {
  return [
    Facts.flowStarted({
      runId,
      flowId: "f",
      input: {},
      at: new Date(0),
    }),
    Facts.stepStarted(runId, stepId, 1, "signal", new Date(1)),
  ];
}

const runId = "run-decide-signal" as RunId;
const stepId = "transcript" as StepId;
const at = new Date(100);

describe("decideSignal — pre-awaiting state buffering (N7 audit pins)", () => {
  it("[regression] buffers when step state is pending (no step_run row)", () => {
    const runState = foldRun(runId, [
      Facts.flowStarted({ runId, flowId: "f", input: {}, at: new Date(0) }),
    ]);
    const d = decideSignal({
      runState,
      stepId,
      at,
      incoming: { payload: { x: 1 } },
    });
    expect(d.kind).toBe("buffer");
    expect(d.result.tag).toBe("buffered");
    if (d.kind === "buffer") {
      expect(d.fact).not.toBeNull();
    }
  });

  it("[regression] [LYMO-119] buffers when step_run row exists with status='pending'", () => {
    // Pin for run id 62a8ce2e-a58e-49a9-abb1-890a28fe5552: a signal arriving
    // after a step.started fact wrote a row but before the worker entered
    // awaitingSignal. With no step.started fact projected as pending, the
    // step is in the runState.steps map; decideSignal must still buffer.
    // We construct this via a step.abort-requested-then-reset sequence so the
    // map has a non-default entry while the projection sits at pending.
    const stepStarted: StepStartedFact = Facts.stepStarted(
      runId,
      stepId,
      1,
      "signal",
      new Date(1),
    );
    const reset = Facts.stepReset({
      runId,
      stepId,
      at: new Date(2),
    });
    const runState = foldRun(runId, [
      Facts.flowStarted({ runId, flowId: "f", input: {}, at: new Date(0) }),
      stepStarted,
      reset,
    ]);
    expect(runState.steps[stepId]?.tag).toBe("pending");
    const d = decideSignal({
      runState,
      stepId,
      at,
      incoming: { payload: { x: 1 } },
    });
    expect(d.kind).toBe("buffer");
    expect(d.result.tag).toBe("buffered");
  });

  it("buffers when step state is running (claimed but pre-awaitingSignal yield)", () => {
    // A non-signal step's facts: started as a task; decideSignal must still buffer.
    const runState = foldRun(runId, [
      Facts.flowStarted({ runId, flowId: "f", input: {}, at: new Date(0) }),
      Facts.stepStarted(runId, stepId, 1, "task", new Date(1)),
    ]);
    expect(runState.steps[stepId]?.tag).toBe("running");
    const d = decideSignal({
      runState,
      stepId,
      at,
      incoming: { payload: { x: 1 } },
    });
    expect(d.kind).toBe("buffer");
    expect(d.result.tag).toBe("buffered");
  });

  it("buffers when step state is backoff", () => {
    const runState = foldRun(runId, [
      Facts.flowStarted({ runId, flowId: "f", input: {}, at: new Date(0) }),
      Facts.stepStarted(runId, stepId, 1, "task", new Date(1)),
      Facts.stepRetried(
        runId,
        stepId,
        1,
        new Date(1000),
        { name: "E", message: "x" },
        new Date(2),
      ),
    ]);
    expect(runState.steps[stepId]?.tag).toBe("backoff");
    const d = decideSignal({
      runState,
      stepId,
      at,
      incoming: { payload: { x: 1 } },
    });
    expect(d.kind).toBe("buffer");
  });

  it("buffers when step state is aborting", () => {
    const runState = foldRun(runId, [
      Facts.flowStarted({ runId, flowId: "f", input: {}, at: new Date(0) }),
      Facts.stepStarted(runId, stepId, 1, "task", new Date(1)),
      Facts.stepAbortRequested({
        runId,
        stepId,
        attempt: 1,
        at: new Date(2),
        actor: "operator",
      }),
    ]);
    expect(runState.steps[stepId]?.tag).toBe("aborting");
    const d = decideSignal({
      runState,
      stepId,
      at,
      incoming: { payload: { x: 1 } },
    });
    expect(d.kind).toBe("buffer");
  });

  it("multi-name set buffers correctly pre-claim", () => {
    // decideSignal doesn't dispatch by name (resolveSignalStep does); but it
    // does carry the signalName through into the buffered fact. Pin the shape.
    const runState = foldRun(runId, [
      Facts.flowStarted({ runId, flowId: "f", input: {}, at: new Date(0) }),
    ]);
    const d = decideSignal({
      runState,
      stepId,
      at,
      incoming: { payload: { x: 1 }, signalName: "audioReady" },
    });
    expect(d.kind).toBe("buffer");
    if (d.kind === "buffer" && d.fact !== null) {
      const fact: SignalBufferedFact = d.fact;
      expect(fact.signalName).toBe("audioReady");
    }
  });

  it("on awaitingSignal state delivers immediately (not buffered)", () => {
    const runState = foldRun(runId, startedFacts(runId, stepId));
    expect(runState.steps[stepId]?.tag).toBe("awaitingSignal");
    const d = decideSignal({
      runState,
      stepId,
      at,
      incoming: { payload: { x: 1 } },
    });
    expect(d.kind).toBe("deliver");
    expect(d.result.tag).toBe("delivered");
  });

  it("on terminal step returns noop (no buffer, no throw)", () => {
    const runState = foldRun(runId, [
      Facts.flowStarted({ runId, flowId: "f", input: {}, at: new Date(0) }),
      Facts.stepStarted(runId, stepId, 1, "signal", new Date(1)),
      Facts.stepCompleted(runId, stepId, 1, { ok: true }, new Date(2)),
    ]);
    expect(runState.steps[stepId]?.tag).toBe("completed");
    const d = decideSignal({
      runState,
      stepId,
      at,
      incoming: { payload: { x: 1 } },
    });
    expect(d.kind).toBe("noop");
    expect(d.result.tag).toBe("noop");
  });
});
