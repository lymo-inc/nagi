import type { AttemptNumber, RunId, StepId } from "@nagi-js/core";
import type { Span } from "@opentelemetry/api";
import { BasicTracerProvider } from "@opentelemetry/sdk-trace-base";
import { afterEach, beforeAll, describe, expect, it } from "vitest";
import { getStepSpan, stepKey, stepSpanRegistry } from "./context";

let realSpan: (name: string) => Span;

beforeAll(() => {
  const provider = new BasicTracerProvider();
  const tracer = provider.getTracer("@nagi-js/otel-tests");
  realSpan = (name: string) => tracer.startSpan(name);
});

interface CtxLike {
  readonly runId: RunId;
  readonly stepId: StepId;
  readonly attempt: AttemptNumber;
}

function ctx(overrides: Partial<CtxLike> = {}): CtxLike {
  return {
    runId: "run-1" as RunId,
    stepId: "stepA",
    attempt: 1,
    ...overrides,
  };
}

describe("stepKey + stepSpanRegistry + getStepSpan", () => {
  afterEach(() => {
    stepSpanRegistry.clear();
  });

  it("stepKey distinguishes attempts of the same step", () => {
    const k1 = stepKey("run-1" as RunId, "stepA", 1);
    const k2 = stepKey("run-1" as RunId, "stepA", 2);
    expect(k1).not.toBe(k2);
  });

  it("stepKey distinguishes runs with the same stepId/attempt", () => {
    const k1 = stepKey("run-1" as RunId, "stepA", 1);
    const k2 = stepKey("run-2" as RunId, "stepA", 1);
    expect(k1).not.toBe(k2);
  });

  it("getStepSpan returns the registered span for the matching ctx", () => {
    const span = realSpan("step-A-attempt-1");
    stepSpanRegistry.set(stepKey("run-1" as RunId, "stepA", 1), span);
    expect(getStepSpan(ctx())).toBe(span);
  });

  it("getStepSpan returns undefined when no span is registered", () => {
    expect(getStepSpan(ctx())).toBeUndefined();
  });

  it("getStepSpan does not return a span from a different run/step/attempt", () => {
    stepSpanRegistry.set(stepKey("run-1" as RunId, "stepA", 1), realSpan("right"));
    expect(getStepSpan(ctx({ attempt: 2 }))).toBeUndefined();
    expect(getStepSpan(ctx({ stepId: "stepB" }))).toBeUndefined();
    expect(getStepSpan(ctx({ runId: "run-2" as RunId }))).toBeUndefined();
  });
});
