import type {
  FlowCompleteEvent,
  FlowErrorEvent,
  FlowStartEvent,
  RunId,
  SerializedError,
  SignalReceivedEvent,
  StepCompleteEvent,
  StepErrorEvent,
  StepRetryEvent,
  StepStartEvent,
} from "@nagi-js/core";
import { SpanKind, SpanStatusCode } from "@opentelemetry/api";
import {
  BasicTracerProvider,
  InMemorySpanExporter,
  type ReadableSpan,
  SimpleSpanProcessor,
} from "@opentelemetry/sdk-trace-base";
import { afterEach, beforeAll, beforeEach, describe, expect, it } from "vitest";
import { stepSpanRegistry } from "./context";
import { otelHooks } from "./hooks";

const exporter = new InMemorySpanExporter();
let provider: BasicTracerProvider;

const runId = "run-test" as RunId;
const flowId = "flow-test";
const at = new Date("2026-05-12T00:00:00Z");

const fakeError: SerializedError = {
  name: "TestError",
  message: "boom",
  stack: "TestError: boom\n    at test",
};

beforeAll(() => {
  provider = new BasicTracerProvider({
    spanProcessors: [new SimpleSpanProcessor(exporter)],
  });
});

beforeEach(() => {
  exporter.reset();
  stepSpanRegistry.clear();
});

afterEach(() => {
  exporter.reset();
  stepSpanRegistry.clear();
});

function makeHooks(opts: Parameters<typeof otelHooks>[0] = {}) {
  return otelHooks({
    tracer: provider.getTracer("@nagi-js/otel-tests"),
    ...opts,
  });
}

function flowStart(overrides: Partial<FlowStartEvent> = {}): FlowStartEvent {
  return { runId, flowId, at, input: { x: 1 }, ...overrides };
}

function flowComplete(
  overrides: Partial<FlowCompleteEvent> = {},
): FlowCompleteEvent {
  return { runId, flowId, at, output: null, ...overrides };
}

function flowError(overrides: Partial<FlowErrorEvent> = {}): FlowErrorEvent {
  return { runId, flowId, at, error: fakeError, ...overrides };
}

function stepStart(overrides: Partial<StepStartEvent> = {}): StepStartEvent {
  return {
    runId,
    flowId,
    at,
    stepId: "stepA",
    attempt: 1,
    kind: "task",
    input: null,
    ...overrides,
  };
}

function stepComplete(
  overrides: Partial<StepCompleteEvent> = {},
): StepCompleteEvent {
  return {
    runId,
    flowId,
    at,
    stepId: "stepA",
    attempt: 1,
    kind: "task",
    output: { ok: true },
    durationMs: 25,
    ...overrides,
  };
}

function stepError(overrides: Partial<StepErrorEvent> = {}): StepErrorEvent {
  return {
    runId,
    flowId,
    at,
    stepId: "stepA",
    attempt: 1,
    kind: "task",
    error: fakeError,
    ...overrides,
  };
}

function stepRetry(overrides: Partial<StepRetryEvent> = {}): StepRetryEvent {
  return {
    runId,
    flowId,
    at,
    stepId: "stepA",
    attempt: 1,
    kind: "task",
    error: fakeError,
    nextAttemptAt: new Date(at.getTime() + 500),
    ...overrides,
  };
}

function signalReceived(
  overrides: Partial<SignalReceivedEvent> = {},
): SignalReceivedEvent {
  return {
    runId,
    flowId,
    at,
    stepId: "sigA",
    attempt: 1,
    kind: "signal",
    payload: { name: "go" },
    ...overrides,
  };
}

function bySpanName(
  spans: ReadableSpan[],
  name: string,
): ReadableSpan | undefined {
  return spans.find((s) => s.name === name);
}

describe("otelHooks — happy path", () => {
  it("emits a flow span and a step span on a single-task run", async () => {
    const hooks = makeHooks();
    await hooks.onFlowStart!(flowStart());
    await hooks.onStepStart!(stepStart());
    await hooks.onStepComplete!(stepComplete());
    await hooks.onFlowComplete!(flowComplete());

    const spans = exporter.getFinishedSpans();
    expect(spans.length).toBe(2);

    const stepSpan = bySpanName(spans, "step stepA");
    const flowSpan = bySpanName(spans, "flow flow-test");
    expect(stepSpan).toBeDefined();
    expect(flowSpan).toBeDefined();

    // Step is a child of flow (parent-span-id matches flow's span id).
    expect(stepSpan!.parentSpanId).toBe(flowSpan!.spanContext().spanId);
    // Both share a trace id.
    expect(stepSpan!.spanContext().traceId).toBe(
      flowSpan!.spanContext().traceId,
    );
  });

  it("stamps the documented nagi.* attributes on step spans", async () => {
    const hooks = makeHooks();
    await hooks.onFlowStart!(flowStart());
    await hooks.onStepStart!(stepStart());
    await hooks.onStepComplete!(stepComplete());
    await hooks.onFlowComplete!(flowComplete());

    const stepSpan = bySpanName(exporter.getFinishedSpans(), "step stepA");
    expect(stepSpan!.attributes).toMatchObject({
      "nagi.flow.id": flowId,
      "nagi.run.id": runId,
      "nagi.step.id": "stepA",
      "nagi.step.attempt": 1,
      "nagi.step.kind": "task",
      "nagi.step.duration_ms": 25,
    });
    expect(stepSpan!.kind).toBe(SpanKind.INTERNAL);
  });

  it("merges defaultAttributes onto every span", async () => {
    const hooks = makeHooks({
      defaultAttributes: { "deployment.environment": "test" },
    });
    await hooks.onFlowStart!(flowStart());
    await hooks.onStepStart!(stepStart());
    await hooks.onStepComplete!(stepComplete());
    await hooks.onFlowComplete!(flowComplete());

    for (const s of exporter.getFinishedSpans()) {
      expect(s.attributes["deployment.environment"]).toBe("test");
    }
  });

  it("leaves status UNSET on success (per OTel spec)", async () => {
    const hooks = makeHooks();
    await hooks.onFlowStart!(flowStart());
    await hooks.onStepStart!(stepStart());
    await hooks.onStepComplete!(stepComplete());
    await hooks.onFlowComplete!(flowComplete());

    for (const s of exporter.getFinishedSpans()) {
      expect(s.status.code).toBe(SpanStatusCode.UNSET);
    }
  });
});

describe("otelHooks — errors", () => {
  it("records exception + ERROR status on step error", async () => {
    const hooks = makeHooks();
    await hooks.onFlowStart!(flowStart());
    await hooks.onStepStart!(stepStart());
    await hooks.onStepError!(stepError());
    await hooks.onFlowError!(flowError());

    const stepSpan = bySpanName(exporter.getFinishedSpans(), "step stepA");
    expect(stepSpan!.status.code).toBe(SpanStatusCode.ERROR);
    expect(stepSpan!.status.message).toBe(fakeError.message);
    expect(stepSpan!.attributes["error.type"]).toBe(fakeError.name);
    expect(stepSpan!.events.some((e) => e.name === "exception")).toBe(true);
  });

  it("records exception + ERROR status on flow error", async () => {
    const hooks = makeHooks();
    await hooks.onFlowStart!(flowStart());
    await hooks.onFlowError!(flowError());

    const flowSpan = bySpanName(exporter.getFinishedSpans(), "flow flow-test");
    expect(flowSpan!.status.code).toBe(SpanStatusCode.ERROR);
    expect(flowSpan!.events.some((e) => e.name === "exception")).toBe(true);
  });
});

describe("otelHooks — retry", () => {
  it("ends the failed attempt and opens a fresh sibling for attempt+1", async () => {
    const hooks = makeHooks();
    await hooks.onFlowStart!(flowStart());
    await hooks.onStepStart!(stepStart({ attempt: 1 }));
    await hooks.onStepRetry!(stepRetry({ attempt: 1 }));
    await hooks.onStepStart!(stepStart({ attempt: 2 }));
    await hooks.onStepComplete!(stepComplete({ attempt: 2 }));
    await hooks.onFlowComplete!(flowComplete());

    const stepSpans = exporter
      .getFinishedSpans()
      .filter((s) => s.name === "step stepA");
    expect(stepSpans.length).toBe(2);

    const a1 = stepSpans.find((s) => s.attributes["nagi.step.attempt"] === 1)!;
    const a2 = stepSpans.find((s) => s.attributes["nagi.step.attempt"] === 2)!;

    expect(a1.status.code).toBe(SpanStatusCode.ERROR);
    expect(a2.status.code).toBe(SpanStatusCode.UNSET);

    // Both siblings have the same flow-span parent.
    expect(a1.parentSpanId).toBe(a2.parentSpanId);

    const flowSpan = bySpanName(exporter.getFinishedSpans(), "flow flow-test")!;
    const retryEvent = flowSpan.events.find(
      (e) => e.name === "nagi.retry.scheduled",
    );
    expect(retryEvent).toBeDefined();
    expect(retryEvent!.attributes!["nagi.step.id"]).toBe("stepA");
    expect(retryEvent!.attributes!["nagi.step.attempt"]).toBe(1);
  });
});

describe("otelHooks — signal received", () => {
  it("ends the waiting signal step span and records a payload-presence event", async () => {
    const hooks = makeHooks();
    await hooks.onFlowStart!(flowStart());
    await hooks.onStepStart!(stepStart({ stepId: "sigA", kind: "signal" }));
    await hooks.onSignalReceived!(signalReceived());
    await hooks.onFlowComplete!(flowComplete());

    const sigSpan = bySpanName(exporter.getFinishedSpans(), "step sigA");
    expect(sigSpan).toBeDefined();
    expect(sigSpan!.events.some((e) => e.name === "nagi.signal.received")).toBe(
      true,
    );
  });
});

describe("otelHooks — resilience", () => {
  it("does not throw when onFlowComplete fires without a preceding onFlowStart", () => {
    // Out-of-order events can happen if a hook subscription is added mid-run.
    // The adapter must absorb this rather than crash the workflow.
    const hooks = makeHooks();
    expect(() => hooks.onFlowComplete!(flowComplete())).not.toThrow();
    expect(() => hooks.onFlowError!(flowError())).not.toThrow();
    expect(() => hooks.onStepComplete!(stepComplete())).not.toThrow();
    expect(exporter.getFinishedSpans().length).toBe(0);
  });
});
