import type { FlowStartEvent, RunId, StepId } from "@nagi-js/core";
import { SpanKind } from "@opentelemetry/api";
import {
  BasicTracerProvider,
  InMemorySpanExporter,
  type ReadableSpan,
  SimpleSpanProcessor,
} from "@opentelemetry/sdk-trace-base";
import { afterEach, beforeAll, beforeEach, describe, expect, it } from "vitest";
import { stepKey, stepSpanRegistry } from "./context";
import { otelHooks } from "./hooks";

const exporter = new InMemorySpanExporter();
let provider: BasicTracerProvider;

const childRunId = "run-child" as RunId;
const childFlowId = "child-flow";
const parentRunId = "run-parent" as RunId;
const parentStepId = "subflowStep" as StepId;
const at = new Date("2026-05-20T00:00:00Z");

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
    tracer: provider.getTracer("@nagi-js/otel-subflow-linkage-tests"),
    ...opts,
  });
}

function childFlowStart(
  overrides: Partial<FlowStartEvent> = {},
): FlowStartEvent {
  return {
    runId: childRunId,
    flowId: childFlowId,
    at,
    input: { url: "https://example.com" },
    parent: { runId: parentRunId, stepId: parentStepId, attempt: 1 },
    ...overrides,
  };
}

function bySpanName(
  spans: ReadableSpan[],
  name: string,
): ReadableSpan | undefined {
  return spans.find((s) => s.name === name);
}

function registerParentStepSpan(
  tracer = provider.getTracer("@nagi-js/otel-subflow-linkage-tests"),
) {
  const span = tracer.startSpan("step subflowStep", {
    kind: SpanKind.INTERNAL,
    startTime: at,
  });
  stepSpanRegistry.set(stepKey(parentRunId, parentStepId, 1), span);
  return span;
}

describe("otelHooks — subflow span linkage", () => {
  it("nests child flow span under parent step span when event.parent is set and parent step span is registered", async () => {
    const hooks = makeHooks();
    const parentStepSpan = registerParentStepSpan();
    await hooks.onFlowStart!(childFlowStart());
    await hooks.onFlowComplete!({
      runId: childRunId,
      flowId: childFlowId,
      at,
      output: null,
    });
    parentStepSpan.end(at);

    const spans = exporter.getFinishedSpans();
    const childSpan = bySpanName(spans, `flow ${childFlowId}`);
    expect(childSpan).toBeDefined();
    expect(childSpan!.parentSpanId).toBe(parentStepSpan.spanContext().spanId);
    expect(childSpan!.spanContext().traceId).toBe(
      parentStepSpan.spanContext().traceId,
    );
  });

  it("falls back to parent flow span when parent step span is not in the registry", async () => {
    const hooks = makeHooks();
    // Open the parent flow span via the same hooks so its ctx lives in
    // flowCtxs, but do NOT register a parent step span.
    await hooks.onFlowStart!({
      runId: parentRunId,
      flowId: "parent-flow",
      at,
      input: null,
    });
    await hooks.onFlowStart!(childFlowStart());
    await hooks.onFlowComplete!({
      runId: childRunId,
      flowId: childFlowId,
      at,
      output: null,
    });
    await hooks.onFlowComplete!({
      runId: parentRunId,
      flowId: "parent-flow",
      at,
      output: null,
    });

    const spans = exporter.getFinishedSpans();
    const parentFlowSpan = bySpanName(spans, "flow parent-flow");
    const childSpan = bySpanName(spans, `flow ${childFlowId}`);
    expect(parentFlowSpan).toBeDefined();
    expect(childSpan).toBeDefined();
    expect(childSpan!.parentSpanId).toBe(parentFlowSpan!.spanContext().spanId);
    expect(childSpan!.spanContext().traceId).toBe(
      parentFlowSpan!.spanContext().traceId,
    );
  });

  it("falls back to OTel active context when neither parent step span nor parent flow context is local", async () => {
    const hooks = makeHooks();
    // No parent step span in registry, no parent flow ctx in flowCtxs:
    // resolveParentContext must fall through to context.active(). Under
    // the default noop context manager that means ROOT_CONTEXT → the
    // child span ends up as a trace root. The crucial assertion is that
    // the hook DOES NOT THROW and the span is exported (graceful
    // degradation, per RFC scenario row 4).
    expect(() => hooks.onFlowStart!(childFlowStart())).not.toThrow();
    await hooks.onFlowComplete!({
      runId: childRunId,
      flowId: childFlowId,
      at,
      output: null,
    });

    const spans = exporter.getFinishedSpans();
    const childSpan = bySpanName(spans, `flow ${childFlowId}`);
    expect(childSpan).toBeDefined();
    // Parent attributes still recorded so cross-process linkage is
    // queryable even when the in-process anchor is missing.
    expect(childSpan!.attributes["nagi.parent.run.id"]).toBe(parentRunId);
  });

  it("starts child flow span as a root when event.parent is undefined", async () => {
    const hooks = makeHooks();
    await hooks.onFlowStart!({
      runId: childRunId,
      flowId: childFlowId,
      at,
      input: null,
    });
    await hooks.onFlowComplete!({
      runId: childRunId,
      flowId: childFlowId,
      at,
      output: null,
    });

    const spans = exporter.getFinishedSpans();
    const childSpan = bySpanName(spans, `flow ${childFlowId}`);
    expect(childSpan).toBeDefined();
    // No active context, no parent → span is a root.
    expect(childSpan!.parentSpanId).toBeUndefined();
  });

  it("records nagi.parent.run.id, nagi.parent.step.id, nagi.parent.step.attempt as attributes when event.parent is set", async () => {
    const hooks = makeHooks();
    // Register the parent step span at attempt 3 so the lookup key actually
    // hits — this proves the attempt threads into stepKey, not just attr 1.
    const parentStepSpan = provider
      .getTracer("@nagi-js/otel-subflow-linkage-tests")
      .startSpan("step subflowStep", {
        kind: SpanKind.INTERNAL,
        startTime: at,
      });
    stepSpanRegistry.set(stepKey(parentRunId, parentStepId, 3), parentStepSpan);
    await hooks.onFlowStart!(
      childFlowStart({
        parent: { runId: parentRunId, stepId: parentStepId, attempt: 3 },
      }),
    );
    await hooks.onFlowComplete!({
      runId: childRunId,
      flowId: childFlowId,
      at,
      output: null,
    });
    parentStepSpan.end(at);

    const spans = exporter.getFinishedSpans();
    const childSpan = bySpanName(spans, `flow ${childFlowId}`);
    expect(childSpan!.attributes).toMatchObject({
      "nagi.parent.run.id": parentRunId,
      "nagi.parent.step.id": parentStepId,
      "nagi.parent.step.attempt": 3,
    });
    // Nesting resolved via the attempt-3 key, confirming attempt is used.
    expect(childSpan!.parentSpanId).toBe(parentStepSpan.spanContext().spanId);
  });

  it("does not record parent attributes when event.parent is undefined", async () => {
    const hooks = makeHooks();
    await hooks.onFlowStart!({
      runId: childRunId,
      flowId: childFlowId,
      at,
      input: null,
    });
    await hooks.onFlowComplete!({
      runId: childRunId,
      flowId: childFlowId,
      at,
      output: null,
    });

    const spans = exporter.getFinishedSpans();
    const childSpan = bySpanName(spans, `flow ${childFlowId}`);
    expect(childSpan!.attributes["nagi.parent.run.id"]).toBeUndefined();
    expect(childSpan!.attributes["nagi.parent.step.id"]).toBeUndefined();
    expect(childSpan!.attributes["nagi.parent.step.attempt"]).toBeUndefined();
  });

  it("does not throw when event.parent.runId references a runId that was never registered (cross-process)", async () => {
    const hooks = makeHooks();
    // Neither registry nor flowCtxs has anything for the parent; hook
    // must degrade to context.active() instead of throwing.
    expect(() =>
      hooks.onFlowStart!({
        runId: childRunId,
        flowId: childFlowId,
        at,
        input: null,
        parent: {
          runId: "run-unknown-parent" as RunId,
          stepId: "ghostStep" as StepId,
          attempt: 1,
        },
      }),
    ).not.toThrow();
    await hooks.onFlowComplete!({
      runId: childRunId,
      flowId: childFlowId,
      at,
      output: null,
    });

    const spans = exporter.getFinishedSpans();
    const childSpan = bySpanName(spans, `flow ${childFlowId}`);
    expect(childSpan).toBeDefined();
    expect(childSpan!.attributes["nagi.parent.run.id"]).toBe(
      "run-unknown-parent",
    );
  });
});
