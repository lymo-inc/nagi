import type {
  AttemptNumber,
  FlowCompleteEvent,
  FlowErrorEvent,
  FlowHooks,
  FlowStartEvent,
  RunId,
  SerializedError,
  SignalReceivedEvent,
  StepCompleteEvent,
  StepErrorEvent,
  StepEvent,
  StepId,
  StepRetryEvent,
  StepStartEvent,
} from "@nagi-js/core";
import {
  type Attributes,
  type Context,
  context,
  type Span,
  SpanKind,
  SpanStatusCode,
  type Tracer,
  trace,
} from "@opentelemetry/api";
import { stepKey, stepSpanRegistry } from "./context";

const TRACER_NAME = "@nagi-js/otel";
const TRACER_VERSION = "0.0.0";

const DEFAULT_FLOW_PREFIX = "flow";
const DEFAULT_STEP_PREFIX = "step";

export interface OtelHooksOpts {
  readonly tracer?: Tracer;
  readonly spanNamePrefix?: { readonly flow?: string; readonly step?: string };
  readonly defaultAttributes?: Attributes;
}

export function otelHooks(opts: OtelHooksOpts = {}): FlowHooks {
  const tracer: Tracer =
    opts.tracer ?? trace.getTracer(TRACER_NAME, TRACER_VERSION);
  const flowPrefix = opts.spanNamePrefix?.flow ?? DEFAULT_FLOW_PREFIX;
  const stepPrefix = opts.spanNamePrefix?.step ?? DEFAULT_STEP_PREFIX;
  const baseAttrs: Attributes = opts.defaultAttributes ?? {};

  const flowCtxs = new Map<RunId, Context>();
  const stepStartTimes = new Map<string, Date>();

  function withGuard<E>(
    name: string,
    fn: (event: E) => void,
  ): (event: E) => void {
    return (event: E) => {
      try {
        fn(event);
      } catch (err) {
        console.error(`[@nagi-js/otel] ${name} hook failed`, err);
      }
    };
  }

  function flowAttrs(event: {
    readonly runId: RunId;
    readonly flowId: string;
  }): Attributes {
    return {
      ...baseAttrs,
      "nagi.flow.id": event.flowId,
      "nagi.run.id": event.runId,
    };
  }

  function stepAttrs(event: StepEvent): Attributes {
    return {
      ...flowAttrs(event),
      "nagi.step.id": event.stepId,
      "nagi.step.attempt": event.attempt,
      "nagi.step.kind": event.kind,
    };
  }

  function startStepSpan(event: StepStartEvent): Span {
    const parentCtx = flowCtxs.get(event.runId) ?? context.active();
    return tracer.startSpan(
      `${stepPrefix} ${event.stepId}`,
      {
        kind: SpanKind.INTERNAL,
        startTime: event.at,
        attributes: stepAttrs(event),
      },
      parentCtx,
    );
  }

  function endStepSpanOk(event: StepCompleteEvent, span: Span): void {
    const k = stepKey(event.runId, event.stepId, event.attempt);
    const startedAt = stepStartTimes.get(k);
    const durationMs =
      event.kind === "match" && startedAt
        ? event.at.getTime() - startedAt.getTime()
        : event.durationMs;
    span.setAttribute("nagi.step.duration_ms", durationMs);
    span.end(event.at);
  }

  function endStepSpanErr(
    span: Span,
    error: SerializedError,
    endAt: Date,
  ): void {
    span.recordException(toError(error));
    span.setAttribute("error.type", error.name);
    span.setStatus({ code: SpanStatusCode.ERROR, message: error.message });
    span.end(endAt);
  }

  function consumeStepSpan(
    runId: RunId,
    stepId: StepId,
    attempt: AttemptNumber,
  ): Span | undefined {
    const k = stepKey(runId, stepId, attempt);
    const span = stepSpanRegistry.get(k);
    stepSpanRegistry.delete(k);
    stepStartTimes.delete(k);
    return span;
  }

  return {
    onFlowStart: withGuard<FlowStartEvent>("onFlowStart", (event) => {
      const span = tracer.startSpan(`${flowPrefix} ${event.flowId}`, {
        kind: SpanKind.INTERNAL,
        startTime: event.at,
        attributes: flowAttrs(event),
      });
      flowCtxs.set(event.runId, trace.setSpan(context.active(), span));
    }),

    onFlowComplete: withGuard<FlowCompleteEvent>("onFlowComplete", (event) => {
      const ctx = flowCtxs.get(event.runId);
      flowCtxs.delete(event.runId);
      const span = ctx ? trace.getSpan(ctx) : undefined;
      span?.end(event.at);
    }),

    onFlowError: withGuard<FlowErrorEvent>("onFlowError", (event) => {
      const ctx = flowCtxs.get(event.runId);
      flowCtxs.delete(event.runId);
      const span = ctx ? trace.getSpan(ctx) : undefined;
      if (span) endStepSpanErr(span, event.error, event.at);
    }),

    onStepStart: withGuard<StepStartEvent>("onStepStart", (event) => {
      const span = startStepSpan(event);
      const k = stepKey(event.runId, event.stepId, event.attempt);
      stepSpanRegistry.set(k, span);
      stepStartTimes.set(k, event.at);
    }),

    onStepComplete: withGuard<StepCompleteEvent>("onStepComplete", (event) => {
      const span = consumeStepSpan(event.runId, event.stepId, event.attempt);
      if (span) endStepSpanOk(event, span);
    }),

    onStepError: withGuard<StepErrorEvent>("onStepError", (event) => {
      const span = consumeStepSpan(event.runId, event.stepId, event.attempt);
      if (span) endStepSpanErr(span, event.error, event.at);
    }),

    onStepRetry: withGuard<StepRetryEvent>("onStepRetry", (event) => {
      const span = consumeStepSpan(event.runId, event.stepId, event.attempt);
      if (span) endStepSpanErr(span, event.error, event.at);

      const flowCtx = flowCtxs.get(event.runId);
      const flowSpan = flowCtx ? trace.getSpan(flowCtx) : undefined;
      flowSpan?.addEvent(
        "nagi.retry.scheduled",
        {
          "nagi.step.id": event.stepId,
          "nagi.step.attempt": event.attempt,
          "nagi.next_attempt_at": event.nextAttemptAt.toISOString(),
        },
        event.at,
      );
    }),

    onSignalReceived: withGuard<SignalReceivedEvent>(
      "onSignalReceived",
      (event) => {
        const span = consumeStepSpan(event.runId, event.stepId, event.attempt);
        if (!span) return;
        span.addEvent(
          "nagi.signal.received",
          { "nagi.signal.payload_present": event.payload !== null },
          event.at,
        );
        span.setAttribute("nagi.step.duration_ms", 0);
        span.end(event.at);
      },
    ),
  };
}

function toError(err: SerializedError): Error {
  const e = new Error(err.message);
  e.name = err.name;
  if (err.stack !== undefined) e.stack = err.stack;
  return e;
}
