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
  trace,
  type Tracer,
} from "@opentelemetry/api";
import { stepKey, stepSpanRegistry } from "./context";

const TRACER_NAME = "@nagi-js/otel";
const TRACER_VERSION = "0.0.0";

const DEFAULT_FLOW_PREFIX = "flow";
const DEFAULT_STEP_PREFIX = "step";

export interface OtelHooksOpts {
  /**
   * Custom `Tracer`. Defaults to `trace.getTracer("@nagi-js/otel", "0.0.0")` —
   * a no-op when no SDK is registered (per the OTel API contract).
   */
  readonly tracer?: Tracer;
  /** Override `"flow"`/`"step"` span-name prefixes (low cardinality; do NOT use IDs). */
  readonly spanNamePrefix?: { readonly flow?: string; readonly step?: string };
  /** Extra attributes stamped on every span (e.g. `deployment.environment`). */
  readonly defaultAttributes?: Attributes;
}

/**
 * `FlowHooks` adapter that maps Nagi lifecycle events to OpenTelemetry spans.
 *
 * Hierarchy: one `flow {flowId}` span per run; per-step-attempt sibling `step
 * {stepId}` spans underneath. All spans are `INTERNAL` kind. Errors call
 * `span.recordException` + `setStatus(ERROR)`; success leaves status `UNSET`
 * per the Tracing API spec recommendation for instrumentation libraries.
 *
 * Adapter errors are swallowed: a misconfigured tracer must never crash a
 * workflow. Throws are logged via `console.error`.
 */
export function otelHooks(opts: OtelHooksOpts = {}): FlowHooks {
  const tracer: Tracer = opts.tracer ?? trace.getTracer(TRACER_NAME, TRACER_VERSION);
  const flowPrefix = opts.spanNamePrefix?.flow ?? DEFAULT_FLOW_PREFIX;
  const stepPrefix = opts.spanNamePrefix?.step ?? DEFAULT_STEP_PREFIX;
  const baseAttrs: Attributes = opts.defaultAttributes ?? {};

  // Per-run flow-span context. Used as the parent for step spans.
  const flowCtxs = new Map<RunId, Context>();
  // Per-step match-duration tracking: dispatch.ts hard-codes durationMs=0 on
  // match completion, so we compute it ourselves from the stashed start time.
  const stepStartTimes = new Map<string, Date>();

  function withGuard<E>(name: string, fn: (event: E) => void): (event: E) => void {
    return (event: E) => {
      try {
        fn(event);
      } catch (err) {
        console.error(`[@nagi-js/otel] ${name} hook failed`, err);
      }
    };
  }

  function flowAttrs(event: { readonly runId: RunId; readonly flowId: string }): Attributes {
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
    // Match steps get a hard-coded durationMs=0 from the runtime; recover the
    // real value from the stashed start time when available.
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
      // End the failed attempt's span with ERROR status. The next onStepStart
      // for attempt+1 opens a fresh sibling span under the same flow span.
      const span = consumeStepSpan(event.runId, event.stepId, event.attempt);
      if (span) endStepSpanErr(span, event.error, event.at);

      // Surface the retry on the flow span so users can see retry latency in
      // the run-level trace without inspecting individual step attempts.
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

    // onSignalSent is declared in core/types.ts but never fired by the runtime;
    // intentionally omitted until core wires a sender path.
    onSignalReceived: withGuard<SignalReceivedEvent>("onSignalReceived", (event) => {
      const span = consumeStepSpan(event.runId, event.stepId, event.attempt);
      if (!span) return;
      span.addEvent(
        "nagi.signal.received",
        { "nagi.signal.payload_present": event.payload !== null },
        event.at,
      );
      span.setAttribute("nagi.step.duration_ms", 0);
      span.end(event.at);
    }),
  };
}

function toError(err: SerializedError): Error {
  // `recordException` accepts `{ name, message, stack }`. We rebuild a
  // shape-compatible object rather than a real Error to preserve the original
  // name/stack exactly.
  const e = new Error(err.message);
  e.name = err.name;
  if (err.stack !== undefined) e.stack = err.stack;
  return e;
}

