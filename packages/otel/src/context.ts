import type { AttemptNumber, RunId, StepCtx, StepId } from "@nagi-js/core";
import type { Span } from "@opentelemetry/api";

/**
 * ASCII unit separator. Composite-key delimiter for `(runId, stepId, attempt)`.
 * Unambiguous because `runId` is `run-<uuid>` and `stepId` is a dotted identifier;
 * neither contains control chars.
 */
const SEP = "\x1f";

export function stepKey(
  runId: RunId,
  stepId: StepId,
  attempt: AttemptNumber,
): string {
  return `${runId}${SEP}${stepId}${SEP}${attempt}`;
}

/**
 * Process-global registry of in-flight step spans, keyed by `(runId, stepId, attempt)`.
 * Populated by `otelHooks()` on `onStepStart`; cleared on completion / error / retry.
 *
 * Single-process, single-runtime: Nagi already assumes a runtime per process, so a
 * module-level Map is the right granularity. Bounded by steps-in-flight, which the
 * runtime itself bounds.
 */
export const stepSpanRegistry = new Map<string, Span>();

/**
 * Returns the OTel `Span` tracking the current step attempt, or `undefined` when
 * no `otelHooks()` is wired or the step is not in-flight (e.g. running under a
 * different `nagi()` instance, or after the step has completed).
 *
 * Useful inside user handlers to add custom attributes or open child spans:
 *
 * ```ts
 * b.task({
 *   run: async (ctx) => {
 *     const span = getStepSpan(ctx);
 *     span?.setAttribute("my.custom.attr", 42);
 *     ...
 *   },
 * });
 * ```
 *
 * Note: this span is NOT installed as the active context — `trace.getActiveSpan()`
 * inside the handler returns the caller's context, not this span.
 */
export function getStepSpan(
  ctx: Pick<StepCtx<unknown>, "runId" | "stepId" | "attempt">,
): Span | undefined {
  return stepSpanRegistry.get(stepKey(ctx.runId, ctx.stepId, ctx.attempt));
}
