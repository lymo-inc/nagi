import type { AttemptNumber, RunId, StepCtx, StepId } from "@nagi-js/core";
import type { Span } from "@opentelemetry/api";

const SEP = "\x1f";

export function stepKey(
  runId: RunId,
  stepId: StepId,
  attempt: AttemptNumber,
): string {
  return `${runId}${SEP}${stepId}${SEP}${attempt}`;
}

export const stepSpanRegistry = new Map<string, Span>();

export function getStepSpan(
  ctx: Pick<StepCtx<unknown>, "runId" | "stepId" | "attempt">,
): Span | undefined {
  return stepSpanRegistry.get(stepKey(ctx.runId, ctx.stepId, ctx.attempt));
}
