import type {
  FlowCompleteEvent,
  FlowErrorEvent,
  FlowHooks,
  FlowStartEvent,
  SignalReceivedEvent,
  SignalSentEvent,
  StepCompleteEvent,
  StepErrorEvent,
  StepRetryEvent,
  StepStartEvent,
} from "@nagi-js/core";

/**
 * Fan out a single `FlowHooks` call site to multiple subscribers, awaiting each in
 * declaration order. A throw from one subscriber is logged via `console.error` and
 * does NOT prevent later subscribers (or the workflow runtime) from running.
 *
 * Nagi's runtime accepts a single `hooks` value — this is how users wire
 * `otelHooks()` alongside their own logger / metrics hooks.
 */
export function composeHooks(...hooks: readonly FlowHooks[]): FlowHooks {
  // Build a local FlowHooks-shaped object. Optional fields are only set when at
  // least one subscriber provides them — required by `exactOptionalPropertyTypes`.
  const result: {
    onFlowStart?: (event: FlowStartEvent) => Promise<void>;
    onFlowComplete?: (event: FlowCompleteEvent) => Promise<void>;
    onFlowError?: (event: FlowErrorEvent) => Promise<void>;
    onStepStart?: (event: StepStartEvent) => Promise<void>;
    onStepComplete?: (event: StepCompleteEvent) => Promise<void>;
    onStepError?: (event: StepErrorEvent) => Promise<void>;
    onStepRetry?: (event: StepRetryEvent) => Promise<void>;
    onSignalSent?: (event: SignalSentEvent) => Promise<void>;
    onSignalReceived?: (event: SignalReceivedEvent) => Promise<void>;
  } = {};

  const fs = pick(hooks, "onFlowStart");
  if (fs.length > 0) result.onFlowStart = fanout("onFlowStart", fs);

  const fc = pick(hooks, "onFlowComplete");
  if (fc.length > 0) result.onFlowComplete = fanout("onFlowComplete", fc);

  const fe = pick(hooks, "onFlowError");
  if (fe.length > 0) result.onFlowError = fanout("onFlowError", fe);

  const ss = pick(hooks, "onStepStart");
  if (ss.length > 0) result.onStepStart = fanout("onStepStart", ss);

  const sc = pick(hooks, "onStepComplete");
  if (sc.length > 0) result.onStepComplete = fanout("onStepComplete", sc);

  const se = pick(hooks, "onStepError");
  if (se.length > 0) result.onStepError = fanout("onStepError", se);

  const sr = pick(hooks, "onStepRetry");
  if (sr.length > 0) result.onStepRetry = fanout("onStepRetry", sr);

  const sigS = pick(hooks, "onSignalSent");
  if (sigS.length > 0) result.onSignalSent = fanout("onSignalSent", sigS);

  const sigR = pick(hooks, "onSignalReceived");
  if (sigR.length > 0)
    result.onSignalReceived = fanout("onSignalReceived", sigR);

  return result;
}

function pick<K extends keyof FlowHooks>(
  hooks: readonly FlowHooks[],
  name: K,
): Array<NonNullable<FlowHooks[K]>> {
  const out: Array<NonNullable<FlowHooks[K]>> = [];
  for (const h of hooks) {
    const fn = h[name];
    if (fn !== undefined) out.push(fn);
  }
  return out;
}

function fanout<E>(
  name: string,
  subs: ReadonlyArray<(event: E) => void | Promise<void>>,
): (event: E) => Promise<void> {
  return async (event: E) => {
    for (const fn of subs) {
      try {
        await fn(event);
      } catch (err) {
        console.error(
          `[@nagi-js/otel] composeHooks: ${name} subscriber threw`,
          err,
        );
      }
    }
  };
}
