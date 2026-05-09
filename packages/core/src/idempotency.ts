import type { Json, RunId, StepId, Store } from "./types";

/**
 * Stable key generator for external APIs (Stripe, Mux, etc.).
 *
 * Per the boundary contract: `hash(runId + stepId + scope)`. Identical across
 * retries of the same step, different across runs.
 *
 * v0 uses concat (`nagi:<runId>:<stepId>:<scope>`) — sync, debuggable,
 * Stripe-friendly (under the 255 char limit). Switch to `crypto.subtle`
 * SHA-256 if compactness ever matters.
 */
export function makeIdempotencyKey(
  runId: RunId,
  stepId: StepId,
): (scope: string) => string {
  return (scope) => `nagi:${runId}:${stepId}:${scope}`;
}

/**
 * Durable per-effect memoization. The first successful call for
 * `(runId, stepId, scope)` persists its return value; subsequent calls
 * (including post-crash retries) return the cached value without invoking `fn`.
 */
export function makeOnce(args: {
  readonly runId: RunId;
  readonly stepId: StepId;
  readonly store: Store;
}): <T extends Json>(scope: string, fn: () => Promise<T>) => Promise<T> {
  const { runId, stepId, store } = args;
  return async function once<T extends Json>(
    scope: string,
    fn: () => Promise<T>,
  ): Promise<T> {
    const cached = await store.getOnce(runId, stepId, scope);
    if (cached !== null) return cached as T;

    const value = await fn();
    await store.recordOnce(runId, stepId, scope, value);
    return value;
  };
}
