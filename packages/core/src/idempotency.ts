import type { Json, RunId, StepId, Store } from "./types";

export function makeIdempotencyKey(
  runId: RunId,
  stepId: StepId,
): (scope: string) => string {
  return (scope) => `nagi:${runId}:${stepId}:${scope}`;
}

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
