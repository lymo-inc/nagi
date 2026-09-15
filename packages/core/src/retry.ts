import type { Millis, RetryPolicy, SnapshotGonePolicy } from "./types";

// Also the canonical fill-in for a step's omitted initialDelayMs/maxDelayMs,
// so these values are hashed into every flow that declares a retry policy.
// Changing them re-hashes those flows and strands their in-flight runs as
// snapshot-gone; canonicalize.test.ts pins them.
export const DEFAULT_RETRY = {
  maxAttempts: 3,
  backoff: "exponential",
  initialDelayMs: 1_000,
  maxDelayMs: 60_000,
} as const satisfies RetryPolicy;

export function resolveRetry(
  handlerPolicy: RetryPolicy | undefined,
  runtimeDefault: RetryPolicy | undefined,
): RetryPolicy {
  return handlerPolicy ?? runtimeDefault ?? DEFAULT_RETRY;
}

// Every backoff curve has this shape: the 1-based ordinal of the attempt being
// scheduled in, the delay before it out. Curves that need configuration are
// built by a factory that closes over it.
export type Backoff = (attempt: number) => Millis;

export function stepBackoff(policy: RetryPolicy): Backoff {
  const initial = policy.initialDelayMs ?? DEFAULT_RETRY.initialDelayMs;
  const max = policy.maxDelayMs ?? DEFAULT_RETRY.maxDelayMs;
  return (attempt) => {
    switch (policy.backoff) {
      case "exponential":
        return Math.min(initial * 2 ** Math.max(0, attempt - 1), max);
      case "linear":
        return Math.min(initial * Math.max(1, attempt), max);
      case "fixed":
        return Math.min(initial, max);
    }
  };
}

const MAX_DEQUEUE_BACKOFF_MS: Millis = 30_000;
const MIN_DEQUEUE_BACKOFF_MS: Millis = 50;

// Exponential, capped: a one-off blip costs one poll interval, a real outage
// settles to a 30s cadence instead of hammering a down database and flooding
// logs. Anchored on pollIntervalMs (default 1s, so the default curve is
// 1s -> 30s) and floored so a 0/near-0 poll interval cannot turn a persistent
// failure into a tight loop.
export function dequeueBackoff(pollIntervalMs: Millis): Backoff {
  const base = Math.max(pollIntervalMs, MIN_DEQUEUE_BACKOFF_MS);
  return (consecutiveFailures) =>
    Math.min(base * 2 ** (consecutiveFailures - 1), MAX_DEQUEUE_BACKOFF_MS);
}

// Quadratic backoff capped at 5 min, budget 60 deliveries ≈ a 4.2h retry
// window. Rationale: the window must comfortably exceed the longest plausible
// old/new worker overlap (a rolling deploy is minutes, but a wedged draining
// task has been observed hanging on for hours), and 4h matches the house
// signal-timeout constant while staying under typical stuck-run alerting
// thresholds (6h) — so a run that is GOING to fail fails before it pages as
// stuck. Within the window: fast redelivery early (sub-minute, when a frozen
// worker most likely still exists), quiet later (5 min cadence, ~60 log lines
// total for a run that never recovers — vs 660k at set_vt(0)).
const SNAPSHOT_GONE_DELIVERY_BUDGET = 60;
const MAX_SNAPSHOT_GONE_BACKOFF_MS: Millis = 300_000;

export const snapshotGoneBackoff: Backoff = (readCount) =>
  Math.min(readCount * readCount * 1_000, MAX_SNAPSHOT_GONE_BACKOFF_MS);

// Bounds redelivery of a message whose run is pinned to a flow snapshot no
// longer in any live registry. "retry" exists ONLY for the rolling-deploy
// window, where a not-yet-replaced worker may still hold the pinned code and
// can finish the run. Once that window has clearly passed, retrying is pure
// poison: the observed failure mode was set_vt(0) redelivery ~1/s for 8 days
// (read_ct 660k) drowning staging logs. "fail" is the honest terminal state —
// the run cannot advance on any current code — and unlike the old behavior it
// leaves a workflow_run.error a human can triage, then admin-restart.
export const defaultSnapshotGonePolicy: SnapshotGonePolicy = (readCount) =>
  readCount > SNAPSHOT_GONE_DELIVERY_BUDGET
    ? { action: "fail" }
    : { action: "retry", delayMs: snapshotGoneBackoff(readCount) };

// A reaped step re-dispatches immediately: its body already paid wall-clock
// waiting for the dead worker's lease to expire.
export const reapBackoff: Backoff = () => 0;
