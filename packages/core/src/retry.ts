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

// Every message is read at most MAX_DELIVERIES times: past that, whatever
// keeps it from settling is not transient, and the run ends typed instead of
// looping. Observed without a bound: snapshot-gone at set_vt(0), read_ct
// 660,576 over 8 days (staging, 2026-07); a throwing drift synthesis, ~80k
// reads per run (dev, 2026-09-30).
export const MAX_DELIVERIES = 10;

// One redelivery a minute: 9 retries span ~9 minutes, which covers a rolling
// deploy's old/new worker overlap. A run that is not picked up by a
// frozen-version worker in that window fails typed and can be replayed.
const SNAPSHOT_GONE_RETRY_DELAY_MS: Millis = 60_000;

// Bounds redelivery of a message whose run is pinned to a flow snapshot no
// longer in any live registry. "retry" exists ONLY for the rolling-deploy
// window, where a not-yet-replaced worker may still hold the pinned code and
// can finish the run. Once that window has clearly passed, retrying is pure
// poison: the observed failure mode was set_vt(0) redelivery ~1/s for 8 days
// (read_ct 660k) drowning staging logs. "fail" is the honest terminal state —
// the run cannot advance on any current code — and unlike the old behavior it
// leaves a workflow_run.error a human can triage, then admin-restart.
export const defaultSnapshotGonePolicy: SnapshotGonePolicy = (readCount) =>
  readCount >= MAX_DELIVERIES
    ? { action: "fail" }
    : { action: "retry", delayMs: SNAPSHOT_GONE_RETRY_DELAY_MS };

// A reaped step re-dispatches immediately: its body already paid wall-clock
// waiting for the dead worker's lease to expire.
export const reapBackoff: Backoff = () => 0;
