import { describe, expect, it } from "vitest";
import {
  type Backoff,
  DEFAULT_RETRY,
  defaultSnapshotGonePolicy,
  dequeueBackoff,
  reapBackoff,
  resolveRetry,
  snapshotGoneBackoff,
  stepBackoff,
} from "../retry";
import type { RetryPolicy } from "../types";

const exp: RetryPolicy = {
  maxAttempts: 99,
  backoff: "exponential",
  initialDelayMs: 100,
  maxDelayMs: 10_000,
};
const lin: RetryPolicy = {
  maxAttempts: 99,
  backoff: "linear",
  initialDelayMs: 100,
  maxDelayMs: 10_000,
};
const fix: RetryPolicy = {
  maxAttempts: 99,
  backoff: "fixed",
  initialDelayMs: 250,
};

describe("backoff curves", () => {
  it.each<[string, Backoff, number, number]>([
    ["step exponential, attempt 1", stepBackoff(exp), 1, 100],
    ["step exponential, attempt 2", stepBackoff(exp), 2, 200],
    ["step exponential, attempt 4", stepBackoff(exp), 4, 800],
    [
      "step exponential, attempt 8 (capped)",
      stepBackoff({ ...exp, maxDelayMs: 500 }),
      8,
      500,
    ],
    ["step linear, attempt 1", stepBackoff(lin), 1, 100],
    ["step linear, attempt 5", stepBackoff(lin), 5, 500],
    ["step linear, attempt 200 (capped)", stepBackoff(lin), 200, 10_000],
    ["step fixed, any attempt", stepBackoff(fix), 99, 250],
    [
      "step fixed, capped by maxDelay",
      stepBackoff({ ...fix, maxDelayMs: 100 }),
      1,
      100,
    ],
    [
      "step omitted delays fall back to DEFAULT_RETRY",
      stepBackoff({ maxAttempts: 3, backoff: "exponential" }),
      2,
      2_000,
    ],
    ["dequeue default poll (1s), failure 1", dequeueBackoff(1_000), 1, 1_000],
    ["dequeue default poll (1s), failure 3", dequeueBackoff(1_000), 3, 4_000],
    [
      "dequeue default poll (1s), failure 10 (capped 30s)",
      dequeueBackoff(1_000),
      10,
      30_000,
    ],
    ["dequeue poll 5ms floors to 50ms, failure 1", dequeueBackoff(5), 1, 50],
    ["dequeue poll 5ms floors to 50ms, failure 3", dequeueBackoff(5), 3, 200],
    ["dequeue poll 0 cannot tight-loop", dequeueBackoff(0), 1, 50],
    ["snapshot-gone delivery 1", snapshotGoneBackoff, 1, 1_000],
    ["snapshot-gone delivery 10", snapshotGoneBackoff, 10, 100_000],
    [
      "snapshot-gone delivery 18 (capped 5min)",
      snapshotGoneBackoff,
      18,
      300_000,
    ],
    ["snapshot-gone delivery 60", snapshotGoneBackoff, 60, 300_000],
    ["reap re-dispatch, any attempt", reapBackoff, 7, 0],
  ])("%s → %d ms", (_label, curve, attempt, expected) => {
    expect(curve(attempt)).toBe(expected);
  });
});

describe("defaultSnapshotGonePolicy", () => {
  it("retries on the curve through 60 deliveries, then fails", () => {
    expect(defaultSnapshotGonePolicy(1)).toEqual({
      action: "retry",
      delayMs: 1_000,
    });
    expect(defaultSnapshotGonePolicy(60)).toEqual({
      action: "retry",
      delayMs: 300_000,
    });
    expect(defaultSnapshotGonePolicy(61)).toEqual({ action: "fail" });
  });

  it("total window sits between the 4h signal-timeout constant and 6h stuck-run alerting", () => {
    let totalMs = 0;
    for (let readCount = 1; readCount <= 60; readCount++) {
      totalMs += snapshotGoneBackoff(readCount);
    }
    expect(totalMs).toBeGreaterThan(4 * 3_600_000);
    expect(totalMs).toBeLessThan(6 * 3_600_000);
  });
});

describe("resolveRetry", () => {
  const handler: RetryPolicy = { maxAttempts: 7, backoff: "fixed" };
  const runtime: RetryPolicy = { maxAttempts: 5, backoff: "linear" };

  it.each<
    [string, RetryPolicy | undefined, RetryPolicy | undefined, RetryPolicy]
  >([
    ["handler wins over runtime", handler, runtime, handler],
    ["runtime wins over built-in", undefined, runtime, runtime],
    ["built-in when neither set", undefined, undefined, DEFAULT_RETRY],
  ])("%s", (_label, h, r, expected) => {
    expect(resolveRetry(h, r)).toBe(expected);
  });
});
