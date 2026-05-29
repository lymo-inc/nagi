import type { StepRunStatus } from "./run-view";
import type { AttemptNumber, Millis, RunId, StepId } from "./types";

export type ReaperDecision =
  | { readonly tag: "skip"; readonly reason: "terminal" | "still-live" }
  | {
      readonly tag: "reap";
      readonly nextAttempt: AttemptNumber;
      readonly backoffMs: Millis;
    };

// Pure: the adapter passes a lease row + its joined step status + now. No I/O,
// no clock reads. A terminal step's lease lingers only if a settle path crashed
// mid-write; the sweeper treats it as a no-op cleanup (skip). A still-live lease
// means an active worker is heartbeating; reaping it would double-dispatch a
// running step. Reap re-enqueues at attempt+1 with no backoff (the step body
// already paid wall-clock waiting for the worker to die).
export function decideExpiredLeaseAction(args: {
  readonly lease: {
    readonly runId: RunId;
    readonly stepId: StepId;
    readonly attempt: AttemptNumber;
    readonly expiresAt: Date;
  };
  readonly stepStatus: StepRunStatus;
  readonly now: Date;
}): ReaperDecision {
  const { lease, stepStatus, now } = args;
  if (
    stepStatus === "completed" ||
    stepStatus === "failed" ||
    stepStatus === "canceled" ||
    stepStatus === "skipped"
  ) {
    return { tag: "skip", reason: "terminal" };
  }
  if (lease.expiresAt > now) {
    return { tag: "skip", reason: "still-live" };
  }
  return {
    tag: "reap",
    nextAttempt: (lease.attempt + 1) as AttemptNumber,
    backoffMs: 0 as Millis,
  };
}

export interface ReapedLease {
  readonly runId: RunId;
  readonly stepId: StepId;
  readonly attempt: AttemptNumber;
  readonly nextAttempt: AttemptNumber;
}

export const DEFAULT_REAPER_INTERVAL_MS: Millis = 30_000;
