import type { StepRunStatus } from "./run-view";
import type { AttemptNumber, Millis, RunId, StepId } from "./types";

export type ReaperDecision =
  | {
      readonly tag: "skip";
      readonly reason: "terminal" | "still-live" | "child-active";
    }
  | {
      readonly tag: "reap";
      readonly nextAttempt: AttemptNumber;
      readonly backoffMs: Millis;
    };

// Pure: the adapter passes a lease row + its joined step status + whether an
// active child exists for it + now. No I/O, no clock reads. A terminal step's
// lease lingers only if a settle path crashed mid-write; the sweeper treats it
// as a no-op cleanup (skip). A subflow step parked on a STILL-ACTIVE child is
// not stuck — the child's terminal transition (or a recovery re-dispatch once
// the child ends) wakes the parent; re-dispatching now would just re-park and
// churn step.started facts. So skip while the child runs; once the child is
// terminal/gone, fall through and reap — the re-entrant subflow handler then
// settles the parent (terminal child) or re-spawns (missing child). A still-live
// lease means an active worker is heartbeating; reaping it would double-dispatch
// a running step. Reap re-enqueues at attempt+1 with no backoff (the step body
// already paid wall-clock waiting for the worker to die).
export function decideExpiredLeaseAction(args: {
  readonly lease: {
    readonly runId: RunId;
    readonly stepId: StepId;
    readonly attempt: AttemptNumber;
    readonly expiresAt: Date;
  };
  readonly stepStatus: StepRunStatus;
  readonly childActive: boolean;
  readonly now: Date;
}): ReaperDecision {
  const { lease, stepStatus, childActive, now } = args;
  if (
    stepStatus === "completed" ||
    stepStatus === "failed" ||
    stepStatus === "canceled" ||
    stepStatus === "skipped"
  ) {
    return { tag: "skip", reason: "terminal" };
  }
  if (childActive) {
    return { tag: "skip", reason: "child-active" };
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
