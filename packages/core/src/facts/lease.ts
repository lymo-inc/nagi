import type { AttemptNumber, RunId, StepId } from "../types";
import type { FactBase, KindTable } from "./base";

// Written by the lease reaper when an expired lease on a non-terminal step is
// reclaimed. Audit hook: surfaces the otherwise-invisible "worker died, lease
// timed out, step re-dispatched" event that is the load-bearing crash recovery
// path. attempt is the lease's original attempt; the new dispatch carries
// attempt+1.
export interface LeaseReapedFact extends FactBase {
  readonly kind: "lease.reaped";
  readonly stepId: StepId;
  readonly attempt: AttemptNumber;
  readonly reapedAt: Date;
  readonly reason: "expired";
}

export type LeaseFact = LeaseReapedFact;

export const leaseFacts = {
  leaseReaped(a: {
    readonly runId: RunId;
    readonly stepId: StepId;
    readonly attempt: AttemptNumber;
    readonly at: Date;
    readonly reapedAt: Date;
  }): LeaseReapedFact {
    return {
      kind: "lease.reaped",
      runId: a.runId,
      stepId: a.stepId,
      attempt: a.attempt,
      at: a.at,
      reapedAt: a.reapedAt,
      reason: "expired",
    };
  },
} as const;

export const leaseKinds = {
  // Audit-only: the reaper re-enqueues at attempt+1; the projection updates
  // when the next attempt writes its step.started/step.failed.
  "lease.reaped": { fold: () => {}, rows: null },
} satisfies KindTable<LeaseFact>;
