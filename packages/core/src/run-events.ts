import type { Fact } from "./facts";
import type { RunEvent, RunEventEnvelope, RunId } from "./types";

// Facts are the source of truth; a run event is a projection of one. Returns
// null for facts with no lifecycle meaning to an observer (leases, timers,
// once-records, arm selection), so a subscriber sees a stream it can act on
// rather than the whole log.
export function runEventOf(fact: Fact): RunEvent | null {
  switch (fact.kind) {
    case "flow.started":
      return { type: "flow.started", flowId: fact.flowId };
    case "flow.completed":
      return { type: "flow.completed", output: fact.output };
    case "flow.failed":
      return { type: "flow.failed", error: fact.error };
    case "flow.canceled":
      return fact.cause === "concurrency"
        ? {
            type: "flow.canceled",
            cause: "concurrency",
            canceledByRunId: fact.canceledByRunId,
          }
        : { type: "flow.canceled", cause: fact.cause };
    case "step.started":
      return {
        type: "step.started",
        stepId: fact.stepId,
        attempt: fact.attempt,
      };
    case "step.completed":
      return {
        type: "step.completed",
        stepId: fact.stepId,
        attempt: fact.attempt,
        output: fact.output,
      };
    case "step.failed":
      return {
        type: "step.failed",
        stepId: fact.stepId,
        attempt: fact.attempt,
        error: fact.error,
      };
    case "step.retried":
      return {
        type: "step.retried",
        stepId: fact.stepId,
        // Carries the attempt that FAILED; the next one is attempt + 1.
        attempt: fact.attempt,
      };
    case "step.skipped":
      return { type: "step.skipped", stepId: fact.stepId, reason: fact.reason };
    default:
      return null;
  }
}

// Terminal run events. A per-run watcher drops its subscribers after one of
// these so a long-lived process does not accumulate a listener per finished run.
export function isTerminalRunEvent(event: RunEvent): boolean {
  return (
    event.type === "flow.completed" ||
    event.type === "flow.failed" ||
    event.type === "flow.canceled"
  );
}

type Handler = (event: RunEventEnvelope) => void;

// Process-local fan-out, shared by every Store adapter the same way
// InMemoryStreamHub is. Adapters differ only in how envelopes REACH the hub:
// the in-memory store calls publish() directly, Postgres feeds it from NOTIFY.
export class InMemoryRunEventHub {
  private readonly perRun = new Map<RunId, Set<Handler>>();
  private readonly all = new Set<Handler>();

  publish(envelope: RunEventEnvelope): void {
    // Snapshot before dispatch: a handler is allowed to unsubscribe itself, and
    // mutating the live set mid-iteration would skip its neighbour.
    const targeted = this.perRun.get(envelope.runId);
    if (targeted !== undefined) {
      for (const h of [...targeted]) safely(h, envelope);
    }
    for (const h of [...this.all]) safely(h, envelope);

    if (targeted !== undefined && isTerminalRunEvent(envelope)) {
      this.perRun.delete(envelope.runId);
    }
  }

  watchRun(runId: RunId, handler: Handler): () => void {
    let set = this.perRun.get(runId);
    if (set === undefined) {
      set = new Set();
      this.perRun.set(runId, set);
    }
    set.add(handler);
    return () => {
      const current = this.perRun.get(runId);
      if (current === undefined) return;
      current.delete(handler);
      if (current.size === 0) this.perRun.delete(runId);
    };
  }

  watchRuns(handler: Handler): () => void {
    this.all.add(handler);
    return () => {
      this.all.delete(handler);
    };
  }
}

// A subscriber's callback is consumer code. Letting it throw here would abort
// the fact write that produced the event, so observation could break execution.
function safely(handler: Handler, envelope: RunEventEnvelope): void {
  try {
    handler(envelope);
  } catch {
    /* observation must never fail the run it observes */
  }
}
