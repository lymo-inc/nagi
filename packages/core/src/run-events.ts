import type { RunEvent, RunEventEnvelope, RunId } from "./types";

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
