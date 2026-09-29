import { Facts } from "./facts";
import type {
  ConcurrencyMode,
  FlowCanceledByConcurrencyFact,
  FlowStartedFact,
  PrunableStatus,
  RunId,
  RunStatus,
} from "./types";

// Store policy the adapters must not re-decide. An adapter owns only its
// transaction boundary (locks, rows, maps); every decision below is pure and
// shared, so two stores can only disagree by not calling it. Companions:
// factConsequences (facts/), decideSignal / decideTimeout (signals.ts),
// decideExpiredLeaseAction (lease-reaper.ts). `storeContract` (./testing) asserts each one per adapter.

export const DEFAULT_SWEEP_LIMIT = 100;

// Expiry filter BEFORE limit (Postgres: `WHERE deadline < now LIMIT n`): a live
// row never occupies a slot, so an expired one cannot starve behind `limit`
// live ones however the adapter happens to iterate.
export function selectExpired<T>(
  items: Iterable<T>,
  args: {
    readonly now: Date;
    readonly limit: number;
    readonly deadline: (item: T) => Date;
  },
): T[] {
  const nowMs = args.now.getTime();
  const out: T[] = [];
  for (const item of items) {
    if (out.length >= args.limit) break;
    if (args.deadline(item).getTime() < nowMs) out.push(item);
  }
  return out;
}

export interface PruneCandidate {
  readonly runId: RunId;
  readonly status: RunStatus;
  readonly completedAt: Date | null;
}

// One pruneFacts call drains every eligible run; the adapter loops this until
// it returns empty, each batch its own atomic unit. Oldest-first ordering keeps
// concurrent pruners from contending on the same rows.
export function selectPruneBatch<T extends PruneCandidate>(
  candidates: Iterable<T>,
  opts: {
    readonly olderThan: Date;
    readonly statuses: ReadonlyArray<PrunableStatus>;
    readonly batchSize: number;
  },
): T[] {
  const cutoff = opts.olderThan.getTime();
  const wanted = new Set<RunStatus>(opts.statuses);
  const eligible: Array<{ readonly item: T; readonly at: number }> = [];
  for (const item of candidates) {
    if (item.completedAt === null || !wanted.has(item.status)) continue;
    const at = item.completedAt.getTime();
    if (at < cutoff) eligible.push({ item, at });
  }
  eligible.sort(
    (a, b) =>
      a.at - b.at ||
      (a.item.runId < b.item.runId ? -1 : a.item.runId > b.item.runId ? 1 : 0),
  );
  return eligible.slice(0, opts.batchSize).map((e) => e.item);
}

// A run ends once. The adapter reads the run's row status under the lock that
// serializes its run ends (Postgres: the workflow_run row, FOR UPDATE) and, when
// this refuses, writes nothing: no fact, no consequence. No row = never started.
export function admitsRunEnd(status: RunStatus | undefined): boolean {
  return status === "pending" || status === "running";
}

export interface Superseded {
  readonly runId: RunId;
  readonly fact: FlowCanceledByConcurrencyFact;
}

// The runs a new start cancels, and the facts that record it. The adapter's
// share is finding `priors` (the active runs on the same flowId and key) under
// its own lock, then writing each fact before the new run takes the slot.
export function supersede(args: {
  readonly start: FlowStartedFact;
  readonly concurrency: {
    readonly key: string;
    readonly mode: ConcurrencyMode;
  };
  readonly priors: Iterable<RunId>;
}): Superseded[] {
  const { start, concurrency } = args;
  switch (concurrency.mode) {
    case "cancel-in-progress":
      return Array.from(args.priors, (runId) => ({
        runId,
        fact: Facts.flowCanceledByConcurrency({
          runId,
          canceledByRunId: start.runId,
          concurrencyKey: concurrency.key,
          at: start.at,
        }),
      }));
  }
}
