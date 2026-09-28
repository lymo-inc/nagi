import { NagiConcurrencyConflictError } from "./errors";
import {
  factConsequences,
  foldRun,
  isRunEnd,
  type Release,
  type RowDelta,
} from "./facts";
import { decideExpiredLeaseAction, type ReapedLease } from "./lease-reaper";
import {
  nextRunRow,
  nextStepRow,
  type RunRow,
  type StepRow,
} from "./read-model";
import { InMemoryRunEventHub } from "./run-events";
import type { RunDescription, StepView } from "./run-view";
import { decideSignal, decideTimeout } from "./signals";
import {
  admitsRunEnd,
  clampQueryLimit,
  compareRunOrder,
  DEFAULT_SWEEP_LIMIT,
  decodeRunCursor,
  encodeRunCursor,
  isPastCursor,
  jsonContains,
  type PruneCandidate,
  type Superseded,
  selectExpired,
  selectPruneBatch,
  supersede,
} from "./store-policy";
import { InMemoryStreamHub, isStreamOver } from "./stream-hub";
import type {
  AttemptNumber,
  ClaimToken,
  Clock,
  ConcurrencyMode,
  Fact,
  FlowStartedFact,
  GetOnceResult,
  GlobalFact,
  Json,
  Millis,
  PruneOpts,
  PruneResult,
  QueryRunsOpts,
  QueryRunsResult,
  Queue,
  QueueDequeueOpts,
  QueueEnqueueOpts,
  QueueInspectEntry,
  QueueMessage,
  RunEndFact,
  RunEventTransport,
  RunId,
  RunState,
  RunSummary,
  SettleSignalResult,
  StepCanceledFact,
  StepCompletedFact,
  StepFailedFact,
  StepId,
  Store,
  StreamEvent,
  StreamTransport,
  TimedOutSignal,
  Tx,
} from "./types";

interface MemoryLease {
  readonly runId: RunId;
  readonly stepId: StepId;
  readonly attempt: AttemptNumber;
  readonly token: ClaimToken;
  readonly expiresAt: number;
}

interface MemoryTimer {
  readonly runId: RunId;
  readonly stepId: StepId;
  readonly fireAt: Date;
}

function leaseKey(runId: RunId, stepId: StepId, attempt: AttemptNumber) {
  return `${runId}::${stepId}::${attempt}`;
}

function timerKey(runId: RunId, stepId: StepId) {
  return `${runId}::${stepId}`;
}

export interface InMemoryStoreOpts {
  readonly leaseMs?: Millis;
}

const DEFAULT_STORE_LEASE_MS: Millis = 60_000;

export class InMemoryStore implements Store {
  private readonly facts = new Map<string, Fact[]>();
  private readonly onces = new Map<string, Json>();
  private readonly leases = new Map<string, MemoryLease>();
  private readonly snapshots = new Map<
    string,
    { readonly flowId: string; readonly dag: Json }
  >();
  private readonly refs = new Map<string, string>();
  private readonly globalFacts: GlobalFact[] = [];
  private readonly activeByKey = new Map<string, RunId>();
  private readonly keyByActiveRun = new Map<RunId, string>();
  // Kept past terminal facts so a reopened run can re-take its slot.
  private readonly slotByRun = new Map<RunId, string>();
  // The read model (Postgres: workflow_run / step_run), fed only by row deltas.
  private readonly runRows = new Map<RunId, RunRow>();
  private readonly stepRows = new Map<RunId, Map<StepId, StepRow>>();
  private readonly streamHub = new InMemoryStreamHub();
  // Durable signal-timeout deadlines (the Postgres `timer` table), distinct
  // from InMemoryClock's setTimeout-based scheduler.
  private readonly signalTimers = new Map<string, MemoryTimer>();
  private readonly leaseMs: Millis;

  constructor(opts: InMemoryStoreOpts = {}) {
    this.leaseMs = opts.leaseMs ?? DEFAULT_STORE_LEASE_MS;
  }

  async appendFact(
    runId: RunId,
    fact: Exclude<Fact, RunEndFact>,
  ): Promise<void> {
    this.writeFact(runId, fact);
  }

  async endRun(runId: RunId, fact: RunEndFact): Promise<boolean> {
    return this.writeFact(runId, fact);
  }

  // The one fact-write path. Synchronous on purpose: with no await between the
  // read that led to a write and the write itself, every caller gets the
  // atomicity the Postgres store takes a transaction for.
  private writeFact(runId: RunId, fact: Fact): boolean {
    if (isRunEnd(fact) && !admitsRunEnd(this.runRows.get(runId)?.status)) {
      return false;
    }
    const list = this.facts.get(runId) ?? [];
    if (fact.kind === "step.reset") {
      // Mirror of the Postgres partial unique index: a reopened run re-takes
      // its concurrency slot, and must not steal it from another active run.
      const prior = foldRun(runId, list);
      if (prior.phase.tag === "completed" || prior.phase.tag === "failed") {
        const slot = this.slotByRun.get(runId);
        if (slot !== undefined) {
          const holder = this.activeByKey.get(slot);
          if (holder !== undefined && holder !== runId) {
            throw new NagiConcurrencyConflictError({
              runId,
              flowId: prior.flowId,
              concurrencyKey: slot.slice(slot.indexOf("::") + 2),
            });
          }
          this.activeByKey.set(slot, runId);
          this.keyByActiveRun.set(runId, slot);
        }
      }
    }
    list.push(fact);
    this.facts.set(runId, list);
    const { rows, release, stream, event } = factConsequences(fact);
    if (rows !== null) this.applyRows(runId, rows);
    if (release !== null) this.release(runId, release);
    if (stream !== null) this.streamHub.apply(runId, stream);
    if (event !== null) this.eventHub.publish({ ...event, runId });
    return true;
  }

  private applyRows(runId: RunId, delta: RowDelta): void {
    const run = nextRunRow(this.runRows.get(runId), delta);
    if (run !== undefined) this.runRows.set(runId, run);
    if (delta.row === "run") return;
    const steps = this.stepRows.get(runId) ?? new Map<StepId, StepRow>();
    const step = nextStepRow(steps.get(delta.stepId), delta);
    if (step === undefined) steps.delete(delta.stepId);
    else steps.set(delta.stepId, step);
    this.stepRows.set(runId, steps);
  }

  private release(runId: RunId, release: Release): void {
    switch (release.tag) {
      case "release-step":
        this.releaseLeases(runId, release.stepId);
        if (release.timer) {
          this.signalTimers.delete(timerKey(runId, release.stepId));
        }
        return;
      case "release-lease":
        this.leases.delete(leaseKey(runId, release.stepId, release.attempt));
        return;
      case "release-run": {
        const slot = this.keyByActiveRun.get(runId);
        if (slot === undefined) return;
        this.keyByActiveRun.delete(runId);
        if (this.activeByKey.get(slot) === runId) {
          this.activeByKey.delete(slot);
        }
        return;
      }
    }
  }

  private releaseLeases(runId: RunId, stepId: StepId): void {
    for (const [key, lease] of this.leases) {
      if (lease.runId === runId && lease.stepId === stepId) {
        this.leases.delete(key);
      }
    }
  }

  async tryStartRun(
    runId: RunId,
    fact: FlowStartedFact,
    concurrency?: {
      readonly key: string;
      readonly mode: ConcurrencyMode;
    },
  ): Promise<{
    readonly started: boolean;
    readonly canceled: ReadonlyArray<Superseded>;
  }> {
    // A run pruned with keepSummary keeps its row, so its runId stays taken.
    if (this.runRows.has(runId)) {
      return { started: false, canceled: [] };
    }

    const canceled: Superseded[] = [];
    if (concurrency !== undefined) {
      const slot = `${fact.flowId}::${concurrency.key}`;
      const prior = this.activeByKey.get(slot);
      for (const c of supersede({
        start: fact,
        concurrency,
        priors: prior === undefined ? [] : [prior],
      })) {
        if (this.writeFact(c.runId, c.fact)) canceled.push(c);
      }
      this.activeByKey.set(slot, runId);
      this.keyByActiveRun.set(runId, slot);
      this.slotByRun.set(runId, slot);
    }

    this.writeFact(runId, fact);
    return { started: true, canceled };
  }

  // No real tx to join: every write here is visible, and announced, the moment
  // it happens, and nothing the caller does later can roll it back. Events stay
  // exactly as durable as the facts they report.
  async tryStartRunOnTx(
    _tx: Tx,
    runId: RunId,
    fact: FlowStartedFact,
    concurrency?: {
      readonly key: string;
      readonly mode: ConcurrencyMode;
    },
  ): Promise<{
    readonly started: boolean;
    readonly canceled: ReadonlyArray<Superseded>;
  }> {
    return this.tryStartRun(runId, fact, concurrency);
  }

  async listChildren(parentRunId: RunId): Promise<ReadonlyArray<RunId>> {
    return this.childRows(parentRunId).map(([runId]) => runId);
  }

  private childRows(parentRunId: RunId): Array<[RunId, RunRow]> {
    return [...this.runRows].filter(
      ([, row]) => row.parent?.runId === parentRunId,
    );
  }

  // Does a non-terminal child exist for this (parentRunId, stepId)? Used by the
  // reaper to leave a subflow step parked while its child still runs (instead
  // of re-dispatching it at attempt+1).
  private hasActiveChild(parentRunId: RunId, parentStepId: StepId): boolean {
    return this.childRows(parentRunId).some(
      ([, row]) =>
        row.parent?.stepId === parentStepId &&
        (row.status === "pending" || row.status === "running"),
    );
  }

  async loadRunState(runId: RunId): Promise<RunState> {
    return foldRun(runId, this.facts.get(runId) ?? []);
  }

  async claimStep(
    runId: RunId,
    stepId: StepId,
    attempt: AttemptNumber,
  ): Promise<ClaimToken | null> {
    const key = leaseKey(runId, stepId, attempt);
    const now = Date.now();
    const existing = this.leases.get(key);
    if (existing && existing.expiresAt > now) {
      return null;
    }
    const token = `lease-${crypto.randomUUID()}` as ClaimToken;
    this.leases.set(key, {
      runId,
      stepId,
      attempt,
      token,
      expiresAt: now + this.leaseMs,
    });
    return token;
  }

  async extendLease(
    runId: RunId,
    stepId: StepId,
    attempt: AttemptNumber,
    leaseMs: Millis,
  ): Promise<void> {
    const key = leaseKey(runId, stepId, attempt);
    const existing = this.leases.get(key);
    if (existing === undefined) return; // idempotent: settled/reaped → no-op
    this.leases.set(key, { ...existing, expiresAt: Date.now() + leaseMs });
  }

  async sweepLeases(args: {
    readonly now: Date;
    readonly queue: Queue;
    readonly limit?: number;
  }): Promise<readonly ReapedLease[]> {
    const { now, queue, limit = DEFAULT_SWEEP_LIMIT } = args;
    const reaped: ReapedLease[] = [];

    const candidates = selectExpired(this.leases.values(), {
      now,
      limit,
      deadline: (l) => new Date(l.expiresAt),
    });

    for (const lease of candidates) {
      const { runId, stepId, attempt } = lease;
      const flowId = this.runRows.get(runId)?.flowId;
      const decision = decideExpiredLeaseAction({
        lease: {
          runId,
          stepId,
          attempt,
          expiresAt: new Date(lease.expiresAt),
        },
        stepStatus: this.stepRows.get(runId)?.get(stepId)?.status ?? "pending",
        childActive: this.hasActiveChild(runId, stepId),
        now,
      });
      if (decision.tag === "skip") continue;

      // No tx to roll back: take the lease before the await so a concurrent
      // sweep cannot reap it twice, and hand it back if the enqueue fails so
      // nothing is left half-reaped.
      const key = leaseKey(runId, stepId, attempt);
      this.leases.delete(key);
      try {
        await queue.enqueue(runId, stepId, {
          attempt: decision.nextAttempt,
          delayMs: decision.backoffMs,
          ...(flowId !== undefined ? { flowId } : {}),
        });
      } catch (err) {
        this.leases.set(key, lease);
        throw err;
      }
      this.writeFact(runId, decision.fact);
      reaped.push({
        runId,
        stepId,
        attempt,
        nextAttempt: decision.nextAttempt,
      });
    }

    return reaped;
  }

  async settleSignal(args: {
    readonly runId: RunId;
    readonly stepId: StepId;
    readonly at: Date;
    readonly incoming?: {
      readonly payload: Json;
      readonly signalName?: string;
    };
  }): Promise<SettleSignalResult> {
    const { runId, stepId, at, incoming } = args;
    // Single process, single thread: foldRun → decide → append runs to
    // completion with no interleaving await, giving the same atomicity the
    // postgres store gets from a per-run advisory lock.
    const runState = foldRun(runId, this.facts.get(runId) ?? []);
    const decision = decideSignal({ runState, stepId, at, incoming });
    switch (decision.kind) {
      case "noop":
        return decision.result;
      case "buffer":
        if (decision.fact !== null) this.writeFact(runId, decision.fact);
        return decision.result;
      case "deliver":
        this.writeFact(runId, decision.received);
        this.writeFact(runId, decision.completed);
        return decision.result;
    }
  }

  async upsertTimer(runId: RunId, stepId: StepId, fireAt: Date): Promise<void> {
    // Insert-if-absent (Postgres: ON CONFLICT DO NOTHING) so a lease-reap
    // re-dispatch of a still-parked signal can't push the deadline out. A
    // genuine restart deletes the row first (step.reset).
    const key = timerKey(runId, stepId);
    if (!this.signalTimers.has(key)) {
      this.signalTimers.set(key, { runId, stepId, fireAt });
    }
  }

  async sweepSignalTimeouts(args: {
    readonly now: Date;
    readonly limit?: number;
  }): Promise<readonly TimedOutSignal[]> {
    const { now, limit = DEFAULT_SWEEP_LIMIT } = args;
    const timedOut: TimedOutSignal[] = [];

    const due = selectExpired(this.signalTimers.values(), {
      now,
      limit,
      deadline: (t) => t.fireAt,
    });

    // Single process, single thread: fold → decide → append runs with no
    // interleaving await, the same atomicity argument settleSignal relies on.
    for (const { runId, stepId, fireAt } of due) {
      const runState = foldRun(runId, this.facts.get(runId) ?? []);
      const decision = decideTimeout({ runState, stepId, fireAt, at: now });
      // Fired once: consumed whether or not the step was still awaiting.
      this.signalTimers.delete(timerKey(runId, stepId));
      if (decision.kind === "noop") continue;
      this.writeFact(runId, decision.fact);
      timedOut.push({ runId, stepId, attempt: decision.attempt, fireAt });
    }

    return timedOut;
  }

  async recordOnce(
    runId: RunId,
    stepId: StepId,
    scope: string,
    value: Json,
  ): Promise<void> {
    const key = `${runId}::${stepId}::${scope}`;
    if (!this.onces.has(key)) this.onces.set(key, value);
  }

  async getOnce(
    runId: RunId,
    stepId: StepId,
    scope: string,
  ): Promise<GetOnceResult> {
    const value = this.onces.get(`${runId}::${stepId}::${scope}`);
    return value === undefined ? { tag: "miss" } : { tag: "hit", value };
  }

  async runStep<T extends Json>(
    runId: RunId,
    _stepId: StepId,
    _attempt: AttemptNumber,
    body: (tx: Tx) => Promise<{
      readonly output: T;
      readonly fact: StepCompletedFact | StepFailedFact | StepCanceledFact;
    }>,
  ): Promise<T> {
    const result = await body(undefined as unknown as Tx);
    this.writeFact(runId, result.fact);
    return result.output;
  }

  async upsertSnapshot(args: {
    readonly flowHash: string;
    readonly flowId: string;
    readonly dag: Json;
  }): Promise<void> {
    if (this.snapshots.has(args.flowHash)) return;
    this.snapshots.set(args.flowHash, { flowId: args.flowId, dag: args.dag });
  }

  async getRef(flowId: string): Promise<string | null> {
    return this.refs.get(flowId) ?? null;
  }

  async setRef(flowId: string, flowHash: string): Promise<void> {
    this.refs.set(flowId, flowHash);
  }

  async loadSnapshot(
    flowHash: string,
  ): Promise<{ readonly flowId: string; readonly dag: Json } | null> {
    return this.snapshots.get(flowHash) ?? null;
  }

  async appendGlobalFact(fact: GlobalFact): Promise<void> {
    this.globalFacts.push(fact);
  }

  readGlobalFacts(): ReadonlyArray<GlobalFact> {
    return this.globalFacts;
  }

  async queryRuns(opts: QueryRunsOpts): Promise<QueryRunsResult> {
    const where = opts.where ?? {};
    const wanted = where.status && new Set(where.status);

    const matches = (summary: RunSummary): boolean =>
      (where.flowId === undefined || summary.flowId === where.flowId) &&
      (!wanted || wanted.has(summary.status)) &&
      (where.input === undefined || jsonContains(summary.input, where.input));

    const summaries: RunSummary[] = [];
    for (const [runId, row] of this.runRows) {
      const summary = summaryOf(runId, row);
      if (matches(summary)) summaries.push(summary);
    }
    summaries.sort(compareRunOrder);

    if (opts.latest === true) {
      return { runs: summaries.slice(0, 1), cursor: null };
    }

    const limit = clampQueryLimit(opts.limit);
    const cursor =
      opts.cursor === undefined ? null : decodeRunCursor(opts.cursor);
    const rest =
      cursor === null
        ? summaries
        : summaries.filter((s) => isPastCursor(s, cursor));
    const page = rest.slice(0, limit);
    const last = page[page.length - 1];
    const next =
      rest.length > limit && last !== undefined
        ? encodeRunCursor({ startedAt: last.startedAt, runId: last.runId })
        : null;
    return { runs: page, cursor: next };
  }

  async describe(runId: RunId): Promise<RunDescription> {
    const row = this.runRows.get(runId);
    if (row === undefined) return null;
    const slot = this.slotByRun.get(runId);
    const children = this.childRows(runId)
      .sort(([, a], [, b]) => a.startedAt.getTime() - b.startedAt.getTime())
      .map(([childId]) => childId);
    const steps = [...(this.stepRows.get(runId) ?? [])]
      .map(([stepId, step]) => this.stepView(runId, stepId, step))
      .sort(compareStepOrder);
    return {
      run: {
        runId,
        flowId: row.flowId,
        flowHash: row.flowHash ?? "",
        status: row.status,
        startedAt: row.startedAt,
        input: row.input,
        children,
        ...(row.completedAt !== null ? { completedAt: row.completedAt } : {}),
        ...(row.output !== null ? { output: row.output } : {}),
        ...(row.error !== null ? { error: row.error as unknown as Json } : {}),
        // Postgres: ON DELETE SET NULL — the view never names a pruned run.
        ...(row.canceledByRunId !== null &&
        this.runRows.has(row.canceledByRunId)
          ? { canceledByRunId: row.canceledByRunId }
          : {}),
        ...(slot !== undefined
          ? { concurrencyKey: slot.slice(row.flowId.length + 2) }
          : {}),
        ...(row.parent !== null ? { parent: row.parent } : {}),
      },
      steps,
    };
  }

  private stepView(runId: RunId, stepId: StepId, row: StepRow): StepView {
    const lease = this.leases.get(leaseKey(runId, stepId, row.attempt));
    const live = lease !== undefined && lease.expiresAt > Date.now();
    return {
      stepId,
      attempt: row.attempt,
      status: row.status,
      ...(row.startedAt !== null ? { startedAt: row.startedAt } : {}),
      ...(row.completedAt !== null ? { completedAt: row.completedAt } : {}),
      ...(row.output !== null ? { output: row.output } : {}),
      ...(row.error !== null ? { error: row.error as unknown as Json } : {}),
      ...(live ? { lease: { expiresAt: new Date(lease.expiresAt) } } : {}),
    };
  }

  async pruneFacts(opts: Required<PruneOpts>): Promise<PruneResult> {
    let runsPruned = 0;
    let factsPruned = 0;
    for (;;) {
      const batch = selectPruneBatch(this.pruneCandidates(), opts);
      if (batch.length === 0) break;
      for (const { runId, factCount } of batch) {
        this.facts.delete(runId);
        this.stepRows.delete(runId);
        deleteByRunPrefix(this.onces, runId);
        deleteByRunPrefix(this.leases, runId);
        deleteByRunPrefix(this.signalTimers, runId);
        if (!opts.keepSummary) {
          this.runRows.delete(runId);
          this.slotByRun.delete(runId);
        }
        runsPruned += 1;
        factsPruned += factCount;
      }
    }
    return { runsPruned, factsPruned };
  }

  private pruneCandidates(): PruneVictim[] {
    const out: PruneVictim[] = [];
    for (const [runId, row] of this.runRows) {
      const factList = this.facts.get(runId);
      if (factList === undefined) continue;
      out.push({
        runId,
        status: row.status,
        completedAt: row.completedAt,
        factCount: factList.length,
      });
    }
    return out;
  }

  private readonly eventHub = new InMemoryRunEventHub();

  readonly events: RunEventTransport = {
    watchRun: (runId, handler) => this.eventHub.watchRun(runId, handler),
    watchRuns: (handler) => this.eventHub.watchRuns(handler),
  };

  readonly stream: StreamTransport = {
    subscribeStream: (
      runId: RunId,
      stepId: StepId,
      opts?: { readonly replayBuffered?: boolean },
    ): AsyncIterable<StreamEvent<Json>> => {
      // Delegating to a finished step would open a channel that hangs.
      if (isStreamOver(foldRun(runId, this.facts.get(runId) ?? []), stepId)) {
        return EMPTY_CLOSED_STREAM;
      }
      return this.streamHub.subscribeStream(runId, stepId, opts);
    },

    publishChunk: (runId: RunId, stepId: StepId, chunk: Json): void => {
      this.streamHub.publishChunk(runId, stepId, chunk);
    },
  };
}

interface PruneVictim extends PruneCandidate {
  readonly factCount: number;
}

function summaryOf(runId: RunId, row: RunRow): RunSummary {
  return {
    runId,
    flowId: row.flowId,
    status: row.status,
    startedAt: row.startedAt,
    completedAt: row.completedAt,
    input: row.input,
  };
}

// Postgres: ORDER BY started_at ASC NULLS LAST, step_id ASC.
function compareStepOrder(a: StepView, b: StepView): number {
  const ta = a.startedAt?.getTime() ?? Number.POSITIVE_INFINITY;
  const tb = b.startedAt?.getTime() ?? Number.POSITIVE_INFINITY;
  if (ta !== tb) return ta - tb;
  return a.stepId < b.stepId ? -1 : a.stepId > b.stepId ? 1 : 0;
}

const EMPTY_CLOSED_STREAM: AsyncIterable<StreamEvent<Json>> = {
  [Symbol.asyncIterator](): AsyncIterator<StreamEvent<Json>> {
    return { next: async () => ({ value: undefined, done: true }) };
  },
};

function deleteByRunPrefix<V>(map: Map<string, V>, runId: RunId): void {
  const prefix = `${runId}::`;
  for (const key of map.keys()) {
    if (key.startsWith(prefix)) map.delete(key);
  }
}

export { foldRun as projectRunState } from "./facts";

interface QueuedItem extends QueueMessage {
  readonly enqueuedAt: number;
  readonly visibleAt: number;
}

export interface InMemoryQueueOpts {
  readonly leaseMs?: Millis;
}

const DEFAULT_QUEUE_LEASE_MS: Millis = 60_000;

export class InMemoryQueue implements Queue {
  private readonly pending: QueuedItem[] = [];
  private readonly leased = new Map<string, QueuedItem>();
  private readonly leaseMs: Millis;

  constructor(opts: InMemoryQueueOpts = {}) {
    this.leaseMs = opts.leaseMs ?? DEFAULT_QUEUE_LEASE_MS;
  }

  async enqueue(
    runId: RunId,
    stepId: StepId,
    opts?: QueueEnqueueOpts,
  ): Promise<void> {
    const now = Date.now();
    const item: QueuedItem = {
      receipt: crypto.randomUUID(),
      runId,
      stepId,
      payload: opts?.payload ?? null,
      attempt: opts?.attempt ?? 1,
      readCount: 0,
      enqueuedAt: now,
      visibleAt: now + (opts?.delayMs ?? 0),
      ...(opts?.flowId !== undefined ? { flowId: opts.flowId } : {}),
    };
    this.pending.push(item);
  }

  async dequeue(opts: QueueDequeueOpts): Promise<readonly QueueMessage[]> {
    const now = Date.now();
    const claimed: QueueMessage[] = [];
    for (
      let i = 0;
      i < this.pending.length && claimed.length < opts.count;
      i++
    ) {
      const item = this.pending[i];
      if (item === undefined) continue;
      if (item.visibleAt > now) continue;
      const delivered: QueuedItem = {
        ...item,
        readCount: item.readCount + 1,
        visibleAt: now + this.leaseMs,
      };
      this.pending.splice(i, 1);
      i--;
      this.leased.set(item.receipt, delivered);
      claimed.push(delivered);
    }
    return claimed;
  }

  async ack(receipt: string): Promise<void> {
    this.leased.delete(receipt);
  }

  async nack(receipt: string, opts?: { delayMs?: Millis }): Promise<void> {
    const item = this.leased.get(receipt);
    if (!item) return;
    this.leased.delete(receipt);
    const requeued: QueuedItem = {
      ...item,
      visibleAt: Date.now() + (opts?.delayMs ?? 0),
    };
    this.pending.push(requeued);
  }

  async extend(receipt: string, leaseMs: Millis): Promise<void> {
    const item = this.leased.get(receipt);
    if (!item) return;
    this.leased.set(receipt, { ...item, visibleAt: Date.now() + leaseMs });
  }

  async inspect(runId: RunId): Promise<readonly QueueInspectEntry[]> {
    const all = [...this.pending, ...this.leased.values()];
    return all
      .filter((m) => m.runId === runId)
      .map((m) => ({
        stepId: m.stepId,
        attempt: m.attempt,
        readCount: m.readCount,
        visibleAt: new Date(m.visibleAt),
      }));
  }
}

export class InMemoryClock implements Clock {
  now(): Date {
    return new Date();
  }

  async sleep(ms: Millis, signal?: AbortSignal): Promise<void> {
    if (signal?.aborted) {
      throw signal.reason ?? new Error("aborted");
    }
    return new Promise<void>((resolve, reject) => {
      const timer = setTimeout(() => {
        signal?.removeEventListener("abort", onAbort);
        resolve();
      }, ms);
      const onAbort = () => {
        clearTimeout(timer);
        reject(signal?.reason ?? new Error("aborted"));
      };
      signal?.addEventListener("abort", onAbort, { once: true });
    });
  }
}
