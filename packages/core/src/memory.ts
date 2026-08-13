import { Facts } from "./facts";
import { decideExpiredLeaseAction, type ReapedLease } from "./lease-reaper";
import type {
  RunDescription,
  RunView,
  StepRunStatus,
  StepView,
} from "./run-view";
import { decideSignal, decideTimeout } from "./signals";
import {
  attemptOf,
  errorOf,
  foldRun,
  isTerminalRun,
  outputOf,
  runStatusOf,
  stepStateOf,
  stepStatusOf,
} from "./state";
import { InMemoryStreamHub } from "./stream-hub";
import type {
  AttemptNumber,
  ClaimToken,
  Clock,
  ConcurrencyMode,
  Fact,
  FlowCanceledByConcurrencyFact,
  FlowStartedFact,
  GlobalFact,
  Json,
  Millis,
  PrunableStatus,
  PruneOpts,
  PruneResult,
  QueryRunsOpts,
  QueryRunsResult,
  Queue,
  QueueDequeueOpts,
  QueueEnqueueOpts,
  QueueMessage,
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
  Trigger,
  Tx,
} from "./types";

interface MemoryLease {
  readonly token: ClaimToken;
  readonly expiresAt: number;
}

export interface InMemoryStoreOpts {
  readonly leaseMs?: Millis;
}

const DEFAULT_STORE_LEASE_MS: Millis = 60_000;

export class InMemoryStore implements Store, StreamTransport {
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
  private readonly childrenByParent = new Map<RunId, Set<RunId>>();
  private readonly summaries = new Map<RunId, RunSummary>();
  private readonly streamHub = new InMemoryStreamHub();
  // Durable signal-timeout deadlines, keyed `${runId}::${stepId}` → fire_at.
  // The Postgres analogue is the nagi.timer table; this is its in-memory twin
  // (distinct from the InMemoryClock's setTimeout-based scheduler timers).
  private readonly signalTimers = new Map<string, Date>();
  private readonly leaseMs: Millis;

  constructor(opts: InMemoryStoreOpts = {}) {
    this.leaseMs = opts.leaseMs ?? DEFAULT_STORE_LEASE_MS;
  }

  async appendFact(runId: RunId, fact: Fact): Promise<void> {
    const list = this.facts.get(runId) ?? [];
    list.push(fact);
    this.facts.set(runId, list);
    // Drive the stream hub off the durable fact log. These fire for ALL steps;
    // the hub's close*/closeRun are safe no-ops for non-streaming steps (no
    // channel exists, so nothing is created and nothing leaks).
    switch (fact.kind) {
      case "step.completed":
        this.streamHub.closeOk(runId, fact.stepId);
        break;
      case "step.failed":
        // Only written on terminal failure (retries use step.retried), so this
        // is always retries-exhausted.
        this.streamHub.closeError(runId, fact.stepId, fact.error);
        break;
      case "step.retried":
        // The retry event carries the NEXT attempt number.
        this.streamHub.signalRetry(
          runId,
          fact.stepId,
          (fact.attempt + 1) as AttemptNumber,
        );
        break;
      case "flow.completed":
      case "flow.failed":
      case "flow.canceled":
        // Close every still-open channel for the run so a subscriber to a
        // skipped/typo'd/never-emitting step never hangs.
        this.streamHub.closeRun(runId);
        break;
    }
    if (
      fact.kind === "flow.completed" ||
      fact.kind === "flow.failed" ||
      fact.kind === "flow.canceled"
    ) {
      const slot = this.keyByActiveRun.get(runId);
      if (slot !== undefined) {
        this.keyByActiveRun.delete(runId);
        if (this.activeByKey.get(slot) === runId) {
          this.activeByKey.delete(slot);
        }
      }
    }
    if (fact.kind === "step.reset") {
      const leasePrefix = `${runId}::${fact.stepId}::`;
      for (const key of this.leases.keys()) {
        if (key.startsWith(leasePrefix)) this.leases.delete(key);
      }
      this.signalTimers.delete(`${runId}::${fact.stepId}`);
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
    readonly canceled: ReadonlyArray<{
      readonly runId: RunId;
      readonly fact: FlowCanceledByConcurrencyFact;
    }>;
  }> {
    if (this.facts.has(runId) || this.summaries.has(runId)) {
      return { started: false, canceled: [] };
    }

    const canceled: Array<{
      runId: RunId;
      fact: FlowCanceledByConcurrencyFact;
    }> = [];
    if (concurrency !== undefined) {
      const slot = `${fact.flowId}::${concurrency.key}`;
      const priorRunId = this.activeByKey.get(slot);
      if (priorRunId !== undefined) {
        const cancelFact = Facts.flowCanceledByConcurrency({
          runId: priorRunId,
          at: fact.at,
          canceledByRunId: runId,
          concurrencyKey: concurrency.key,
        });
        await this.appendFact(priorRunId, cancelFact);
        canceled.push({ runId: priorRunId, fact: cancelFact });
      }
      this.activeByKey.set(slot, runId);
      this.keyByActiveRun.set(runId, slot);
    }

    this.facts.set(runId, [fact]);
    const parentRunId = fact.parent?.runId;
    if (parentRunId !== undefined) {
      const set = this.childrenByParent.get(parentRunId) ?? new Set<RunId>();
      set.add(runId);
      this.childrenByParent.set(parentRunId, set);
    }
    return { started: true, canceled };
  }

  // In-memory has no real tx; we share the underlying maps with tryStartRun,
  // so the implementation is intentionally a straight delegate. The `tx`
  // parameter is accepted for shape-compat with the Store contract.
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
    readonly canceled: ReadonlyArray<{
      readonly runId: RunId;
      readonly fact: FlowCanceledByConcurrencyFact;
    }>;
  }> {
    return this.tryStartRun(runId, fact, concurrency);
  }

  async listChildren(parentRunId: RunId): Promise<ReadonlyArray<RunId>> {
    const set = this.childrenByParent.get(parentRunId);
    if (set === undefined) return [];
    return Array.from(set);
  }

  // Does a non-terminal child exist for this (parentRunId, stepId)? Used by the
  // reaper to leave a subflow step parked while its child still runs (instead
  // of re-dispatching it at attempt+1). childrenByParent is keyed by parent run
  // only, so filter on the child's parent.stepId.
  private hasActiveChild(parentRunId: RunId, parentStepId: StepId): boolean {
    const childIds = this.childrenByParent.get(parentRunId);
    if (childIds === undefined) return false;
    for (const childId of childIds) {
      const factList = this.facts.get(childId);
      if (factList === undefined) continue;
      const child = foldRun(childId, factList);
      if (child.parent?.stepId === parentStepId && !isTerminalRun(child)) {
        return true;
      }
    }
    return false;
  }

  async loadRunState(runId: RunId): Promise<RunState> {
    return foldRun(runId, this.facts.get(runId) ?? []);
  }

  async claimStep(
    runId: RunId,
    stepId: StepId,
    attempt: AttemptNumber,
  ): Promise<ClaimToken | null> {
    const key = `${runId}::${stepId}::${attempt}`;
    const now = Date.now();
    const existing = this.leases.get(key);
    if (existing && existing.expiresAt > now) {
      return null;
    }
    const token = `lease-${crypto.randomUUID()}` as ClaimToken;
    this.leases.set(key, { token, expiresAt: now + this.leaseMs });
    return token;
  }

  async extendLease(
    runId: RunId,
    stepId: StepId,
    attempt: AttemptNumber,
    leaseMs: Millis,
  ): Promise<void> {
    const key = `${runId}::${stepId}::${attempt}`;
    const existing = this.leases.get(key);
    if (existing === undefined) return; // idempotent: settled/reaped → no-op
    this.leases.set(key, {
      token: existing.token,
      expiresAt: Date.now() + leaseMs,
    });
  }

  async sweepLeases(args: {
    readonly now: Date;
    readonly queue: Queue;
    readonly limit?: number;
  }): Promise<readonly ReapedLease[]> {
    const { now, queue, limit = 100 } = args;
    const reaped: ReapedLease[] = [];

    const candidates: Array<{
      key: string;
      runId: RunId;
      stepId: StepId;
      attempt: AttemptNumber;
      expiresAt: Date;
    }> = [];
    for (const [key, lease] of this.leases) {
      if (candidates.length >= limit) break;
      const parts = key.split("::");
      if (parts.length !== 3) continue;
      const [runId, stepId, attemptStr] = parts as [string, string, string];
      candidates.push({
        key,
        runId: runId as RunId,
        stepId,
        attempt: Number(attemptStr) as AttemptNumber,
        expiresAt: new Date(lease.expiresAt),
      });
    }

    for (const c of candidates) {
      const factList = this.facts.get(c.runId);
      const state = factList ? foldRun(c.runId, factList) : null;
      const ss = state ? stepStateOf(state, c.stepId) : null;
      const stepStatus: StepRunStatus =
        ss === null ? "pending" : stepStatusOf(ss);
      const decision = decideExpiredLeaseAction({
        lease: {
          runId: c.runId,
          stepId: c.stepId,
          attempt: c.attempt,
          expiresAt: c.expiresAt,
        },
        stepStatus,
        childActive: this.hasActiveChild(c.runId, c.stepId),
        now,
      });
      if (decision.tag === "skip") continue;

      this.leases.delete(c.key);
      await this.appendFact(
        c.runId,
        Facts.leaseReaped({
          runId: c.runId,
          stepId: c.stepId,
          attempt: c.attempt,
          at: now,
          reapedAt: now,
        }),
      );
      await queue.enqueue(c.runId, c.stepId, {
        attempt: decision.nextAttempt,
        delayMs: decision.backoffMs,
        ...(state?.flowId !== undefined ? { flowId: state.flowId } : {}),
      });
      reaped.push({
        runId: c.runId,
        stepId: c.stepId,
        attempt: c.attempt,
        nextAttempt: decision.nextAttempt,
      });
    }

    return reaped;
  }

  async settleStep(
    runId: RunId,
    _stepId: StepId,
    fact: StepCompletedFact | StepFailedFact,
  ): Promise<void> {
    await this.appendFact(runId, fact);
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
        if (decision.fact !== null) await this.appendFact(runId, decision.fact);
        return decision.result;
      case "deliver":
        await this.appendFact(runId, decision.received);
        await this.appendFact(runId, decision.completed);
        // The step resolved by delivery; drop its timeout so the next sweep
        // doesn't fold a now-completed step just to no-op (Postgres parity).
        this.signalTimers.delete(`${runId}::${stepId}`);
        return decision.result;
    }
  }

  async upsertTimer(runId: RunId, stepId: StepId, fireAt: Date): Promise<void> {
    // Keep the earliest deadline (Postgres parity: ON CONFLICT DO NOTHING) so a
    // lease-reap re-dispatch of a still-parked signal can't push the deadline
    // out. A genuine restart deletes the row first (step.reset).
    const key = `${runId}::${stepId}`;
    if (!this.signalTimers.has(key)) this.signalTimers.set(key, fireAt);
  }

  async sweepSignalTimeouts(args: {
    readonly now: Date;
    readonly limit?: number;
  }): Promise<readonly TimedOutSignal[]> {
    const { now, limit = 100 } = args;
    const timedOut: TimedOutSignal[] = [];

    const due: Array<{ key: string; runId: RunId; stepId: StepId }> = [];
    for (const [key, fireAt] of this.signalTimers) {
      if (due.length >= limit) break;
      if (fireAt > now) continue;
      const sep = key.indexOf("::");
      due.push({
        key,
        runId: key.slice(0, sep) as RunId,
        stepId: key.slice(sep + 2),
      });
    }

    // Single process, single thread: fold → decide → append runs with no
    // interleaving await, the same atomicity argument settleSignal relies on.
    for (const { key, runId, stepId } of due) {
      const fireAt = this.signalTimers.get(key);
      if (fireAt === undefined) continue;
      const runState = foldRun(runId, this.facts.get(runId) ?? []);
      const decision = decideTimeout({ runState, stepId, fireAt, at: now });
      this.signalTimers.delete(key);
      if (decision.kind === "noop") continue;
      await this.appendFact(runId, decision.fact);
      // Drop the parked step's stale lease, mirroring settleSignal's deliver
      // path (and the Postgres timeout path).
      const leasePrefix = `${runId}::${stepId}::`;
      for (const k of this.leases.keys()) {
        if (k.startsWith(leasePrefix)) this.leases.delete(k);
      }
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
    this.onces.set(`${runId}::${stepId}::${scope}`, value);
  }

  async getOnce(
    runId: RunId,
    stepId: StepId,
    scope: string,
  ): Promise<Json | null> {
    return this.onces.get(`${runId}::${stepId}::${scope}`) ?? null;
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
    await this.appendFact(runId, result.fact);
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
      (where.input === undefined || containsJson(summary.input, where.input));

    const summaries: RunSummary[] = [];
    for (const [runId, factList] of this.facts) {
      const summary = summarize(runId as RunId, factList);
      if (summary === null) continue;
      if (matches(summary)) summaries.push(summary);
    }
    for (const summary of this.summaries.values()) {
      if (matches(summary)) summaries.push(summary);
    }

    summaries.sort((a, b) => {
      const t = b.startedAt.getTime() - a.startedAt.getTime();
      if (t !== 0) return t;
      return a.runId < b.runId ? 1 : a.runId > b.runId ? -1 : 0;
    });

    if (opts.latest === true) {
      return { runs: summaries.slice(0, 1), cursor: null };
    }

    const limit = clampLimit(opts.limit);
    let start = 0;
    if (opts.cursor !== undefined) {
      const c = decodeCursor(opts.cursor);
      start = summaries.findIndex(
        (s) =>
          s.startedAt.getTime() < c.t ||
          (s.startedAt.getTime() === c.t && s.runId < c.r),
      );
      if (start === -1) start = summaries.length;
    }

    const page = summaries.slice(start, start + limit);
    const hasMore = start + limit < summaries.length;
    const last = page[page.length - 1];
    const cursor =
      hasMore && last !== undefined
        ? encodeCursor({ t: last.startedAt.getTime(), r: last.runId })
        : null;
    return { runs: page, cursor };
  }

  async describe(runId: RunId): Promise<RunDescription> {
    const factList = this.facts.get(runId);
    if (factList === undefined || factList.length === 0) return null;
    const first = factList[0];
    if (first === undefined || first.kind !== "flow.started") return null;

    const state = foldRun(runId, factList);
    const status = runStatusOf(state);

    let completedAt: Date | undefined;
    let output: Json | undefined;
    let error: Json | undefined;
    let canceledByRunId: RunId | undefined;
    let concurrencyKey: string | undefined;
    for (let i = factList.length - 1; i >= 0; i--) {
      const f = factList[i];
      if (f === undefined) continue;
      if (f.kind === "flow.completed") {
        completedAt = f.at;
        output = f.output;
        break;
      }
      if (f.kind === "flow.failed") {
        completedAt = f.at;
        error = f.error as unknown as Json;
        break;
      }
      if (f.kind === "flow.canceled") {
        completedAt = f.at;
        if (f.cause === "concurrency") {
          canceledByRunId = f.canceledByRunId;
        }
        break;
      }
    }
    const slot = this.keyByActiveRun.get(runId);
    if (slot !== undefined) {
      const sep = slot.indexOf("::");
      if (sep >= 0) concurrencyKey = slot.slice(sep + 2);
    }
    if (concurrencyKey === undefined) {
      for (const f of factList) {
        if (f.kind === "flow.canceled" && f.cause === "concurrency") {
          concurrencyKey = f.concurrencyKey;
          break;
        }
      }
    }

    const childSet = this.childrenByParent.get(runId);
    const children: RunId[] = childSet ? Array.from(childSet) : [];
    const parent = first.parent;

    const run: RunView = {
      runId,
      flowId: first.flowId,
      flowHash: first.flowHash ?? "",
      status,
      startedAt: first.at,
      input: first.input,
      children,
      ...(completedAt !== undefined ? { completedAt } : {}),
      ...(output !== undefined ? { output } : {}),
      ...(error !== undefined ? { error } : {}),
      ...(canceledByRunId !== undefined ? { canceledByRunId } : {}),
      ...(concurrencyKey !== undefined ? { concurrencyKey } : {}),
      ...(parent !== undefined ? { parent } : {}),
    };

    const steps: StepView[] = [];
    const startedAtByStep = new Map<StepId, Date>();
    const completedAtByStep = new Map<StepId, Date>();
    for (const f of factList) {
      if (f.kind === "step.started" && !startedAtByStep.has(f.stepId)) {
        startedAtByStep.set(f.stepId, f.at);
      }
      if (
        (f.kind === "step.completed" ||
          f.kind === "step.failed" ||
          f.kind === "step.canceled" ||
          f.kind === "step.skipped") &&
        !completedAtByStep.has(f.stepId)
      ) {
        completedAtByStep.set(f.stepId, f.at);
      }
    }
    const now = Date.now();
    for (const [stepId, stepState] of Object.entries(state.steps)) {
      const status = stepStatusOf(stepState);
      const attempt = attemptOf(stepState);
      const out = outputOf(stepState);
      const err = errorOf(stepState);
      const startedAt = startedAtByStep.get(stepId);
      const completedAt = completedAtByStep.get(stepId);
      let lease: { readonly expiresAt: Date } | undefined;
      const leasePrefix = `${runId}::${stepId}::`;
      let bestExpiry = 0;
      for (const [key, l] of this.leases) {
        if (!key.startsWith(leasePrefix)) continue;
        if (l.expiresAt > now && l.expiresAt > bestExpiry)
          bestExpiry = l.expiresAt;
      }
      if (bestExpiry > 0) lease = { expiresAt: new Date(bestExpiry) };
      steps.push({
        stepId,
        attempt,
        status,
        ...(startedAt !== undefined ? { startedAt } : {}),
        ...(completedAt !== undefined ? { completedAt } : {}),
        ...(out !== null ? { output: out } : {}),
        ...(err !== undefined ? { error: err as unknown as Json } : {}),
        ...(lease !== undefined ? { lease } : {}),
      });
    }
    steps.sort((a, b) => {
      const ta = a.startedAt?.getTime() ?? Number.POSITIVE_INFINITY;
      const tb = b.startedAt?.getTime() ?? Number.POSITIVE_INFINITY;
      if (ta !== tb) return ta - tb;
      return a.stepId < b.stepId ? -1 : a.stepId > b.stepId ? 1 : 0;
    });

    return { run, steps };
  }

  async pruneFacts(opts: Required<PruneOpts>): Promise<PruneResult> {
    const olderThanMs = opts.olderThan.getTime();
    const statusSet = new Set<PrunableStatus>(opts.statuses);

    interface Victim {
      readonly runId: RunId;
      readonly summary: RunSummary;
      readonly factCount: number;
      readonly parentRunId: RunId | undefined;
    }
    const victims: Victim[] = [];
    for (const [runId, factList] of this.facts) {
      const summary = summarize(runId as RunId, factList);
      if (summary === null) continue;
      if (summary.completedAt === null) continue;
      if (!statusSet.has(summary.status as PrunableStatus)) continue;
      if (summary.completedAt.getTime() >= olderThanMs) continue;
      const first = factList[0];
      const parentRunId =
        first !== undefined && first.kind === "flow.started"
          ? first.parent?.runId
          : undefined;
      victims.push({
        runId: runId as RunId,
        summary,
        factCount: factList.length,
        parentRunId,
      });
    }

    let runsPruned = 0;
    let factsPruned = 0;
    for (const v of victims) {
      this.facts.delete(v.runId);
      deleteByRunPrefix(this.onces, v.runId);
      deleteByRunPrefix(this.leases, v.runId);
      this.childrenByParent.delete(v.runId);
      if (v.parentRunId !== undefined) {
        const siblings = this.childrenByParent.get(v.parentRunId);
        if (siblings !== undefined) {
          siblings.delete(v.runId);
          if (siblings.size === 0) this.childrenByParent.delete(v.parentRunId);
        }
      }
      if (opts.keepSummary) {
        this.summaries.set(v.runId, v.summary);
      } else {
        this.summaries.delete(v.runId);
      }
      runsPruned += 1;
      factsPruned += v.factCount;
    }

    return { runsPruned, factsPruned };
  }

  subscribeStream(
    runId: RunId,
    stepId: StepId,
    opts?: { readonly replayBuffered?: boolean },
  ): AsyncIterable<StreamEvent<Json>> {
    // Durable facts decide whether the stream is over, not the hub (which may
    // never have held a channel for this step). Delegating to a terminal step
    // would open a channel that hangs, so hand back an empty iterable instead.
    const state = foldRun(runId, this.facts.get(runId) ?? []);
    const stepStatus = stepStatusOf(stepStateOf(state, stepId));
    const runIsTerminal = isTerminalRun(state);
    if (
      runIsTerminal ||
      stepStatus === "completed" ||
      stepStatus === "failed" ||
      stepStatus === "canceled" ||
      stepStatus === "skipped"
    ) {
      return EMPTY_CLOSED_STREAM;
    }
    return this.streamHub.subscribeStream(runId, stepId, opts);
  }

  publishChunk(runId: RunId, stepId: StepId, chunk: Json): void {
    this.streamHub.publishChunk(runId, stepId, chunk);
  }
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

function summarize(runId: RunId, facts: readonly Fact[]): RunSummary | null {
  const first = facts[0];
  if (first === undefined || first.kind !== "flow.started") return null;
  const projected = foldRun(runId, facts);
  let completedAt: Date | null = null;
  for (let i = facts.length - 1; i >= 0; i--) {
    const f = facts[i];
    if (f === undefined) continue;
    if (
      f.kind === "flow.completed" ||
      f.kind === "flow.failed" ||
      f.kind === "flow.canceled"
    ) {
      completedAt = f.at;
      break;
    }
  }
  return {
    runId,
    flowId: first.flowId,
    status: runStatusOf(projected),
    startedAt: first.at,
    completedAt,
    input: first.input,
  };
}

const DEFAULT_LIMIT = 50;
const MAX_LIMIT = 500;

function clampLimit(limit: number | undefined): number {
  if (limit === undefined) return DEFAULT_LIMIT;
  if (!Number.isInteger(limit) || limit < 1) return DEFAULT_LIMIT;
  return Math.min(limit, MAX_LIMIT);
}

interface DecodedCursor {
  readonly t: number;
  readonly r: string;
}

function encodeCursor(c: DecodedCursor): string {
  const bytes = new TextEncoder().encode(JSON.stringify(c));
  let binary = "";
  for (let i = 0; i < bytes.length; i++) {
    binary += String.fromCharCode(bytes[i] as number);
  }
  return btoa(binary)
    .replace(/\+/g, "-")
    .replace(/\//g, "_")
    .replace(/=+$/, "");
}

function decodeCursor(s: string): DecodedCursor {
  try {
    const b64 = s.replace(/-/g, "+").replace(/_/g, "/");
    const binary = atob(b64);
    const bytes = new Uint8Array(binary.length);
    for (let i = 0; i < binary.length; i++) bytes[i] = binary.charCodeAt(i);
    const parsed: unknown = JSON.parse(new TextDecoder().decode(bytes));
    if (
      typeof parsed === "object" &&
      parsed !== null &&
      typeof (parsed as DecodedCursor).t === "number" &&
      typeof (parsed as DecodedCursor).r === "string"
    ) {
      return parsed as DecodedCursor;
    }
    throw new Error("malformed cursor body");
  } catch (err) {
    throw new Error(
      `queryRuns: invalid cursor — ${err instanceof Error ? err.message : String(err)}`,
    );
  }
}

function containsJson(haystack: Json, needle: Json): boolean {
  if (Array.isArray(needle)) {
    if (!Array.isArray(haystack)) return false;
    return needle.every((n) => haystack.some((h) => containsJson(h, n)));
  }
  if (needle !== null && typeof needle === "object") {
    if (
      haystack === null ||
      Array.isArray(haystack) ||
      typeof haystack !== "object"
    )
      return false;
    return Object.entries(needle).every(
      ([k, v]) => k in haystack && containsJson(haystack[k] as Json, v as Json),
    );
  }
  return haystack === needle;
}

export { foldRun as projectRunState } from "./state";

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
}

export interface InMemoryClockOpts {
  readonly trigger?: InMemoryTrigger;
}

export class InMemoryClock implements Clock {
  private readonly timers = new Map<string, ReturnType<typeof setTimeout>>();
  private readonly trigger: InMemoryTrigger | undefined;

  constructor(opts: InMemoryClockOpts = {}) {
    this.trigger = opts.trigger;
  }

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

  async schedule(at: Date, runId: RunId, stepId: StepId): Promise<void> {
    const key = `${runId}::${stepId}`;
    const existing = this.timers.get(key);
    if (existing !== undefined) clearTimeout(existing);

    const delay = Math.max(0, at.getTime() - Date.now());
    const handle = setTimeout(() => {
      this.timers.delete(key);
      this.trigger?.fire(runId);
    }, delay);
    this.timers.set(key, handle);
  }

  dispose(): void {
    for (const t of this.timers.values()) clearTimeout(t);
    this.timers.clear();
  }
}

export class InMemoryTrigger implements Trigger {
  private handlers: Array<(runId: RunId) => void> = [];

  subscribe(handler: (runId: RunId) => void): () => void {
    this.handlers.push(handler);
    return () => {
      this.handlers = this.handlers.filter((h) => h !== handler);
    };
  }

  fire(runId: RunId): void {
    for (const h of this.handlers) h(runId);
  }
}
