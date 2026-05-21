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
  RunStatus,
  RunSummary,
  SerializedError,
  StepCanceledFact,
  StepCompletedFact,
  StepFailedFact,
  StepId,
  StepState,
  Store,
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

export class InMemoryStore implements Store {
  private readonly facts = new Map<string, Fact[]>();
  private readonly outputs = new Map<string, Json>();
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
  private readonly leaseMs: Millis;

  constructor(opts: InMemoryStoreOpts = {}) {
    this.leaseMs = opts.leaseMs ?? DEFAULT_STORE_LEASE_MS;
  }

  async appendFact(runId: RunId, fact: Fact): Promise<void> {
    const list = this.facts.get(runId) ?? [];
    list.push(fact);
    this.facts.set(runId, list);
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
      this.outputs.delete(`${runId}::${fact.stepId}`);
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
        const cancelFact: FlowCanceledByConcurrencyFact = {
          kind: "flow.canceled",
          cause: "concurrency",
          runId: priorRunId,
          at: fact.at,
          canceledByRunId: runId,
          concurrencyKey: concurrency.key,
        };
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

  async listChildren(parentRunId: RunId): Promise<ReadonlyArray<RunId>> {
    const set = this.childrenByParent.get(parentRunId);
    if (set === undefined) return [];
    return Array.from(set);
  }

  async loadRunState(runId: RunId): Promise<RunState> {
    return projectRunState(runId, this.facts.get(runId) ?? []);
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

  async completeStep(
    runId: RunId,
    stepId: StepId,
    output: Json,
    fact: Fact,
  ): Promise<void> {
    this.outputs.set(`${runId}::${stepId}`, output);
    await this.appendFact(runId, fact);
  }

  async failStep(
    runId: RunId,
    _stepId: StepId,
    _error: SerializedError,
    fact: Fact,
  ): Promise<void> {
    await this.appendFact(runId, fact);
  }

  async getStepOutput(runId: RunId, stepId: StepId): Promise<Json | null> {
    return this.outputs.get(`${runId}::${stepId}`) ?? null;
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
    stepId: StepId,
    _attempt: AttemptNumber,
    body: (tx: Tx) => Promise<{
      readonly output: T;
      readonly fact: StepCompletedFact | StepFailedFact | StepCanceledFact;
    }>,
  ): Promise<T> {
    const result = await body(undefined as unknown as Tx);
    if (result.fact.kind === "step.completed") {
      await this.completeStep(runId, stepId, result.output, result.fact);
    } else if (result.fact.kind === "step.failed") {
      await this.failStep(runId, stepId, result.fact.error, result.fact);
    } else {
      await this.appendFact(runId, result.fact);
    }
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
    const statuses =
      where.status === undefined
        ? undefined
        : Array.isArray(where.status)
          ? where.status
          : [where.status as RunStatus];

    const matches = (summary: RunSummary): boolean => {
      if (where.flowId !== undefined && summary.flowId !== where.flowId)
        return false;
      if (statuses !== undefined && !statuses.includes(summary.status))
        return false;
      if (
        where.input !== undefined &&
        !containsJson(summary.input, where.input)
      )
        return false;
      return true;
    };

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
      deleteByRunPrefix(this.outputs, v.runId);
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
}

function deleteByRunPrefix<V>(map: Map<string, V>, runId: RunId): void {
  const prefix = `${runId}::`;
  for (const key of map.keys()) {
    if (key.startsWith(prefix)) map.delete(key);
  }
}

function summarize(runId: RunId, facts: readonly Fact[]): RunSummary | null {
  const first = facts[0];
  if (first === undefined || first.kind !== "flow.started") return null;
  const projected = projectRunState(runId, facts);
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
    status: projected.status,
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

export function projectRunState(
  runId: RunId,
  facts: readonly Fact[],
): RunState {
  let flowId = "";
  let status: RunStatus = "pending";
  let flowHash: string | undefined;
  let codeVersion: string | undefined;
  const steps: Record<string, StepState> = {};

  for (const fact of facts) {
    switch (fact.kind) {
      case "flow.started":
        flowId = fact.flowId;
        status = "running";
        flowHash = fact.flowHash;
        codeVersion = fact.codeVersion;
        break;
      case "flow.completed":
        status = "completed";
        break;
      case "flow.failed":
        status = "failed";
        break;
      case "flow.canceled":
        status = "canceled";
        break;
      case "step.started":
        steps[fact.stepId] = {
          stepId: fact.stepId,
          status: "running",
          attempts: fact.attempt,
        };
        break;
      case "step.completed":
        steps[fact.stepId] = {
          stepId: fact.stepId,
          status: "completed",
          attempts: fact.attempt,
          output: fact.output,
        };
        break;
      case "step.failed":
        steps[fact.stepId] = {
          stepId: fact.stepId,
          status: "failed",
          attempts: fact.attempt,
          error: fact.error,
        };
        break;
      case "step.canceled":
        steps[fact.stepId] = {
          stepId: fact.stepId,
          status: "canceled",
          attempts: fact.attempt,
          ...(fact.error !== undefined ? { error: fact.error } : {}),
        };
        break;
      case "step.skipped":
        steps[fact.stepId] = {
          stepId: fact.stepId,
          status: "skipped",
          attempts: 0,
        };
        break;
      case "step.reset":
        delete steps[fact.stepId];
        break;
      case "step.retried":
      case "step.abort-requested":
      case "signal.sent":
      case "signal.received":
      case "once.recorded":
      case "match.arm-selected":
        break;
    }
  }

  return {
    runId,
    flowId,
    status,
    steps,
    facts,
    ...(flowHash !== undefined ? { flowHash } : {}),
    ...(codeVersion !== undefined ? { codeVersion } : {}),
  };
}

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
      enqueuedAt: now,
      visibleAt: now + (opts?.delayMs ?? 0),
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
      const next: QueuedItem = { ...item, visibleAt: now + this.leaseMs };
      this.pending.splice(i, 1);
      i--;
      this.leased.set(item.receipt, next);
      claimed.push(item);
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
