// In-memory implementations of the four plugin contracts.
//
// Edge-compliant: only Web Standards (`Map`, `crypto.randomUUID`, `setTimeout`,
// `Date`, `AbortSignal`). No `node:*` imports.
//
// Functional enough to run trivial flows in tests / examples / dev with zero
// infra. Real-world durability still requires the adapter packages.

import type {
  AttemptNumber,
  ClaimToken,
  Clock,
  Fact,
  Json,
  Millis,
  Queue,
  QueueDequeueOpts,
  QueueEnqueueOpts,
  QueueMessage,
  RunId,
  RunState,
  RunStatus,
  SerializedError,
  StepId,
  StepState,
  Store,
  Trigger,
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
  private readonly leaseMs: Millis;

  constructor(opts: InMemoryStoreOpts = {}) {
    this.leaseMs = opts.leaseMs ?? DEFAULT_STORE_LEASE_MS;
  }

  async appendFact(runId: RunId, fact: Fact): Promise<void> {
    const list = this.facts.get(runId) ?? [];
    list.push(fact);
    this.facts.set(runId, list);
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

  async completeStep(runId: RunId, stepId: StepId, output: Json, fact: Fact): Promise<void> {
    this.outputs.set(`${runId}::${stepId}`, output);
    await this.appendFact(runId, fact);
  }

  async failStep(runId: RunId, _stepId: StepId, _error: SerializedError, fact: Fact): Promise<void> {
    await this.appendFact(runId, fact);
  }

  async getStepOutput(runId: RunId, stepId: StepId): Promise<Json | null> {
    return this.outputs.get(`${runId}::${stepId}`) ?? null;
  }

  async recordOnce(runId: RunId, stepId: StepId, scope: string, value: Json): Promise<void> {
    this.onces.set(`${runId}::${stepId}::${scope}`, value);
  }

  async getOnce(runId: RunId, stepId: StepId, scope: string): Promise<Json | null> {
    return this.onces.get(`${runId}::${stepId}::${scope}`) ?? null;
  }
}

function projectRunState(runId: RunId, facts: readonly Fact[]): RunState {
  let flowId = "";
  let status: RunStatus = "pending";
  const steps: Record<string, StepState> = {};

  for (const fact of facts) {
    switch (fact.kind) {
      case "flow.started":
        flowId = fact.flowId;
        status = "running";
        break;
      case "flow.completed":
        status = "completed";
        break;
      case "flow.failed":
        status = "failed";
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
      case "step.skipped":
        steps[fact.stepId] = {
          stepId: fact.stepId,
          status: "skipped",
          attempts: 0,
        };
        break;
      case "step.retried":
      case "signal.sent":
      case "signal.received":
      case "once.recorded":
      case "match.arm-selected":
        // Don't affect the step-state projection directly; live in the fact log only.
        break;
    }
  }

  return { runId, flowId, status, steps, facts };
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

  async enqueue(runId: RunId, stepId: StepId, opts?: QueueEnqueueOpts): Promise<void> {
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
    for (let i = 0; i < this.pending.length && claimed.length < opts.count; i++) {
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
    // attempt is owned by the dispatcher (store-side tracking) — nack does not
    // increment. The same message returns to the queue on lease expiry / nack.
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
  /**
   * Wires `schedule()` wake-ups to a trigger. When the persistent timer fires,
   * the clock calls `trigger.fire(runId)` so scheduler subscribers can resume
   * the run. Without this, `schedule()` is a no-op on the worker loop and any
   * step that depends on it (time-based gates, long-delayed retries) will
   * stall in tests.
   */
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

  /** Clear any pending scheduled timers. Call from test teardown. */
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

  /**
   * Test-only: synchronously dispatch a wake-up to all subscribers.
   * Real triggers fire from a Store change (NOTIFY/LISTEN, polling, etc.).
   */
  fire(runId: RunId): void {
    for (const h of this.handlers) h(runId);
  }
}
