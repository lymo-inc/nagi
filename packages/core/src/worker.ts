import { type DispatchDeps, type Dispatcher, makeDispatcher } from "./dispatch";
import { NagiFlowSnapshotGoneError, serializeError } from "./errors";
import { Facts } from "./facts";
import {
  type Backoff,
  defaultSnapshotGonePolicy,
  dequeueBackoff,
} from "./retry";
import type {
  Clock,
  Millis,
  QueueMessage,
  SnapshotGonePolicy,
  Worker,
  WorkerConfig,
  WorkerRunOnceOpts,
  WorkerRunResult,
  WorkerRunUntilEmptyOpts,
} from "./types";

const DEFAULT_CONCURRENCY = 4;
const DEFAULT_POLL_INTERVAL_MS: Millis = 1_000;
const DEFAULT_TIMER_SWEEP_INTERVAL_MS: Millis = 30_000;
const SNAPSHOT_GONE_FALLBACK_NACK_DELAY_MS: Millis = 30_000;

export interface WorkerDeps extends DispatchDeps {
  readonly clock: Clock;
}

export function makeWorker(deps: WorkerDeps, config?: WorkerConfig): Worker {
  return new WorkerImpl(deps, config);
}

class WorkerImpl implements Worker {
  private inFlight = 0;
  private readonly concurrency: number;
  private readonly pollIntervalMs: Millis;
  private readonly dequeueBackoffMs: Backoff;
  private readonly signal: AbortSignal | undefined;
  private readonly dispatcher: Dispatcher;
  private readonly timerSweepIntervalMs: Millis;
  private readonly snapshotGonePolicy: SnapshotGonePolicy;
  private readonly maxPerFlow: number | undefined;
  private readonly inFlightByFlow = new Map<string, number>();
  private lastTimerSweepAt: number;

  constructor(
    private readonly deps: WorkerDeps,
    config?: WorkerConfig,
  ) {
    this.concurrency = Math.max(1, config?.concurrency ?? DEFAULT_CONCURRENCY);
    this.pollIntervalMs = config?.pollIntervalMs ?? DEFAULT_POLL_INTERVAL_MS;
    this.dequeueBackoffMs = dequeueBackoff(this.pollIntervalMs);
    this.signal = config?.signal;
    this.dispatcher = makeDispatcher(deps);
    this.snapshotGonePolicy =
      config?.snapshotGonePolicy ?? defaultSnapshotGonePolicy;
    this.maxPerFlow =
      config?.maxConcurrencyPerFlow !== undefined
        ? Math.max(1, config.maxConcurrencyPerFlow)
        : undefined;
    this.timerSweepIntervalMs =
      config?.timerSweepIntervalMs ?? DEFAULT_TIMER_SWEEP_INTERVAL_MS;
    // Anchor the first sweep one interval out, like the lease-reaper sleeps
    // before its first pass — no sweep storm at boot.
    this.lastTimerSweepAt = this.deps.clock.now().getTime();
  }

  // Per-flow blast-radius bound (maxConcurrencyPerFlow). A message over its
  // flow's cap is DEFERRED — delayed nack, redelivered after ~pollIntervalMs —
  // not dropped. Messages without flowId (enqueued pre-upgrade) are exempt.
  // Deferral does raise pgmq read_ct, which the snapshot-gone policy reads;
  // that only matters if the same flow later goes snapshot-gone, where an
  // earlier fail is acceptable.
  private admitFlow(msg: QueueMessage): boolean {
    if (this.maxPerFlow === undefined || msg.flowId === undefined) return true;
    return (this.inFlightByFlow.get(msg.flowId) ?? 0) < this.maxPerFlow;
  }

  // Returns the release fn; caller must invoke it exactly once when done.
  private trackFlow(msg: QueueMessage): () => void {
    const { flowId } = msg;
    if (flowId === undefined) return () => {};
    this.inFlightByFlow.set(flowId, (this.inFlightByFlow.get(flowId) ?? 0) + 1);
    return () => {
      const n = (this.inFlightByFlow.get(flowId) ?? 1) - 1;
      if (n <= 0) this.inFlightByFlow.delete(flowId);
      else this.inFlightByFlow.set(flowId, n);
    };
  }

  private async defer(msg: QueueMessage): Promise<void> {
    try {
      await this.deps.queue.nack(msg.receipt, { delayMs: this.pollIntervalMs });
    } catch {}
  }

  // MUST settle only via the abort signal. A rejection escaping this loop
  // silently ends ALL flow processing in a process that otherwise looks
  // healthy — observed in prod as one transient `pgmq.read` connection
  // timeout killing the loop, then 37h of fleet-wide stuck runs because
  // nothing restarts it. Every await below is therefore guarded.
  async run(): Promise<void> {
    let dequeueFailures = 0;
    while (!this.aborted()) {
      // Time-based, not idle-gated: a worker pinned at full concurrency still
      // reaches this each iteration (the slots-full branch loops every ~50ms),
      // so signal timeouts fire on cadence regardless of load.
      await this.maybeSweepTimers();
      const slots = this.concurrency - this.inFlight;
      if (slots <= 0) {
        await this.sleep(50);
        continue;
      }

      let messages: readonly QueueMessage[];
      try {
        messages = await this.dequeue(slots);
      } catch (err) {
        dequeueFailures++;
        this.deps.emitLog({
          level: "error",
          msg: "worker.dequeue failed; backing off",
          attrs: {
            error: String(err),
            consecutiveFailures: dequeueFailures,
            backoffMs: this.dequeueBackoffMs(dequeueFailures),
          },
        });
        await this.sleep(this.dequeueBackoffMs(dequeueFailures));
        continue;
      }
      dequeueFailures = 0;

      if (messages.length === 0) {
        await this.sleep(this.pollIntervalMs);
        continue;
      }

      for (const msg of messages) {
        if (this.admitFlow(msg)) this.fire(msg);
        else await this.defer(msg);
      }
    }
    await this.drain();
  }

  async runOnce(opts?: WorkerRunOnceOpts): Promise<WorkerRunResult> {
    const limit = Math.max(1, opts?.maxSteps ?? this.concurrency);
    return this.pump({
      shouldContinue: (processed) => processed < limit,
      batchSize: (processed) => Math.min(limit - processed, this.concurrency),
    });
  }

  async runUntilEmpty(
    opts?: WorkerRunUntilEmptyOpts,
  ): Promise<WorkerRunResult> {
    const deadline = opts?.deadline;
    return this.pump({
      shouldContinue: () => deadline === undefined || Date.now() < deadline,
      batchSize: () => this.concurrency,
    });
  }

  // Unlike run(), a dequeue failure here PROPAGATES. These are bounded calls
  // that hand a result back to an awaiting caller, so a rejection is observed,
  // not silent — the outage shape run() guards against cannot happen. The
  // alternatives are both worse: retrying would let runUntilEmpty() hang for
  // the length of a database outage, and swallowing would report an
  // unreachable queue as a drained one.
  private async pump(opts: {
    shouldContinue: (processed: number) => boolean;
    batchSize: (processed: number) => number;
  }): Promise<WorkerRunResult> {
    let processed = 0;
    while (!this.aborted() && opts.shouldContinue(processed)) {
      const messages = await this.dequeue(opts.batchSize(processed));
      if (messages.length === 0) break;

      const admitted: Array<{
        readonly m: QueueMessage;
        readonly release: () => void;
      }> = [];
      for (const m of messages) {
        // Sequential admission: each admit sees the batch-mates already
        // admitted, so a whole batch of one flow cannot leapfrog the cap.
        if (this.admitFlow(m)) {
          admitted.push({ m, release: this.trackFlow(m) });
        } else {
          await this.defer(m);
        }
      }
      await Promise.all(
        admitted.map(({ m }) => this.dispatchSafely(m)),
      ).finally(() => {
        for (const { release } of admitted) release();
      });
      processed += admitted.length;
      if (admitted.length === 0) break;
    }
    return { processed };
  }

  private async dequeue(count: number): Promise<readonly QueueMessage[]> {
    return this.deps.queue.dequeue({ count: Math.max(1, count) });
  }

  private fire(msg: QueueMessage): void {
    this.inFlight++;
    const release = this.trackFlow(msg);
    void this.dispatchSafely(msg).finally(() => {
      release();
      this.inFlight = Math.max(0, this.inFlight - 1);
    });
  }

  private async dispatchSafely(msg: QueueMessage): Promise<void> {
    try {
      await this.dispatcher.dispatchMessage(msg);
    } catch (err) {
      if (err instanceof NagiFlowSnapshotGoneError) {
        try {
          await this.handleSnapshotGone(msg, err);
        } catch (handleErr) {
          // A broken policy or store failure must neither escape (fire() has
          // no rejection handler) nor tight-loop the message — the delayed
          // nack keeps the redelivery cadence bounded until it's fixed.
          this.deps.emitLog({
            level: "error",
            msg: "worker.dispatch: snapshot-gone handling failed",
            attrs: { runId: err.runId, error: String(handleErr) },
          });
          try {
            await this.deps.queue.nack(msg.receipt, {
              delayMs: SNAPSHOT_GONE_FALLBACK_NACK_DELAY_MS,
            });
          } catch {}
        }
        return;
      }
      this.deps.emitLog({
        level: "error",
        msg: "worker.dispatch threw uncaught",
        attrs: { error: String(err) },
      });
      try {
        await this.deps.queue.nack(msg.receipt);
      } catch {}
    }
  }

  // A snapshot-gone message is retried (nack, delayed) only while the policy
  // says a frozen-version worker might still claim it; past that budget the
  // run is terminally failed with the snapshot-gone error and the message
  // acked, so poison messages cannot outlive the deploy that orphaned them.
  // (Terminal runs never reach here — dispatchMessage acks them itself.)
  private async handleSnapshotGone(
    msg: QueueMessage,
    err: NagiFlowSnapshotGoneError,
  ): Promise<void> {
    const attrs = {
      runId: err.runId,
      flowId: err.flowId,
      pinnedHash: err.pinnedHash,
      currentHash: err.currentHash,
      readCount: msg.readCount,
    };
    const disposition = this.snapshotGonePolicy(msg.readCount);

    if (disposition.action === "retry") {
      // Do not emit step.failed: the step never ran. Logged at warn so
      // consumer ops see the diagnostic hash pair while the deploy settles.
      this.deps.emitLog({
        level: "warn",
        msg: "worker.dispatch: flow snapshot gone — nacking for a frozen-version worker",
        attrs,
      });
      try {
        await this.deps.queue.nack(msg.receipt, {
          delayMs: disposition.delayMs,
        });
      } catch {}
      return;
    }

    this.deps.emitLog({
      level: "error",
      msg: "worker.dispatch: flow snapshot gone — redelivery budget exhausted, failing run",
      attrs,
    });
    const serialized = serializeError(err);
    await this.deps.store.appendFact(
      msg.runId,
      Facts.flowFailed(msg.runId, serialized, this.deps.clock.now()),
    );
    // Best-effort: a parked parent subflow step is only woken by propagation.
    // The parent's flow may be gone too (same deploy) — then its own messages
    // meet this same policy, so swallow and log rather than block the ack.
    try {
      await this.dispatcher.propagateToParent(msg.runId, {
        kind: "failed",
        error: serialized,
      });
    } catch (propagateErr) {
      this.deps.emitLog({
        level: "warn",
        msg: "worker.dispatch: snapshot-gone fail could not propagate to parent",
        attrs: { ...attrs, error: String(propagateErr) },
      });
    }
    await this.deps.queue.ack(msg.receipt);
  }

  private async drain(): Promise<void> {
    while (this.inFlight > 0) {
      await this.deps.clock.sleep(50);
    }
  }

  // Sweep elapsed signal timeouts on cadence. Runs inline in the dispatch loop
  // (not fired concurrently) so two sweeps never overlap; at 30s with a handful
  // of timers the cost is negligible. Errors are logged, never thrown — a failed
  // sweep must not kill the worker; the next tick retries.
  private async maybeSweepTimers(): Promise<void> {
    if (this.timerSweepIntervalMs <= 0) return;
    const now = this.deps.clock.now();
    if (now.getTime() - this.lastTimerSweepAt < this.timerSweepIntervalMs)
      return;
    this.lastTimerSweepAt = now.getTime();
    try {
      await this.dispatcher.sweepTimers(now);
    } catch (err) {
      this.deps.emitLog({
        level: "warn",
        msg: "worker: signal-timeout sweep failed",
        attrs: { error: String(err) },
      });
    }
  }

  private aborted(): boolean {
    return this.signal?.aborted === true;
  }

  private async sleep(ms: Millis): Promise<void> {
    try {
      await this.deps.clock.sleep(ms, this.signal);
    } catch {}
  }
}
