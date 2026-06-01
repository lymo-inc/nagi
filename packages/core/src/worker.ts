import { type DispatchDeps, type Dispatcher, makeDispatcher } from "./dispatch";
import { NagiFlowSnapshotGoneError } from "./errors";
import type {
  Clock,
  Millis,
  QueueMessage,
  Worker,
  WorkerConfig,
  WorkerRunOnceOpts,
  WorkerRunResult,
  WorkerRunUntilEmptyOpts,
} from "./types";

const DEFAULT_CONCURRENCY = 4;
const DEFAULT_POLL_INTERVAL_MS: Millis = 1_000;
const DEFAULT_TIMER_SWEEP_INTERVAL_MS: Millis = 30_000;

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
  private readonly signal: AbortSignal | undefined;
  private readonly dispatcher: Dispatcher;
  private readonly timerSweepIntervalMs: Millis;
  private lastTimerSweepAt: number;

  constructor(
    private readonly deps: WorkerDeps,
    config?: WorkerConfig,
  ) {
    this.concurrency = Math.max(1, config?.concurrency ?? DEFAULT_CONCURRENCY);
    this.pollIntervalMs = config?.pollIntervalMs ?? DEFAULT_POLL_INTERVAL_MS;
    this.signal = config?.signal;
    this.dispatcher = makeDispatcher(deps);
    this.timerSweepIntervalMs =
      config?.timerSweepIntervalMs ?? DEFAULT_TIMER_SWEEP_INTERVAL_MS;
    // Anchor the first sweep one interval out, like the lease-reaper sleeps
    // before its first pass — no sweep storm at boot.
    this.lastTimerSweepAt = this.deps.clock.now().getTime();
  }

  async run(): Promise<void> {
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

      const messages = await this.dequeue(slots);
      if (messages.length === 0) {
        await this.sleep(this.pollIntervalMs);
        continue;
      }

      for (const msg of messages) this.fire(msg);
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

  private async pump(opts: {
    shouldContinue: (processed: number) => boolean;
    batchSize: (processed: number) => number;
  }): Promise<WorkerRunResult> {
    let processed = 0;
    while (!this.aborted() && opts.shouldContinue(processed)) {
      const messages = await this.dequeue(opts.batchSize(processed));
      if (messages.length === 0) break;

      await Promise.all(messages.map((m) => this.dispatchSafely(m)));
      processed += messages.length;
    }
    return { processed };
  }

  private async dequeue(count: number): Promise<readonly QueueMessage[]> {
    return this.deps.queue.dequeue({ count: Math.max(1, count) });
  }

  private fire(msg: QueueMessage): void {
    this.inFlight++;
    void this.dispatchSafely(msg).finally(() => {
      this.inFlight = Math.max(0, this.inFlight - 1);
    });
  }

  private async dispatchSafely(msg: QueueMessage): Promise<void> {
    try {
      await this.dispatcher.dispatchMessage(msg);
    } catch (err) {
      if (err instanceof NagiFlowSnapshotGoneError) {
        // Nack (do not emit step.failed) so a frozen-version worker can pick
        // the message up. Logged at warn so consumer ops see the diagnostic
        // hash pair and grep/restart against the pinned code.
        this.deps.emitLog({
          level: "warn",
          msg: "worker.dispatch: flow snapshot gone — nacking",
          attrs: {
            runId: err.runId,
            flowId: err.flowId,
            pinnedHash: err.pinnedHash,
            currentHash: err.currentHash,
          },
        });
        try {
          await this.deps.queue.nack(msg.receipt);
        } catch {}
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
