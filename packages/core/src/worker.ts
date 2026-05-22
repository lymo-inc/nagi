import { type DispatchDeps, type Dispatcher, makeDispatcher } from "./dispatch";
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

  constructor(
    private readonly deps: WorkerDeps,
    config?: WorkerConfig,
  ) {
    this.concurrency = Math.max(1, config?.concurrency ?? DEFAULT_CONCURRENCY);
    this.pollIntervalMs = config?.pollIntervalMs ?? DEFAULT_POLL_INTERVAL_MS;
    this.signal = config?.signal;
    this.dispatcher = makeDispatcher(deps);
  }

  async run(): Promise<void> {
    while (!this.aborted()) {
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

  private aborted(): boolean {
    return this.signal?.aborted === true;
  }

  private async sleep(ms: Millis): Promise<void> {
    try {
      await this.deps.clock.sleep(ms, this.signal);
    } catch {}
  }
}
