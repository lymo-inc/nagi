import type {
  AttemptNumber,
  Json,
  RunId,
  SerializedError,
  StepId,
  StreamEvent,
} from "./types";

export const STREAM_SUBSCRIBER_BUFFER_CAP = 256;

export const STREAM_REPLAY_BUFFER_CAP = 256;

interface Wake {
  promise: Promise<void>;
  resolve: () => void;
}

function makeWake(): Wake {
  let resolve!: () => void;
  const promise = new Promise<void>((r) => {
    resolve = r;
  });
  return { promise, resolve };
}

class Subscriber {
  private readonly buffer: StreamEvent<Json>[] = [];
  private pendingDropped = 0;
  private closed = false;
  private wake: Wake | null = null;

  push(event: StreamEvent<Json>): void {
    if (this.closed) return;
    if (this.buffer.length >= STREAM_SUBSCRIBER_BUFFER_CAP) {
      // Never drop control events (retry/error): drop the oldest chunk instead.
      const idx = this.buffer.findIndex((e) => e.kind === "chunk");
      if (idx === -1) {
        // Buffer holds only control events; drop the incoming chunk itself.
        if (event.kind === "chunk") {
          this.pendingDropped += 1;
          this.signal();
          return;
        }
      } else {
        this.buffer.splice(idx, 1);
        this.pendingDropped += 1;
      }
    }
    this.buffer.push(event);
    this.signal();
  }

  close(): void {
    if (this.closed) return;
    this.closed = true;
    this.signal();
  }

  private signal(): void {
    if (this.wake !== null) {
      const w = this.wake;
      this.wake = null;
      w.resolve();
    }
  }

  async next(): Promise<StreamEvent<Json> | null> {
    for (;;) {
      if (this.pendingDropped > 0) {
        const count = this.pendingDropped;
        this.pendingDropped = 0;
        return { kind: "dropped", count };
      }
      const head = this.buffer.shift();
      if (head !== undefined) return head;
      if (this.closed) return null;
      const w = makeWake();
      this.wake = w;
      await w.promise;
    }
  }
}

interface Channel {
  readonly subscribers: Set<Subscriber>;
  readonly replay: StreamEvent<Json>[];
  closed: boolean;
}

export class InMemoryStreamHub {
  private readonly channels = new Map<string, Channel>();

  private static key(runId: RunId, stepId: StepId): string {
    return `${runId}::${stepId}`;
  }

  private getOrCreate(runId: RunId, stepId: StepId): Channel {
    const key = InMemoryStreamHub.key(runId, stepId);
    let channel = this.channels.get(key);
    if (channel === undefined) {
      channel = { subscribers: new Set(), replay: [], closed: false };
      this.channels.set(key, channel);
    }
    return channel;
  }

  publishChunk(runId: RunId, stepId: StepId, chunk: Json): void {
    const channel = this.getOrCreate(runId, stepId);
    if (channel.closed) return;
    const event: StreamEvent<Json> = { kind: "chunk", chunk };
    channel.replay.push(event);
    if (channel.replay.length > STREAM_REPLAY_BUFFER_CAP)
      channel.replay.shift();
    for (const sub of channel.subscribers) sub.push(event);
  }

  signalRetry(runId: RunId, stepId: StepId, attempt: AttemptNumber): void {
    const channel = this.getOrCreate(runId, stepId);
    if (channel.closed) return;
    // Reset replay so a late subscriber can't replay a superseded attempt.
    channel.replay.length = 0;
    const event: StreamEvent<Json> = { kind: "retry", attempt };
    for (const sub of channel.subscribers) sub.push(event);
  }

  // NB: no-op when no channel exists. InMemoryStore.appendFact fires this for
  // EVERY step.completed — including non-streaming steps that never published —
  // so creating a channel here would leak one per such step. Closed-ness is
  // authoritative in the durable facts, not the hub.
  closeOk(runId: RunId, stepId: StepId): void {
    const channel = this.channels.get(InMemoryStreamHub.key(runId, stepId));
    if (channel === undefined || channel.closed) return;
    channel.closed = true;
    channel.replay.length = 0;
    for (const sub of channel.subscribers) sub.close();
    channel.subscribers.clear();
  }

  closeError(runId: RunId, stepId: StepId, error: SerializedError): void {
    const channel = this.channels.get(InMemoryStreamHub.key(runId, stepId));
    if (channel === undefined || channel.closed) return;
    channel.closed = true;
    channel.replay.length = 0;
    const event: StreamEvent<Json> = { kind: "error", error };
    for (const sub of channel.subscribers) {
      sub.push(event);
      sub.close();
    }
    channel.subscribers.clear();
  }

  closeRun(runId: RunId): void {
    const prefix = `${runId}::`;
    for (const [key, channel] of this.channels) {
      if (!key.startsWith(prefix) || channel.closed) continue;
      channel.closed = true;
      channel.replay.length = 0;
      for (const sub of channel.subscribers) sub.close();
      channel.subscribers.clear();
    }
  }

  subscribeStream(
    runId: RunId,
    stepId: StepId,
    opts?: { readonly replayBuffered?: boolean },
  ): AsyncIterable<StreamEvent<Json>> {
    const channel = this.getOrCreate(runId, stepId);

    if (channel.closed) {
      return {
        [Symbol.asyncIterator](): AsyncIterator<StreamEvent<Json>> {
          return {
            next: async () => ({ value: undefined, done: true }),
          };
        },
      };
    }

    const sub = new Subscriber();
    if (opts?.replayBuffered === true) {
      for (const event of channel.replay) sub.push(event);
    }
    channel.subscribers.add(sub);

    const remove = (): void => {
      channel.subscribers.delete(sub);
    };

    return {
      [Symbol.asyncIterator](): AsyncIterator<StreamEvent<Json>> {
        return {
          next: async (): Promise<IteratorResult<StreamEvent<Json>>> => {
            const event = await sub.next();
            if (event === null) {
              remove();
              return { value: undefined, done: true };
            }
            return { value: event, done: false };
          },
          return: async (): Promise<IteratorResult<StreamEvent<Json>>> => {
            // Consumer broke early: unsubscribe so it stops accumulating.
            sub.close();
            remove();
            return { value: undefined, done: true };
          },
        };
      },
    };
  }
}
