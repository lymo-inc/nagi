import type {
  AttemptNumber,
  Json,
  RunId,
  SerializedError,
  StepId,
  StreamEvent,
} from "./types";

/**
 * Maximum number of {@link StreamEvent} entries a single subscriber buffers
 * before backpressure kicks in. On overflow the oldest *chunk* is dropped and a
 * `{ kind: "dropped", count }` marker is surfaced before the next event (O3).
 * Control events (`retry`/`error`) are never dropped.
 */
export const STREAM_SUBSCRIBER_BUFFER_CAP = 256;

/**
 * Maximum number of chunks retained in a channel's replay buffer for late
 * subscribers that opt in via `{ replayBuffered: true }` (D7). Bounded by item
 * count for v1. The buffer is dropped when the channel closes, so replaying
 * after termination yields nothing.
 */
export const STREAM_REPLAY_BUFFER_CAP = 256;

/**
 * A pending "wake" handle: a Promise plus the resolver that fulfils it. The
 * iterator awaits `promise` when its buffer is empty; producers call `resolve`
 * after pushing (or on close) to wake the consumer. Rebuilt after each wake so
 * subsequent waits get a fresh Promise. No `node:events`/EventEmitter — plain
 * Promise hand-off.
 */
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

/** Per-subscriber state: an independent FIFO cursor over the channel (D6). */
class Subscriber {
  /** FIFO buffer of events still to be yielded. */
  private readonly buffer: StreamEvent<Json>[] = [];
  /** Chunks dropped (oldest-first) since the last `dropped` marker was queued. */
  private pendingDropped = 0;
  /** Set once the channel closes; the iterator ends after draining `buffer`. */
  private closed = false;
  /** Resolver woken on push/close; null when no consumer is currently waiting. */
  private wake: Wake | null = null;

  /** Enqueue an event, applying drop-oldest backpressure for chunks (O3). */
  push(event: StreamEvent<Json>): void {
    if (this.closed) return;
    if (this.buffer.length >= STREAM_SUBSCRIBER_BUFFER_CAP) {
      // Overflow: drop the oldest *chunk* to make room. Never drop control
      // events (retry/error) — scan from the front for the first chunk.
      const idx = this.buffer.findIndex((e) => e.kind === "chunk");
      if (idx === -1) {
        // Buffer is full of un-droppable control events; the incoming event is
        // a chunk (control events arrive via close/retry which are rare and
        // bounded). Drop the incoming chunk itself rather than a control event.
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

  /** Mark the channel closed for this subscriber and wake any waiter. */
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

  /**
   * Dequeue the next event, materializing a pending `dropped` marker first so a
   * lagging consumer learns it lagged before seeing the next real event (O3).
   * Resolves to `null` once the channel is closed and fully drained (D3).
   */
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
      // Buffer empty and still open: park until the producer signals.
      const w = makeWake();
      this.wake = w;
      await w.promise;
    }
  }
}

/** Per-`(runId, stepId)` channel: its subscribers plus a bounded replay buffer. */
interface Channel {
  readonly subscribers: Set<Subscriber>;
  /** Bounded ring of chunk events for opt-in late-subscriber replay (D7). */
  readonly replay: StreamEvent<Json>[];
  /** Set once the channel reaches a terminal state; new subscribes get nothing. */
  closed: boolean;
}

/**
 * In-process broadcast hub for ephemeral streaming-step chunks (D5). State is
 * keyed by `(runId, stepId)`; events never cross channels. Built from plain
 * Promises + async iterators — no `node:events`, no dependencies, Edge-safe.
 *
 * Lifecycle (wired by dispatch/runtime in Phase C):
 * `publishChunk` / `signalRetry` during the run, then exactly one of
 * `closeOk` / `closeError` when the durable terminal fact lands.
 */
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

  /**
   * Fan out a raw chunk to every current subscriber as `{ kind: "chunk" }` and
   * append it to the bounded replay buffer. Synchronous and non-blocking: a
   * slow/absent consumer never stalls the producer or other consumers (O3/D6).
   * A no-op once the channel is closed (emit-after-terminal is swallowed).
   */
  publishChunk(runId: RunId, stepId: StepId, chunk: Json): void {
    const channel = this.getOrCreate(runId, stepId);
    if (channel.closed) return;
    const event: StreamEvent<Json> = { kind: "chunk", chunk };
    channel.replay.push(event);
    if (channel.replay.length > STREAM_REPLAY_BUFFER_CAP)
      channel.replay.shift();
    for (const sub of channel.subscribers) sub.push(event);
  }

  /**
   * Fan out a `{ kind: "retry", attempt }` marker so consumers can clear and
   * re-render before the next attempt's chunks (O5). A retry resets the replay
   * buffer so a late subscriber doesn't replay a superseded attempt's chunks.
   * A no-op once closed.
   */
  signalRetry(runId: RunId, stepId: StepId, attempt: AttemptNumber): void {
    const channel = this.getOrCreate(runId, stepId);
    if (channel.closed) return;
    channel.replay.length = 0;
    const event: StreamEvent<Json> = { kind: "retry", attempt };
    for (const sub of channel.subscribers) sub.push(event);
  }

  /**
   * Close the channel on `step.completed`: drain buffered events to each
   * subscriber, then end their iterators. No event emitted. Drops the replay
   * buffer and marks the channel closed so later subscribes yield nothing (D3).
   *
   * NB: this is a no-op when no channel exists. `InMemoryStore.appendFact` fires
   * it for *every* `step.completed` fact — including non-streaming steps that
   * never published — so creating a channel here would leak one per such step.
   * Closed-ness is authoritative in the durable facts, not the hub, so a
   * never-created channel is correct: `InMemoryStore.subscribeStream` consults
   * the terminal fact before delegating here.
   */
  closeOk(runId: RunId, stepId: StepId): void {
    const channel = this.channels.get(InMemoryStreamHub.key(runId, stepId));
    if (channel === undefined || channel.closed) return;
    channel.closed = true;
    channel.replay.length = 0;
    for (const sub of channel.subscribers) sub.close();
    channel.subscribers.clear();
  }

  /**
   * Close the channel on terminal `step.failed` (retries exhausted): fan out a
   * final `{ kind: "error", error }`, then end every iterator. Drops the replay
   * buffer and marks the channel closed (D3/O4).
   *
   * Like {@link closeOk}, a no-op when no channel exists (fired for every
   * terminal `step.failed`, streaming or not — see that method's note).
   */
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

  /**
   * Close *every* channel belonging to a run on a run-terminal fact
   * (`flow.completed`/`flow.failed`/`flow.canceled`). Guarantees a subscriber to
   * a skipped, typo'd, or never-emitting step never hangs past run end: any
   * still-open channel for the run is drained and ended (clean close, no `error`
   * event — per-step failures already surfaced their own `error` via
   * {@link closeError}). A no-op for runs with no live channels.
   */
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

  /**
   * Subscribe to a channel. Default future-only; with `{ replayBuffered: true }`
   * the new subscriber is seeded with the channel's current replay buffer (as
   * `chunk` events) before live ones, subject to the same drop-oldest cap (D7).
   * Subscribing to an already-closed channel yields an immediately-ended,
   * empty stream (the replay buffer was dropped on close).
   */
  subscribeStream(
    runId: RunId,
    stepId: StepId,
    opts?: { readonly replayBuffered?: boolean },
  ): AsyncIterable<StreamEvent<Json>> {
    const channel = this.getOrCreate(runId, stepId);

    // Already closed → an iterator that yields nothing and ends immediately.
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
          // Called when a consumer breaks early; unsubscribe so it stops
          // accumulating (no leak).
          return: async (): Promise<IteratorResult<StreamEvent<Json>>> => {
            sub.close();
            remove();
            return { value: undefined, done: true };
          },
        };
      },
    };
  }
}
