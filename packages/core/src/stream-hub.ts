import type { StreamEffect } from "./facts";
import { isStepTerminal, isTerminalRun, stepStateOf } from "./state";
import type {
  AttemptNumber,
  Json,
  RunId,
  RunState,
  StepId,
  StreamEvent,
} from "./types";

// Durable facts decide whether a step's stream is over, not the hub, which may
// never have held a channel for it (the close fired before anyone listened).
export function isStreamOver(state: RunState, stepId: StepId): boolean {
  return isTerminalRun(state) || isStepTerminal(stepStateOf(state, stepId));
}

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

// Closed keys are remembered so a chunk landing after its close (a handler that
// outlives its step, a NOTIFY behind the close frame) is dropped instead of
// reopening a channel nothing will ever close. Bounded because late chunks
// trail their close closely; a late subscriber is already settled by the
// adapters' durable liveness checks.
export const STREAM_CLOSED_KEYS_CAP = 1024;

interface Channel {
  readonly subscribers: Set<Subscriber>;
  readonly replay: StreamEvent<Json>[];
}

const EMPTY_CLOSED_STREAM: AsyncIterable<StreamEvent<Json>> = {
  [Symbol.asyncIterator](): AsyncIterator<StreamEvent<Json>> {
    return { next: async () => ({ value: undefined, done: true }) };
  },
};

export class InMemoryStreamHub {
  // Open channels only: closing one deletes it and records its key.
  private readonly channels = new Map<string, Channel>();
  private readonly closedKeys = new Set<string>();

  private static key(runId: RunId, stepId: StepId): string {
    return `${runId}::${stepId}`;
  }

  private openChannel(key: string): Channel | undefined {
    if (this.closedKeys.has(key)) return undefined;
    let channel = this.channels.get(key);
    if (channel === undefined) {
      channel = { subscribers: new Set(), replay: [] };
      this.channels.set(key, channel);
    }
    return channel;
  }

  // Returns the subscribers to end, or undefined when no channel was open.
  private closeChannel(key: string): Subscriber[] | undefined {
    const channel = this.channels.get(key);
    if (channel === undefined) return undefined;
    this.channels.delete(key);
    this.closedKeys.add(key);
    if (this.closedKeys.size > STREAM_CLOSED_KEYS_CAP) {
      const oldest = this.closedKeys.values().next().value;
      if (oldest !== undefined) this.closedKeys.delete(oldest);
    }
    const subs = [...channel.subscribers];
    // An outstanding iterator still references its channel; don't let it pin
    // the buffers.
    channel.subscribers.clear();
    channel.replay.length = 0;
    return subs;
  }

  publishChunk(runId: RunId, stepId: StepId, chunk: Json): void {
    const channel = this.openChannel(InMemoryStreamHub.key(runId, stepId));
    if (channel === undefined) return;
    const event: StreamEvent<Json> = { kind: "chunk", chunk };
    channel.replay.push(event);
    if (channel.replay.length > STREAM_REPLAY_BUFFER_CAP)
      channel.replay.shift();
    for (const sub of channel.subscribers) sub.push(event);
  }

  // NB: signalRetry and the close* methods are no-ops when no channel exists.
  // Both adapters fire them for EVERY step — Postgres cluster-wide — including
  // non-streaming ones, so creating a channel here would leak one per step.
  signalRetry(runId: RunId, stepId: StepId, attempt: AttemptNumber): void {
    const channel = this.channels.get(InMemoryStreamHub.key(runId, stepId));
    if (channel === undefined) return;
    // Reset replay so a late subscriber can't replay a superseded attempt.
    channel.replay.length = 0;
    const event: StreamEvent<Json> = { kind: "retry", attempt };
    for (const sub of channel.subscribers) sub.push(event);
  }

  closeOk(runId: RunId, stepId: StepId): void {
    const subs = this.closeChannel(InMemoryStreamHub.key(runId, stepId));
    for (const sub of subs ?? []) sub.close();
  }

  closeError(runId: RunId, stepId: StepId): void {
    const subs = this.closeChannel(InMemoryStreamHub.key(runId, stepId));
    const event: StreamEvent<Json> = { kind: "error" };
    for (const sub of subs ?? []) {
      sub.push(event);
      sub.close();
    }
  }

  // A reset step runs again: forget that it closed, and drop the superseded
  // run's buffered chunks from any channel still open.
  reopen(runId: RunId, stepId: StepId): void {
    const key = InMemoryStreamHub.key(runId, stepId);
    this.closedKeys.delete(key);
    const channel = this.channels.get(key);
    if (channel !== undefined) channel.replay.length = 0;
  }

  apply(runId: RunId, effect: StreamEffect): void {
    switch (effect.tag) {
      case "close-ok":
        this.closeOk(runId, effect.stepId);
        return;
      case "close-error":
        this.closeError(runId, effect.stepId);
        return;
      case "retry":
        this.signalRetry(runId, effect.stepId, effect.nextAttempt);
        return;
      case "reopen":
        this.reopen(runId, effect.stepId);
        return;
      case "close-run":
        this.closeRun(runId);
        return;
    }
  }

  closeRun(runId: RunId): void {
    const prefix = `${runId}::`;
    for (const key of this.channels.keys()) {
      if (!key.startsWith(prefix)) continue;
      for (const sub of this.closeChannel(key) ?? []) sub.close();
    }
  }

  subscribeStream(
    runId: RunId,
    stepId: StepId,
    opts?: { readonly replayBuffered?: boolean },
  ): AsyncIterable<StreamEvent<Json>> {
    const channel = this.openChannel(InMemoryStreamHub.key(runId, stepId));
    if (channel === undefined) return EMPTY_CLOSED_STREAM;

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
