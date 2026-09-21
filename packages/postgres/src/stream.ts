import type {
  AttemptNumber,
  InMemoryStreamHub,
  Json,
  RunId,
  SerializedError,
  StepId,
} from "@nagi-js/core";

// A LISTEN connection, supplied by the consumer. Used for BOTH the chunk
// channel and the run-event channel — one connection, two channels.
//
// Kysely deliberately hides the driver: PostgresConnection holds the `pg`
// client privately and exposes no notification event, and this package keeps
// `pg` a devDependency so neon / postgres.js / pglite users are not forced onto
// it. So the adapter cannot open a listening connection by itself — the caller
// wires one, the same way it already wires `db`.
//
// `listen` MUST deliver every NOTIFY on `channel` to `onNotify` until the
// returned disposer is called.
export interface StreamListener {
  listen(
    channel: string,
    onNotify: (payload: string) => void,
  ): Promise<() => void | Promise<void>>;
}

// PostgreSQL caps a NOTIFY payload at 8000 bytes. We reserve headroom for the
// envelope's own keys so the limit a caller has to reason about is the chunk's
// serialized size, not the frame's.
export const MAX_CHUNK_BYTES = 7000;

export type StreamFrame =
  | {
      readonly k: "chunk";
      readonly r: RunId;
      readonly s: StepId;
      readonly c: Json;
    }
  | {
      readonly k: "retry";
      readonly r: RunId;
      readonly s: StepId;
      readonly a: number;
    }
  | { readonly k: "ok"; readonly r: RunId; readonly s: StepId }
  | {
      readonly k: "err";
      readonly r: RunId;
      readonly s: StepId;
      readonly e: SerializedError;
    }
  | { readonly k: "run"; readonly r: RunId };

export function encodeFrame(frame: StreamFrame): string {
  return JSON.stringify(frame);
}

// Frames arrive from the database, not from a trusted caller, so a malformed or
// truncated payload must not take down the listening connection.
export function decodeFrame(payload: string): StreamFrame | null {
  try {
    const parsed = JSON.parse(payload) as StreamFrame;
    if (parsed === null || typeof parsed !== "object") return null;
    return typeof parsed.k === "string" ? parsed : null;
  } catch {
    return null;
  }
}

// Applies a decoded frame to the process-local hub. Split out from the store so
// it is testable without a database.
export function applyFrame(hub: InMemoryStreamHub, frame: StreamFrame): void {
  switch (frame.k) {
    case "chunk":
      hub.publishChunk(frame.r, frame.s, frame.c);
      return;
    case "retry":
      hub.signalRetry(frame.r, frame.s, frame.a as AttemptNumber);
      return;
    case "ok":
      hub.closeOk(frame.r, frame.s);
      return;
    case "err":
      hub.closeError(frame.r, frame.s, frame.e);
      return;
    case "run":
      hub.closeRun(frame.r);
      return;
  }
}
