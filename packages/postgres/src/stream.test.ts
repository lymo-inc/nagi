import { InMemoryStreamHub, type RunId, type StepId } from "@nagi-js/core";
import { describe, expect, it } from "vitest";
import {
  applyFrame,
  decodeFrame,
  encodeFrame,
  type StreamFrame,
} from "./stream";

const R = "run-1" as RunId;
const S = "gen" as StepId;

async function drain(
  iter: AsyncIterable<unknown>,
  max: number,
): Promise<unknown[]> {
  const out: unknown[] = [];
  for await (const e of iter) {
    out.push(e);
    if (out.length >= max) break;
  }
  return out;
}

describe("stream frame codec", () => {
  it("round-trips every frame kind", () => {
    const frames: StreamFrame[] = [
      { k: "chunk", r: R, s: S, c: { token: "hi" } },
      { k: "retry", r: R, s: S, a: 2 },
      { k: "ok", r: R, s: S },
      { k: "err", r: R, s: S, e: { name: "E", message: "m" } },
      { k: "run", r: R },
    ];
    for (const f of frames) {
      expect(decodeFrame(encodeFrame(f))).toEqual(f);
    }
  });

  it("returns null for malformed payloads rather than throwing", () => {
    // Frames come off a database connection, so a truncated or foreign payload
    // must not tear down the listener.
    expect(decodeFrame("not json")).toBeNull();
    expect(decodeFrame("null")).toBeNull();
    expect(decodeFrame("123")).toBeNull();
    expect(decodeFrame('{"no":"kind"}')).toBeNull();
    expect(decodeFrame('{"k":')).toBeNull();
  });
});

describe("applyFrame drives the hub", () => {
  it("delivers chunks to a subscriber", async () => {
    const hub = new InMemoryStreamHub();
    const sub = hub.subscribeStream(R, S);
    applyFrame(hub, { k: "chunk", r: R, s: S, c: { token: "a" } });
    applyFrame(hub, { k: "ok", r: R, s: S });
    expect(await drain(sub, 5)).toEqual([
      { kind: "chunk", chunk: { token: "a" } },
    ]);
  });

  it("closes the stream on an ok frame", async () => {
    const hub = new InMemoryStreamHub();
    const sub = hub.subscribeStream(R, S);
    applyFrame(hub, { k: "ok", r: R, s: S });
    expect(await drain(sub, 5)).toEqual([]);
  });

  it("emits an error event then closes on an err frame", async () => {
    const hub = new InMemoryStreamHub();
    const sub = hub.subscribeStream(R, S);
    const error = { name: "Boom", message: "upstream died" };
    applyFrame(hub, { k: "err", r: R, s: S, e: error });
    expect(await drain(sub, 5)).toEqual([{ kind: "error", error }]);
  });

  it("closes every channel of a run on a run frame", async () => {
    const hub = new InMemoryStreamHub();
    const a = hub.subscribeStream(R, "a" as StepId);
    const b = hub.subscribeStream(R, "b" as StepId);
    applyFrame(hub, { k: "run", r: R });
    expect(await drain(a, 5)).toEqual([]);
    expect(await drain(b, 5)).toEqual([]);
  });

  it("drops a superseded attempt's replay on a retry frame", async () => {
    const hub = new InMemoryStreamHub();
    const sub = hub.subscribeStream(R, S);
    applyFrame(hub, { k: "chunk", r: R, s: S, c: "stale" });
    applyFrame(hub, { k: "retry", r: R, s: S, a: 2 });
    applyFrame(hub, { k: "chunk", r: R, s: S, c: "fresh" });
    applyFrame(hub, { k: "ok", r: R, s: S });

    const late = hub.subscribeStream(R, S, { replayBuffered: true });
    expect(await drain(late, 5)).toEqual([]);

    expect(await drain(sub, 5)).toEqual([
      { kind: "chunk", chunk: "stale" },
      { kind: "retry", attempt: 2 },
      { kind: "chunk", chunk: "fresh" },
    ]);
  });
});
