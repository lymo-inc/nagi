import { describe, expect, it } from "vitest";
import { InMemoryStreamHub, STREAM_SUBSCRIBER_BUFFER_CAP } from "../stream-hub";
import type { AttemptNumber, Json, RunId, StepId, StreamEvent } from "../types";

const RUN = "run-1" as RunId;
const STEP = "gen" as StepId;

async function collect(
  iter: AsyncIterable<StreamEvent<Json>>,
): Promise<StreamEvent<Json>[]> {
  const out: StreamEvent<Json>[] = [];
  for await (const ev of iter) out.push(ev);
  return out;
}

const tick = (): Promise<void> => new Promise((r) => setTimeout(r, 0));

describe("InMemoryStreamHub — happy path & ordering", () => {
  it("subscribe-before-publish receives all chunks in emit order as {kind:chunk}", async () => {
    const hub = new InMemoryStreamHub();
    const sub = hub.subscribeStream(RUN, STEP);

    const got = collect(sub);
    hub.publishChunk(RUN, STEP, { t: "a" });
    hub.publishChunk(RUN, STEP, { t: "b" });
    hub.publishChunk(RUN, STEP, { t: "c" });
    hub.closeOk(RUN, STEP);

    expect(await got).toEqual([
      { kind: "chunk", chunk: { t: "a" } },
      { kind: "chunk", chunk: { t: "b" } },
      { kind: "chunk", chunk: { t: "c" } },
    ]);
  });
});

describe("InMemoryStreamHub — fan-out (D6)", () => {
  it("two subscribers each independently receive every chunk", async () => {
    const hub = new InMemoryStreamHub();
    const a = collect(hub.subscribeStream(RUN, STEP));
    const b = collect(hub.subscribeStream(RUN, STEP));

    hub.publishChunk(RUN, STEP, 1);
    hub.publishChunk(RUN, STEP, 2);
    hub.closeOk(RUN, STEP);

    const expected: StreamEvent<Json>[] = [
      { kind: "chunk", chunk: 1 },
      { kind: "chunk", chunk: 2 },
    ];
    expect(await a).toEqual(expected);
    expect(await b).toEqual(expected);
  });
});

describe("InMemoryStreamHub — late subscribers (D7)", () => {
  it("a late subscriber is future-only by default (misses earlier chunks)", async () => {
    const hub = new InMemoryStreamHub();
    hub.publishChunk(RUN, STEP, "early-1");
    hub.publishChunk(RUN, STEP, "early-2");

    const late = collect(hub.subscribeStream(RUN, STEP));
    await tick();
    hub.publishChunk(RUN, STEP, "live-1");
    hub.closeOk(RUN, STEP);

    expect(await late).toEqual([{ kind: "chunk", chunk: "live-1" }]);
  });

  it("with {replayBuffered:true} it gets earlier chunks then live ones, in order", async () => {
    const hub = new InMemoryStreamHub();
    hub.publishChunk(RUN, STEP, "early-1");
    hub.publishChunk(RUN, STEP, "early-2");

    const late = collect(
      hub.subscribeStream(RUN, STEP, { replayBuffered: true }),
    );
    await tick();
    hub.publishChunk(RUN, STEP, "live-1");
    hub.closeOk(RUN, STEP);

    expect(await late).toEqual([
      { kind: "chunk", chunk: "early-1" },
      { kind: "chunk", chunk: "early-2" },
      { kind: "chunk", chunk: "live-1" },
    ]);
  });
});

describe("InMemoryStreamHub — backpressure / overflow (O3)", () => {
  it("a non-consuming subscriber sees a {kind:dropped, count} marker before subsequent events", async () => {
    const hub = new InMemoryStreamHub();
    const iter = hub.subscribeStream(RUN, STEP)[Symbol.asyncIterator]();

    const overflow = 3;
    const total = STREAM_SUBSCRIBER_BUFFER_CAP + overflow;
    for (let i = 0; i < total; i++) hub.publishChunk(RUN, STEP, i);
    hub.closeOk(RUN, STEP);

    const events: StreamEvent<Json>[] = [];
    for (;;) {
      const r = await iter.next();
      if (r.done) break;
      events.push(r.value);
    }

    const first = events[0];
    expect(first).toEqual({ kind: "dropped", count: overflow });

    const chunks = events.slice(1);
    expect(chunks).toHaveLength(STREAM_SUBSCRIBER_BUFFER_CAP);
    expect(chunks[0]).toEqual({ kind: "chunk", chunk: overflow });
    expect(chunks[chunks.length - 1]).toEqual({
      kind: "chunk",
      chunk: total - 1,
    });
  });
});

describe("InMemoryStreamHub — termination & control events (D3/O4/O5)", () => {
  it("closeOk ends the for-await loop after draining", async () => {
    const hub = new InMemoryStreamHub();
    const got = collect(hub.subscribeStream(RUN, STEP));
    hub.publishChunk(RUN, STEP, "x");
    hub.closeOk(RUN, STEP);
    expect(await got).toEqual([{ kind: "chunk", chunk: "x" }]);
  });

  it("closeError delivers a final {kind:error} then ends", async () => {
    const hub = new InMemoryStreamHub();
    const got = collect(hub.subscribeStream(RUN, STEP));
    hub.publishChunk(RUN, STEP, "x");
    const error = { name: "Boom", message: "exhausted" };
    hub.closeError(RUN, STEP, error);

    expect(await got).toEqual([
      { kind: "chunk", chunk: "x" },
      { kind: "error", error },
    ]);
  });

  it("signalRetry delivers {kind:retry, attempt}", async () => {
    const hub = new InMemoryStreamHub();
    const got = collect(hub.subscribeStream(RUN, STEP));
    hub.publishChunk(RUN, STEP, "attempt1");
    hub.signalRetry(RUN, STEP, 2 as AttemptNumber);
    hub.publishChunk(RUN, STEP, "attempt2");
    hub.closeOk(RUN, STEP);

    expect(await got).toEqual([
      { kind: "chunk", chunk: "attempt1" },
      { kind: "retry", attempt: 2 },
      { kind: "chunk", chunk: "attempt2" },
    ]);
  });
});

describe("InMemoryStreamHub — subscribe after close (D3)", () => {
  it("after closeOk → empty, immediately-closed stream", async () => {
    const hub = new InMemoryStreamHub();
    hub.publishChunk(RUN, STEP, "gone");
    hub.closeOk(RUN, STEP);

    expect(await collect(hub.subscribeStream(RUN, STEP))).toEqual([]);
    expect(
      await collect(hub.subscribeStream(RUN, STEP, { replayBuffered: true })),
    ).toEqual([]);
  });

  it("after closeError → empty, immediately-closed stream (no replayed error)", async () => {
    const hub = new InMemoryStreamHub();
    hub.publishChunk(RUN, STEP, "gone");
    hub.closeError(RUN, STEP, { name: "E", message: "m" });

    expect(await collect(hub.subscribeStream(RUN, STEP))).toEqual([]);
  });
});

describe("InMemoryStreamHub — early break cleanup", () => {
  it("breaking out of the loop removes the subscriber; later publishes don't accumulate or throw", async () => {
    const hub = new InMemoryStreamHub();
    const iter = hub.subscribeStream(RUN, STEP);
    const collected: Json[] = [];

    const loop = (async () => {
      for await (const ev of iter) {
        if (ev.kind === "chunk") collected.push(ev.chunk);
        break;
      }
    })();
    hub.publishChunk(RUN, STEP, "first");
    await loop;
    expect(collected).toEqual(["first"]);

    expect(() => hub.publishChunk(RUN, STEP, "second")).not.toThrow();
    expect(() => hub.signalRetry(RUN, STEP, 2 as AttemptNumber)).not.toThrow();
    expect(() => hub.closeOk(RUN, STEP)).not.toThrow();
  });
});

describe("InMemoryStreamHub — close*/closeRun never create a channel (leak-free)", () => {
  it("closeOk on a never-published channel does not create one (subscribe stays live)", async () => {
    const hub = new InMemoryStreamHub();
    hub.closeOk(RUN, STEP);

    const got = collect(hub.subscribeStream(RUN, STEP));
    await tick();
    hub.publishChunk(RUN, STEP, "live");
    hub.closeOk(RUN, STEP);
    expect(await got).toEqual([{ kind: "chunk", chunk: "live" }]);
  });

  it("closeError / closeRun on a never-published channel are safe no-ops", () => {
    const hub = new InMemoryStreamHub();
    expect(() =>
      hub.closeError(RUN, STEP, { name: "E", message: "m" }),
    ).not.toThrow();
    expect(() => hub.closeRun(RUN)).not.toThrow();
  });

  it("closeRun closes every open channel of a run (clean close, no error event)", async () => {
    const hub = new InMemoryStreamHub();
    const stepA = "a" as StepId;
    const stepB = "b" as StepId;
    const onA = collect(hub.subscribeStream(RUN, stepA));
    const onB = collect(hub.subscribeStream(RUN, stepB));
    hub.publishChunk(RUN, stepA, "a1");
    hub.publishChunk(RUN, stepB, "b1");

    hub.closeRun(RUN);

    expect(await onA).toEqual([{ kind: "chunk", chunk: "a1" }]);
    expect(await onB).toEqual([{ kind: "chunk", chunk: "b1" }]);
  });

  it("closeRun does not touch another run's channel", async () => {
    const hub = new InMemoryStreamHub();
    const runX = "run-x" as RunId;
    const runY = "run-y" as RunId;
    const onY = collect(hub.subscribeStream(runY, STEP));

    hub.publishChunk(runY, STEP, "y1");
    hub.closeRun(runX);
    hub.publishChunk(runY, STEP, "y2");
    hub.closeOk(runY, STEP);

    expect(await onY).toEqual([
      { kind: "chunk", chunk: "y1" },
      { kind: "chunk", chunk: "y2" },
    ]);
  });
});

describe("InMemoryStreamHub — scoping by (runId, stepId)", () => {
  it("a publish to (run, stepA) is not seen by a subscriber of (run, stepB)", async () => {
    const hub = new InMemoryStreamHub();
    const stepA = "a" as StepId;
    const stepB = "b" as StepId;

    const onB = collect(hub.subscribeStream(RUN, stepB));
    hub.publishChunk(RUN, stepA, "for-a");
    hub.closeOk(RUN, stepA);
    hub.closeOk(RUN, stepB);

    expect(await onB).toEqual([]);
  });

  it("a publish to (runX, step) is not seen by a subscriber of (runY, step)", async () => {
    const hub = new InMemoryStreamHub();
    const runX = "run-x" as RunId;
    const runY = "run-y" as RunId;

    const onY = collect(hub.subscribeStream(runY, STEP));
    hub.publishChunk(runX, STEP, "for-x");
    hub.closeOk(runX, STEP);
    hub.closeOk(runY, STEP);

    expect(await onY).toEqual([]);
  });
});
