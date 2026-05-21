import { describe, expect, it } from "vitest";
import { flow } from "../builder";
import { InMemoryStore } from "../memory";
import { NagiRuntimeError, nagi } from "../runtime";
import { unwrap } from "../state";
import type { Json, RunId, StepId, StreamEvent } from "../types";
import { makeHarness, passthroughSchema } from "./test-helpers";

/** Drain an async iterable of stream events to completion into an array. */
async function collect<C = Json>(
  iter: AsyncIterable<StreamEvent<C>>,
): Promise<StreamEvent<C>[]> {
  const out: StreamEvent<C>[] = [];
  for await (const ev of iter) out.push(ev);
  return out;
}

/** Pull only the data chunks out of a collected event array. */
function chunks<C>(events: readonly StreamEvent<C>[]): C[] {
  return events.flatMap((e) => (e.kind === "chunk" ? [e.chunk] : []));
}

describe("streamingTask — emit → subscribe happy path", () => {
  it("delivers emitted chunks in order as {kind:chunk} and captures the durable return", async () => {
    const f = flow({
      id: "stream-happy",
      input: passthroughSchema<{ n: number }>(),
      build: (b) => {
        const gen = b.streamingTask<
          Record<string, never>,
          { final: string },
          { token: string }
        >({
          run: async ({ input, ctx }) => {
            for (let i = 0; i < input.n; i++) {
              await ctx.emit({ token: `t${i}` });
            }
            return { final: `done:${input.n}` };
          },
        });
        return { gen };
      },
    });

    const h = await makeHarness(f);
    // Deterministic: subscribe BEFORE any dispatch (no background worker), so the
    // hub subscriber is attached, then drive dispatch inline with drain(). This
    // removes the "worker completes the step before subscribe runs" race.
    const runId = await h.wf.start(f, { n: 3 });
    const sub = h.wf.subscribe<{ token: string }>(runId, "gen" as StepId);
    const collected = collect(sub);
    await h.drain();
    const events = await collected;

    expect(chunks(events)).toEqual([
      { token: "t0" },
      { token: "t1" },
      { token: "t2" },
    ]);
    // No error/retry on a clean run.
    expect(events.every((e) => e.kind === "chunk")).toBe(true);

    const result = await h.waitForEnd(runId);
    expect(result.status).toBe("completed");
    // The final return is the durable step.output (byte-identical to b.task).
    expect(result.output("gen")).toEqual({ final: "done:3" });
  });

  it("threads the durable output into a downstream needs step (chunks never reach it)", async () => {
    const f = flow({
      id: "stream-needs",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => {
        const gen = b.streamingTask<
          Record<string, never>,
          { text: string },
          string
        >({
          run: async ({ ctx }) => {
            await ctx.emit("a");
            await ctx.emit("b");
            return { text: "AB" };
          },
        });
        const consume = b.task({
          needs: { gen },
          run: async ({ needs }) => ({ echoed: unwrap(needs.gen).text }),
        });
        return { gen, consume };
      },
    });

    const result = await (async () => {
      const h = await makeHarness(f);
      const worker = h.startWorker();
      try {
        const runId = await h.wf.start(f, {});
        return await h.waitForEnd(runId);
      } finally {
        await worker.stop();
      }
    })();

    expect(result.status).toBe("completed");
    expect(result.output("gen")).toEqual({ text: "AB" });
    expect(result.output("consume")).toEqual({ echoed: "AB" });
  });
});

describe("streamingTask — chunks are ephemeral, not in the fact log", () => {
  it("emitted chunks never become facts; step.completed count is unaffected", async () => {
    const f = flow({
      id: "stream-no-facts",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => {
        const gen = b.streamingTask<Record<string, never>, number, number>({
          run: async ({ ctx }) => {
            await ctx.emit(1);
            await ctx.emit(2);
            await ctx.emit(3);
            return 99;
          },
        });
        return { gen };
      },
    });

    const h = await makeHarness(f);
    const worker = h.startWorker();
    try {
      const runId = await h.wf.start(f, {});
      const result = await h.waitForEnd(runId);

      // Exactly one step.completed for the single streaming step.
      expect(result.factCount("step.completed")).toBe(1);
      // No fact carries a chunk value: scan every fact's serialized form for the
      // emitted token values that are NOT the final output (1, 2 — the final is
      // 99). The chunk payloads must be absent from the durable log entirely.
      const factBlob = JSON.stringify(result.raw.facts);
      // The final output 99 IS in the log (step.completed.output); the mid
      // chunks 1/2/3 are emitted but only 99 returned — assert no chunk envelope
      // leaked.
      expect(factBlob).not.toContain('"kind":"chunk"');
      expect(result.output("gen")).toBe(99);
    } finally {
      await worker.stop();
    }
  });
});

describe("streamingTask — fan-out (D6)", () => {
  it("two subscribers each receive every chunk independently", async () => {
    const f = flow({
      id: "stream-fanout",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => {
        const gen = b.streamingTask<Record<string, never>, null, string>({
          run: async ({ ctx }) => {
            await ctx.emit("x");
            await ctx.emit("y");
            await ctx.emit("z");
            return null;
          },
        });
        return { gen };
      },
    });

    const h = await makeHarness(f);
    const runId = await h.wf.start(f, {});
    // Both subscribers attach before dispatch → each must see every chunk.
    const a = collect(h.wf.subscribe<string>(runId, "gen" as StepId));
    const b = collect(h.wf.subscribe<string>(runId, "gen" as StepId));
    await h.drain();
    const [ea, eb] = await Promise.all([a, b]);
    expect(chunks(ea)).toEqual(["x", "y", "z"]);
    expect(chunks(eb)).toEqual(["x", "y", "z"]);
    await h.waitForEnd(runId);
  });
});

describe("streamingTask — termination signaling (D3/O4)", () => {
  it("the consumer loop ends on step.completed (no hang)", async () => {
    const f = flow({
      id: "stream-end-ok",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => {
        const gen = b.streamingTask<Record<string, never>, string, string>({
          run: async ({ ctx }) => {
            await ctx.emit("only");
            return "fin";
          },
        });
        return { gen };
      },
    });

    const h = await makeHarness(f);
    const runId = await h.wf.start(f, {});
    // If the loop hung, collect() would never resolve and the test times out.
    const collected = collect(h.wf.subscribe<string>(runId, "gen" as StepId));
    await h.drain();
    const events = await collected;
    expect(chunks(events)).toEqual(["only"]);
    expect(events.some((e) => e.kind === "error")).toBe(false);
    await h.waitForEnd(runId);
  });

  it("on terminal step.failed the consumer sees a final {kind:error} then ends", async () => {
    const f = flow({
      id: "stream-end-fail",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => {
        const gen = b.streamingTask<Record<string, never>, never, string>({
          // No retry → maxAttempts default still > 1, so pin maxAttempts:1 so the
          // first failure is terminal.
          retry: { maxAttempts: 1, backoff: "fixed", initialDelayMs: 0 },
          run: async ({ ctx }) => {
            await ctx.emit("partial");
            throw new Error("boom");
          },
        });
        return { gen };
      },
    });

    const h = await makeHarness(f);
    const runId = await h.wf.start(f, {});
    const collected = collect(h.wf.subscribe<string>(runId, "gen" as StepId));
    await h.drain();
    const events = await collected;
    // Saw the partial chunk, then a terminal error envelope (not a rejection).
    expect(chunks(events)).toEqual(["partial"]);
    const last = events[events.length - 1];
    expect(last?.kind).toBe("error");
    if (last?.kind === "error") {
      expect(last.error.message).toBe("boom");
    }
    const result = await h.waitForEnd(runId);
    expect(result.status).toBe("failed");
  });
});

describe("streamingTask — retry (O5)", () => {
  it("a step that fails once then succeeds emits {kind:retry,attempt:2} between attempts; durable output is the success", async () => {
    let calls = 0;
    const f = flow({
      id: "stream-retry",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => {
        const gen = b.streamingTask<
          Record<string, never>,
          { ok: true },
          string
        >({
          retry: { maxAttempts: 3, backoff: "fixed", initialDelayMs: 0 },
          run: async ({ ctx }) => {
            calls += 1;
            if (calls === 1) {
              await ctx.emit("attempt1-chunk");
              throw new Error("transient");
            }
            await ctx.emit("attempt2-chunk");
            return { ok: true };
          },
        });
        return { gen };
      },
    });

    const h = await makeHarness(f);
    const runId = await h.wf.start(f, {});
    const collected = collect(h.wf.subscribe<string>(runId, "gen" as StepId));
    await h.drain();
    const events = await collected;

    // Order: attempt-1 chunk, retry marker (next attempt = 2), attempt-2 chunk.
    const retryIdx = events.findIndex((e) => e.kind === "retry");
    expect(retryIdx).toBeGreaterThanOrEqual(0);
    const retryEv = events[retryIdx];
    if (retryEv?.kind === "retry") expect(retryEv.attempt).toBe(2);
    expect(chunks(events)).toEqual(["attempt1-chunk", "attempt2-chunk"]);
    // A retried-but-eventually-successful step emits NO error envelope.
    expect(events.some((e) => e.kind === "error")).toBe(false);

    const result = await h.waitForEnd(runId);
    expect(result.status).toBe("completed");
    expect(result.output("gen")).toEqual({ ok: true });
  });
});

describe("streamingTask — INVARIANT GUARDS", () => {
  it("subscribe AFTER the step completed → empty stream that ends (no hang)", async () => {
    const f = flow({
      id: "stream-after-complete",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => {
        const gen = b.streamingTask<Record<string, never>, string, string>({
          run: async ({ ctx }) => {
            await ctx.emit("gone");
            return "fin";
          },
        });
        return { gen };
      },
    });

    const h = await makeHarness(f);
    const worker = h.startWorker();
    try {
      const runId = await h.wf.start(f, {});
      await h.waitForEnd(runId); // step already completed
      const events = await collect(
        h.wf.subscribe<string>(runId, "gen" as StepId),
      );
      expect(events).toEqual([]);
    } finally {
      await worker.stop();
    }
  });

  it("a streaming step that emits nothing then completes, subscribed afterward → empty + ends", async () => {
    const f = flow({
      id: "stream-silent",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => {
        const gen = b.streamingTask<Record<string, never>, string, string>({
          run: async () => "no-emit",
        });
        return { gen };
      },
    });

    const h = await makeHarness(f);
    const worker = h.startWorker();
    try {
      const runId = await h.wf.start(f, {});
      await h.waitForEnd(runId);
      const events = await collect(h.wf.subscribe(runId, "gen" as StepId));
      expect(events).toEqual([]);
      expect((await h.result(runId)).output("gen")).toBe("no-emit");
    } finally {
      await worker.stop();
    }
  });

  it("subscribe to an unknown / typo'd stepId → throws NagiRuntimeError", async () => {
    const f = flow({
      id: "stream-typo",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => {
        const gen = b.streamingTask<Record<string, never>, null, string>({
          run: async () => null,
        });
        return { gen };
      },
    });

    const h = await makeHarness(f);
    const runId = await h.wf.start(f, {});
    // Typo: "genn" is not a streaming step.
    expect(() => h.wf.subscribe(runId, "genn" as StepId)).toThrow(
      NagiRuntimeError,
    );
    // A real, non-streaming step is also rejected by subscribe.
  });

  it("a non-streaming step is not subscribable (throws NagiRuntimeError)", async () => {
    const f = flow({
      id: "stream-nonstreaming-subscribe",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => {
        const plain = b.task({ run: async () => 1 });
        return { plain };
      },
    });
    const h = await makeHarness(f);
    const runId = await h.wf.start(f, {});
    expect(() => h.wf.subscribe(runId, "plain" as StepId)).toThrow(
      NagiRuntimeError,
    );
  });

  it("a subscriber to a skipped streaming step (subscribed after run end) gets empty + ends", async () => {
    // The streaming step is gated off by `when:false` so it is SKIPPED — it
    // never runs/emits. A consumer subscribing after run-end gets an empty,
    // ended stream via the durable-fact guard (the skipped step is terminal).
    const f = flow({
      id: "stream-skipped-step",
      input: passthroughSchema<{ go: boolean }>(),
      build: (b) => {
        const gen = b.streamingTask<Record<string, never>, null, string>({
          when: ({ input }) => (input as { go: boolean }).go,
          run: async ({ ctx }) => {
            await ctx.emit("never");
            return null;
          },
        });
        const tail = b.task({ run: async () => "tail-done" });
        return { gen, tail };
      },
    });

    const h = await makeHarness(f);
    const worker = h.startWorker();
    try {
      const runId = await h.wf.start(f, { go: false });
      const result = await h.waitForEnd(runId);
      expect(result.status).toBe("completed");
      expect(result.stepStatus("gen")).toBe("skipped");
      const events = await collect(h.wf.subscribe(runId, "gen" as StepId));
      expect(events).toEqual([]);
    } finally {
      await worker.stop();
    }
  });

  it("a LIVE subscriber to a step that never completes is ended by the run-terminal fact (closeRun)", async () => {
    // The precise closeRun invariant, tested deterministically at the store
    // layer: open a live channel for a PENDING step of a RUNNING run (so the
    // fact guard delegates to the hub), then land a run-terminal fact. closeRun
    // must end the subscriber's iterator — no hang past run end, no error event.
    const store = new InMemoryStore();
    const runId = "run-closerun" as RunId;
    const stepId = "gen" as StepId;

    await store.appendFact(runId, {
      kind: "flow.started",
      runId,
      flowId: "f",
      input: null,
      at: new Date(),
    });
    // Step is pending (no step fact), run is running → a LIVE hub subscription.
    const collected = collect(store.subscribeStream(runId, stepId));
    // A chunk fans out to the live subscriber (proves the channel is real/open).
    store.publishChunk(runId, stepId, "live-chunk");

    // Run terminates without the step ever completing.
    await store.appendFact(runId, {
      kind: "flow.completed",
      runId,
      output: null,
      at: new Date(),
    });

    const events = await collected; // must resolve (no hang)
    expect(chunks(events)).toEqual(["live-chunk"]);
    // Clean close — closeRun injects no error envelope.
    expect(events.some((e) => e.kind === "error")).toBe(false);
  });
});

describe("streamingTask — capability gating (D4)", () => {
  it("registering a streaming flow against a store without subscribeStream throws at nagi()", async () => {
    const f = flow({
      id: "stream-capability",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => {
        const gen = b.streamingTask<Record<string, never>, null, string>({
          run: async () => null,
        });
        return { gen };
      },
    });

    // A store missing the streaming capability: strip subscribeStream/publishChunk.
    const store = new InMemoryStore();
    const crippled = new Proxy(store, {
      get(target, prop, receiver) {
        if (prop === "subscribeStream" || prop === "publishChunk")
          return undefined;
        return Reflect.get(target, prop, receiver);
      },
    });

    await expect(
      nagi({
        flows: [f],
        store: crippled,
        queue: new (await import("../memory")).InMemoryQueue(),
      }),
    ).rejects.toThrow(NagiRuntimeError);
  });

  it("a non-streaming flow against a store without subscribeStream registers fine", async () => {
    const f = flow({
      id: "no-stream-capability",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => {
        const plain = b.task({ run: async () => 1 });
        return { plain };
      },
    });

    const store = new InMemoryStore();
    const crippled = new Proxy(store, {
      get(target, prop, receiver) {
        if (prop === "subscribeStream" || prop === "publishChunk")
          return undefined;
        return Reflect.get(target, prop, receiver);
      },
    });

    const { InMemoryQueue } = await import("../memory");
    const wf = await nagi({
      flows: [f],
      store: crippled,
      queue: new InMemoryQueue(),
    });
    expect(wf).toBeDefined();
  });
});

describe("streamingTask — scoping by (runId, stepId)", () => {
  it("chunks for one run never reach a subscriber of another run", async () => {
    const f = flow({
      id: "stream-scope-run",
      input: passthroughSchema<{ tag: string }>(),
      build: (b) => {
        const gen = b.streamingTask<Record<string, never>, null, string>({
          run: async ({ input, ctx }) => {
            await ctx.emit(`chunk-${input.tag}`);
            return null;
          },
        });
        return { gen };
      },
    });

    const h = await makeHarness(f);
    const runA = await h.wf.start(f, { tag: "A" });
    const runB = await h.wf.start(f, { tag: "B" });
    const eventsA = collect(h.wf.subscribe<string>(runA, "gen" as StepId));
    const eventsB = collect(h.wf.subscribe<string>(runB, "gen" as StepId));
    await h.drain();
    const [a, b] = await Promise.all([eventsA, eventsB]);
    expect(chunks(a)).toEqual(["chunk-A"]);
    expect(chunks(b)).toEqual(["chunk-B"]);
    await h.waitForEnd(runA);
    await h.waitForEnd(runB);
  });

  it("chunks for one step never reach a subscriber of a sibling step in the same run", async () => {
    const f = flow({
      id: "stream-scope-step",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => {
        const genA = b.streamingTask<Record<string, never>, null, string>({
          run: async ({ ctx }) => {
            await ctx.emit("from-A");
            return null;
          },
        });
        const genB = b.streamingTask<Record<string, never>, null, string>({
          run: async ({ ctx }) => {
            await ctx.emit("from-B");
            return null;
          },
        });
        return { genA, genB };
      },
    });

    const h = await makeHarness(f);
    const runId = await h.wf.start(f, {});
    const onA = collect(h.wf.subscribe<string>(runId, "genA" as StepId));
    const onB = collect(h.wf.subscribe<string>(runId, "genB" as StepId));
    await h.drain();
    const [a, b] = await Promise.all([onA, onB]);
    expect(chunks(a)).toEqual(["from-A"]);
    expect(chunks(b)).toEqual(["from-B"]);
    await h.waitForEnd(runId);
  });
});

describe("streamingTask — emit ergonomics", () => {
  it("await ctx.emit(x) resolves even with ZERO subscribers (no subscriber ever attaches)", async () => {
    const emitResolutions: number[] = [];
    const f = flow({
      id: "stream-emit-no-subscriber",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => {
        const gen = b.streamingTask<Record<string, never>, string, number>({
          run: async ({ ctx }) => {
            // No subscriber is attached; each emit must still resolve so the
            // handler can make progress and complete (fan-out is at-most-once,
            // non-blocking — the producer never waits on a consumer).
            for (let i = 0; i < 3; i++) {
              await ctx.emit(i);
              emitResolutions.push(i);
            }
            return "completed-without-consumer";
          },
        });
        return { gen };
      },
    });

    const h = await makeHarness(f);
    const worker = h.startWorker();
    try {
      const runId = await h.wf.start(f, {});
      const result = await h.waitForEnd(runId);
      // All three emits resolved (the loop ran to completion) and the step
      // captured its durable output.
      expect(emitResolutions).toEqual([0, 1, 2]);
      expect(result.status).toBe("completed");
      expect(result.output("gen")).toBe("completed-without-consumer");
    } finally {
      await worker.stop();
    }
  });

  it("a {replayBuffered:true} subscriber attached mid-run gets earlier chunks then live ones in order (end-to-end)", async () => {
    // End-to-end version of the hub replayBuffered unit test: drive a real run
    // via a worker, park the handler on a barrier after it has emitted the
    // early chunks, attach a late `replayBuffered` subscriber while the step is
    // still running (channel open, buffer populated), then release the barrier
    // so the handler emits a live chunk and completes.
    let releaseProceed!: () => void;
    const proceed = new Promise<void>((r) => {
      releaseProceed = r;
    });
    let signalEarlyEmitted!: () => void;
    const earlyEmitted = new Promise<void>((r) => {
      signalEarlyEmitted = r;
    });

    const f = flow({
      id: "stream-replaybuffered-midrun",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => {
        const gen = b.streamingTask<Record<string, never>, string, string>({
          run: async ({ ctx }) => {
            await ctx.emit("early-1");
            await ctx.emit("early-2");
            signalEarlyEmitted();
            // Park here so the test can attach the late subscriber while the
            // step is still running (not yet terminal → channel still open).
            await proceed;
            await ctx.emit("live-1");
            return "fin";
          },
        });
        return { gen };
      },
    });

    const h = await makeHarness(f);
    const worker = h.startWorker();
    try {
      const runId = await h.wf.start(f, {});
      // Wait until the early chunks are buffered in the channel.
      await earlyEmitted;
      // Attach the late subscriber with replayBuffered → it must be seeded with
      // the buffered early chunks, then receive the subsequent live chunk.
      const collected = collect(
        h.wf.subscribe<string>(runId, "gen" as StepId, {
          replayBuffered: true,
        }),
      );
      // Release the handler so it emits the live chunk and completes (closing
      // the channel, which ends the subscriber's loop).
      releaseProceed();
      const events = await collected;

      expect(chunks(events)).toEqual(["early-1", "early-2", "live-1"]);
      const result = await h.waitForEnd(runId);
      expect(result.output("gen")).toBe("fin");
    } finally {
      releaseProceed(); // ensure the handler is never left parked
      await worker.stop();
    }
  });

  it("a default (future-only) subscriber attached mid-run misses earlier chunks but gets live ones (end-to-end)", async () => {
    // The contrast case to replayBuffered: a mid-run subscriber WITHOUT the opt
    // sees only chunks emitted after it attached.
    let releaseProceed!: () => void;
    const proceed = new Promise<void>((r) => {
      releaseProceed = r;
    });
    let signalEarlyEmitted!: () => void;
    const earlyEmitted = new Promise<void>((r) => {
      signalEarlyEmitted = r;
    });

    const f = flow({
      id: "stream-futureonly-midrun",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => {
        const gen = b.streamingTask<Record<string, never>, string, string>({
          run: async ({ ctx }) => {
            await ctx.emit("early-1");
            await ctx.emit("early-2");
            signalEarlyEmitted();
            await proceed;
            await ctx.emit("live-1");
            return "fin";
          },
        });
        return { gen };
      },
    });

    const h = await makeHarness(f);
    const worker = h.startWorker();
    try {
      const runId = await h.wf.start(f, {});
      await earlyEmitted;
      const collected = collect(h.wf.subscribe<string>(runId, "gen" as StepId));
      releaseProceed();
      const events = await collected;

      expect(chunks(events)).toEqual(["live-1"]);
      await h.waitForEnd(runId);
    } finally {
      releaseProceed();
      await worker.stop();
    }
  });
});
