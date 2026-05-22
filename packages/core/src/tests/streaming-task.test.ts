import { describe, expect, it } from "vitest";
import { flow } from "../builder";
import { InMemoryStore } from "../memory";
import { NagiRuntimeError, nagi } from "../runtime";
import { unwrap } from "../state";
import type { Json, RunId, StepId, StreamEvent } from "../types";
import { makeHarness, passthroughSchema } from "./test-helpers";

async function collect<C = Json>(
  iter: AsyncIterable<StreamEvent<C>>,
): Promise<StreamEvent<C>[]> {
  const out: StreamEvent<C>[] = [];
  for await (const ev of iter) out.push(ev);
  return out;
}

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
    expect(events.every((e) => e.kind === "chunk")).toBe(true);

    const result = await h.waitForEnd(runId);
    expect(result.status).toBe("completed");
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

      expect(result.factCount("step.completed")).toBe(1);
      const factBlob = JSON.stringify(result.raw.facts);
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

    const retryIdx = events.findIndex((e) => e.kind === "retry");
    expect(retryIdx).toBeGreaterThanOrEqual(0);
    const retryEv = events[retryIdx];
    if (retryEv?.kind === "retry") expect(retryEv.attempt).toBe(2);
    expect(chunks(events)).toEqual(["attempt1-chunk", "attempt2-chunk"]);
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
      await h.waitForEnd(runId);
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
    expect(() => h.wf.subscribe(runId, "genn" as StepId)).toThrow(
      NagiRuntimeError,
    );
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
    const collected = collect(store.subscribeStream(runId, stepId));
    store.publishChunk(runId, stepId, "live-chunk");

    await store.appendFact(runId, {
      kind: "flow.completed",
      runId,
      output: null,
      at: new Date(),
    });

    const events = await collected;
    expect(chunks(events)).toEqual(["live-chunk"]);
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
      expect(emitResolutions).toEqual([0, 1, 2]);
      expect(result.status).toBe("completed");
      expect(result.output("gen")).toBe("completed-without-consumer");
    } finally {
      await worker.stop();
    }
  });

  it("a {replayBuffered:true} subscriber attached mid-run gets earlier chunks then live ones in order (end-to-end)", async () => {
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
      const collected = collect(
        h.wf.subscribe<string>(runId, "gen" as StepId, {
          replayBuffered: true,
        }),
      );
      releaseProceed();
      const events = await collected;

      expect(chunks(events)).toEqual(["early-1", "early-2", "live-1"]);
      const result = await h.waitForEnd(runId);
      expect(result.output("gen")).toBe("fin");
    } finally {
      releaseProceed();
      await worker.stop();
    }
  });

  it("a default (future-only) subscriber attached mid-run misses earlier chunks but gets live ones (end-to-end)", async () => {
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
