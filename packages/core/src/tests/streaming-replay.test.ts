import { describe, expect, it } from "vitest";
import { flow } from "../builder";
import type { Json, StepId, StreamEvent } from "../types";
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

describe("streamingTask — replay is emit-inert, durable output survives", () => {
  it("re-deriving the run (loadRunState) reproduces the same step.output without re-invoking the handler", async () => {
    let calls = 0;
    const f = flow({
      id: "stream-replay-rederive",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => {
        const gen = b.streamingTask<
          Record<string, never>,
          { final: string },
          string
        >({
          run: async ({ ctx }) => {
            calls += 1;
            await ctx.emit("a");
            await ctx.emit("b");
            return { final: "durable" };
          },
        });
        return { gen };
      },
    });

    const h = await makeHarness(f);
    const runId = await h.wf.start(f, {});
    await h.drain();
    expect(calls).toBe(1);
    const first = await h.result(runId);
    expect(first.output("gen")).toEqual({ final: "durable" });

    const reDerived = await h.result(runId);
    expect(reDerived.output("gen")).toEqual({ final: "durable" });
    expect(calls).toBe(1);
  });

  it("inspect-mode replay is a pure no-op: handler not re-invoked, output unchanged", async () => {
    let calls = 0;
    const f = flow({
      id: "stream-replay-inspect",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => {
        const gen = b.streamingTask<Record<string, never>, string, string>({
          run: async ({ ctx }) => {
            calls += 1;
            await ctx.emit("x");
            return "fin";
          },
        });
        return { gen };
      },
    });

    const h = await makeHarness(f);
    const runId = await h.wf.start(f, {});
    await h.drain();
    expect(calls).toBe(1);

    await h.wf.replay(runId, { mode: "inspect" });
    expect(calls).toBe(1);
    expect((await h.result(runId)).output("gen")).toBe("fin");
  });

  it("continue-mode replay of a completed run re-invokes nothing and re-emits no chunks to a live subscriber", async () => {
    let calls = 0;
    const f = flow({
      id: "stream-replay-continue",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => {
        const gen = b.streamingTask<Record<string, never>, string, string>({
          run: async ({ ctx }) => {
            calls += 1;
            await ctx.emit("chunk-from-original-run");
            return "fin";
          },
        });
        return { gen };
      },
    });

    const h = await makeHarness(f);
    const runId = await h.wf.start(f, {});
    await h.drain();
    expect(calls).toBe(1);

    await h.wf.replay(runId, { mode: "continue" });
    const sub = collect(h.wf.subscribe<string>(runId, "gen" as StepId));
    await h.drain();
    const events = await sub;

    expect(calls).toBe(1);
    expect(events).toEqual([]);
    expect((await h.result(runId)).output("gen")).toBe("fin");
  });

  it("subscribing after a completed streaming run yields an empty, ended stream (terminal-fact guard)", async () => {
    const f = flow({
      id: "stream-replay-subscribe-after",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => {
        const gen = b.streamingTask<Record<string, never>, string, string>({
          run: async ({ ctx }) => {
            await ctx.emit("ephemeral");
            return "kept";
          },
        });
        return { gen };
      },
    });

    const h = await makeHarness(f);
    const runId = await h.wf.start(f, {});
    await h.drain();

    const events = await collect(
      h.wf.subscribe<string>(runId, "gen" as StepId),
    );
    expect(events).toEqual([]);
    expect((await h.result(runId)).output("gen")).toBe("kept");
  });

  it("forced re-execution via replay({ from }) DOES re-run the streaming handler and updates the durable output", async () => {
    let calls = 0;
    const f = flow({
      id: "stream-replay-from",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => {
        const gen = b.streamingTask<Record<string, never>, string, string>({
          run: async ({ ctx }) => {
            calls += 1;
            await ctx.emit(`call${calls}-chunk`);
            return `out${calls}`;
          },
        });
        return { gen };
      },
    });

    const h = await makeHarness(f);
    const runId = await h.wf.start(f, {});
    await h.drain();
    expect(calls).toBe(1);
    expect((await h.result(runId)).output("gen")).toBe("out1");

    await h.wf.replay(runId, { mode: "continue", from: "gen" });
    const sub = collect(h.wf.subscribe<string>(runId, "gen" as StepId));
    await h.drain();
    const events = await sub;

    expect(calls).toBe(2);
    const result = await h.result(runId);
    expect(result.status).toBe("completed");
    expect(result.output("gen")).toBe("out2");
    expect(chunks(events)).toEqual([]);
  });
});

describe("streamingTask — retried attempt chunks stay ephemeral", () => {
  it("chunks from BOTH the failed and the successful attempt never enter the fact log; durable output is the success", async () => {
    let calls = 0;
    const f = flow({
      id: "stream-retry-ephemeral",
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
              await ctx.emit("attempt1-only-chunk");
              throw new Error("transient");
            }
            await ctx.emit("attempt2-only-chunk");
            return { ok: true };
          },
        });
        return { gen };
      },
    });

    const h = await makeHarness(f);
    const runId = await h.wf.start(f, {});
    await h.drain();
    const result = await h.result(runId);

    expect(result.status).toBe("completed");
    expect(calls).toBe(2);
    expect(result.output("gen")).toEqual({ ok: true });

    const factBlob = JSON.stringify(result.raw.facts);
    expect(factBlob).not.toContain('"kind":"chunk"');
    expect(factBlob).not.toContain("attempt1-only-chunk");
    expect(factBlob).not.toContain("attempt2-only-chunk");
    expect(result.factCount("step.completed")).toBe(1);
  });

  it("a live subscriber across the retry sees attempt-1 chunk, a retry marker, then attempt-2 chunk (ephemeral, never durable)", async () => {
    let calls = 0;
    const f = flow({
      id: "stream-retry-live-sub",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => {
        const gen = b.streamingTask<Record<string, never>, string, string>({
          retry: { maxAttempts: 3, backoff: "fixed", initialDelayMs: 0 },
          run: async ({ ctx }) => {
            calls += 1;
            if (calls === 1) {
              await ctx.emit("a1");
              throw new Error("transient");
            }
            await ctx.emit("a2");
            return "done";
          },
        });
        return { gen };
      },
    });

    const h = await makeHarness(f);
    const runId = await h.wf.start(f, {});
    const sub = collect(h.wf.subscribe<string>(runId, "gen" as StepId));
    await h.drain();
    const events = await sub;

    expect(chunks(events)).toEqual(["a1", "a2"]);
    const retryIdx = events.findIndex((e) => e.kind === "retry");
    const a1Idx = events.findIndex(
      (e) => e.kind === "chunk" && e.chunk === "a1",
    );
    const a2Idx = events.findIndex(
      (e) => e.kind === "chunk" && e.chunk === "a2",
    );
    expect(retryIdx).toBeGreaterThan(a1Idx);
    expect(retryIdx).toBeLessThan(a2Idx);
    expect(events.some((e) => e.kind === "error")).toBe(false);

    const result = await h.result(runId);
    expect(result.output("gen")).toBe("done");
    expect(JSON.stringify(result.raw.facts)).not.toContain('"kind":"chunk"');
  });
});
