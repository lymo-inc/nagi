import { describe, expect, it } from "vitest";
import { flow } from "../builder";
import { InMemoryClock, InMemoryQueue, InMemoryStore } from "../memory";
import { NagiValidationError, nagi } from "../runtime";
import type { Fact, FlowStartedFact, RunId, StandardSchemaV1 } from "../types";

function passthroughSchema<T>(): StandardSchemaV1<T, T> {
  return {
    "~standard": {
      version: 1,
      vendor: "test",
      validate: (v) => ({ value: v as T }),
    },
  };
}

async function seedRuns(
  cases: ReadonlyArray<{
    readonly flowId: string;
    readonly input: Record<string, unknown>;
    readonly startedAtMs: number;
    readonly terminal?: "completed" | "failed" | "canceled";
  }>,
): Promise<{ store: InMemoryStore; runIds: RunId[] }> {
  const store = new InMemoryStore();
  const runIds: RunId[] = [];
  for (let i = 0; i < cases.length; i++) {
    const c = cases[i];
    if (c === undefined) continue;
    const runId =
      `run-${String(i).padStart(4, "0")}-${crypto.randomUUID()}` as RunId;
    const startFact: FlowStartedFact = {
      kind: "flow.started",
      runId,
      flowId: c.flowId,
      input: c.input as never,
      at: new Date(c.startedAtMs),
    };
    await store.tryStartRun(runId, startFact);
    if (c.terminal !== undefined) {
      const at = new Date(c.startedAtMs + 1000);
      const tFact: Fact =
        c.terminal === "completed"
          ? { kind: "flow.completed", runId, at, output: null }
          : c.terminal === "failed"
            ? {
                kind: "flow.failed",
                runId,
                at,
                error: { name: "E", message: "x" },
              }
            : {
                kind: "flow.canceled",
                cause: "concurrency",
                runId,
                at,
                canceledByRunId: runId,
                concurrencyKey: "k",
              };
      await store.appendFact(runId, tFact);
    }
    runIds.push(runId);
  }
  return { store, runIds };
}

describe("InMemoryStore.queryRuns — filters", () => {
  it("matches by exact-key input containment", async () => {
    const { store } = await seedRuns([
      { flowId: "f", input: { videoId: "abc" }, startedAtMs: 1000 },
      { flowId: "f", input: { videoId: "xyz" }, startedAtMs: 2000 },
    ]);
    const r = await store.queryRuns({ where: { input: { videoId: "abc" } } });
    expect(r.runs.map((x) => x.input)).toEqual([{ videoId: "abc" }]);
  });

  it("returns nothing when input filter mismatches", async () => {
    const { store } = await seedRuns([
      { flowId: "f", input: { videoId: "abc" }, startedAtMs: 1000 },
    ]);
    const r = await store.queryRuns({ where: { input: { videoId: "MISS" } } });
    expect(r.runs).toEqual([]);
  });

  it("filter is a subset of the row (containment, not equality)", async () => {
    const { store } = await seedRuns([
      {
        flowId: "f",
        input: { videoId: "abc", userId: 7 },
        startedAtMs: 1000,
      },
    ]);
    const r = await store.queryRuns({ where: { input: { videoId: "abc" } } });
    expect(r.runs).toHaveLength(1);
  });

  it("filter superset of the row → no match", async () => {
    const { store } = await seedRuns([
      { flowId: "f", input: { videoId: "abc" }, startedAtMs: 1000 },
    ]);
    const r = await store.queryRuns({
      where: { input: { videoId: "abc", userId: 7 } },
    });
    expect(r.runs).toEqual([]);
  });

  it("nested containment matches (recursive @>)", async () => {
    const { store } = await seedRuns([
      {
        flowId: "f",
        input: { customer: { id: 1, tier: "pro", plan: { seats: 5 } } },
        startedAtMs: 1000,
      },
    ]);
    const r = await store.queryRuns({
      where: { input: { customer: { plan: { seats: 5 } } } },
    });
    expect(r.runs).toHaveLength(1);
  });

  it("array containment: needle elements ⊆ haystack elements", async () => {
    const { store } = await seedRuns([
      { flowId: "f", input: { tags: ["a", "b", "c"] }, startedAtMs: 1000 },
    ]);
    const yes = await store.queryRuns({
      where: { input: { tags: ["a", "b"] } },
    });
    expect(yes.runs).toHaveLength(1);

    const no = await store.queryRuns({
      where: { input: { tags: ["a", "z"] } },
    });
    expect(no.runs).toEqual([]);
  });

  it("type mismatch between needle and haystack → no match", async () => {
    const { store } = await seedRuns([
      { flowId: "f", input: { x: "string" }, startedAtMs: 1000 },
    ]);
    const r = await store.queryRuns({ where: { input: { x: 42 } as never } });
    expect(r.runs).toEqual([]);
  });

  it("status: single value", async () => {
    const { store } = await seedRuns([
      { flowId: "f", input: {}, startedAtMs: 1000 },
      { flowId: "f", input: {}, startedAtMs: 2000, terminal: "completed" },
    ]);
    const r = await store.queryRuns({ where: { status: "completed" } });
    expect(r.runs).toHaveLength(1);
    expect(r.runs[0]?.status).toBe("completed");
  });

  it("status: array of values", async () => {
    const { store } = await seedRuns([
      { flowId: "f", input: {}, startedAtMs: 1000 },
      { flowId: "f", input: {}, startedAtMs: 2000, terminal: "completed" },
      { flowId: "f", input: {}, startedAtMs: 3000, terminal: "failed" },
    ]);
    const r = await store.queryRuns({
      where: { status: ["completed", "failed"] },
    });
    expect(r.runs.map((x) => x.status).sort()).toEqual(["completed", "failed"]);
  });

  it("flowId filter narrows to a single flow", async () => {
    const { store } = await seedRuns([
      { flowId: "a", input: {}, startedAtMs: 1000 },
      { flowId: "b", input: {}, startedAtMs: 2000 },
    ]);
    const r = await store.queryRuns({ where: { flowId: "b" } });
    expect(r.runs).toHaveLength(1);
    expect(r.runs[0]?.flowId).toBe("b");
  });
});

describe("InMemoryStore.queryRuns — ordering, latest, pagination", () => {
  it("orders results newest-first", async () => {
    const { store } = await seedRuns([
      { flowId: "f", input: {}, startedAtMs: 1000 },
      { flowId: "f", input: {}, startedAtMs: 3000 },
      { flowId: "f", input: {}, startedAtMs: 2000 },
    ]);
    const r = await store.queryRuns({});
    const ts = r.runs.map((x) => x.startedAt.getTime());
    expect(ts).toEqual([3000, 2000, 1000]);
  });

  it("`latest: true` returns at most one run, newest", async () => {
    const { store } = await seedRuns([
      { flowId: "f", input: {}, startedAtMs: 1000 },
      { flowId: "f", input: {}, startedAtMs: 3000 },
    ]);
    const r = await store.queryRuns({ latest: true });
    expect(r.runs).toHaveLength(1);
    expect(r.runs[0]?.startedAt.getTime()).toBe(3000);
    expect(r.cursor).toBeNull();
  });

  it("`latest: true` on empty store returns no rows", async () => {
    const store = new InMemoryStore();
    const r = await store.queryRuns({ latest: true });
    expect(r.runs).toEqual([]);
    expect(r.cursor).toBeNull();
  });

  it("cursor pagination walks all rows", async () => {
    const seeds = Array.from({ length: 5 }, (_, i) => ({
      flowId: "f",
      input: {} as Record<string, unknown>,
      startedAtMs: 1000 + i * 100,
    }));
    const { store } = await seedRuns(seeds);

    const first = await store.queryRuns({ limit: 2 });
    expect(first.runs).toHaveLength(2);
    expect(first.cursor).not.toBeNull();

    const second = await store.queryRuns({
      limit: 2,
      cursor: first.cursor as string,
    });
    expect(second.runs).toHaveLength(2);
    expect(second.cursor).not.toBeNull();

    const third = await store.queryRuns({
      limit: 2,
      cursor: second.cursor as string,
    });
    expect(third.runs).toHaveLength(1);
    expect(third.cursor).toBeNull();

    const ids = [...first.runs, ...second.runs, ...third.runs].map(
      (x) => x.runId,
    );
    expect(new Set(ids).size).toBe(ids.length);
  });

  it("empty store returns an empty page with no cursor", async () => {
    const store = new InMemoryStore();
    const r = await store.queryRuns({});
    expect(r).toEqual({ runs: [], cursor: null });
  });
});

describe("wf.queryRuns — runtime layer", () => {
  it("delegates to the store and exposes the result", async () => {
    const f = flow({
      id: "video-ingest",
      input: passthroughSchema<{ videoId: string }>(),
      build: (b) => ({
        only: b.task({ run: async ({ input }) => input }),
      }),
    });
    const wf = await nagi({
      flows: [f],
      store: new InMemoryStore(),
      queue: new InMemoryQueue(),
      clock: new InMemoryClock(),
    });
    await wf.start(f, { videoId: "abc-123" });
    await wf.start(f, { videoId: "def-456" });

    const r = await wf.queryRuns({
      where: { input: { videoId: "abc-123" } },
      latest: true,
    });
    expect(r.runs).toHaveLength(1);
    expect(r.runs[0]?.input).toEqual({ videoId: "abc-123" });
  });

  it("rejects `latest: true` mixed with limit/cursor at runtime too", async () => {
    const wf = await nagi({
      flows: [],
      store: new InMemoryStore(),
      queue: new InMemoryQueue(),
      clock: new InMemoryClock(),
    });
    await expect(
      // biome-ignore lint/suspicious/noExplicitAny: testing the runtime guard
      wf.queryRuns({ latest: true, limit: 5 } as any),
    ).rejects.toBeInstanceOf(NagiValidationError);
  });
});

describe("wf.queryRuns — no runtime delta after RFC 0018 typing", () => {
  const f = flow({
    id: "a",
    input: passthroughSchema<{ videoId: string }>(),
    build: (b) => ({ only: b.task({ run: async ({ input }) => input }) }),
  });
  const g = flow({
    id: "b",
    input: passthroughSchema<{ dealId: string }>(),
    build: (b) => ({ only: b.task({ run: async ({ input }) => input }) }),
  });

  async function setup() {
    const wf = await nagi({
      flows: [f, g],
      store: new InMemoryStore(),
      queue: new InMemoryQueue(),
      clock: new InMemoryClock(),
    });
    await wf.start(f, { videoId: "v-1" });
    await wf.start(g, { dealId: "d-1" });
    return wf;
  }

  it("returns the same { runs, cursor } shape; flowId values are the registered id strings", async () => {
    const wf = await setup();
    const r = await wf.queryRuns();
    expect(Object.keys(r).sort()).toEqual(["cursor", "runs"]);
    expect(Array.isArray(r.runs)).toBe(true);
    expect(r.runs).toHaveLength(2);
    // The literal type narrows at compile time; the runtime value is the
    // plain registered id string — unchanged from before.
    expect(r.runs.map((x) => x.flowId).sort()).toEqual(["a", "b"]);
    expect(r.cursor === null || typeof r.cursor === "string").toBe(true);
  });

  it("flowId filter still narrows to a single flow at runtime", async () => {
    const wf = await setup();
    const r = await wf.queryRuns({ where: { flowId: "b" } });
    expect(r.runs).toHaveLength(1);
    expect(r.runs[0]?.flowId).toBe("b");
  });

  it("`latest: true` still returns at most the newest single run", async () => {
    const wf = await setup();
    const r = await wf.queryRuns({ latest: true });
    expect(r.runs).toHaveLength(1);
    expect(r.cursor).toBeNull();
  });

  it("limit/cursor pagination still walks all rows", async () => {
    const wf = await setup();
    const first = await wf.queryRuns({ limit: 1 });
    expect(first.runs).toHaveLength(1);
    expect(first.cursor).not.toBeNull();
    const second = await wf.queryRuns({
      limit: 1,
      cursor: first.cursor as string,
    });
    expect(second.runs).toHaveLength(1);
    const ids = [...first.runs, ...second.runs].map((x) => x.runId);
    expect(new Set(ids).size).toBe(2);
  });
});
