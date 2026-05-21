import { describe, expect, it } from "vitest";
import { flow } from "./builder";
import { InMemoryClock, InMemoryQueue, InMemoryStore } from "./memory";
import { NagiValidationError, nagi } from "./runtime";
import { passthroughSchema } from "./test-helpers";
import type { Fact, FlowStartedFact, PrunableStatus, RunId } from "./types";

interface SeedCase {
  readonly flowId: string;
  readonly input?: Record<string, unknown>;
  readonly startedAtMs: number;
  readonly terminal?: {
    readonly kind: PrunableStatus;
    readonly atMs: number;
  };
}

async function seedRuns(
  cases: ReadonlyArray<SeedCase>,
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
      input: (c.input ?? {}) as never,
      at: new Date(c.startedAtMs),
    };
    await store.tryStartRun(runId, startFact);
    if (c.terminal !== undefined) {
      const at = new Date(c.terminal.atMs);
      const tFact: Fact =
        c.terminal.kind === "completed"
          ? { kind: "flow.completed", runId, at, output: null }
          : c.terminal.kind === "failed"
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

function defaults(overrides: {
  readonly olderThan: Date;
  readonly statuses?: ReadonlyArray<PrunableStatus>;
  readonly batchSize?: number;
  readonly keepSummary?: boolean;
}) {
  return {
    olderThan: overrides.olderThan,
    statuses: overrides.statuses ?? (["completed"] as const),
    batchSize: overrides.batchSize ?? 1000,
    keepSummary: overrides.keepSummary ?? true,
  };
}

describe("InMemoryStore.pruneFacts — selection", () => {
  it("prunes completed terminal runs older than the cutoff, leaves running untouched", async () => {
    const { store, runIds } = await seedRuns([
      {
        flowId: "f",
        startedAtMs: 1000,
        terminal: { kind: "completed", atMs: 2000 },
      },
      {
        flowId: "f",
        startedAtMs: 1500,
        terminal: { kind: "completed", atMs: 2500 },
      },
      {
        flowId: "f",
        startedAtMs: 2000,
        terminal: { kind: "completed", atMs: 3000 },
      },
      { flowId: "f", startedAtMs: 1100 },
    ]);
    const r = await store.pruneFacts(defaults({ olderThan: new Date(10_000) }));
    expect(r.runsPruned).toBe(3);
    expect(r.factsPruned).toBe(6);
    const remaining = await store.queryRuns({});
    expect(remaining.runs.map((s) => s.runId)).toContain(runIds[3]);
    expect(remaining.runs).toHaveLength(4);
  });

  it("respects the olderThan cutoff", async () => {
    const { store } = await seedRuns([
      {
        flowId: "f",
        startedAtMs: 1000,
        terminal: { kind: "completed", atMs: 2000 },
      },
      {
        flowId: "f",
        startedAtMs: 1000,
        terminal: { kind: "completed", atMs: 9000 },
      },
    ]);
    const r = await store.pruneFacts(defaults({ olderThan: new Date(5000) }));
    expect(r.runsPruned).toBe(1);
  });

  it("default statuses = ['completed'] — failed and canceled stay", async () => {
    const { store } = await seedRuns([
      {
        flowId: "f",
        startedAtMs: 1000,
        terminal: { kind: "completed", atMs: 2000 },
      },
      {
        flowId: "f",
        startedAtMs: 1000,
        terminal: { kind: "failed", atMs: 2000 },
      },
      {
        flowId: "f",
        startedAtMs: 1000,
        terminal: { kind: "canceled", atMs: 2000 },
      },
    ]);
    const r = await store.pruneFacts(defaults({ olderThan: new Date(10_000) }));
    expect(r.runsPruned).toBe(1);
  });

  it("statuses array prunes every listed terminal status", async () => {
    const { store } = await seedRuns([
      {
        flowId: "f",
        startedAtMs: 1000,
        terminal: { kind: "completed", atMs: 2000 },
      },
      {
        flowId: "f",
        startedAtMs: 1000,
        terminal: { kind: "failed", atMs: 2000 },
      },
      {
        flowId: "f",
        startedAtMs: 1000,
        terminal: { kind: "canceled", atMs: 2000 },
      },
    ]);
    const r = await store.pruneFacts(
      defaults({
        olderThan: new Date(10_000),
        statuses: ["completed", "failed", "canceled"],
      }),
    );
    expect(r.runsPruned).toBe(3);
  });

  it("empty store → 0 / 0", async () => {
    const { store } = await seedRuns([]);
    const r = await store.pruneFacts(defaults({ olderThan: new Date(10_000) }));
    expect(r).toEqual({ runsPruned: 0, factsPruned: 0 });
  });

  it("batchSize parameter does not affect total drained per call", async () => {
    const cases: SeedCase[] = [];
    for (let i = 0; i < 5; i++) {
      cases.push({
        flowId: "f",
        startedAtMs: 1000 + i,
        terminal: { kind: "completed", atMs: 2000 + i },
      });
    }
    const { store } = await seedRuns(cases);
    const r = await store.pruneFacts(
      defaults({ olderThan: new Date(10_000), batchSize: 2 }),
    );
    expect(r.runsPruned).toBe(5);
  });
});

describe("InMemoryStore.pruneFacts — keepSummary", () => {
  it("keepSummary: true → run still listed by queryRuns", async () => {
    const { store, runIds } = await seedRuns([
      {
        flowId: "f",
        input: { videoId: "abc" },
        startedAtMs: 1000,
        terminal: { kind: "completed", atMs: 2000 },
      },
    ]);
    const runId = runIds[0];
    await store.pruneFacts(
      defaults({ olderThan: new Date(10_000), keepSummary: true }),
    );
    const r = await store.queryRuns({});
    expect(r.runs).toHaveLength(1);
    expect(r.runs[0]?.runId).toBe(runId);
    expect(r.runs[0]?.status).toBe("completed");
    expect(r.runs[0]?.input).toEqual({ videoId: "abc" });
  });

  it("keepSummary: false → run no longer listed by queryRuns", async () => {
    const { store } = await seedRuns([
      {
        flowId: "f",
        startedAtMs: 1000,
        terminal: { kind: "completed", atMs: 2000 },
      },
    ]);
    await store.pruneFacts(
      defaults({ olderThan: new Date(10_000), keepSummary: false }),
    );
    const r = await store.queryRuns({});
    expect(r.runs).toEqual([]);
  });

  it("keepSummary: true → tryStartRun with the same runId returns started:false", async () => {
    const { store, runIds } = await seedRuns([
      {
        flowId: "f",
        startedAtMs: 1000,
        terminal: { kind: "completed", atMs: 2000 },
      },
    ]);
    const runId = runIds[0] as RunId;
    await store.pruneFacts(
      defaults({ olderThan: new Date(10_000), keepSummary: true }),
    );
    const result = await store.tryStartRun(runId, {
      kind: "flow.started",
      runId,
      flowId: "f",
      input: null,
      at: new Date(20_000),
    });
    expect(result.started).toBe(false);
  });
});

describe("InMemoryStore.pruneFacts — secondary state cleanup", () => {
  it("removes outputs / onces / leases / childrenByParent for pruned runs", async () => {
    const store = new InMemoryStore();
    const parentId = "run-parent" as RunId;
    const childId = "run-child" as RunId;

    await store.tryStartRun(parentId, {
      kind: "flow.started",
      runId: parentId,
      flowId: "parent",
      input: null,
      at: new Date(1000),
    });
    await store.tryStartRun(childId, {
      kind: "flow.started",
      runId: childId,
      flowId: "child",
      input: null,
      at: new Date(1100),
      parentRunId: parentId,
    });
    await store.completeStep(
      childId,
      "s1",
      { ok: 1 },
      {
        kind: "step.completed",
        runId: childId,
        stepId: "s1",
        attempt: 1,
        at: new Date(1200),
        output: { ok: 1 },
      },
    );
    await store.recordOnce(childId, "s1", "scope", { recorded: true });
    await store.claimStep(childId, "s2", 1);
    await store.appendFact(childId, {
      kind: "flow.completed",
      runId: childId,
      at: new Date(1500),
      output: null,
    });

    await store.pruneFacts(
      defaults({ olderThan: new Date(10_000), keepSummary: false }),
    );

    expect(await store.listChildren(parentId)).toEqual([]);
    expect(await store.getStepOutput(childId, "s1")).toBeNull();
    expect(await store.getOnce(childId, "s1", "scope")).toBeNull();
    expect(await store.claimStep(childId, "s2", 1)).toBeTruthy();
  });
});

describe("wf.pruneFacts — runtime validation", () => {
  const wfFlow = flow({
    id: "f",
    input: passthroughSchema<Record<string, never>>(),
    build: (b) => ({ a: b.task({ run: async () => null }) }),
  });

  async function makeWf() {
    return nagi({
      flows: [wfFlow],
      store: new InMemoryStore(),
      queue: new InMemoryQueue(),
      clock: new InMemoryClock(),
    });
  }

  it("rejects non-Date olderThan", async () => {
    const wf = await makeWf();
    await expect(
      wf.pruneFacts({ olderThan: "yesterday" as unknown as Date }),
    ).rejects.toBeInstanceOf(NagiValidationError);
  });

  it("rejects invalid Date olderThan", async () => {
    const wf = await makeWf();
    await expect(
      wf.pruneFacts({ olderThan: new Date("not-a-date") }),
    ).rejects.toBeInstanceOf(NagiValidationError);
  });

  it("rejects statuses including a non-terminal value (runtime bypass)", async () => {
    const wf = await makeWf();
    await expect(
      wf.pruneFacts({
        olderThan: new Date(0),
        statuses: ["running"] as unknown as ReadonlyArray<PrunableStatus>,
      }),
    ).rejects.toBeInstanceOf(NagiValidationError);
  });

  it("rejects non-positive batchSize", async () => {
    const wf = await makeWf();
    await expect(
      wf.pruneFacts({ olderThan: new Date(0), batchSize: 0 }),
    ).rejects.toBeInstanceOf(NagiValidationError);
    await expect(
      wf.pruneFacts({ olderThan: new Date(0), batchSize: 1.5 }),
    ).rejects.toBeInstanceOf(NagiValidationError);
  });

  it("applies defaults and returns a PruneResult", async () => {
    const wf = await makeWf();
    const r = await wf.pruneFacts({ olderThan: new Date(0) });
    expect(r).toEqual({ runsPruned: 0, factsPruned: 0 });
  });
});
