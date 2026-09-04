import { describe, expect, it } from "vitest";
import { flow } from "../builder";
import { NagiFlowSnapshotGoneError, NagiSnapshotDriftError } from "../errors";
import { InMemoryClock, InMemoryQueue, InMemoryStore } from "../memory";
import { nagi } from "../runtime";
import type { Fact, RunId } from "../types";
import { passthroughSchema } from "./test-helpers";

function makeFlowA() {
  return flow({
    id: "fA",
    input: passthroughSchema<Record<string, never>>(),
    build: (b) => ({
      s: b.task({ run: async () => ({ ok: true }) }),
    }),
  });
}

function makeFlowB() {
  return flow({
    id: "fA",
    input: passthroughSchema<Record<string, never>>(),
    build: (b) => ({
      s: b.task({ run: async () => ({ different: true }) }),
      added: b.task({ run: async () => ({ added: true }) }),
    }),
  });
}

describe("NagiFlowSnapshotGoneError", () => {
  it("dispatch on a run whose flow_hash is not in the current registry throws NagiFlowSnapshotGoneError", async () => {
    const store = new InMemoryStore();
    const queue = new InMemoryQueue();
    const clock = new InMemoryClock();

    // Process A: register flow + start run + leave message in queue.
    const fA = makeFlowA();
    const wfA = await nagi({ flows: [fA], store, queue, clock });
    const runId = await wfA.start(fA, {});

    // Process B: same store, but flow body differs → different flowHash. The
    // queued message is for the old hash; flowFor must throw.
    const fB = makeFlowB();
    const wfB = await nagi({ flows: [fB], store, queue, clock });

    const deps = (
      wfB as unknown as {
        __dispatchDeps: { flowFor: (r: RunId) => Promise<unknown> };
      }
    ).__dispatchDeps;
    await expect(deps.flowFor(runId)).rejects.toBeInstanceOf(
      NagiFlowSnapshotGoneError,
    );
  });

  it("includes pinnedHash and currentHash for diagnostic", async () => {
    const store = new InMemoryStore();
    const queue = new InMemoryQueue();
    const clock = new InMemoryClock();

    const fA = makeFlowA();
    const wfA = await nagi({ flows: [fA], store, queue, clock });
    const runId = await wfA.start(fA, {});

    const fB = makeFlowB();
    const wfB = await nagi({ flows: [fB], store, queue, clock });
    const deps = (
      wfB as unknown as {
        __dispatchDeps: { flowFor: (r: RunId) => Promise<unknown> };
      }
    ).__dispatchDeps;
    try {
      await deps.flowFor(runId);
      throw new Error("expected NagiFlowSnapshotGoneError");
    } catch (err) {
      expect(err).toBeInstanceOf(NagiFlowSnapshotGoneError);
      const e = err as NagiFlowSnapshotGoneError;
      expect(e.runId).toBe(runId);
      expect(e.flowId).toBe("fA");
      expect(typeof e.pinnedHash).toBe("string");
      expect(e.pinnedHash.length).toBeGreaterThan(0);
      expect(typeof e.currentHash).toBe("string");
      expect(e.currentHash).not.toBe(e.pinnedHash);
    }
  });

  it("wf.cancel on a run with a gone flow_hash succeeds (cancel bypasses flow registry)", async () => {
    const store = new InMemoryStore();
    const queue = new InMemoryQueue();
    const clock = new InMemoryClock();

    const fA = makeFlowA();
    const wfA = await nagi({ flows: [fA], store, queue, clock });
    const runId = await wfA.start(fA, {});

    // Now switch process: register a flow under a different id so the original
    // flowId is missing entirely. wf.cancel must NOT throw — cancel is fact-only.
    const fOther = flow({
      id: "different-flow",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => ({ s: b.task({ run: async () => ({}) }) }),
    });
    const wfB = await nagi({ flows: [fOther], store, queue, clock });
    await expect(
      wfB.cancel(runId, { reason: "test" }),
    ).resolves.toBeUndefined();

    const state = await store.loadRunState(runId);
    expect(state.phase.tag).toBe("canceled");
  });

  it("wf.signal on a run with a gone flow_hash also bypasses (fact-only)", async () => {
    const store = new InMemoryStore();
    const queue = new InMemoryQueue();
    const clock = new InMemoryClock();

    // Original flow has a signal step.
    const fA = flow({
      id: "signal-flow",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => ({
        wait: b.signal({
          timeoutMs: "unbounded" as const,
          schema: passthroughSchema<{ ok: true }>(),
        }),
      }),
    });
    const wfA = await nagi({ flows: [fA], store, queue, clock });
    const runId = await wfA.start(fA, {});

    // New process: same flowId but different shape → different hash.
    const fNew = flow({
      id: "signal-flow",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => ({
        wait: b.signal({
          timeoutMs: "unbounded" as const,
          schema: passthroughSchema<{ ok: true }>(),
        }),
        extra: b.task({ run: async () => ({}) }),
      }),
    });
    const wfB = await nagi({ flows: [fNew], store, queue, clock });

    // wf.signal must NOT throw — it bypasses the hash check (uses requireForRun
    // without pinnedHash). The signal lands as a buffered fact under the
    // already-known step id.
    await expect(
      wfB.signal(runId, "wait", { ok: true } as never),
    ).resolves.toBeUndefined();
  });

  it("NagiFlowSnapshotGoneError not raised when current registry has matching hash", async () => {
    const store = new InMemoryStore();
    const queue = new InMemoryQueue();
    const clock = new InMemoryClock();

    const fA = makeFlowA();
    const wfA = await nagi({ flows: [fA], store, queue, clock });
    const runId = await wfA.start(fA, {});

    // Re-create nagi with the SAME flow definition → same hash.
    const wfA2 = await nagi({ flows: [fA], store, queue, clock });
    const deps = (
      wfA2 as unknown as {
        __dispatchDeps: { flowFor: (r: RunId) => Promise<unknown> };
      }
    ).__dispatchDeps;
    await expect(deps.flowFor(runId)).resolves.toBeDefined();
  });

  it("is exported from @nagi-js/core", async () => {
    const mod = await import("../index");
    expect(typeof mod.NagiFlowSnapshotGoneError).toBe("function");
    const e = new mod.NagiFlowSnapshotGoneError({
      runId: "r" as RunId,
      flowId: "f",
      pinnedHash: "abcdef0123456789",
      currentHash: "zzzzzzzzzzzzzzzz",
    });
    expect(e).toBeInstanceOf(mod.NagiFlowSnapshotGoneError);
    expect(e.name).toBe("NagiFlowSnapshotGoneError");
  });

  it("existing snapshot-drift detection on replay still works (NagiSnapshotDriftError unchanged)", async () => {
    const store = new InMemoryStore();
    const queue = new InMemoryQueue();
    const clock = new InMemoryClock();

    const fA = makeFlowA();
    const wfA = await nagi({ flows: [fA], store, queue, clock });
    const runId = await wfA.start(fA, {});

    // Drain the run to completion under fA so it can be replayed.
    const ac = new AbortController();
    const worker = wfA.worker({ pollIntervalMs: 1, signal: ac.signal });
    const done = worker.run();
    try {
      const start = Date.now();
      while (Date.now() - start < 2_000) {
        const s = await store.loadRunState(runId);
        if (s.phase.tag === "completed") break;
        await new Promise((r) => setTimeout(r, 5));
      }
    } finally {
      ac.abort();
      await done;
    }
    const facts = (await store.loadRunState(runId)).facts as readonly Fact[];
    expect(facts.some((f) => f.kind === "flow.completed")).toBe(true);

    // New nagi with different hash (drift) → replay throws SnapshotDriftError,
    // NOT FlowSnapshotGoneError. Confirms the two errors stay separate.
    const fB = makeFlowB();
    const wfB = await nagi({ flows: [fB], store, queue, clock });
    await expect(
      wfB.replay(runId, { mode: "continue" }),
    ).rejects.toBeInstanceOf(NagiSnapshotDriftError);
  });
});

describe("snapshot-gone poison handling", () => {
  it("acks the message of a run canceled after its snapshot went gone (no nack loop)", async () => {
    const store = new InMemoryStore();
    const queue = new InMemoryQueue();
    const clock = new InMemoryClock();

    const fA = makeFlowA();
    const wfA = await nagi({ flows: [fA], store, queue, clock });
    const runId = await wfA.start(fA, {});

    const fB = makeFlowB();
    const wfB = await nagi({ flows: [fB], store, queue, clock });
    await wfB.cancel(runId, { reason: "operator cleanup" });

    // The policy must never be consulted: dispatchMessage acks terminal runs
    // before the error ever reaches the worker.
    const worker = wfB.worker({
      timerSweepIntervalMs: 0,
      snapshotGonePolicy: () => {
        throw new Error("policy must not run for a terminal run");
      },
    });
    const { processed } = await worker.runUntilEmpty();
    expect(processed).toBe(1);

    expect(await queue.dequeue({ count: 10 })).toHaveLength(0);
    expect((await store.loadRunState(runId)).phase.tag).toBe("canceled");
  });

  it("retries with the policy's delay while budgeted, then terminally fails the run", async () => {
    const store = new InMemoryStore();
    const queue = new InMemoryQueue();
    const clock = new InMemoryClock();

    const fA = makeFlowA();
    const wfA = await nagi({ flows: [fA], store, queue, clock });
    const runId = await wfA.start(fA, {});

    const fB = makeFlowB();
    const wfB = await nagi({ flows: [fB], store, queue, clock });

    const seenReadCounts: number[] = [];
    const worker = wfB.worker({
      timerSweepIntervalMs: 0,
      snapshotGonePolicy: (readCount) => {
        seenReadCounts.push(readCount);
        return readCount < 3
          ? { action: "retry", delayMs: 0 }
          : { action: "fail" };
      },
    });
    await worker.runUntilEmpty();

    // Redelivery count grew across nacks; the budget fired on the 3rd read.
    expect(seenReadCounts).toEqual([1, 2, 3]);
    expect(await queue.dequeue({ count: 10 })).toHaveLength(0);

    const state = await store.loadRunState(runId);
    expect(state.phase.tag).toBe("failed");
    if (state.phase.tag === "failed") {
      expect(state.phase.error.name).toBe("NagiFlowSnapshotGoneError");
    }
  });

  it("defaultSnapshotGonePolicy: quadratic backoff capped at 5min, fails past 60 deliveries", async () => {
    const { defaultSnapshotGonePolicy } = await import("../retry");
    expect(defaultSnapshotGonePolicy(1)).toEqual({
      action: "retry",
      delayMs: 1_000,
    });
    expect(defaultSnapshotGonePolicy(10)).toEqual({
      action: "retry",
      delayMs: 100_000,
    });
    // Cap: 18² = 324s exceeds the 300s ceiling.
    expect(defaultSnapshotGonePolicy(18)).toEqual({
      action: "retry",
      delayMs: 300_000,
    });
    expect(defaultSnapshotGonePolicy(60)).toEqual({
      action: "retry",
      delayMs: 300_000,
    });
    expect(defaultSnapshotGonePolicy(61)).toEqual({ action: "fail" });
    // The whole window is ~4.2h — above the 4h signal-timeout house constant,
    // below 6h stuck-run alerting. Pin it so a tweak is a conscious choice.
    let totalMs = 0;
    for (let readCount = 1; readCount <= 60; readCount++) {
      const d = defaultSnapshotGonePolicy(readCount);
      if (d.action === "retry") totalMs += d.delayMs;
    }
    expect(totalMs).toBeGreaterThan(4 * 3_600_000);
    expect(totalMs).toBeLessThan(6 * 3_600_000);
  });

  it("a failing policy falls back to a delayed nack instead of crashing or tight-looping", async () => {
    const store = new InMemoryStore();
    const queue = new InMemoryQueue();
    const clock = new InMemoryClock();

    const fA = makeFlowA();
    const wfA = await nagi({ flows: [fA], store, queue, clock });
    const runId = await wfA.start(fA, {});

    const fB = makeFlowB();
    const wfB = await nagi({ flows: [fB], store, queue, clock });
    const worker = wfB.worker({
      timerSweepIntervalMs: 0,
      snapshotGonePolicy: () => {
        throw new Error("broken policy");
      },
    });
    await worker.runUntilEmpty();

    // Message survived (delayed, not visible now) and the run is untouched —
    // the fallback keeps redelivery bounded without inventing a disposition.
    expect(await queue.dequeue({ count: 10 })).toHaveLength(0);
    expect((await store.loadRunState(runId)).phase.tag).toBe("running");
  });
});
