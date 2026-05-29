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
        wait: b.signal({ schema: passthroughSchema<{ ok: true }>() }),
      }),
    });
    const wfA = await nagi({ flows: [fA], store, queue, clock });
    const runId = await wfA.start(fA, {});

    // New process: same flowId but different shape → different hash.
    const fNew = flow({
      id: "signal-flow",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => ({
        wait: b.signal({ schema: passthroughSchema<{ ok: true }>() }),
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
