import { describe, expect, it } from "vitest";
import { flow } from "./builder";
import { canonicalize, sha256Canonical } from "./canonicalize";
import {
  InMemoryClock,
  InMemoryQueue,
  InMemoryStore,
} from "./memory";
import { nagi, NagiSnapshotDriftError } from "./runtime";
import { passthroughSchema } from "./test-helpers";
import type { RunId } from "./types";

function makeStores() {
  return {
    store: new InMemoryStore(),
    queue: new InMemoryQueue(),
    clock: new InMemoryClock(),
  };
}

describe("snapshot store — boot wiring", () => {
  it("upserts a snapshot and sets the ref on first boot", async () => {
    const f = flow({
      id: "boot-once",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => ({ x: b.task({ run: async () => null }) }),
    });
    const { store, queue, clock } = makeStores();
    await nagi({ flows: [f], store, queue, clock });

    const expectedHash = await sha256Canonical(await canonicalize(f));
    const snap = await store.loadSnapshot(expectedHash);
    expect(snap).not.toBeNull();
    expect(snap?.flowId).toBe("boot-once");

    expect(await store.getRef("boot-once")).toBe(expectedHash);

    const globals = store.readGlobalFacts();
    expect(globals).toHaveLength(1);
    expect(globals[0]).toMatchObject({
      kind: "flow_ref.updated",
      flowId: "boot-once",
      from: null,
      to: expectedHash,
    });
  });

  it("is a no-op when the hash matches the existing ref", async () => {
    const f = flow({
      id: "boot-idempotent",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => ({ x: b.task({ run: async () => null }) }),
    });
    const { store, queue, clock } = makeStores();
    await nagi({ flows: [f], store, queue, clock });
    await nagi({ flows: [f], store, queue, clock });

    expect(store.readGlobalFacts()).toHaveLength(1);
  });

  it("rotates the ref and appends a global fact when topology changes", async () => {
    const f1 = flow({
      id: "boot-rotate",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => ({ x: b.task({ run: async () => null }) }),
    });
    const f2 = flow({
      id: "boot-rotate",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => ({
        x: b.task({ run: async () => null }),
        y: b.task({ run: async () => null }),
      }),
    });
    const { store, queue, clock } = makeStores();
    await nagi({ flows: [f1], store, queue, clock });
    const firstHash = await store.getRef("boot-rotate");
    await nagi({ flows: [f2], store, queue, clock });
    const secondHash = await store.getRef("boot-rotate");

    expect(secondHash).not.toBe(firstHash);
    const globals = store.readGlobalFacts();
    expect(globals).toHaveLength(2);
    expect(globals[1]).toMatchObject({
      kind: "flow_ref.updated",
      flowId: "boot-rotate",
      from: firstHash,
      to: secondHash,
    });
  });

  it("pins flow_hash + code_version onto the flow.started fact", async () => {
    const f = flow({
      id: "pinned",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => ({ x: b.task({ run: async () => null }) }),
    });
    const { store, queue, clock } = makeStores();
    const wf = await nagi({
      flows: [f],
      store,
      queue,
      clock,
      codeVersion: "abc1234",
    });

    const runId = await wf.start(f, {});
    const state = await store.loadRunState(runId);
    expect(state.flowHash).toBe(await sha256Canonical(await canonicalize(f)));
    expect(state.codeVersion).toBe("abc1234");
  });
});

describe("wf.replay() — drift detection", () => {
  it("throws NagiSnapshotDriftError when the live flow's hash differs", async () => {
    const original = flow({
      id: "drift",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => ({ x: b.task({ run: async () => null }) }),
    });
    const drifted = flow({
      id: "drift",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => ({
        x: b.task({ run: async () => null }),
        y: b.task({ run: async () => null }),
      }),
    });
    const { store, queue, clock } = makeStores();
    const wf1 = await nagi({ flows: [original], store, queue, clock });
    const runId = await wf1.start(original, {});

    // Boot a second nagi() with the drifted flow shape but the same store.
    const wf2 = await nagi({ flows: [drifted], store, queue, clock });

    await expect(wf2.replay(runId, { mode: "continue" })).rejects.toThrow(
      NagiSnapshotDriftError,
    );
  });

  it("proceeds when live hash matches (no drift)", async () => {
    const f = flow({
      id: "no-drift",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => ({ x: b.task({ run: async () => null }) }),
    });
    const { store, queue, clock } = makeStores();
    const wf = await nagi({ flows: [f], store, queue, clock });
    const runId = await wf.start(f, {});

    // Same flow, same nagi instance — no drift expected.
    await expect(
      wf.replay(runId, { mode: "continue" }),
    ).resolves.toBeUndefined();
  });

  it("inspect mode skips drift check entirely", async () => {
    const original = flow({
      id: "drift-inspect",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => ({ x: b.task({ run: async () => null }) }),
    });
    const drifted = flow({
      id: "drift-inspect",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => ({
        x: b.task({ run: async () => null }),
        y: b.task({ run: async () => null }),
      }),
    });
    const { store, queue, clock } = makeStores();
    const wf1 = await nagi({ flows: [original], store, queue, clock });
    const runId = await wf1.start(original, {});
    const wf2 = await nagi({ flows: [drifted], store, queue, clock });

    // inspect mode is a probe — no execution, no drift check.
    await expect(
      wf2.replay(runId, { mode: "inspect" }),
    ).resolves.toBeUndefined();
  });

  it("allowDrift: true proceeds without throwing", async () => {
    const original = flow({
      id: "drift-allow",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => ({ x: b.task({ run: async () => null }) }),
    });
    const drifted = flow({
      id: "drift-allow",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => ({
        x: b.task({ run: async () => null }),
        y: b.task({ run: async () => null }),
      }),
    });
    const { store, queue, clock } = makeStores();
    const wf1 = await nagi({ flows: [original], store, queue, clock });
    const runId = await wf1.start(original, {});
    const wf2 = await nagi({ flows: [drifted], store, queue, clock });

    await expect(
      wf2.replay(runId, { mode: "continue", allowDrift: true }),
    ).resolves.toBeUndefined();
  });

  it("legacy runs (no pinned flowHash) skip the drift check", async () => {
    // Simulate a pre-snapshot-store run by appending a flow.started fact
    // without flowHash. (Real legacy runs migrated from a prior version of
    // the schema would land here.)
    const f = flow({
      id: "legacy",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => ({ x: b.task({ run: async () => null }) }),
    });
    const { store, queue, clock } = makeStores();
    const wf = await nagi({ flows: [f], store, queue, clock });

    const runId = "run-legacy" as RunId;
    await store.appendFact(runId, {
      kind: "flow.started",
      runId,
      flowId: "legacy",
      input: {},
      at: clock.now(),
      // no flowHash — simulating legacy
    });

    await expect(
      wf.replay(runId, { mode: "continue" }),
    ).resolves.toBeUndefined();
  });
});
