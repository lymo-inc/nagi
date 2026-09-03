import { describe, expect, it } from "vitest";
import { flow } from "../builder";
import { NagiFlowSnapshotGoneError, NagiRuntimeError } from "../errors";
import { Facts } from "../facts";
import {
  type FlowGone,
  type FlowResolution,
  registerFlows,
  requireCurrent,
} from "../flows";
import { asStepMapWithDefs, getDef } from "../internal";
import { InMemoryClock, InMemoryStore } from "../memory";
import { foldRun } from "../state";
import type { Fact, Flow, RunId } from "../types";
import { passthroughSchema } from "./test-helpers";

const RUN = "run-1" as RunId;

function flowV1() {
  return flow({
    id: "f",
    input: passthroughSchema<Record<string, never>>(),
    build: (b) => ({
      s: b.task({ run: async () => ({ v: 1 }) }),
    }),
  });
}

function flowV2() {
  return flow({
    id: "f",
    input: passthroughSchema<Record<string, never>>(),
    build: (b) => ({
      s: b.task({ run: async () => ({ v: 2 }) }),
      added: b.task({ run: async () => ({ added: true }) }),
    }),
  });
}

function unrelated() {
  return flow({
    id: "unrelated",
    input: passthroughSchema<Record<string, never>>(),
    build: (b) => ({ s: b.task({ run: async () => ({}) }) }),
  });
}

async function register(
  flows: ReadonlyArray<Flow>,
  store = new InMemoryStore(),
) {
  const clock = new InMemoryClock();
  return { store, registry: await registerFlows({ flows, store, clock }) };
}

function started(flowId: string, flowHash?: string): Fact {
  return Facts.flowStarted({
    runId: RUN,
    flowId,
    input: {},
    at: new Date(0),
    ...(flowHash !== undefined ? { flowHash } : {}),
  });
}

function gone(r: FlowResolution): FlowGone {
  if (r.kind === "current") throw new Error("expected a gone arm");
  return r;
}

describe("registerFlows — registration", () => {
  it("rejects duplicate flow ids", async () => {
    await expect(register([flowV1(), flowV1()])).rejects.toThrow(
      /Duplicate flow id "f"/,
    );
  });

  it("upserts the snapshot, bumps the ref, and appends flow_ref.updated only when the hash changes", async () => {
    const store = new InMemoryStore();
    const { registry: first } = await register([flowV1()], store);
    const v1Hash = first.hashOf("f");
    expect(await store.getRef("f")).toBe(v1Hash);
    expect(await store.loadSnapshot(v1Hash)).toMatchObject({ flowId: "f" });
    expect(store.readGlobalFacts()).toEqual([
      expect.objectContaining({
        kind: "flow_ref.updated",
        flowId: "f",
        from: null,
        to: v1Hash,
      }),
    ]);

    await register([flowV1()], store);
    expect(store.readGlobalFacts()).toHaveLength(1);

    const { registry: third } = await register([flowV2()], store);
    const v2Hash = third.hashOf("f");
    expect(v2Hash).not.toBe(v1Hash);
    expect(await store.getRef("f")).toBe(v2Hash);
    expect(store.readGlobalFacts()).toHaveLength(2);
    expect(store.readGlobalFacts()[1]).toMatchObject({
      from: v1Hash,
      to: v2Hash,
    });
    // Both snapshots stay loadable — the old one is what drift replay needs.
    expect(await store.loadSnapshot(v1Hash)).not.toBeNull();
  });

  it("get / has / require / hashOf agree on what is registered", async () => {
    const f = flowV1();
    const { registry } = await register([f]);
    expect(registry.get("f")).toBe(f);
    expect(registry.has("f")).toBe(true);
    expect(registry.require("f")).toBe(f);
    expect(typeof registry.hashOf("f")).toBe("string");
    expect(registry.get("nope")).toBeUndefined();
    expect(registry.has("nope")).toBe(false);
    expect(() => registry.require("nope")).toThrow(NagiRuntimeError);
    expect(() => registry.hashOf("nope")).toThrow(NagiRuntimeError);
  });

  it("indexes streaming step ids", async () => {
    const streaming = flow({
      id: "st",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => ({
        chunks: b.streamingTask({ run: async () => ({ done: true }) }),
        plain: b.task({ run: async () => ({}) }),
      }),
    });
    const { registry } = await register([streaming]);
    expect(registry.isStreaming("chunks")).toBe(true);
    expect(registry.isStreaming("plain")).toBe(false);
  });
});

describe("registry.resolve — every arm", () => {
  it("current: pinned hash matches the registered flow", async () => {
    const f = flowV1();
    const { registry } = await register([f]);
    const r = registry.resolve(
      foldRun(RUN, [started("f", registry.hashOf("f"))]),
    );
    expect(r).toEqual({ kind: "current", flow: f });
    expect(requireCurrent(r)).toBe(f);
  });

  it("current: legacy run with no pinned hash resolves by id alone", async () => {
    const f = flowV1();
    const { registry } = await register([f]);
    expect(registry.resolve(foldRun(RUN, [started("f")]))).toEqual({
      kind: "current",
      flow: f,
    });
  });

  it("throws NagiRuntimeError for an unpinned run whose flow id is unregistered", async () => {
    const { registry } = await register([flowV1()]);
    expect(() => registry.resolve(foldRun(RUN, [started("other")]))).toThrow(
      NagiRuntimeError,
    );
  });

  it("gone-live: hash replaced by a deploy while the run is still running", async () => {
    const { registry: old } = await register([flowV1()]);
    const pinned = old.hashOf("f");
    const v2 = flowV2();
    const { registry } = await register([v2]);

    const r = registry.resolve(foldRun(RUN, [started("f", pinned)]));
    expect(r.kind).toBe("gone-live");
    const g = gone(r);
    expect(g.live).toBe(v2);
    expect(g.error).toBeInstanceOf(NagiFlowSnapshotGoneError);
    expect(g.error.runId).toBe(RUN);
    expect(g.error.flowId).toBe("f");
    expect(g.error.pinnedHash).toBe(pinned);
    expect(g.error.currentHash).toBe(registry.hashOf("f"));
    expect(g.error.currentHash).not.toBe(pinned);
    expect(() => requireCurrent(r)).toThrow(g.error);
  });

  it("gone-live: flow id no longer registered at all — live undefined, currentHash null", async () => {
    const { registry: old } = await register([flowV1()]);
    const pinned = old.hashOf("f");
    const { registry } = await register([unrelated()]);
    const g = gone(registry.resolve(foldRun(RUN, [started("f", pinned)])));
    expect(g.kind).toBe("gone-live");
    expect(g.live).toBeUndefined();
    expect(g.error.currentHash).toBeNull();
    expect(g.error.message).toMatch(/not registered with the current nagi/);
  });

  it.each([
    ["completed", () => Facts.flowCompleted(RUN, {}, new Date(1))],
    [
      "failed",
      () => Facts.flowFailed(RUN, { name: "E", message: "boom" }, new Date(1)),
    ],
    [
      "canceled",
      () =>
        Facts.flowCanceled(
          RUN,
          { cause: "explicit", reason: "test" },
          new Date(1),
        ),
    ],
  ])("gone-terminal: pinned hash gone and the run already %s", async (_, end) => {
    const { registry: old } = await register([flowV1()]);
    const pinned = old.hashOf("f");
    const v2 = flowV2();
    const { registry } = await register([v2]);
    const r = registry.resolve(foldRun(RUN, [started("f", pinned), end()]));
    expect(r.kind).toBe("gone-terminal");
    const g = gone(r);
    expect(g.live).toBe(v2);
    expect(g.error.pinnedHash).toBe(pinned);
    expect(() => requireCurrent(r)).toThrow(NagiFlowSnapshotGoneError);
  });

  it("a terminal run with a matching hash is still current — terminal-ness alone is not gone", async () => {
    const f = flowV1();
    const { registry } = await register([f]);
    const r = registry.resolve(
      foldRun(RUN, [
        started("f", registry.hashOf("f")),
        Facts.flowCompleted(RUN, {}, new Date(1)),
      ]),
    );
    expect(r).toEqual({ kind: "current", flow: f });
  });
});

describe("registry.synthesize — drift-allowed replay flow", () => {
  it("builds the pinned snapshot's DAG with the live flow's handlers attached", async () => {
    const store = new InMemoryStore();
    const { registry: old } = await register([flowV1()], store);
    const pinned = old.hashOf("f");
    const { registry } = await register([flowV2()], store);
    const g = gone(registry.resolve(foldRun(RUN, [started("f", pinned)])));

    const synthesized = await registry.synthesize(g);
    expect(synthesized.id).toBe("f");
    // v1's shape: only `s`; v2's `added` must not leak in.
    expect(Object.keys(synthesized.steps)).toEqual(["s"]);
    // Handler borrowed from live v2.
    const step = asStepMapWithDefs(synthesized.steps)["s"];
    expect(step).toBeDefined();
    const def = getDef(step!);
    expect(def.kind).toBe("task");
    if (def.kind === "task") {
      await expect(
        def.run({ input: {}, needs: {}, ctx: {} as never }),
      ).resolves.toEqual({ v: 2 });
    }
  });

  it("throws NagiRuntimeError when no flow is registered under the id", async () => {
    const store = new InMemoryStore();
    const { registry: old } = await register([flowV1()], store);
    const pinned = old.hashOf("f");
    const { registry } = await register([unrelated()], store);
    const g = gone(registry.resolve(foldRun(RUN, [started("f", pinned)])));
    await expect(registry.synthesize(g)).rejects.toThrow(
      /references flow "f" which is not registered/,
    );
  });

  it("throws NagiRuntimeError when the pinned snapshot is not in the store", async () => {
    const { registry } = await register([flowV2()]);
    const g = gone(
      registry.resolve(foldRun(RUN, [started("f", "deadbeefdeadbeef")])),
    );
    await expect(registry.synthesize(g)).rejects.toThrow(
      /no snapshot with that hash was found/,
    );
  });
});
