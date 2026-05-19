import { describe, expect, it } from "vitest";
import { flow } from "./builder";
import { fingerprintFlows } from "./canonicalize";
import { InMemoryClock, InMemoryQueue, InMemoryStore } from "./memory";
import { nagi } from "./runtime";
import { passthroughSchema } from "./test-helpers";

function makeStores() {
  return {
    store: new InMemoryStore(),
    queue: new InMemoryQueue(),
    clock: new InMemoryClock(),
  };
}

function single(id = "fp-single") {
  return flow({
    id,
    input: passthroughSchema<Record<string, never>>(),
    build: (b) => ({
      only: b.task({ run: async () => ({ v: 1 }) }),
    }),
  });
}

describe("fingerprintFlows — byte-stability invariants (same hash)", () => {
  it("is deterministic across calls for the same flow set", async () => {
    expect(await fingerprintFlows([single()])).toBe(
      await fingerprintFlows([single()]),
    );
  });

  it("ignores the order of flows in the input array", async () => {
    const a = flow({
      id: "alpha",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => ({ only: b.task({ run: async () => null }) }),
    });
    const z = flow({
      id: "zulu",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => ({ only: b.task({ run: async () => null }) }),
    });
    expect(await fingerprintFlows([a, z])).toBe(await fingerprintFlows([z, a]));
  });

  it("ignores `run` handler body changes", async () => {
    const f1 = flow({
      id: "body-ignored",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => ({
        only: b.task({ run: async () => ({ v: 1 }) }),
      }),
    });
    const f2 = flow({
      id: "body-ignored",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => ({
        only: b.task({ run: async () => ({ v: 999, extra: "stuff" }) }),
      }),
    });
    expect(await fingerprintFlows([f1])).toBe(await fingerprintFlows([f2]));
  });
});

describe("fingerprintFlows — byte-difference invariants (different hash)", () => {
  it("differs when a flow is added", async () => {
    const a = single("a");
    const b = single("b");
    expect(await fingerprintFlows([a])).not.toBe(
      await fingerprintFlows([a, b]),
    );
  });

  it("differs when a flow id is renamed", async () => {
    expect(await fingerprintFlows([single("one")])).not.toBe(
      await fingerprintFlows([single("two")]),
    );
  });

  it("differs when a step is added to a registered flow", async () => {
    const f1 = flow({
      id: "grow",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => ({
        only: b.task({ run: async () => null }),
      }),
    });
    const f2 = flow({
      id: "grow",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => ({
        only: b.task({ run: async () => null }),
        extra: b.task({ run: async () => null }),
      }),
    });
    expect(await fingerprintFlows([f1])).not.toBe(await fingerprintFlows([f2]));
  });

  it("differs when a `needs` edge is added", async () => {
    const f1 = flow({
      id: "edges",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => {
        const a = b.task({ run: async () => null });
        const z = b.task({ run: async () => null });
        return { a, z };
      },
    });
    const f2 = flow({
      id: "edges",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => {
        const a = b.task({ run: async () => null });
        const z = b.task({
          needs: { a },
          run: async () => null,
        });
        return { a, z };
      },
    });
    expect(await fingerprintFlows([f1])).not.toBe(await fingerprintFlows([f2]));
  });

  it("differs when a `when` predicate is added", async () => {
    const f1 = flow({
      id: "when-gain",
      input: passthroughSchema<{ ok: boolean }>(),
      build: (b) => ({
        only: b.task({ run: async () => null }),
      }),
    });
    const f2 = flow({
      id: "when-gain",
      input: passthroughSchema<{ ok: boolean }>(),
      build: (b) => ({
        only: b.task({
          when: ({ input }) => input.ok,
          run: async () => null,
        }),
      }),
    });
    expect(await fingerprintFlows([f1])).not.toBe(await fingerprintFlows([f2]));
  });
});

describe("nagi() — codeVersion auto-default integration", () => {
  it("uses fingerprintFlows when codeVersion is omitted", async () => {
    const f = single("auto-default");
    const { store, queue, clock } = makeStores();
    const wf = await nagi({ flows: [f], store, queue, clock });
    const runId = await wf.start(f, {});
    const state = await store.loadRunState(runId);
    expect(state.codeVersion).toBe(await fingerprintFlows([f]));
  });

  it("uses the explicit codeVersion as-is when supplied", async () => {
    const f = single("explicit");
    const { store, queue, clock } = makeStores();
    const wf = await nagi({
      flows: [f],
      store,
      queue,
      clock,
      codeVersion: "manual-tag-v3",
    });
    const runId = await wf.start(f, {});
    const state = await store.loadRunState(runId);
    expect(state.codeVersion).toBe("manual-tag-v3");
  });

  it("explicit codeVersion does not equal the auto-fingerprint", async () => {
    const f = single("override-vs-auto");
    const { store, queue, clock } = makeStores();
    const wf = await nagi({
      flows: [f],
      store,
      queue,
      clock,
      codeVersion: "manual-tag-v3",
    });
    const runId = await wf.start(f, {});
    const state = await store.loadRunState(runId);
    expect(state.codeVersion).not.toBe(await fingerprintFlows([f]));
  });

  it("produces the same codeVersion across two boots of the same flow set", async () => {
    const f1 = single("stable-across-boots");
    const f2 = single("stable-across-boots");
    const s1 = makeStores();
    const s2 = makeStores();
    const wf1 = await nagi({ flows: [f1], ...s1 });
    const wf2 = await nagi({ flows: [f2], ...s2 });
    const id1 = await wf1.start(f1, {});
    const id2 = await wf2.start(f2, {});
    const state1 = await s1.store.loadRunState(id1);
    const state2 = await s2.store.loadRunState(id2);
    expect(state1.codeVersion).toBe(state2.codeVersion);
  });

  it("shifts codeVersion across boots when topology changes", async () => {
    const before = flow({
      id: "shifts",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => ({ only: b.task({ run: async () => null }) }),
    });
    const after = flow({
      id: "shifts",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => ({
        only: b.task({ run: async () => null }),
        extra: b.task({ run: async () => null }),
      }),
    });
    const s1 = makeStores();
    const s2 = makeStores();
    const wf1 = await nagi({ flows: [before], ...s1 });
    const wf2 = await nagi({ flows: [after], ...s2 });
    const id1 = await wf1.start(before, {});
    const id2 = await wf2.start(after, {});
    const state1 = await s1.store.loadRunState(id1);
    const state2 = await s2.store.loadRunState(id2);
    expect(state1.codeVersion).not.toBe(state2.codeVersion);
  });
});
