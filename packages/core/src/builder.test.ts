import { describe, expect, it } from "vitest";
import { flow } from "./builder";
import { getDef } from "./internal";
import { passthroughSchema } from "./test-helpers";

describe("flow()", () => {
  it("assigns each step's id from the build's return key", () => {
    const f = flow({
      id: "test",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => {
        const fetchRecording = b.task({ run: async () => ({ url: "x" }) });
        const transcribe = b.task({ run: async () => ({ text: "y" }) });
        return { fetchRecording, transcribe };
      },
    });

    expect(f.steps.fetchRecording?.id).toBe("fetchRecording");
    expect(f.steps.transcribe?.id).toBe("transcribe");
  });

  it("rewrites needs so each upstream Step carries its assigned id", () => {
    const f = flow({
      id: "needs-rewrite",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => {
        const upstream = b.task({ run: async () => ({ v: 1 }) });
        const downstream = b.task({
          needs: { foo: upstream },
          run: async ({ needs }) => ({ v: needs.foo.v + 1 }),
        });
        return { upstream, downstream };
      },
    });

    const downstreamDef = getDef(f.steps.downstream as never);
    // biome-ignore lint/complexity/useLiteralKeys: index-signature access requires bracket notation under TS strict
    const upstreamRef = downstreamDef.needs["foo"] as { id: string };
    expect(upstreamRef.id).toBe("upstream");
  });

  it("preserves rename: local needs key is independent of upstream id", () => {
    const f = flow({
      id: "rename",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => {
        const a = b.task({ run: async () => ({ v: 1 }) });
        const downstream = b.task({
          needs: { localName: a },
          run: async () => null,
        });
        return { a, downstream };
      },
    });

    const def = getDef(f.steps.downstream as never);
    expect(Object.keys(def.needs)).toEqual(["localName"]);
    // biome-ignore lint/complexity/useLiteralKeys: index-signature access requires bracket notation under TS strict
    expect((def.needs["localName"] as { id: string }).id).toBe("a");
  });

  it("throws when build returns a step that needs an unreturned upstream", () => {
    expect(() =>
      flow({
        id: "dangling",
        input: passthroughSchema<Record<string, never>>(),
        build: (b) => {
          const orphan = b.task({ run: async () => ({ v: 1 }) });
          const downstream = b.task({
            needs: { o: orphan },
            run: async () => null,
          });
          // `orphan` is intentionally NOT returned
          return { downstream };
        },
      }),
    ).toThrow(/upstream step that was not returned from build/);
  });

  it("throws with a cross-flow message when a step from another flow is returned", () => {
    const inner = flow({
      id: "inner",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => ({ shared: b.task({ run: async () => ({ v: 1 }) }) }),
    });
    expect(() =>
      flow({
        id: "outer",
        input: passthroughSchema<Record<string, never>>(),
        // biome-ignore lint/suspicious/noExplicitAny: testing cross-flow misuse
        build: () => ({ x: inner.steps.shared as any }),
      }),
    ).toThrow(/produced by a different flow\(\) call/);
  });

  it("throws with a cross-flow message when a needs upstream is from another flow", () => {
    const inner = flow({
      id: "inner",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => ({ shared: b.task({ run: async () => ({ v: 1 }) }) }),
    });
    expect(() =>
      flow({
        id: "outer",
        input: passthroughSchema<Record<string, never>>(),
        build: (b) => ({
          downstream: b.task({
            needs: { foreign: inner.steps.shared },
            run: async () => null,
          }),
        }),
      }),
    ).toThrow(/upstream step from a different flow\(\) call/);
  });

  it("preserves the step kind (task vs signal)", () => {
    const f = flow({
      id: "kinds",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => {
        const t = b.task({ run: async () => null });
        const s = b.signal({ schema: passthroughSchema<{ ok: boolean }>() });
        return { t, s };
      },
    });

    expect(f.steps.t?.kind).toBe("task");
    expect(f.steps.s?.kind).toBe("signal");
    expect(getDef(f.steps.t as never).kind).toBe("task");
    expect(getDef(f.steps.s as never).kind).toBe("signal");
  });

  it("preserves the flow's id and input schema reference", () => {
    const inputSchema = passthroughSchema<{ q: string }>();
    const f = flow({
      id: "preserves",
      input: inputSchema,
      build: () => ({}),
    });
    expect(f.id).toBe("preserves");
    expect(f.input).toBe(inputSchema);
  });
});

describe("b.step chain API", () => {
  it("assigns step id from the chain key", () => {
    const f = flow({
      id: "step-id",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) =>
        b
          .step("fetchRecording", { run: async () => ({ url: "x" }) })
          .step("transcribe", { run: async () => ({ text: "y" }) }),
    });

    expect(f.steps.fetchRecording?.id).toBe("fetchRecording");
    expect(f.steps.transcribe?.id).toBe("transcribe");
  });

  it("rewrites needs[] to upstream Step refs carrying assigned ids", () => {
    const f = flow({
      id: "step-needs",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) =>
        b
          .step("upstream", { run: async () => ({ v: 1 }) })
          .step("downstream", {
            needs: ["upstream"],
            run: async ({ needs }) => ({ v: needs.upstream.v + 1 }),
          }),
    });

    const downstreamDef = getDef(f.steps.downstream as never);
    // biome-ignore lint/complexity/useLiteralKeys: index-signature access under TS strict
    const upstreamRef = downstreamDef.needs["upstream"] as { id: string };
    expect(upstreamRef.id).toBe("upstream");
  });

  it("local sibling key in needs[] equals the upstream key (no rename)", () => {
    const f = flow({
      id: "step-no-rename",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) =>
        b
          .step("a", { run: async () => ({ v: 1 }) })
          .step("b", {
            needs: ["a"],
            run: async ({ needs }) => ({ v: needs.a.v + 1 }),
          }),
    });

    const def = getDef(f.steps.b as never);
    expect(Object.keys(def.needs)).toEqual(["a"]);
    // biome-ignore lint/complexity/useLiteralKeys: index-signature access under TS strict
    expect((def.needs["a"] as { id: string }).id).toBe("a");
  });

  it("preserves all step kinds as task", () => {
    const f = flow({
      id: "step-kinds",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) =>
        b
          .step("a", { run: async () => null })
          .step("b", { needs: ["a"], run: async () => null }),
    });
    expect(f.steps.a?.kind).toBe("task");
    expect(f.steps.b?.kind).toBe("task");
    expect(getDef(f.steps.a as never).kind).toBe("task");
    expect(getDef(f.steps.b as never).kind).toBe("task");
  });

  it("passes through retry / timeoutMs / when on each entry", () => {
    const f = flow({
      id: "step-passthrough",
      input: passthroughSchema<{ on: boolean }>(),
      build: (b) =>
        b.step("a", {
          retry: { maxAttempts: 3, backoff: "fixed", initialDelayMs: 10 },
          timeoutMs: 5_000,
          when: ({ input }) => input.on,
          run: async () => null,
        }),
    });
    const def = getDef(f.steps.a as never);
    if (def.kind !== "task") throw new Error("expected task def");
    expect(def.retry).toEqual({
      maxAttempts: 3,
      backoff: "fixed",
      initialDelayMs: 10,
    });
    expect(def.timeoutMs).toBe(5_000);
    expect(typeof def.when).toBe("function");
  });

  it("coexists with b.task / b.match via .include()", () => {
    const f = flow({
      id: "step-mix",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => {
        const standalone = b.task({ run: async () => ({ v: 1 }) });
        return b
          .step("a", { run: async () => ({ x: 1 }) })
          .step("c", { needs: ["a"], run: async () => ({ y: 2 }) })
          .include("standalone", standalone);
      },
    });
    expect(f.steps.a?.id).toBe("a");
    expect(f.steps.c?.id).toBe("c");
    expect(f.steps.standalone?.id).toBe("standalone");
  });

  it("rejects a step from another flow included in the chain", () => {
    const inner = flow({
      id: "inner",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => b.step("shared", { run: async () => ({ v: 1 }) }),
    });
    expect(() =>
      flow({
        id: "outer",
        input: passthroughSchema<Record<string, never>>(),
        build: (b) =>
          b
            .step("a", { run: async () => null })
            // biome-ignore lint/suspicious/noExplicitAny: testing cross-flow misuse
            .include("alias", inner.steps.shared as any),
      }),
    ).toThrow(/produced by a different flow\(\) call/);
  });

  it("rejects a duplicate key at chain time", () => {
    expect(() =>
      flow({
        id: "dup",
        input: passthroughSchema<Record<string, never>>(),
        // biome-ignore lint/suspicious/noExplicitAny: TS prevents this at compile time; we still test the runtime guard
        build: (b: any) =>
          b
            .step("a", { run: async () => null })
            .step("a", { run: async () => null }),
      }),
    ).toThrow(/duplicate key/);
  });

  it("rejects an unknown sibling key in needs", () => {
    expect(() =>
      flow({
        id: "unknown",
        input: passthroughSchema<Record<string, never>>(),
        // biome-ignore lint/suspicious/noExplicitAny: TS prevents this at compile time; we still test the runtime guard
        build: (b: any) =>
          b
            .step("a", { run: async () => null })
            .step("b", { needs: ["nope"], run: async () => null }),
      }),
    ).toThrow(/unknown sibling/);
  });
});
