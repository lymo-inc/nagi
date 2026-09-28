import { describe, expect, it } from "vitest";
import { flow } from "../builder";
import { canonicalize } from "../canonicalize";
import { diffSnapshots } from "../diff";
import { passthroughSchema } from "./test-helpers";

describe("diffSnapshots", () => {
  it("reports identical DAGs as empty diff", async () => {
    const f = flow({
      id: "same",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => ({ a: b.task({ run: async () => null }) }),
    });
    const dag = await canonicalize(f);
    expect(diffSnapshots(dag, dag)).toEqual({
      changedFlowFields: [],
      addedSteps: [],
      removedSteps: [],
      changedEdges: [],
      changedPredicates: [],
    });
  });

  it("detects added and removed steps", async () => {
    const before = flow({
      id: "addrem",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => ({
        a: b.task({ run: async () => null }),
        toRemove: b.task({ run: async () => null }),
      }),
    });
    const after = flow({
      id: "addrem",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => ({
        a: b.task({ run: async () => null }),
        added: b.task({ run: async () => null }),
      }),
    });
    const diff = diffSnapshots(
      await canonicalize(before),
      await canonicalize(after),
    );
    expect(diff.addedSteps).toEqual(["added"]);
    expect(diff.removedSteps).toEqual(["toRemove"]);
    expect(diff.changedEdges).toEqual([]);
  });

  it("detects added needs edges", async () => {
    const before = flow({
      id: "edges",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => {
        const a = b.task({ run: async () => null });
        const z = b.task({ run: async () => null });
        return { a, z };
      },
    });
    const after = flow({
      id: "edges",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => {
        const a = b.task({ run: async () => null });
        const z = b.task({ needs: { a }, run: async () => null });
        return { a, z };
      },
    });
    const diff = diffSnapshots(
      await canonicalize(before),
      await canonicalize(after),
    );
    expect(diff.changedEdges).toEqual([
      { from: "a", to: "z", before: "absent", after: "needed" },
    ]);
  });

  it("detects removed needs edges", async () => {
    const before = flow({
      id: "edges-rem",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => {
        const a = b.task({ run: async () => null });
        const z = b.task({ needs: { a }, run: async () => null });
        return { a, z };
      },
    });
    const after = flow({
      id: "edges-rem",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => {
        const a = b.task({ run: async () => null });
        const z = b.task({ run: async () => null });
        return { a, z };
      },
    });
    const diff = diffSnapshots(
      await canonicalize(before),
      await canonicalize(after),
    );
    expect(diff.changedEdges).toEqual([
      { from: "a", to: "z", before: "needed", after: "absent" },
    ]);
  });

  it("detects when-predicate changes", async () => {
    const before = flow({
      id: "pred",
      input: passthroughSchema<{ ok: boolean }>(),
      build: (b) => ({
        x: b.task({
          when: ({ input }) => input.ok === true,
          run: async () => null,
        }),
      }),
    });
    const after = flow({
      id: "pred",
      input: passthroughSchema<{ ok: boolean }>(),
      build: (b) => ({
        x: b.task({
          when: ({ input }) => input.ok === false,
          run: async () => null,
        }),
      }),
    });
    const diff = diffSnapshots(
      await canonicalize(before),
      await canonicalize(after),
    );
    expect(diff.changedPredicates).toEqual([{ stepId: "x", field: "when" }]);
  });

  it("detects retry policy changes", async () => {
    const before = flow({
      id: "retry-diff",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => ({
        x: b.task({
          retry: { maxAttempts: 3, backoff: "exponential" },
          run: async () => null,
        }),
      }),
    });
    const after = flow({
      id: "retry-diff",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => ({
        x: b.task({
          retry: { maxAttempts: 5, backoff: "exponential" },
          run: async () => null,
        }),
      }),
    });
    const diff = diffSnapshots(
      await canonicalize(before),
      await canonicalize(after),
    );
    expect(diff.changedPredicates).toEqual([{ stepId: "x", field: "retry" }]);
  });

  it("detects signal name changes", async () => {
    const make = (names: [string, ...string[]]) =>
      flow({
        id: "signal-names",
        input: passthroughSchema<Record<string, never>>(),
        build: (b) => ({
          s: b.signal({
            timeoutMs: "unbounded",
            names,
            schema: passthroughSchema<{ v: number }>(),
          }),
        }),
      });
    const diff = diffSnapshots(
      await canonicalize(make(["a"])),
      await canonicalize(make(["a", "b"])),
    );
    expect(diff.changedPredicates).toEqual([
      { stepId: "s", field: "signalNames" },
    ]);
  });

  it("detects subflow child flow changes", async () => {
    const child = (id: string) =>
      flow({
        id,
        input: passthroughSchema<{ x: number }>(),
        build: (b) => ({ echo: b.task({ run: async () => null }) }),
      });
    const parent = (childId: string) =>
      flow({
        id: "subflow-child",
        input: passthroughSchema<{ x: number }>(),
        build: (b) => ({
          kid: b.subflow(child(childId), {
            input: ({ input }) => ({ x: input.x }),
          }),
        }),
      });
    const diff = diffSnapshots(
      await canonicalize(parent("child-a")),
      await canonicalize(parent("child-b")),
    );
    expect(diff.changedPredicates).toEqual([
      { stepId: "kid", field: "childFlowId" },
    ]);
  });

  it("detects subflow input builder changes", async () => {
    const child = flow({
      id: "subflow-input-child",
      input: passthroughSchema<{ x: number }>(),
      build: (b) => ({ echo: b.task({ run: async () => null }) }),
    });
    const before = flow({
      id: "subflow-input",
      input: passthroughSchema<{ x: number }>(),
      build: (b) => ({
        kid: b.subflow(child, { input: ({ input }) => ({ x: input.x }) }),
      }),
    });
    const after = flow({
      id: "subflow-input",
      input: passthroughSchema<{ x: number }>(),
      build: (b) => ({
        kid: b.subflow(child, { input: ({ input }) => ({ x: input.x + 1 }) }),
      }),
    });
    const diff = diffSnapshots(
      await canonicalize(before),
      await canonicalize(after),
    );
    expect(diff.changedPredicates).toEqual([
      { stepId: "kid", field: "subflowInput" },
    ]);
  });

  it("detects flow input schema changes", async () => {
    const make = (validate: (value: unknown) => { value: unknown }) =>
      flow({
        id: "input-schema",
        input: { "~standard": { version: 1, vendor: "nagi-test", validate } },
        build: (b) => ({ a: b.task({ run: async () => null }) }),
      });
    const diff = diffSnapshots(
      await canonicalize(make((value) => ({ value }))),
      await canonicalize(make((value) => ({ value: { value } }))),
    );
    expect(diff.changedFlowFields).toEqual(["inputSchema"]);
    expect(diff.changedPredicates).toEqual([]);
  });

  it("detects flow id changes", async () => {
    const make = (id: string) =>
      flow({
        id,
        input: passthroughSchema<Record<string, never>>(),
        build: (b) => ({ a: b.task({ run: async () => null }) }),
      });
    const diff = diffSnapshots(
      await canonicalize(make("flow-a")),
      await canonicalize(make("flow-b")),
    );
    expect(diff.changedFlowFields).toEqual(["flowId"]);
  });

  it("sorts output deterministically", async () => {
    const before = flow({
      id: "sorted",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => ({
        z: b.task({ run: async () => null }),
        a: b.task({ run: async () => null }),
      }),
    });
    const after = flow({
      id: "sorted",
      input: passthroughSchema<Record<string, never>>(),
      build: (_b) => ({}),
    });
    const diff = diffSnapshots(
      await canonicalize(before),
      await canonicalize(after),
    );
    expect(diff.removedSteps).toEqual(["a", "z"]);
  });
});
