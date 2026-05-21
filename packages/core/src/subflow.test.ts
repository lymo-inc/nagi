import { describe, expect, it } from "vitest";
import { flow } from "./builder";
import { makeHarness, passthroughSchema } from "./test-helpers";
import type { FlowStartedFact } from "./types";

describe("b.subflow — happy path", () => {
  it("starts a child run, awaits completion, exposes { childRunId, output }", async () => {
    const child = flow({
      id: "child-double",
      input: passthroughSchema<{ x: number }>(),
      build: (b) => ({
        double: b.task({
          run: async ({ input }) => ({ doubled: input.x * 2 }),
        }),
      }),
      output: (steps) => steps.double,
    });

    const parent = flow({
      id: "parent-uses-double",
      input: passthroughSchema<{ n: number }>(),
      build: (b) => {
        const sub = b.subflow(child, {
          input: ({ input }) => ({ x: input.n }),
        });
        const consume = b.task({
          needs: { sub },
          run: async ({ needs }) => ({
            tripled: needs.sub.output.doubled * 1.5,
            via: needs.sub.childRunId,
          }),
        });
        return { sub, consume };
      },
    });

    const h = await makeHarness([parent, child]);
    const parentRunId = await h.wf.start(parent, { n: 5 });
    await h.drain();

    const result = await h.result(parentRunId);
    expect(result.status).toBe("completed");
    const consumeOutput = result.output("consume") as {
      tripled: number;
      via: string;
    };
    expect(consumeOutput.tripled).toBe(15);
    expect(consumeOutput.via).toMatch(/^run-/);

    const subOutput = result.output("sub") as {
      childRunId: string;
      output: { doubled: number };
    };
    expect(subOutput.childRunId).toBe(consumeOutput.via);
    expect(subOutput.output).toEqual({ doubled: 10 });

    const childState = await h.store.loadRunState(
      subOutput.childRunId as ReturnType<
        typeof h.store.loadRunState
      > extends Promise<infer R>
        ? R extends { runId: infer I }
          ? I
          : never
        : never,
    );
    expect(childState.status).toBe("completed");
    expect(childState.flowId).toBe("child-double");

    const startedFact = childState.facts.find(
      (f) => f.kind === "flow.started",
    ) as FlowStartedFact | undefined;
    expect(startedFact?.parent?.runId).toBe(parentRunId);
    expect(startedFact?.parent?.stepId).toBe("sub");
  });

  it("passes parent input AND needs through to the buildInput callback", async () => {
    const child = flow({
      id: "child-combine",
      input: passthroughSchema<{ a: number; b: number }>(),
      build: (b) => ({
        sum: b.task({
          run: async ({ input }) => ({ sum: input.a + input.b }),
        }),
      }),
      output: (steps) => steps.sum,
    });

    const parent = flow({
      id: "parent-combines",
      input: passthroughSchema<{ base: number }>(),
      build: (b) => {
        const upstream = b.task({
          run: async ({ input }) => ({ multiplier: input.base * 10 }),
        });
        const sub = b.subflow(child, {
          needs: { upstream },
          input: ({ input, needs }) => ({
            a: input.base,
            b: needs.upstream.multiplier,
          }),
        });
        return { upstream, sub };
      },
    });

    const h = await makeHarness([parent, child]);
    const runId = await h.wf.start(parent, { base: 7 });
    await h.drain();
    const result = await h.result(runId);
    expect(result.status).toBe("completed");
    const subOutput = result.output("sub") as {
      childRunId: string;
      output: { sum: number };
    };
    expect(subOutput.output).toEqual({ sum: 77 });
  });
});

describe("b.subflow — failure propagation", () => {
  it("child failure surfaces as parent's subflow step.failed", async () => {
    const child = flow({
      id: "child-fails",
      input: passthroughSchema<{ x: number }>(),
      build: (b) => ({
        boom: b.task({
          retry: {
            maxAttempts: 1,
            backoff: "fixed",
            initialDelayMs: 0,
          },
          run: async () => {
            throw new Error("child blew up");
          },
        }),
      }),
    });

    const parent = flow({
      id: "parent-of-failing-child",
      input: passthroughSchema<{ n: number }>(),
      build: (b) => ({
        sub: b.subflow(child, { input: ({ input }) => ({ x: input.n }) }),
      }),
    });

    const h = await makeHarness([parent, child]);
    const runId = await h.wf.start(parent, { n: 1 });
    await h.drain();

    const result = await h.result(runId);
    expect(result.status).toBe("failed");
    const err = result.error("sub");
    expect(err.message).toContain("child blew up");
  });
});

describe("b.subflow — nesting", () => {
  it("parent → child → grandchild chain completes, parentRunId chains link up", async () => {
    const grandchild = flow({
      id: "gc-square",
      input: passthroughSchema<{ x: number }>(),
      build: (b) => ({
        sq: b.task({
          run: async ({ input }) => ({ squared: input.x * input.x }),
        }),
      }),
      output: (steps) => steps.sq,
    });

    const child = flow({
      id: "c-square-plus-one",
      input: passthroughSchema<{ y: number }>(),
      build: (b) => {
        const gc = b.subflow(grandchild, {
          input: ({ input }) => ({ x: input.y }),
        });
        const inc = b.task({
          needs: { gc },
          run: async ({ needs }) => ({
            result: needs.gc.output.squared + 1,
          }),
        });
        return { gc, inc };
      },
      output: (steps) => steps.inc,
    });

    const parent = flow({
      id: "p-wraps-c",
      input: passthroughSchema<{ z: number }>(),
      build: (b) => ({
        sub: b.subflow(child, { input: ({ input }) => ({ y: input.z }) }),
      }),
    });

    const h = await makeHarness([parent, child, grandchild]);
    const parentRunId = await h.wf.start(parent, { z: 4 });
    await h.drain();

    const result = await h.result(parentRunId);
    expect(result.status).toBe("completed");
    const subOutput = result.output("sub") as {
      childRunId: string;
      output: { result: number };
    };
    expect(subOutput.output.result).toBe(17);

    const childState = await h.store.loadRunState(
      subOutput.childRunId as never,
    );
    const childStarted = childState.facts.find(
      (f) => f.kind === "flow.started",
    ) as FlowStartedFact;
    expect(childStarted.parent?.runId).toBe(parentRunId);

    const gcStepOutput = childState.steps["gc"]?.output as {
      childRunId: string;
    };
    const gcState = await h.store.loadRunState(
      gcStepOutput.childRunId as never,
    );
    const gcStarted = gcState.facts.find(
      (f) => f.kind === "flow.started",
    ) as FlowStartedFact;
    expect(gcStarted.parent?.runId).toBe(subOutput.childRunId);
    expect(gcStarted.parent?.stepId).toBe("gc");
  });
});

describe("b.subflow — cancel cascade", () => {
  it("wf.cancel(parent) transitively cancels children and grandchildren", async () => {
    const grandchild = flow({
      id: "gc-parked",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => ({
        wait: b.signal({ schema: passthroughSchema<{ ok: true }>() }),
      }),
      output: (steps) => steps.wait,
    });

    const child = flow({
      id: "c-wraps-gc",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => ({
        gc: b.subflow(grandchild, { input: () => ({}) }),
      }),
    });

    const parent = flow({
      id: "p-wraps-c",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => ({
        sub: b.subflow(child, { input: () => ({}) }),
      }),
    });

    const h = await makeHarness([parent, child, grandchild]);
    const parentRunId = await h.wf.start(parent, {});
    await h.drain();

    const childIds = await h.store.listChildren(parentRunId);
    expect(childIds.length).toBe(1);
    const childRunId = childIds[0] as never;
    const gcIds = await h.store.listChildren(childRunId);
    expect(gcIds.length).toBe(1);
    const gcRunId = gcIds[0] as never;

    expect((await h.store.loadRunState(parentRunId)).status).toBe("running");
    expect((await h.store.loadRunState(childRunId)).status).toBe("running");
    expect((await h.store.loadRunState(gcRunId)).status).toBe("running");

    await h.wf.cancel(parentRunId, { reason: "user pressed cancel" });

    expect((await h.store.loadRunState(parentRunId)).status).toBe("canceled");
    expect((await h.store.loadRunState(childRunId)).status).toBe("canceled");
    expect((await h.store.loadRunState(gcRunId)).status).toBe("canceled");
  });

  it("wf.cancel(child) propagates upward — parent's subflow step fails", async () => {
    const child = flow({
      id: "c-canceled-from-outside",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => ({
        wait: b.signal({ schema: passthroughSchema<{ ok: true }>() }),
      }),
      output: (steps) => steps.wait,
    });

    const parent = flow({
      id: "p-watches-child",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => ({
        sub: b.subflow(child, { input: () => ({}) }),
      }),
    });

    const h = await makeHarness([parent, child]);
    const parentRunId = await h.wf.start(parent, {});
    await h.drain();
    const [childRunId] = await h.store.listChildren(parentRunId);
    expect(childRunId).toBeDefined();

    await h.wf.cancel(childRunId as never, { reason: "operator cancel" });
    await h.drain();

    expect((await h.store.loadRunState(childRunId as never)).status).toBe(
      "canceled",
    );
    const parentResult = await h.result(parentRunId);
    expect(parentResult.status).toBe("failed");
    const err = parentResult.error("sub");
    expect(err.name).toBe("NagiCanceledError");
    expect(err.message).toContain("operator cancel");
  });

  it("wf.cancel is idempotent on already-terminal runs", async () => {
    const child = flow({
      id: "c-idem",
      input: passthroughSchema<{ x: number }>(),
      build: (b) => ({
        echo: b.task({ run: async ({ input }) => ({ x: input.x }) }),
      }),
    });
    const h = await makeHarness([child]);
    const runId = await h.wf.start(child, { x: 1 });
    await h.drain();
    expect((await h.store.loadRunState(runId)).status).toBe("completed");
    await h.wf.cancel(runId);
    const state = await h.store.loadRunState(runId);
    expect(state.status).toBe("completed");
    expect(state.facts.filter((f) => f.kind === "flow.canceled").length).toBe(
      0,
    );
  });
});

describe("b.subflow — registration", () => {
  it("throws at dispatch when the referenced child flow is not registered", async () => {
    const orphanChild = flow({
      id: "orphan",
      input: passthroughSchema<{ x: number }>(),
      build: (b) => ({
        only: b.task({ run: async ({ input }) => ({ y: input.x }) }),
      }),
    });

    const parent = flow({
      id: "parent-orphan",
      input: passthroughSchema<{ n: number }>(),
      build: (b) => ({
        sub: b.subflow(orphanChild, {
          input: ({ input }) => ({ x: input.n }),
        }),
      }),
    });

    const h = await makeHarness([parent], {
      defaultRetry: {
        maxAttempts: 1,
        backoff: "fixed",
        initialDelayMs: 0,
      },
    });
    const runId = await h.wf.start(parent, { n: 1 });
    await h.drain();
    const result = await h.result(runId);
    expect(result.status).toBe("failed");
    const err = result.error("sub");
    expect(err.message).toContain("orphan");
    expect(err.message).toContain("not registered");
  });
});
