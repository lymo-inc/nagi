import { describe, expect, it } from "vitest";
import { flow } from "../builder";
import { InMemoryStore } from "../memory";
import type { RunId } from "../types";
import { makeHarness, passthroughSchema } from "./test-helpers";

interface VideoInput {
  readonly videoId: string;
}

function makeBasicFlow() {
  return flow({
    id: "describe-basic",
    input: passthroughSchema<VideoInput>(),
    build: (b) => ({
      analyze: b.task({
        run: async ({ input }) => ({ analyzed: input.videoId }),
      }),
    }),
  });
}

describe("wf.describe", () => {
  it("returns canonical projection for a running run", async () => {
    const f = makeBasicFlow();
    const h = await makeHarness(f);

    const runId = await h.wf.start(f, { videoId: "v1" });

    const desc = await h.wf.describe(runId);
    expect(desc).not.toBeNull();
    if (desc === null) return;
    expect(desc.run.runId).toBe(runId);
    expect(desc.run.flowId).toBe("describe-basic");
    expect(desc.run.status).toBe("running");
    expect(desc.run.startedAt).toBeInstanceOf(Date);
    expect(desc.run.input).toEqual({ videoId: "v1" });
    expect(desc.run.children).toEqual([]);
    expect(desc.run.completedAt).toBeUndefined();
    expect(desc.run.output).toBeUndefined();
    expect(desc.steps).toEqual([]);
  });

  it("for completed run includes output and completedAt", async () => {
    const f = makeBasicFlow();
    const h = await makeHarness(f);

    const runId = await h.wf.start(f, { videoId: "v1" });
    await h.drain();
    const result = await h.result(runId);
    expect(result.status).toBe("completed");

    const desc = await h.wf.describe(runId);
    expect(desc).not.toBeNull();
    if (desc === null) return;
    expect(desc.run.status).toBe("completed");
    expect(desc.run.completedAt).toBeInstanceOf(Date);
    expect(desc.steps.length).toBeGreaterThan(0);
    const step = desc.steps.find((s) => s.stepId === "analyze");
    expect(step).toBeDefined();
    expect(step?.status).toBe("completed");
    expect(step?.output).toEqual({ analyzed: "v1" });
    expect(step?.completedAt).toBeInstanceOf(Date);
  });

  it("for unknown runId returns null (does not throw)", async () => {
    const f = makeBasicFlow();
    const h = await makeHarness(f);

    const desc = await h.wf.describe("run-does-not-exist" as RunId);
    expect(desc).toBeNull();
  });

  it("includes step lease.expiresAt when a step is mid-execution", async () => {
    const store = new InMemoryStore();
    let release!: () => void;
    const gate = new Promise<void>((r) => {
      release = r;
    });
    const f = flow({
      id: "describe-lease",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => ({
        slow: b.task({
          run: async () => {
            await gate;
            return { ok: true };
          },
        }),
      }),
    });

    const h = await makeHarness(f);
    // Inject the shared store reference for assertion (we already have h.store).
    void store;

    const runId = await h.wf.start(f, {});
    const dispatching = h.drainOnce(1);

    const start = Date.now();
    while (Date.now() - start < 2_000) {
      const s = await h.store.loadRunState(runId);
      if (s.steps["slow"]?.tag === "running") break;
      await new Promise((r) => setTimeout(r, 2));
    }

    const desc = await h.wf.describe(runId);
    expect(desc).not.toBeNull();
    if (desc === null) {
      release();
      await dispatching;
      return;
    }
    const stepView = desc.steps.find((s) => s.stepId === "slow");
    expect(stepView).toBeDefined();
    expect(stepView?.status).toBe("running");
    expect(stepView?.lease?.expiresAt).toBeInstanceOf(Date);

    release();
    await dispatching;
  });

  it("of a subflow child includes parent reference", async () => {
    const child = flow({
      id: "describe-child",
      input: passthroughSchema<{ x: number }>(),
      build: (b) => ({
        echo: b.task({
          run: async ({ input }) => ({ x: input.x }),
        }),
      }),
    });
    const parent = flow({
      id: "describe-parent",
      input: passthroughSchema<{ x: number }>(),
      build: (b) => ({
        kid: b.subflow(child, {
          input: ({ input }) => ({ x: input.x }),
        }),
      }),
    });
    const h = await makeHarness([parent, child]);

    const parentRunId = await h.wf.start(parent, { x: 42 });
    await h.drain();

    const children = await h.store.listChildren(parentRunId);
    expect(children.length).toBe(1);
    const childRunId = children[0];
    if (childRunId === undefined) return;

    const childDesc = await h.wf.describe(childRunId);
    expect(childDesc).not.toBeNull();
    if (childDesc === null) return;
    expect(childDesc.run.parent).toBeDefined();
    expect(childDesc.run.parent?.runId).toBe(parentRunId);
    expect(childDesc.run.parent?.stepId).toBe("kid");
  });

  it("of a parent includes children RunIds", async () => {
    const child = flow({
      id: "describe-child-2",
      input: passthroughSchema<{ x: number }>(),
      build: (b) => ({
        echo: b.task({
          run: async ({ input }) => ({ x: input.x }),
        }),
      }),
    });
    const parent = flow({
      id: "describe-parent-2",
      input: passthroughSchema<{ x: number }>(),
      build: (b) => ({
        kid: b.subflow(child, {
          input: ({ input }) => ({ x: input.x }),
        }),
      }),
    });
    const h = await makeHarness([parent, child]);

    const parentRunId = await h.wf.start(parent, { x: 7 });
    await h.drain();

    const desc = await h.wf.describe(parentRunId);
    expect(desc).not.toBeNull();
    if (desc === null) return;
    expect(desc.run.children.length).toBe(1);
    const childId = desc.run.children[0];
    expect(typeof childId).toBe("string");
  });
});
