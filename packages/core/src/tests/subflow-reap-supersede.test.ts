import { describe, expect, it } from "vitest";
import { flow } from "../builder";
import { Facts } from "../facts";
import { deriveChildRunId } from "../run-id";
import { makeHarness, passthroughSchema } from "./test-helpers";

// Regression suite for the prod incident (2026-05-29 subflow self-supersede)
// and the three guarantees of the fix:
//   A1  child runId keyed on (parent, step, generation) — NOT attempt — so a
//       re-dispatch re-attaches; a replay spawns fresh.
//   A2  a re-dispatched subflow step whose child already finished settles the
//       parent from the child's outcome (recovery; no strand).
//   C   the reaper skips a subflow step parked on a still-active child.

describe("b.subflow — lease-reap does not self-supersede (prod-faithful)", () => {
  it("reaper skips a parked subflow step while its child is active (C)", async () => {
    const child = flow({
      id: "child-parks",
      input: passthroughSchema<{ x: number }>(),
      // Same key + cancel-in-progress: a second child with a different id would
      // cancel the first — the prod child concurrency config.
      concurrency: { keyFn: () => "fixed", mode: "cancel-in-progress" },
      build: (b) => ({
        wait: b.signal({
          timeoutMs: "unbounded" as const,
          schema: passthroughSchema<{ ok: true }>(),
        }),
      }),
      output: (steps) => steps.wait,
    });
    const parent = flow({
      id: "parent-parks",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => ({ sub: b.subflow(child, { input: () => ({ x: 5 }) }) }),
    });

    const h = await makeHarness([parent, child]);
    const parentRunId = await h.wf.start(parent, {});
    await h.drain();

    const before = await h.store.listChildren(parentRunId);
    expect(before.length).toBe(1);
    const firstChild = before[0] as Parameters<typeof h.store.loadRunState>[0];

    // Force every lease to expire, then run the reaper.
    const reaped = await h.store.sweepLeases({
      now: new Date(Date.now() + 10_000_000),
      queue: h.queue,
    });
    await h.drain();

    // C: the parent's subflow step was NOT reaped (its child is still active).
    expect(
      reaped.some((r) => r.runId === parentRunId && r.stepId === "sub"),
    ).toBe(false);

    // No self-supersede: still exactly one child, never canceled.
    const after = await h.store.listChildren(parentRunId);
    expect(after.length).toBe(1);
    expect((await h.store.loadRunState(firstChild)).phase.tag).not.toBe(
      "canceled",
    );
  });

  it("recovers a terminal-but-unwoken parent on re-dispatch (A2, no strand)", async () => {
    const child = flow({
      id: "child-parker",
      input: passthroughSchema<{ x: number }>(),
      build: (b) => ({
        wait: b.signal({
          timeoutMs: "unbounded" as const,
          schema: passthroughSchema<{ y: number }>(),
        }),
      }),
      output: (steps) => steps.wait,
    });
    const parent = flow({
      id: "parent-recovers",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => {
        const sub = b.subflow(child, { input: () => ({ x: 5 }) });
        const after = b.task({
          needs: { sub },
          run: async ({ needs }) => ({ got: needs.sub.output }),
        });
        return { sub, after };
      },
    });

    const h = await makeHarness([parent, child]);
    const parentRunId = await h.wf.start(parent, {});
    await h.drain();

    const childRunId = await deriveChildRunId({
      runId: parentRunId,
      stepId: "sub",
      generation: 0,
    });
    // Simulate the child finishing while the in-process wake is LOST (a crash
    // between the child's terminal commit and propagateToParent): write the
    // child's terminal fact directly, so the parent stays awaitingChild.
    await h.store.appendFact(
      childRunId,
      Facts.flowCompleted(childRunId, { y: 42 }, new Date()),
    );

    // A re-dispatch (what the reaper does once the child is terminal) must
    // settle the parent from the child's outcome — not re-spawn / park forever.
    await h.queue.enqueue(parentRunId, "sub", { attempt: 2 });
    await h.drain();

    const result = await h.result(parentRunId);
    expect(result.status).toBe("completed");
    const after = result.output("after") as { got: { y: number } };
    expect(after.got).toEqual({ y: 42 });
  });

  it("replay spawns a FRESH child — generation bumps the child id (A1)", async () => {
    let executions = 0;
    const child = flow({
      id: "child-counted",
      input: passthroughSchema<{ x: number }>(),
      build: (b) => ({
        work: b.task({
          run: async ({ input }) => {
            executions++;
            return { x: input.x };
          },
        }),
      }),
      output: (steps) => steps.work,
    });
    const parent = flow({
      id: "parent-replayable",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => ({ sub: b.subflow(child, { input: () => ({ x: 7 }) }) }),
    });

    const h = await makeHarness([parent, child]);
    const parentRunId = await h.wf.start(parent, {});
    await h.drain();
    expect((await h.result(parentRunId)).status).toBe("completed");
    expect(executions).toBe(1);

    const gen0 = await deriveChildRunId({
      runId: parentRunId,
      stepId: "sub",
      generation: 0,
    });
    expect(await h.store.listChildren(parentRunId)).toEqual([gen0]);

    // Replay from the subflow step → step.reset → generation 1 → fresh child.
    await h.wf.replay(parentRunId, { mode: "continue", from: "sub" });
    await h.drain();

    expect(executions).toBe(2); // the child re-ran (not a stale re-attach)
    const gen1 = await deriveChildRunId({
      runId: parentRunId,
      stepId: "sub",
      generation: 1,
    });
    expect(gen1).not.toBe(gen0);
    expect(await h.store.listChildren(parentRunId)).toContain(gen1);
  });
});
