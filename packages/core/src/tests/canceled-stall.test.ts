import { describe, expect, it } from "vitest";
import { flow } from "../builder";
import { Facts } from "../facts";
import { makeHarness, passthroughSchema, spyOnLog } from "./test-helpers";

function twoRootsFlow(onYRun: () => void) {
  return flow({
    id: "canceled-stall-two-roots",
    input: passthroughSchema<Record<string, never>>(),
    build: (b) => {
      const x = b.task({ run: async () => ({ x: 1 }) });
      const y = b.task({
        run: async () => {
          onYRun();
          return { y: 1 };
        },
      });
      return { x, y };
    },
  });
}

describe("a canceled step on a live run", () => {
  it("does not complete the run; replay({ from }) recovers it", async () => {
    let yRuns = 0;
    const f = twoRootsFlow(() => {
      yRuns += 1;
    });
    const { onLog, entries } = spyOnLog();
    const h = await makeHarness(f, { onLog });

    const runId = await h.wf.start(f, {});

    const at = h.clock.now();
    await h.store.appendFact(
      runId,
      Facts.stepStarted(runId, "y", 1, "task", at),
    );
    await h.store.appendFact(
      runId,
      Facts.stepAbortRequested({
        runId,
        stepId: "y",
        attempt: 1,
        at,
      }),
    );
    await h.store.appendFact(runId, Facts.stepCanceled(runId, "y", 1, at));

    await h.drain();

    const midState = await h.store.loadRunState(runId);
    expect(midState.phase.tag).toBe("running");
    expect(yRuns).toBe(0);

    const warn = entries.find(
      (e) => e.level === "warn" && e.msg.includes("stalled"),
    );
    expect(warn?.attrs).toMatchObject({ runId, stepIds: ["y"] });

    await h.wf.replay(runId, { mode: "continue", from: "y" });
    await h.drain();

    const result = await h.result(runId);
    expect(result.status).toBe("completed");
    expect(yRuns).toBe(1);
  });

  it("a redelivered message for a canceled step does not re-run it", async () => {
    let yRuns = 0;
    const f = twoRootsFlow(() => {
      yRuns += 1;
    });
    const h = await makeHarness(f);

    const runId = await h.wf.start(f, {});

    const at = h.clock.now();
    await h.store.appendFact(
      runId,
      Facts.stepStarted(runId, "y", 1, "task", at),
    );
    await h.store.appendFact(
      runId,
      Facts.stepAbortRequested({
        runId,
        stepId: "y",
        attempt: 1,
        at,
      }),
    );
    await h.store.appendFact(runId, Facts.stepCanceled(runId, "y", 1, at));

    await h.queue.enqueue(runId, "y", { attempt: 1, flowId: f.id });

    await h.drain();

    expect(yRuns).toBe(0);
    const state = await h.store.loadRunState(runId);
    expect(
      state.facts.filter(
        (fact) => fact.kind === "step.started" && fact.stepId === "y",
      ),
    ).toHaveLength(1);
  });
});
