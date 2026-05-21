import { describe, expect, it } from "vitest";
import { InMemoryClock, InMemoryTrigger } from "../memory";
import type { RunId, StepId } from "../types";

describe("InMemoryClock — wake-up via Trigger", () => {
  it("fires the trigger when a scheduled `at` elapses", async () => {
    const trigger = new InMemoryTrigger();
    const clock = new InMemoryClock({ trigger });

    const wakes: RunId[] = [];
    trigger.subscribe((runId) => wakes.push(runId));

    const runId = "run-wake-1" as RunId;
    await clock.schedule(new Date(Date.now() + 5), runId, "step1" as StepId);

    await new Promise((r) => setTimeout(r, 25));

    expect(wakes).toEqual([runId]);
    clock.dispose();
  });

  it("supersedes an earlier schedule for the same (runId, stepId)", async () => {
    const trigger = new InMemoryTrigger();
    const clock = new InMemoryClock({ trigger });

    const wakes: RunId[] = [];
    trigger.subscribe((runId) => wakes.push(runId));

    const runId = "run-wake-2" as RunId;
    const stepId = "step1" as StepId;

    await clock.schedule(new Date(Date.now() + 1_000), runId, stepId);
    await clock.schedule(new Date(Date.now() + 5), runId, stepId);

    await new Promise((r) => setTimeout(r, 25));

    expect(wakes).toEqual([runId]);
    clock.dispose();
  });

  it("dispose() cancels pending wake-ups", async () => {
    const trigger = new InMemoryTrigger();
    const clock = new InMemoryClock({ trigger });

    const wakes: RunId[] = [];
    trigger.subscribe((runId) => wakes.push(runId));

    await clock.schedule(
      new Date(Date.now() + 5),
      "run-wake-3" as RunId,
      "step1" as StepId,
    );
    clock.dispose();

    await new Promise((r) => setTimeout(r, 25));

    expect(wakes).toEqual([]);
  });

  it("schedule() without a trigger is a no-op (no crash)", async () => {
    const clock = new InMemoryClock();
    await clock.schedule(
      new Date(Date.now() + 5),
      "run-wake-4" as RunId,
      "step1" as StepId,
    );
    await new Promise((r) => setTimeout(r, 15));
    clock.dispose();
  });
});
