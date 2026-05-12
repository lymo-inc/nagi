import type {
  FlowHooks,
  FlowStartEvent,
  RunId,
  StepStartEvent,
} from "@nagi-js/core";
import { describe, expect, it, vi } from "vitest";
import { composeHooks } from "./compose";

const flowStartEvent: FlowStartEvent = {
  runId: "run-1" as RunId,
  flowId: "flow-a",
  at: new Date(0),
  input: { x: 1 },
};

const stepStartEvent: StepStartEvent = {
  runId: "run-1" as RunId,
  flowId: "flow-a",
  at: new Date(0),
  stepId: "stepA",
  attempt: 1,
  kind: "task",
  input: null,
};

describe("composeHooks", () => {
  it("returns an empty FlowHooks when no subscribers are given", async () => {
    const composed = composeHooks();
    expect(composed.onFlowStart).toBeUndefined();
    expect(composed.onStepStart).toBeUndefined();
  });

  it("only exposes hooks that at least one subscriber provides", () => {
    const a: FlowHooks = { onFlowStart: () => {} };
    const b: FlowHooks = { onStepStart: () => {} };
    const composed = composeHooks(a, b);
    expect(typeof composed.onFlowStart).toBe("function");
    expect(typeof composed.onStepStart).toBe("function");
    expect(composed.onStepComplete).toBeUndefined();
  });

  it("calls every subscriber in declaration order", async () => {
    const calls: string[] = [];
    const a: FlowHooks = { onFlowStart: () => void calls.push("a") };
    const b: FlowHooks = { onFlowStart: () => void calls.push("b") };
    const c: FlowHooks = { onFlowStart: () => void calls.push("c") };
    const composed = composeHooks(a, b, c);
    await composed.onFlowStart!(flowStartEvent);
    expect(calls).toEqual(["a", "b", "c"]);
  });

  it("awaits async subscribers before invoking the next one", async () => {
    const calls: string[] = [];
    const a: FlowHooks = {
      onStepStart: async () => {
        await new Promise((r) => setTimeout(r, 5));
        calls.push("a");
      },
    };
    const b: FlowHooks = { onStepStart: () => void calls.push("b") };
    const composed = composeHooks(a, b);
    await composed.onStepStart!(stepStartEvent);
    expect(calls).toEqual(["a", "b"]);
  });

  it("swallows a subscriber error and continues with later subscribers", async () => {
    const spy = vi.spyOn(console, "error").mockImplementation(() => {});
    const calls: string[] = [];
    const a: FlowHooks = { onFlowStart: () => void calls.push("a") };
    const b: FlowHooks = {
      onFlowStart: () => {
        throw new Error("boom");
      },
    };
    const c: FlowHooks = { onFlowStart: () => void calls.push("c") };
    const composed = composeHooks(a, b, c);
    await expect(
      composed.onFlowStart!(flowStartEvent),
    ).resolves.toBeUndefined();
    expect(calls).toEqual(["a", "c"]);
    expect(spy).toHaveBeenCalledOnce();
    spy.mockRestore();
  });
});
