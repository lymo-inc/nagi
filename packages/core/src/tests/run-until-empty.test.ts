import { describe, expect, it } from "vitest";
import { flow } from "../builder";
import { makeHarness, passthroughSchema } from "./test-helpers";

const one = flow({
  id: "one",
  input: passthroughSchema<{ x: number }>(),
  build: (b) => ({
    step: b.task({ run: async ({ input }) => ({ y: input.x }) }),
  }),
});

const QUIET = { timerSweepIntervalMs: 0, reaperIntervalMs: 0 } as const;

describe("runUntilEmpty timeoutMs", () => {
  it("timeoutMs bounds the drain by a duration", async () => {
    const h = await makeHarness(one);
    const runId = await h.wf.start(one, { x: 1 });
    const { processed } = await h.wf
      .worker(QUIET)
      .runUntilEmpty({ timeoutMs: 10_000 });
    expect(processed).toBeGreaterThanOrEqual(1);
    expect((await h.result(runId)).status).toBe("completed");
  });

  it("timeoutMs: 0 stops before the first dequeue", async () => {
    const h = await makeHarness(one);
    const runId = await h.wf.start(one, { x: 1 });
    const { processed } = await h.wf
      .worker(QUIET)
      .runUntilEmpty({ timeoutMs: 0 });
    expect(processed).toBe(0);
    expect((await h.result(runId)).status).not.toBe("completed");
  });
});
