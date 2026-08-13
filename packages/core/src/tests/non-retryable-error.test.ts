import { describe, expect, it } from "vitest";
import { flow } from "../builder";
import { NagiNonRetryableError } from "../errors";
import { InMemoryClock, InMemoryQueue, InMemoryStore } from "../memory";
import { nagi } from "../runtime";
import { passthroughSchema } from "./test-helpers";

const RETRY = { maxAttempts: 3, backoff: "fixed", initialDelayMs: 0 } as const;

function harness() {
  return {
    store: new InMemoryStore(),
    queue: new InMemoryQueue(),
    clock: new InMemoryClock(),
  };
}

describe("NagiNonRetryableError", () => {
  it("fails the step on the first attempt, skipping the remaining retry budget", async () => {
    const { store, queue, clock } = harness();
    let attempts = 0;
    const f = flow({
      id: "nonretryable",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => ({
        s: b.task({
          retry: RETRY,
          run: async () => {
            attempts++;
            throw new NagiNonRetryableError("orphaned input — no owning row");
          },
        }),
      }),
    });
    const wf = await nagi({ flows: [f], store, queue, clock });
    const runId = await wf.start(f, {});
    await wf.worker({ timerSweepIntervalMs: 0 }).runUntilEmpty();

    expect(attempts).toBe(1);
    const state = await store.loadRunState(runId);
    expect(state.phase.tag).toBe("failed");
    if (state.phase.tag === "failed") {
      expect(state.phase.error.name).toBe("NagiNonRetryableError");
    }
  });

  it("is honored anywhere on the cause chain (survives SDK wrapping)", async () => {
    const { store, queue, clock } = harness();
    let attempts = 0;
    const f = flow({
      id: "nonretryable-wrapped",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => ({
        s: b.task({
          retry: RETRY,
          run: async () => {
            attempts++;
            throw new Error("sdk wrapper", {
              cause: new NagiNonRetryableError("permanently unprocessable"),
            });
          },
        }),
      }),
    });
    const wf = await nagi({ flows: [f], store, queue, clock });
    const runId = await wf.start(f, {});
    await wf.worker({ timerSweepIntervalMs: 0 }).runUntilEmpty();

    expect(attempts).toBe(1);
    expect((await store.loadRunState(runId)).phase.tag).toBe("failed");
  });

  it("control: a plain error still consumes the full retry budget", async () => {
    const { store, queue, clock } = harness();
    let attempts = 0;
    const f = flow({
      id: "retryable-control",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => ({
        s: b.task({
          retry: RETRY,
          run: async () => {
            attempts++;
            throw new Error("transient");
          },
        }),
      }),
    });
    const wf = await nagi({ flows: [f], store, queue, clock });
    const runId = await wf.start(f, {});
    await wf.worker({ timerSweepIntervalMs: 0 }).runUntilEmpty();

    expect(attempts).toBe(3);
    expect((await store.loadRunState(runId)).phase.tag).toBe("failed");
  });
});
