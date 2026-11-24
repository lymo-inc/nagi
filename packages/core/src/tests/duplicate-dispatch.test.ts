import { describe, expect, it } from "vitest";
import { flow } from "../builder";
import { InMemoryQueue, InMemoryStore } from "../memory";
import { nagi } from "../runtime";
import { stepStateOf } from "../state";
import type { AttemptNumber, ClaimToken, RunId, StepId } from "../types";
import { passthroughSchema } from "./test-helpers";

// advance() enqueues every pending runnable step, so a step can sit in the
// queue twice. Admission reads the run state, then claims: a copy that read
// the step as still pending, and claims only after the other copy settled it
// (settling deletes the lease), must not get a fresh lease and run it again.
class SettleBeforeSecondClaim extends InMemoryStore {
  private claims = 0;
  secondClaimStarted!: () => void;
  readonly secondClaimReached = new Promise<void>((resolve) => {
    this.secondClaimStarted = resolve;
  });

  override async claimStep(
    runId: RunId,
    stepId: StepId,
    attempt: AttemptNumber,
  ): Promise<ClaimToken | null> {
    this.claims++;
    if (this.claims === 2) {
      this.secondClaimStarted();
      while (
        stepStateOf(await this.loadRunState(runId), stepId).tag !== "completed"
      ) {
        await new Promise((resolve) => setTimeout(resolve, 1));
      }
    }
    return super.claimStep(runId, stepId, attempt);
  }
}

describe("duplicate dispatch", () => {
  it("runs a step once when its duplicate message claims after it settled", async () => {
    const store = new SettleBeforeSecondClaim();
    const queue = new InMemoryQueue();
    let runs = 0;
    const f = flow({
      id: "duplicate-dispatch",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => ({
        s: b.task({
          run: async () => {
            runs++;
            // Hold the first copy until the second has passed admission's read.
            if (runs === 1) await store.secondClaimReached;
            return { ok: true };
          },
        }),
      }),
    });
    const wf = await nagi({ flows: [f], store, queue });

    const runId = await wf.start(f, {});
    await queue.enqueue(runId, "s", { flowId: f.id });

    const ac = new AbortController();
    const done = wf
      .worker({ concurrency: 2, pollIntervalMs: 5, signal: ac.signal })
      .run();
    while ((await wf.describe(runId))?.run.status !== "completed") {
      await new Promise((resolve) => setTimeout(resolve, 5));
    }
    // Give the second copy time to finish its admission before stopping.
    await new Promise((resolve) => setTimeout(resolve, 50));
    ac.abort();
    await done;

    expect(runs).toBe(1);
  });
});
