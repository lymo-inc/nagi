/**
 * Tests for per-flow concurrency groups (cancel-in-progress).
 *
 * Covers the runtime + InMemoryStore behavior end-to-end:
 *   - cancel-in-progress fires flow.onError on the prior run
 *   - different keys don't interfere
 *   - flows without `concurrency` retain the prior unconstrained behavior
 *   - runId-idempotency still wins over concurrency
 *   - keyFn validation rejects non-string / empty returns
 *   - wf.replay throws on canceled runs
 *   - dispatchMessage early-acks canceled runs
 */
import { describe, expect, it } from "vitest";
import { flow } from "./builder";
import { InMemoryClock, InMemoryQueue, InMemoryStore } from "./memory";
import { NagiRuntimeError, NagiValidationError, nagi } from "./runtime";
import { makeHarness, passthroughSchema } from "./test-helpers";
import type {
  FlowErrorEvent,
  FlowStartEvent,
  RunId,
  StepErrorEvent,
} from "./types";

interface VideoInput {
  readonly videoId: string;
  readonly scope?: string;
}

function makeVideoFlow(opts?: {
  delayMs?: number;
  keyFn?: (input: VideoInput) => string;
}) {
  return flow({
    id: "videoAnalysis",
    input: passthroughSchema<VideoInput>(),
    concurrency: {
      keyFn: opts?.keyFn ?? ((input) => input.videoId),
      mode: "cancel-in-progress",
    },
    build: (b) => {
      const analyze = b.task({
        run: async ({ input }) => {
          if (opts?.delayMs) {
            await new Promise((r) => setTimeout(r, opts.delayMs));
          }
          return { videoId: input.videoId, analyzed: true };
        },
      });
      return { analyze };
    },
  });
}

describe("@nagi-js/core — flow concurrency groups (cancel-in-progress)", () => {
  it("cancels the prior run when a second start arrives with the same key", async () => {
    const errors: FlowErrorEvent[] = [];
    const starts: FlowStartEvent[] = [];
    const f = makeVideoFlow();
    const h = await makeHarness(f, {
      hooks: {
        onFlowStart: (e) => {
          starts.push(e);
        },
        onFlowError: (e) => {
          errors.push(e);
        },
      },
    });

    const firstRunId = await h.wf.start(f, { videoId: "v123" });
    const secondRunId = await h.wf.start(f, { videoId: "v123" });

    expect(firstRunId).not.toBe(secondRunId);

    const first = await h.result(firstRunId);
    expect(first.status).toBe("canceled");
    const cancelFacts = first.factsOf("flow.canceled");
    expect(cancelFacts.length).toBe(1);
    expect(cancelFacts[0]?.canceledByRunId).toBe(secondRunId);
    expect(cancelFacts[0]?.concurrencyKey).toBe("v123");

    // Second run is still active (hasn't been drained yet).
    const second = await h.result(secondRunId);
    expect(second.status).toBe("running");

    // onFlowError fired exactly once — for the canceled run.
    expect(errors.length).toBe(1);
    expect(errors[0]?.runId).toBe(firstRunId);
    expect(errors[0]?.error.name).toBe("NagiCanceledError");
    expect(
      (errors[0]?.error.cause as { canceledByRunId: RunId }).canceledByRunId,
    ).toBe(secondRunId);
    expect(
      (errors[0]?.error.cause as { concurrencyKey: string }).concurrencyKey,
    ).toBe("v123");

    // onFlowStart fired for both runs — cancellation doesn't suppress the
    // new run's lifecycle.
    expect(starts.length).toBe(2);
    expect(starts.map((e) => e.runId)).toEqual([firstRunId, secondRunId]);
  });

  it("different keys do not interfere", async () => {
    const f = makeVideoFlow();
    const h = await makeHarness(f);

    const v1 = await h.wf.start(f, { videoId: "v1" });
    const v2 = await h.wf.start(f, { videoId: "v2" });

    const r1 = await h.result(v1);
    const r2 = await h.result(v2);
    expect(r1.status).toBe("running");
    expect(r2.status).toBe("running");
  });

  it("flows without concurrency config skip cancellation entirely", async () => {
    const noConcFlow = flow({
      id: "plain",
      input: passthroughSchema<{ videoId: string }>(),
      build: (b) => ({
        analyze: b.task({
          run: async ({ input }) => ({ id: input.videoId }),
        }),
      }),
    });
    const h = await makeHarness(noConcFlow);

    const a = await h.wf.start(noConcFlow, { videoId: "v1" });
    const b = await h.wf.start(noConcFlow, { videoId: "v1" });
    expect(a).not.toBe(b);

    const ra = await h.result(a);
    const rb = await h.result(b);
    expect(ra.status).toBe("running");
    expect(rb.status).toBe("running");
    expect(ra.factCount("flow.canceled")).toBe(0);
    expect(rb.factCount("flow.canceled")).toBe(0);
  });

  it("runId-idempotency wins over concurrency (same runId is a no-op)", async () => {
    const f = makeVideoFlow();
    const h = await makeHarness(f);
    const fixedId = "run-explicit-1" as RunId;

    const a = await h.wf.start(f, { videoId: "v1" }, { runId: fixedId });
    const b = await h.wf.start(f, { videoId: "v1" }, { runId: fixedId });
    expect(a).toBe(fixedId);
    expect(b).toBe(fixedId);

    const r = await h.result(fixedId);
    expect(r.status).toBe("running");
    expect(r.factCount("flow.started")).toBe(1);
    expect(r.factCount("flow.canceled")).toBe(0);
  });

  it("keyFn returning empty string throws NagiValidationError", async () => {
    const badFlow = makeVideoFlow({ keyFn: () => "" });
    const h = await makeHarness(badFlow);

    await expect(h.wf.start(badFlow, { videoId: "v1" })).rejects.toThrow(
      NagiValidationError,
    );
  });

  it("keyFn returning non-string throws NagiValidationError", async () => {
    const badFlow = makeVideoFlow({ keyFn: () => 42 as unknown as string });
    const h = await makeHarness(badFlow);

    await expect(h.wf.start(badFlow, { videoId: "v1" })).rejects.toThrow(
      NagiValidationError,
    );
  });

  it("wf.replay throws on canceled runs", async () => {
    const f = makeVideoFlow();
    const h = await makeHarness(f);
    const firstRunId = await h.wf.start(f, { videoId: "v1" });
    await h.wf.start(f, { videoId: "v1" });

    await expect(h.wf.replay(firstRunId)).rejects.toThrow(NagiRuntimeError);
    await expect(h.wf.replay(firstRunId)).rejects.toThrow(/canceled/);
  });

  it("dispatchMessage early-acks a queued step for a canceled run", async () => {
    const f = makeVideoFlow();
    const h = await makeHarness(f);

    const firstRunId = await h.wf.start(f, { videoId: "v1" });
    // Before draining, supersede the first run.
    await h.wf.start(f, { videoId: "v1" });

    const firstStateBefore = await h.store.loadRunState(firstRunId);
    expect(firstStateBefore.status).toBe("canceled");

    // Drain: dispatchMessage sees status==='canceled', acks-and-skips —
    // so no `step.started` fact lands for the canceled run.
    const drained = await h.drainOnce(8);
    expect(drained).toBeGreaterThan(0);

    const firstStateAfter = await h.store.loadRunState(firstRunId);
    expect(
      firstStateAfter.facts.find((fact) => fact.kind === "step.started"),
    ).toBeUndefined();
  });

  it("InMemoryStore.tryStartRun returns canceled facts atomically", async () => {
    const store = new InMemoryStore();
    const queue = new InMemoryQueue();
    const clock = new InMemoryClock();
    const f = makeVideoFlow();
    await nagi({ flows: [f], store, queue, clock });

    const firstFact = {
      kind: "flow.started" as const,
      runId: "run-A" as RunId,
      flowId: f.id,
      input: { videoId: "v1" } as never,
      at: clock.now(),
    };
    const secondFact = {
      kind: "flow.started" as const,
      runId: "run-B" as RunId,
      flowId: f.id,
      input: { videoId: "v1" } as never,
      at: clock.now(),
    };

    const r1 = await store.tryStartRun("run-A" as RunId, firstFact, {
      key: "v1",
      mode: "cancel-in-progress",
    });
    expect(r1.started).toBe(true);
    expect(r1.canceled.length).toBe(0);

    const r2 = await store.tryStartRun("run-B" as RunId, secondFact, {
      key: "v1",
      mode: "cancel-in-progress",
    });
    expect(r2.started).toBe(true);
    expect(r2.canceled.length).toBe(1);
    expect(r2.canceled[0]?.runId).toBe("run-A");
    expect(r2.canceled[0]?.fact.canceledByRunId).toBe("run-B");

    // Third start (same key) supersedes B; A was already canceled and is no
    // longer the active row for the slot.
    const thirdFact = {
      kind: "flow.started" as const,
      runId: "run-C" as RunId,
      flowId: f.id,
      input: { videoId: "v1" } as never,
      at: clock.now(),
    };
    const r3 = await store.tryStartRun("run-C" as RunId, thirdFact, {
      key: "v1",
      mode: "cancel-in-progress",
    });
    expect(r3.started).toBe(true);
    expect(r3.canceled.length).toBe(1);
    expect(r3.canceled[0]?.runId).toBe("run-B");
  });

  it("a manually-appended flow.canceled fact terminates the run", async () => {
    // Schedule analyze, inject a flow.canceled fact, then drain.
    // The dispatcher must not run analyze, and advance() must not enqueue
    // further work.
    const f = makeVideoFlow();
    const h = await makeHarness(f);

    const runId = await h.wf.start(f, { videoId: "v1" });

    await h.store.appendFact(runId, {
      kind: "flow.canceled",
      runId,
      at: h.clock.now(),
      canceledByRunId: "external-run-XYZ" as RunId,
      concurrencyKey: "v1",
    });

    const before = await h.store.loadRunState(runId);
    expect(before.status).toBe("canceled");

    await h.drain();

    const after = await h.store.loadRunState(runId);
    expect(after.status).toBe("canceled");
    expect(
      after.facts.find((fact) => fact.kind === "step.started"),
    ).toBeUndefined();
  });

  it("reclassifies a step that returns after the run was canceled mid-flight as step.canceled", async () => {
    // Boundary check in executeTask: handler awaits a barrier; the test fires
    // a second wf.start() (which cancels the first run via tryStartRun); then
    // the handler returns. The dispatcher reloads run state at the boundary
    // and records `step.canceled` instead of `step.completed`.
    const barrier = createBarrier();
    const f = flow({
      id: "videoCancelMidFlight",
      input: passthroughSchema<VideoInput>(),
      concurrency: {
        keyFn: (input) => input.videoId,
        mode: "cancel-in-progress",
      },
      build: (b) => ({
        analyze: b.task({
          run: async () => {
            await barrier.wait;
            return { done: true };
          },
        }),
      }),
    });
    const errors: FlowErrorEvent[] = [];
    const h = await makeHarness(f, {
      hooks: {
        onFlowError: (e) => {
          errors.push(e);
        },
      },
    });

    const firstRunId = await h.wf.start(f, { videoId: "v1" });

    // Dispatch the queued `analyze` message; the handler will block on the
    // barrier. We don't await this promise yet.
    const dispatching = h.drainOnce(1);

    // The dispatcher writes `step.started` synchronously before invoking the
    // handler, so this poll completes once the handler is actually parked.
    await waitFor(async () => {
      const s = await h.store.loadRunState(firstRunId);
      return s.steps["analyze"]?.status === "running";
    });

    // Now cancel the first run by starting a second one with the same key.
    await h.wf.start(f, { videoId: "v1" });
    const midState = await h.store.loadRunState(firstRunId);
    expect(midState.status).toBe("canceled");

    // Release the handler; it returns successfully.
    barrier.release();
    await dispatching;

    const result = await h.result(firstRunId);
    expect(result.factCount("step.canceled")).toBe(1);
    expect(result.factCount("step.completed")).toBe(0);
    expect(result.stepStatus("analyze")).toBe("canceled");
    // The flow's own `flow.canceled` was written by tryStartRun, not by the
    // step boundary reclassification.
    expect(result.factCount("flow.canceled")).toBe(1);
    // flow.onError fires for the canceled run (from tryStartRun's cancel
    // path), but onStepError does NOT — the boundary reclassification is a
    // silent relabel.
    expect(errors.length).toBe(1);
  });

  it("a step that throws after the run was canceled records step.canceled (no retry, no onStepError)", async () => {
    // handleStepError path: handler throws while the run is in canceled
    // status. The dispatcher reloads run state, classifies as
    // `step.canceled`, suppresses retry, and skips the onStepError hook.
    const barrier = createBarrier();
    const f = flow({
      id: "videoThrowOnCanceled",
      input: passthroughSchema<VideoInput>(),
      concurrency: {
        keyFn: (input) => input.videoId,
        mode: "cancel-in-progress",
      },
      build: (b) => ({
        analyze: b.task({
          retry: { maxAttempts: 5, backoff: "fixed", initialDelayMs: 0 },
          run: async () => {
            await barrier.wait;
            throw new Error("boom — handler errored after cancel");
          },
        }),
      }),
    });

    const stepErrors: StepErrorEvent[] = [];
    const h = await makeHarness(f, {
      hooks: {
        onStepError: (e) => {
          stepErrors.push(e);
        },
      },
    });

    const firstRunId = await h.wf.start(f, { videoId: "v1" });
    const dispatching = h.drainOnce(1);

    await waitFor(async () => {
      const s = await h.store.loadRunState(firstRunId);
      return s.steps["analyze"]?.status === "running";
    });

    await h.wf.start(f, { videoId: "v1" });

    barrier.release();
    await dispatching;

    const result = await h.result(firstRunId);
    expect(result.factCount("step.canceled")).toBe(1);
    expect(result.factCount("step.failed")).toBe(0);
    expect(result.factCount("step.retried")).toBe(0);
    expect(result.stepStatus("analyze")).toBe("canceled");
    // The canceled fact preserves the AbortError-shaped error context — here
    // a plain Error, but the dispatcher only tags the fact when err.name is
    // "AbortError", so this run's canceled fact has no error attached.
    const canceled = result.factsOf("step.canceled")[0];
    expect(canceled?.error).toBeUndefined();

    // onStepError does NOT fire on cancellation; it's a relabel, not an error.
    expect(stepErrors.length).toBe(0);
  });

  it("classifies a handler-thrown AbortError on a canceled run as step.canceled and preserves the error", async () => {
    // Same shape as above, but the handler throws AbortError — common pattern
    // when the handler honors ctx.signal composed with a user timeout. We
    // preserve the error on the canceled fact so observability tools can
    // surface "the call was aborted at byte X" downstream.
    const barrier = createBarrier();
    const f = flow({
      id: "videoAbortError",
      input: passthroughSchema<VideoInput>(),
      concurrency: {
        keyFn: (input) => input.videoId,
        mode: "cancel-in-progress",
      },
      build: (b) => ({
        analyze: b.task({
          run: async () => {
            await barrier.wait;
            const abort = new Error("aborted");
            abort.name = "AbortError";
            throw abort;
          },
        }),
      }),
    });
    const h = await makeHarness(f);

    const firstRunId = await h.wf.start(f, { videoId: "v1" });
    const dispatching = h.drainOnce(1);
    await waitFor(async () => {
      const s = await h.store.loadRunState(firstRunId);
      return s.steps["analyze"]?.status === "running";
    });
    await h.wf.start(f, { videoId: "v1" });
    barrier.release();
    await dispatching;

    const result = await h.result(firstRunId);
    const canceled = result.factsOf("step.canceled")[0];
    expect(canceled).toBeDefined();
    expect(canceled?.error?.name).toBe("AbortError");
  });
});

interface Barrier {
  readonly wait: Promise<void>;
  release(): void;
}

function createBarrier(): Barrier {
  let release!: () => void;
  const wait = new Promise<void>((r) => {
    release = r;
  });
  return { wait, release };
}

async function waitFor(
  pred: () => Promise<boolean>,
  timeoutMs = 1_000,
): Promise<void> {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    if (await pred()) return;
    await new Promise((r) => setTimeout(r, 2));
  }
  throw new Error(`waitFor: timeout after ${timeoutMs}ms`);
}
