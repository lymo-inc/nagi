import { describe, expect, it } from "vitest";
import { flow } from "../builder";
import { InMemoryClock, InMemoryQueue, InMemoryStore } from "../memory";
import { NagiRuntimeError, NagiValidationError, nagi } from "../runtime";
import type {
  FlowErrorEvent,
  FlowStartEvent,
  RunId,
  StepErrorEvent,
} from "../types";
import { makeHarness, passthroughSchema } from "./test-helpers";

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
    const cancelFact = cancelFacts[0];
    expect(cancelFact?.cause).toBe("concurrency");
    if (cancelFact?.cause === "concurrency") {
      expect(cancelFact.canceledByRunId).toBe(secondRunId);
      expect(cancelFact.concurrencyKey).toBe("v123");
    }

    const second = await h.result(secondRunId);
    expect(second.status).toBe("running");

    expect(errors.length).toBe(1);
    expect(errors[0]?.runId).toBe(firstRunId);
    expect(errors[0]?.error.name).toBe("NagiCanceledError");
    expect(
      (errors[0]?.error.cause as { canceledByRunId: RunId }).canceledByRunId,
    ).toBe(secondRunId);
    expect(
      (errors[0]?.error.cause as { concurrencyKey: string }).concurrencyKey,
    ).toBe("v123");

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
    await h.wf.start(f, { videoId: "v1" });

    const firstStateBefore = await h.store.loadRunState(firstRunId);
    expect(firstStateBefore.phase.tag).toBe("canceled");

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
    const f = makeVideoFlow();
    const h = await makeHarness(f);

    const runId = await h.wf.start(f, { videoId: "v1" });

    await h.store.appendFact(runId, {
      kind: "flow.canceled",
      cause: "concurrency",
      runId,
      at: h.clock.now(),
      canceledByRunId: "external-run-XYZ" as RunId,
      concurrencyKey: "v1",
    });

    const before = await h.store.loadRunState(runId);
    expect(before.phase.tag).toBe("canceled");

    await h.drain();

    const after = await h.store.loadRunState(runId);
    expect(after.phase.tag).toBe("canceled");
    expect(
      after.facts.find((fact) => fact.kind === "step.started"),
    ).toBeUndefined();
  });

  it("reclassifies a step that returns after the run was canceled mid-flight as step.canceled", async () => {
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

    const dispatching = h.drainOnce(1);

    await waitFor(async () => {
      const s = await h.store.loadRunState(firstRunId);
      return s.steps["analyze"]?.tag === "running";
    });

    await h.wf.start(f, { videoId: "v1" });
    const midState = await h.store.loadRunState(firstRunId);
    expect(midState.phase.tag).toBe("canceled");

    barrier.release();
    await dispatching;

    const result = await h.result(firstRunId);
    expect(result.factCount("step.canceled")).toBe(1);
    expect(result.factCount("step.completed")).toBe(0);
    expect(result.stepStatus("analyze")).toBe("canceled");
    expect(result.factCount("flow.canceled")).toBe(1);
    expect(errors.length).toBe(1);
  });

  it("a step that throws after the run was canceled records step.canceled (no retry, no onStepError)", async () => {
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
      return s.steps["analyze"]?.tag === "running";
    });

    await h.wf.start(f, { videoId: "v1" });

    barrier.release();
    await dispatching;

    const result = await h.result(firstRunId);
    expect(result.factCount("step.canceled")).toBe(1);
    expect(result.factCount("step.failed")).toBe(0);
    expect(result.factCount("step.retried")).toBe(0);
    expect(result.stepStatus("analyze")).toBe("canceled");
    const canceled = result.factsOf("step.canceled")[0];
    expect(canceled?.error).toBeUndefined();

    expect(stepErrors.length).toBe(0);
  });

  it("classifies a handler-thrown AbortError on a canceled run as step.canceled and preserves the error", async () => {
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
      return s.steps["analyze"]?.tag === "running";
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

function makeBareStringVideoFlow() {
  return flow({
    id: "videoAnalysisBareString",
    input: passthroughSchema<VideoInput>(),
    concurrency: "videoId",
    build: (b) => ({
      analyze: b.task({
        run: async ({ input }) => ({ videoId: input.videoId, analyzed: true }),
      }),
    }),
  });
}

describe("@nagi-js/core — flow concurrency shorthands", () => {
  it("bare-string shorthand cancels the prior run on the same key", async () => {
    const f = makeBareStringVideoFlow();
    const h = await makeHarness(f);

    const firstRunId = await h.wf.start(f, { videoId: "v123" });
    const secondRunId = await h.wf.start(f, { videoId: "v123" });

    expect(firstRunId).not.toBe(secondRunId);

    const first = await h.result(firstRunId);
    expect(first.status).toBe("canceled");
    const cancelFacts = first.factsOf("flow.canceled");
    expect(cancelFacts.length).toBe(1);
    const cancelFact = cancelFacts[0];
    expect(cancelFact?.cause).toBe("concurrency");
    if (cancelFact?.cause === "concurrency") {
      expect(cancelFact.canceledByRunId).toBe(secondRunId);
      expect(cancelFact.concurrencyKey).toBe("v123");
    }

    const second = await h.result(secondRunId);
    expect(second.status).toBe("running");
  });

  it("bare-string shorthand: different keys do not interfere", async () => {
    const f = makeBareStringVideoFlow();
    const h = await makeHarness(f);

    const v1 = await h.wf.start(f, { videoId: "v1" });
    const v2 = await h.wf.start(f, { videoId: "v2" });

    const r1 = await h.result(v1);
    const r2 = await h.result(v2);
    expect(r1.status).toBe("running");
    expect(r2.status).toBe("running");
    expect(r1.factCount("flow.canceled")).toBe(0);
    expect(r2.factCount("flow.canceled")).toBe(0);
  });

  it("bare-string shorthand defaults mode to cancel-in-progress", async () => {
    // No `mode` is specified anywhere — cancellation must still occur.
    const f = makeBareStringVideoFlow();
    const h = await makeHarness(f);

    const firstRunId = await h.wf.start(f, { videoId: "vDefault" });
    const secondRunId = await h.wf.start(f, { videoId: "vDefault" });

    const first = await h.result(firstRunId);
    expect(first.status).toBe("canceled");
    const cancelFact = first.factsOf("flow.canceled")[0];
    expect(cancelFact?.cause).toBe("concurrency");
    if (cancelFact?.cause === "concurrency") {
      expect(cancelFact.canceledByRunId).toBe(secondRunId);
    }
  });

  it("bare-string shorthand extracts the key value from the named field", async () => {
    const f = makeBareStringVideoFlow();
    const h = await makeHarness(f);

    const firstRunId = await h.wf.start(f, { videoId: "extracted-key-99" });
    await h.wf.start(f, { videoId: "extracted-key-99" });

    const first = await h.result(firstRunId);
    const cancelFact = first.factsOf("flow.canceled")[0];
    expect(cancelFact?.cause).toBe("concurrency");
    if (cancelFact?.cause === "concurrency") {
      expect(cancelFact.concurrencyKey).toBe("extracted-key-99");
    }
  });

  it("keyFn form with mode omitted defaults to cancel-in-progress", async () => {
    const f = flow({
      id: "videoAnalysisKeyFnNoMode",
      input: passthroughSchema<VideoInput>(),
      concurrency: { keyFn: (input) => input.videoId },
      build: (b) => ({
        analyze: b.task({
          run: async ({ input }) => ({ videoId: input.videoId }),
        }),
      }),
    });
    const h = await makeHarness(f);

    const firstRunId = await h.wf.start(f, { videoId: "vKeyFn" });
    const secondRunId = await h.wf.start(f, { videoId: "vKeyFn" });

    const first = await h.result(firstRunId);
    expect(first.status).toBe("canceled");
    const cancelFact = first.factsOf("flow.canceled")[0];
    expect(cancelFact?.cause).toBe("concurrency");
    if (cancelFact?.cause === "concurrency") {
      expect(cancelFact.canceledByRunId).toBe(secondRunId);
      expect(cancelFact.concurrencyKey).toBe("vKeyFn");
    }
  });

  it("bare-string and verbose forms record an identical concurrencyKey for the same input", async () => {
    const bare = makeBareStringVideoFlow();
    const verbose = makeVideoFlow();

    const hBare = await makeHarness(bare);
    const bareFirst = await hBare.wf.start(bare, { videoId: "same-key" });
    await hBare.wf.start(bare, { videoId: "same-key" });
    const bareResult = await hBare.result(bareFirst);
    const bareFact = bareResult.factsOf("flow.canceled")[0];

    const hVerbose = await makeHarness(verbose);
    const verboseFirst = await hVerbose.wf.start(verbose, {
      videoId: "same-key",
    });
    await hVerbose.wf.start(verbose, { videoId: "same-key" });
    const verboseResult = await hVerbose.result(verboseFirst);
    const verboseFact = verboseResult.factsOf("flow.canceled")[0];

    expect(bareFact?.cause).toBe("concurrency");
    expect(verboseFact?.cause).toBe("concurrency");
    if (
      bareFact?.cause === "concurrency" &&
      verboseFact?.cause === "concurrency"
    ) {
      expect(bareFact.concurrencyKey).toBe(verboseFact.concurrencyKey);
      expect(bareFact.concurrencyKey).toBe("same-key");
    }
  });

  it("existing keyFn+mode object form is unchanged", async () => {
    // Regression guard: mode explicitly present, behaves as before.
    const f = makeVideoFlow();
    const h = await makeHarness(f);

    const firstRunId = await h.wf.start(f, { videoId: "vRegression" });
    const secondRunId = await h.wf.start(f, { videoId: "vRegression" });

    const first = await h.result(firstRunId);
    expect(first.status).toBe("canceled");
    const cancelFact = first.factsOf("flow.canceled")[0];
    expect(cancelFact?.cause).toBe("concurrency");
    if (cancelFact?.cause === "concurrency") {
      expect(cancelFact.canceledByRunId).toBe(secondRunId);
      expect(cancelFact.concurrencyKey).toBe("vRegression");
    }

    const second = await h.result(secondRunId);
    expect(second.status).toBe("running");
  });

  it("runId-idempotency still wins over bare-string shorthand", async () => {
    const f = makeBareStringVideoFlow();
    const h = await makeHarness(f);
    const fixedId = "run-bare-explicit-1" as RunId;

    const a = await h.wf.start(f, { videoId: "v1" }, { runId: fixedId });
    const b = await h.wf.start(f, { videoId: "v1" }, { runId: fixedId });
    expect(a).toBe(fixedId);
    expect(b).toBe(fixedId);

    const r = await h.result(fixedId);
    expect(r.status).toBe("running");
    expect(r.factCount("flow.started")).toBe(1);
    expect(r.factCount("flow.canceled")).toBe(0);
  });

  it("bare-string key whose extracted value is empty throws NagiValidationError", async () => {
    const f = makeBareStringVideoFlow();
    const h = await makeHarness(f);

    await expect(h.wf.start(f, { videoId: "" })).rejects.toThrow(
      NagiValidationError,
    );
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
