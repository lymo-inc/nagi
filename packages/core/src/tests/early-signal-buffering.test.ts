import { describe, expect, it } from "vitest";
import { flow } from "../builder";
import type { SignalReceivedEvent, SignalReceivedFact } from "../types";
import { makeHarness, passthroughSchema, spyOnLog } from "./test-helpers";

// Mirrors the production flow: a single signal step fed by either the Mux
// `audioReady` rendition or the recall `recordingReady` transcript. Whichever
// upstream finishes first signals; the step has no deps, so it is the run root.
function transcriptFlow() {
  return flow({
    id: "early-signal-transcript",
    input: passthroughSchema<Record<string, never>>(),
    build: (b) => ({
      transcript: b.signal({
        names: ["audioReady", "recordingReady"],
        schema: passthroughSchema<
          { audioUrl: string } | { transcript: string }
        >(),
      }),
    }),
  });
}

describe("early-signal buffering", () => {
  it("buffers a signal that lands before the worker claims the step, then delivers on dispatch", async () => {
    const f = transcriptFlow();
    const { onLog, entries } = spyOnLog();
    const h = await makeHarness(f, { onLog });

    const runId = await h.wf.start(f, {});

    // Nothing has dispatched the enqueued signal step yet, so it is still
    // pending (never entered awaitingSignal). The old behavior threw here; now
    // the payload is parked.
    await expect(
      h.wf.signal(runId, "recordingReady", { transcript: "t" }),
    ).resolves.toBeUndefined();

    const parked = await h.store.loadRunState(runId);
    expect(parked.phase.tag).toBe("running");
    expect(parked.bufferedSignals["transcript"]).toEqual({
      payload: { transcript: "t" },
      signalName: "recordingReady",
    });
    expect(
      parked.facts.filter((x) => x.kind === "signal.buffered"),
    ).toHaveLength(1);
    expect(parked.facts.some((x) => x.kind === "signal.received")).toBe(false);

    const bufferedLog = entries.find(
      (e) =>
        e.level === "info" &&
        e.msg.includes("signal buffered before step ready"),
    );
    expect(bufferedLog?.attrs).toMatchObject({
      runId,
      stepId: "transcript",
      signalName: "recordingReady",
    });

    // The worker claims the step (pending -> awaitingSignal) and immediately
    // applies the buffered signal in the same dispatch.
    await h.drain();

    const result = await h.result(runId);
    expect(result.status).toBe("completed");
    expect(result.output("transcript")).toEqual({ transcript: "t" });
    expect(result.factCount("signal.buffered")).toBe(1);
    expect(result.factCount("signal.received")).toBe(1);
    expect(
      (result.factsOf("signal.received")[0] as SignalReceivedFact).signalName,
    ).toBe("recordingReady");
  });

  it("keeps the first early signal and ignores a second (no duplicate fact, first wins)", async () => {
    const f = transcriptFlow();
    const h = await makeHarness(f);

    const runId = await h.wf.start(f, {});
    await h.wf.signal(runId, "audioReady", { audioUrl: "first" });
    await h.wf.signal(runId, "recordingReady", { transcript: "second" });

    const parked = await h.store.loadRunState(runId);
    expect(
      parked.facts.filter((x) => x.kind === "signal.buffered"),
    ).toHaveLength(1);

    await h.drain();

    const result = await h.result(runId);
    expect(result.output("transcript")).toEqual({ audioUrl: "first" });
    expect(result.factCount("signal.received")).toBe(1);
  });

  it("fires onSignalReceived exactly once when a buffered signal is delivered", async () => {
    const f = transcriptFlow();
    const received: Array<{ stepId: string; payload: unknown }> = [];
    const h = await makeHarness(f, {
      hooks: {
        onSignalReceived: (e: SignalReceivedEvent) => {
          received.push({ stepId: e.stepId, payload: e.payload });
        },
      },
    });

    const runId = await h.wf.start(f, {});
    await h.wf.signal(runId, "audioReady", { audioUrl: "u" });
    await h.drain();
    await h.result(runId);

    expect(received).toEqual([
      { stepId: "transcript", payload: { audioUrl: "u" } },
    ]);
  });

  it("delivers normally when the signal arrives after the step is awaiting (no buffering)", async () => {
    const f = transcriptFlow();
    const h = await makeHarness(f);

    const runId = await h.wf.start(f, {});
    // Dispatch first so the step is awaitingSignal before the signal lands.
    await h.drain();
    const awaiting = await h.store.loadRunState(runId);
    expect(awaiting.steps["transcript"]?.tag).toBe("awaitingSignal");

    await h.wf.signal(runId, "audioReady", { audioUrl: "u" });

    const result = await h.result(runId);
    expect(result.status).toBe("completed");
    expect(result.output("transcript")).toEqual({ audioUrl: "u" });
    expect(result.factCount("signal.buffered")).toBe(0);
    expect(result.factCount("signal.received")).toBe(1);
  });
});
