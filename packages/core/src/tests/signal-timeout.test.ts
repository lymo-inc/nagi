import { describe, expect, it } from "vitest";
import { flow } from "../builder";
import { makeDispatcher } from "../dispatch";
import {
  emptySchema,
  makeHarness,
  passthroughSchema,
  spyOnLog,
} from "./test-helpers";

// A signal gate with a downstream task that needs it — the awaitAudio shape from
// lymo's videoAnalysis. timeoutMs omitted ⇒ the signal parks forever (today's
// behavior); set ⇒ the gate fails on deadline and the failure cascades.
function gatedFlow(opts: { id: string; timeoutMs?: number }) {
  return flow({
    id: opts.id,
    input: emptySchema(),
    build: (b) => {
      const awaitAudio = b.signal({
        timeoutMs: "unbounded" as const,
        names: ["audioReady", "recordingReady"],
        schema: passthroughSchema<{ ok: boolean }>(),
        ...(opts.timeoutMs != null ? { timeoutMs: opts.timeoutMs } : {}),
      });
      return {
        awaitAudio,
        transcription: b.task({
          needs: { awaitAudio },
          run: async () => ({ done: true }),
        }),
      };
    },
  });
}

const FAR_FUTURE = () => new Date(Date.now() + 3_600_000);

describe("b.signal timeout", () => {
  it("fails the awaiting signal step (and cascades) once its deadline passes", async () => {
    const f = gatedFlow({ id: "to-fires", timeoutMs: 1_000 });
    const h = await makeHarness(f);
    const dispatcher = makeDispatcher(h.deps);

    const runId = await h.wf.start(f, {});
    await h.drain();
    expect((await h.store.loadRunState(runId)).steps["awaitAudio"]?.tag).toBe(
      "awaitingSignal",
    );

    const failed = await dispatcher.sweepTimers(FAR_FUTURE());
    expect(failed).toBe(1);

    const result = await h.result(runId);
    expect(result.status).toBe("failed");
    expect(result.stepStatus("awaitAudio")).toBe("failed");
    expect(result.error("awaitAudio").name).toBe("NagiSignalTimeoutError");
    // Everything downstream of the failed gate cascades to skipped.
    expect(result.stepStatus("transcription")).toBe("skipped");
    expect(result.factCount("flow.failed")).toBe(1);
  });

  it("does NOT fire before the deadline (fire_at gating)", async () => {
    const f = gatedFlow({ id: "to-not-due", timeoutMs: 1_000 });
    const h = await makeHarness(f);
    const dispatcher = makeDispatcher(h.deps);

    const runId = await h.wf.start(f, {});
    await h.drain();

    // Sweep at ~now: the deadline (parked-at + 1s) hasn't passed.
    const early = await dispatcher.sweepTimers(new Date());
    expect(early).toBe(0);
    expect((await h.store.loadRunState(runId)).steps["awaitAudio"]?.tag).toBe(
      "awaitingSignal",
    );

    // …and it still fires once the deadline is past.
    expect(await dispatcher.sweepTimers(FAR_FUTURE())).toBe(1);
    expect((await h.result(runId)).status).toBe("failed");
  });

  it("never arms a timer for a signal without timeoutMs (parks forever, as before)", async () => {
    const f = gatedFlow({ id: "to-none" });
    const h = await makeHarness(f);
    const dispatcher = makeDispatcher(h.deps);

    const runId = await h.wf.start(f, {});
    await h.drain();

    expect(await dispatcher.sweepTimers(FAR_FUTURE())).toBe(0);
    expect((await h.store.loadRunState(runId)).steps["awaitAudio"]?.tag).toBe(
      "awaitingSignal",
    );
  });

  it("is a no-op once the signal was delivered (delivery wins; timer disarmed)", async () => {
    const f = gatedFlow({ id: "to-delivered", timeoutMs: 1_000 });
    const h = await makeHarness(f);
    const dispatcher = makeDispatcher(h.deps);

    const runId = await h.wf.start(f, {});
    await h.drain();
    await h.wf.signal(runId, "audioReady", { ok: true });
    await h.drain();
    expect((await h.result(runId)).status).toBe("completed");

    // A late sweep finds no armed timer (disarmed on deliver) and no awaiting
    // step — nothing to fail.
    expect(await dispatcher.sweepTimers(FAR_FUTURE())).toBe(0);
    expect((await h.result(runId)).status).toBe("completed");
  });

  it("is idempotent: a second sweep does not re-fail or duplicate facts", async () => {
    const f = gatedFlow({ id: "to-idempotent", timeoutMs: 1_000 });
    const h = await makeHarness(f);
    const dispatcher = makeDispatcher(h.deps);

    const runId = await h.wf.start(f, {});
    await h.drain();
    expect(await dispatcher.sweepTimers(FAR_FUTURE())).toBe(1);
    expect(await dispatcher.sweepTimers(FAR_FUTURE())).toBe(0);

    const result = await h.result(runId);
    expect(result.status).toBe("failed");
    expect(result.factCount("step.failed")).toBe(1);
    expect(result.factCount("flow.failed")).toBe(1);
  });

  it("fires onStepError and onFlowError when a signal times out (parity with other failures)", async () => {
    const f = gatedFlow({ id: "to-hooks", timeoutMs: 1_000 });
    const stepErrors: Array<{ stepId: string; name: string }> = [];
    let flowErrored = false;
    const h = await makeHarness(f, {
      hooks: {
        onStepError: (e) => {
          stepErrors.push({ stepId: e.stepId, name: e.error.name });
        },
        onFlowError: () => {
          flowErrored = true;
        },
      },
    });
    const dispatcher = makeDispatcher(h.deps);

    const runId = await h.wf.start(f, {});
    await h.drain();
    await dispatcher.sweepTimers(FAR_FUTURE());

    expect(stepErrors).toEqual([
      { stepId: "awaitAudio", name: "NagiSignalTimeoutError" },
    ]);
    expect(flowErrored).toBe(true);
  });

  it("keeps the earliest deadline: re-arming does not push the timeout out (reaper-safe)", async () => {
    const f = gatedFlow({ id: "to-earliest", timeoutMs: 1_000 });
    const h = await makeHarness(f);
    const dispatcher = makeDispatcher(h.deps);

    const runId = await h.wf.start(f, {});
    await h.drain();

    // A lease-reap re-dispatch would re-arm; simulate it trying to shove the
    // deadline 10h out. upsertTimer keeps the earliest, so the original ~1s
    // deadline still wins and a sweep 1h out fires it.
    await h.store.upsertTimer(
      runId,
      "awaitAudio",
      new Date(Date.now() + 10 * 3_600_000),
    );
    expect(await dispatcher.sweepTimers(FAR_FUTURE())).toBe(1);
    expect((await h.result(runId)).status).toBe("failed");
  });

  it("the worker self-sweeps on its configured cadence (no external loop)", async () => {
    const f = gatedFlow({ id: "to-worker", timeoutMs: 5 });
    const h = await makeHarness(f);

    const ac = new AbortController();
    const worker = h.wf.worker({
      pollIntervalMs: 5,
      timerSweepIntervalMs: 10,
      signal: ac.signal,
    });
    const done = worker.run();
    try {
      const runId = await h.wf.start(f, {});
      const result = await h.waitForEnd(runId);
      expect(result.status).toBe("failed");
      expect(result.error("awaitAudio").name).toBe("NagiSignalTimeoutError");
    } finally {
      ac.abort();
      await done;
    }
  });

  it("advances the remaining runs when one run's advance throws", async () => {
    const f = gatedFlow({ id: "to-isolated", timeoutMs: 1_000 });
    const { onLog, entries } = spyOnLog();
    const h = await makeHarness(f, { onLog });

    const runA = await h.wf.start(f, {});
    const runB = await h.wf.start(f, {});
    await h.drain();

    // Simulate run A's flow snapshot being gone (deploy replaced it): flowFor
    // throws for A only. advance() calls flowFor first, so A's advance throws.
    const dispatcher = makeDispatcher({
      ...h.deps,
      flowFor: async (id) => {
        if (id === runA) throw new Error("snapshot gone (simulated)");
        return h.deps.flowFor(id);
      },
    });

    // Both timers fire; the store fails both steps regardless.
    expect(await dispatcher.sweepTimers(FAR_FUTURE())).toBe(2);

    // B finalized despite A throwing.
    const b = await h.result(runB);
    expect(b.status).toBe("failed");
    expect(b.factCount("flow.failed")).toBe(1);

    // A: step failed by the store, run not advanced, and an error log names it.
    const a = await h.result(runA);
    expect(a.stepStatus("awaitAudio")).toBe("failed");
    expect(a.status).toBe("running");
    expect(
      entries.some(
        (e) =>
          e.level === "error" &&
          e.attrs?.["runId"] === runA &&
          String(e.msg).includes("signal timeout fired"),
      ),
    ).toBe(true);
  });
});
