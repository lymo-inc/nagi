import { describe, expect, it, vi } from "vitest";
import { flow } from "../builder";
import {
  DEFAULT_REAPER_INTERVAL_MS,
  decideExpiredLeaseAction,
} from "../lease-reaper";
import { InMemoryClock, InMemoryQueue, InMemoryStore } from "../memory";
import { nagi } from "../runtime";
import type { AttemptNumber, Fact, RunId, StepId } from "../types";
import { leasePorts, passthroughSchema } from "./test-helpers";

const RUN_ID = "run-test" as RunId;
const STEP_ID = "s1" as StepId;

describe("decideExpiredLeaseAction", () => {
  it("reaps an expired lease on a non-terminal step", () => {
    const now = new Date("2025-01-01T00:00:00Z");
    const decision = decideExpiredLeaseAction({
      lease: {
        runId: RUN_ID,
        stepId: STEP_ID,
        attempt: 2 as AttemptNumber,
        expiresAt: new Date(now.getTime() - 1_000),
      },
      stepStatus: "running",
      childActive: false,
      now,
    });
    expect(decision.tag).toBe("reap");
    if (decision.tag !== "reap") throw new Error("unreachable");
    expect(decision.nextAttempt).toBe(3);
    expect(decision.backoffMs).toBe(0);
  });

  it("skips a live (heartbeat-recent) lease", () => {
    const now = new Date("2025-01-01T00:00:00Z");
    const decision = decideExpiredLeaseAction({
      lease: {
        runId: RUN_ID,
        stepId: STEP_ID,
        attempt: 1 as AttemptNumber,
        expiresAt: new Date(now.getTime() + 10_000),
      },
      stepStatus: "running",
      childActive: false,
      now,
    });
    expect(decision).toEqual({ tag: "skip", reason: "still-live" });
  });

  it("skips a subflow step parked on a still-active child, even if expired", () => {
    const now = new Date("2025-01-01T00:00:00Z");
    const decision = decideExpiredLeaseAction({
      lease: {
        runId: RUN_ID,
        stepId: STEP_ID,
        attempt: 1 as AttemptNumber,
        expiresAt: new Date(now.getTime() - 1_000),
      },
      stepStatus: "running", // awaitingChild folds to "running"
      childActive: true,
      now,
    });
    expect(decision).toEqual({ tag: "skip", reason: "child-active" });
  });

  it("reaps once the child is terminal/gone (childActive false)", () => {
    const now = new Date("2025-01-01T00:00:00Z");
    const decision = decideExpiredLeaseAction({
      lease: {
        runId: RUN_ID,
        stepId: STEP_ID,
        attempt: 1 as AttemptNumber,
        expiresAt: new Date(now.getTime() - 1_000),
      },
      stepStatus: "running",
      childActive: false,
      now,
    });
    expect(decision.tag).toBe("reap");
  });

  it("skips terminal steps (completed/failed/canceled/skipped)", () => {
    const now = new Date("2025-01-01T00:00:00Z");
    for (const status of [
      "completed",
      "failed",
      "canceled",
      "skipped",
    ] as const) {
      const decision = decideExpiredLeaseAction({
        lease: {
          runId: RUN_ID,
          stepId: STEP_ID,
          attempt: 1 as AttemptNumber,
          expiresAt: new Date(now.getTime() - 1_000),
        },
        stepStatus: status,
        childActive: false,
        now,
      });
      expect(decision).toEqual({ tag: "skip", reason: "terminal" });
    }
  });

  it("emits nextAttempt = lease.attempt + 1", () => {
    const now = new Date("2025-01-01T00:00:00Z");
    for (const attempt of [1, 2, 5, 17]) {
      const decision = decideExpiredLeaseAction({
        lease: {
          runId: RUN_ID,
          stepId: STEP_ID,
          attempt: attempt as AttemptNumber,
          expiresAt: new Date(now.getTime() - 1),
        },
        stepStatus: "pending",
        childActive: false,
        now,
      });
      expect(decision.tag).toBe("reap");
      if (decision.tag !== "reap") throw new Error("unreachable");
      expect(decision.nextAttempt).toBe(attempt + 1);
    }
  });
});

describe("Store.sweepLeases — in-memory", () => {
  function makeFlow() {
    return flow({
      id: "reaper-flow",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => ({
        s1: b.task({ run: async () => ({ ok: true }) }),
      }),
    });
  }

  it("re-enqueues with attempt+1 and writes lease.reaped audit fact", async () => {
    const store = new InMemoryStore({ leaseMs: 10 });
    const queue = new InMemoryQueue();
    const f = makeFlow();
    const wf = await nagi({
      flows: [f],
      store,
      queue,
      clock: new InMemoryClock(),
    });
    const runId = await wf.start(f, {});
    // Worker takes the message + claims the lease, then dies.
    const [msg] = await queue.dequeue({ count: 1 });
    expect(msg).toBeDefined();
    if (msg === undefined) throw new Error("expected dispatch");
    expect(
      await store.claimStep(runId, msg.stepId, msg.attempt),
    ).not.toBeNull();
    // Force lease to expire.
    await new Promise((r) => setTimeout(r, 20));

    const reaped = await store.sweepLeases({ now: new Date(), queue });
    expect(reaped).toHaveLength(1);
    expect(reaped[0]?.nextAttempt).toBe(msg.attempt + 1);

    const facts = (await store.loadRunState(runId)).facts as readonly Fact[];
    const audit = facts.filter((x) => x.kind === "lease.reaped");
    expect(audit).toHaveLength(1);

    const redelivered = await queue.dequeue({ count: 10 });
    const newMsg = redelivered.find(
      (m) => m.runId === runId && m.stepId === msg.stepId,
    );
    expect(newMsg).toBeDefined();
    expect(newMsg?.attempt).toBe(msg.attempt + 1);
  });

  it("is restart-safe (running twice does not double-enqueue)", async () => {
    const store = new InMemoryStore({ leaseMs: 10 });
    const queue = new InMemoryQueue();
    const f = flow({
      id: "reaper-restart",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => ({ s1: b.task({ run: async () => ({}) }) }),
    });
    const wf = await nagi({
      flows: [f],
      store,
      queue,
      clock: new InMemoryClock(),
    });
    const runId = await wf.start(f, {});
    const [msg] = await queue.dequeue({ count: 1 });
    if (msg === undefined) throw new Error("expected dispatch");
    await store.claimStep(runId, msg.stepId, msg.attempt);
    await new Promise((r) => setTimeout(r, 20));

    const reaped1 = await store.sweepLeases({ now: new Date(), queue });
    const reaped2 = await store.sweepLeases({ now: new Date(), queue });
    expect(reaped1).toHaveLength(1);
    expect(reaped2).toHaveLength(0); // second sweep finds no expired lease

    const facts = (await store.loadRunState(runId)).facts as readonly Fact[];
    expect(facts.filter((x) => x.kind === "lease.reaped")).toHaveLength(1);
  });

  it("batches N expired leases in one pass", async () => {
    const store = new InMemoryStore({ leaseMs: 5 });
    const queue = new InMemoryQueue();
    // Manually claim leases for 3 distinct (runId, stepId) pairs.
    for (let i = 0; i < 3; i++) {
      const rid = `run-${i}` as RunId;
      await store.appendFact(rid, {
        kind: "flow.started",
        runId: rid,
        flowId: "f",
        at: new Date(),
        input: {},
      });
      await store.appendFact(rid, {
        kind: "step.started",
        runId: rid,
        stepId: "s",
        attempt: 1 as AttemptNumber,
        stepKind: "task",
        at: new Date(),
      });
      await store.claimStep(rid, "s", 1 as AttemptNumber);
    }
    await new Promise((r) => setTimeout(r, 15));

    const reaped = await store.sweepLeases({
      now: new Date(),
      queue,
      limit: 100,
    });
    expect(reaped).toHaveLength(3);
  });
});

describe("Heartbeat extends both queue VT and store lease", () => {
  it("extends queue VT and store lease atomically per tick", async () => {
    vi.useFakeTimers();
    try {
      const { queue, store, extend, extendLease } = leasePorts();
      const emitLog = vi.fn();
      const { startHeartbeat } = await import("../step-exec");
      const hb = startHeartbeat({
        queue,
        store,
        runId: RUN_ID,
        stepId: STEP_ID,
        attempt: 4,
        receipt: "rcpt",
        intervalMs: 25,
        leaseMs: 100,
        holdWarnMs: 0,
        emitLog,
      });
      await vi.advanceTimersByTimeAsync(80);
      expect(extend.mock.calls.length).toBeGreaterThanOrEqual(3);
      expect(extendLease.mock.calls.length).toBeGreaterThanOrEqual(3);
      hb.stop();
    } finally {
      vi.useRealTimers();
    }
  });

  it("heartbeat-extended lease stays out of sweep range during normal execution", async () => {
    const store = new InMemoryStore({ leaseMs: 50 });
    const queue = new InMemoryQueue();
    const runId = "run-hb" as RunId;
    await store.appendFact(runId, {
      kind: "flow.started",
      runId,
      flowId: "f",
      at: new Date(),
      input: {},
    });
    await store.appendFact(runId, {
      kind: "step.started",
      runId,
      stepId: "s",
      attempt: 1 as AttemptNumber,
      stepKind: "task",
      at: new Date(),
    });
    await store.claimStep(runId, "s", 1 as AttemptNumber);
    // Simulate a heartbeat right before the sweep.
    await store.extendLease(runId, "s", 1 as AttemptNumber, 10_000);

    const reaped = await store.sweepLeases({ now: new Date(), queue });
    expect(reaped).toEqual([]);
  });
});

describe("Reaper interval config", () => {
  it("default interval is half-ish of a 60s default lease (≤30s)", () => {
    expect(DEFAULT_REAPER_INTERVAL_MS).toBeLessThanOrEqual(30_000);
    expect(DEFAULT_REAPER_INTERVAL_MS).toBeGreaterThan(0);
  });

  it("setting reaperIntervalMs to 0 disables the reaper loop", async () => {
    const store = new InMemoryStore({ leaseMs: 1 });
    const sweepSpy = vi.spyOn(store, "sweepLeases");
    const queue = new InMemoryQueue();
    const f = flow({
      id: "reaper-off",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => ({ s1: b.task({ run: async () => ({}) }) }),
    });
    const handle = await nagi.run({
      flows: [f],
      store,
      queue,
      clock: new InMemoryClock(),
      reaperIntervalMs: 0,
    });
    await new Promise((r) => setTimeout(r, 60));
    await handle.stop();
    expect(sweepSpy).not.toHaveBeenCalled();
  });
});
