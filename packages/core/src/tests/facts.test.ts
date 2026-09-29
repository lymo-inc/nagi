import { describe, expect, it } from "vitest";
import {
  type Fact,
  type FactKind,
  Facts,
  factConsequences,
  foldRun,
} from "../facts";
import type { RunId } from "../types";

const runId = "run-facts" as RunId;
const at = new Date("2026-01-01T00:00:00Z");

describe("foldRun on persisted logs", () => {
  it("folds a legacy step.skipped that still carries `cascade`", () => {
    const legacy = {
      ...Facts.stepSkipped({ runId, stepId: "s", reason: "transitive", at }),
      cascade: true,
    } as Fact;
    const state = foldRun(runId, [
      Facts.flowStarted({ runId, flowId: "f", input: null, at }),
      legacy,
    ]);
    expect(state.steps["s"]).toEqual({ tag: "skipped", reason: "transitive" });
  });

  it("treats a kind this build does not declare as a no-op, not a throw", () => {
    const foreign = {
      kind: "signal.sent",
      runId,
      stepId: "s",
      at,
    } as unknown as Fact;
    const started = Facts.stepStarted(runId, "s", 1, "task", at);
    const withForeign = foldRun(runId, [started, foreign]);
    const without = foldRun(runId, [started]);
    expect(withForeign.steps).toEqual(without.steps);
    expect(rowsOf(foreign)).toBeNull();
  });

  it("folds a removed match.arm-selected fact as a no-op", () => {
    const removed = {
      kind: "match.arm-selected",
      runId,
      stepId: "m",
      arm: "otherwise",
      at,
    } as unknown as Fact;
    const started = Facts.stepStarted(runId, "s", 1, "task", at);
    const { facts: _a, ...withRemoved } = foldRun(runId, [started, removed]);
    const { facts: _b, ...without } = foldRun(runId, [started]);
    expect(withRemoved).toEqual(without);
    expect(factConsequences(removed)).toEqual({
      rows: null,
      release: null,
      stream: null,
      event: null,
    });
  });

  it("keeps the prior step state on a contradictory fact", () => {
    const state = foldRun(runId, [
      Facts.stepStarted(runId, "s", 1, "task", at),
      Facts.stepCompleted(runId, "s", 1, 1, at),
      Facts.stepFailed(runId, "s", 1, { name: "E", message: "late" }, at),
    ]);
    expect(state.steps["s"]).toEqual({
      tag: "completed",
      attempt: 1,
      output: 1,
    });
  });
});

const rowsOf = (fact: Fact) => factConsequences(fact).rows;

describe("factConsequences", () => {
  const stepId = "s";
  const error = { name: "E", message: "m" };
  const samples: { readonly [K in FactKind]: Extract<Fact, { kind: K }> } = {
    "flow.started": Facts.flowStarted({ runId, flowId: "f", input: 1, at }),
    "flow.completed": Facts.flowCompleted(runId, 2, at),
    "flow.failed": Facts.flowFailed(runId, error, at),
    "flow.canceled": Facts.flowCanceledByConcurrency({
      runId,
      canceledByRunId: "other" as RunId,
      concurrencyKey: "k",
      at,
    }),
    "step.started": Facts.stepStarted(runId, stepId, 1, "task", at),
    "step.completed": Facts.stepCompleted(runId, stepId, 1, 3, at),
    "step.failed": Facts.stepFailed(runId, stepId, 1, error, at),
    "step.canceled": Facts.stepCanceled(runId, stepId, 1, at),
    "step.retried": Facts.stepRetried(runId, stepId, 1, at, error, at),
    "step.skipped": Facts.stepSkipped({
      runId,
      stepId,
      reason: "when-false",
      at,
    }),
    "step.reset": Facts.stepReset({ runId, stepId, at }),
    "step.abort-requested": Facts.stepAbortRequested({
      runId,
      stepId,
      attempt: 1,
      at,
    }),
    "signal.received": Facts.signalReceived({ runId, stepId, payload: 1, at }),
    "signal.buffered": Facts.signalBuffered({ runId, stepId, payload: 1, at }),
    "lease.reaped": Facts.leaseReaped({
      runId,
      stepId,
      attempt: 1,
      at,
      reapedAt: at,
    }),
  };

  it("declares exactly the audit-only kinds as log-only", () => {
    const logOnly = Object.entries(samples)
      .filter(([, fact]) => rowsOf(fact) === null)
      .map(([kind]) => kind)
      .sort();
    expect(logOnly).toEqual([
      "lease.reaped",
      "signal.buffered",
      "signal.received",
      "step.abort-requested",
    ]);
  });

  it("projects the run/step rows the read model needs", () => {
    expect(rowsOf(samples["flow.canceled"])).toEqual({
      row: "run",
      status: "canceled",
      canceledByRunId: "other",
      completedAt: at,
    });
    expect(
      rowsOf(Facts.flowCanceled(runId, { cause: "explicit", reason: "r" }, at)),
    ).toEqual({
      row: "run",
      status: "canceled",
      canceledByRunId: null,
      completedAt: at,
    });
    expect(rowsOf(samples["step.canceled"])).toEqual({
      row: "step",
      status: "canceled",
      stepId,
      attempt: 1,
      error: null,
      completedAt: at,
    });
    expect(rowsOf(samples["step.skipped"])).toEqual({
      row: "step",
      status: "skipped",
      stepId,
      completedAt: at,
    });
    expect(rowsOf(samples["step.retried"])).toEqual({
      row: "step",
      status: "backoff",
      stepId,
      attempt: 1,
      error,
    });
  });
});
