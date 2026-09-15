import { describe, expect, it } from "vitest";
import { Facts } from "../facts";
import {
  clampQueryLimit,
  compareRunOrder,
  decodeRunCursor,
  encodeRunCursor,
  factEffects,
  isPastCursor,
  jsonContains,
  QUERY_RUNS_DEFAULT_LIMIT,
  QUERY_RUNS_MAX_LIMIT,
  selectExpired,
  selectPruneBatch,
} from "../store-policy";
import type { AttemptNumber, Fact, RunId } from "../types";

const R = "run-1" as RunId;
const A1 = 1 as AttemptNumber;
const AT = new Date(1_700_000_000_000);

describe("queryRuns policy — limit, order, cursor", () => {
  it("clampQueryLimit: default for undefined / non-positive / non-integer, capped at max", () => {
    expect(clampQueryLimit(undefined)).toBe(QUERY_RUNS_DEFAULT_LIMIT);
    expect(clampQueryLimit(0)).toBe(QUERY_RUNS_DEFAULT_LIMIT);
    expect(clampQueryLimit(-5)).toBe(QUERY_RUNS_DEFAULT_LIMIT);
    expect(clampQueryLimit(1.5)).toBe(QUERY_RUNS_DEFAULT_LIMIT);
    expect(clampQueryLimit(7)).toBe(7);
    expect(clampQueryLimit(10_000)).toBe(QUERY_RUNS_MAX_LIMIT);
  });

  it("compareRunOrder: startedAt DESC, then runId DESC", () => {
    const rows = [
      { startedAt: new Date(1000), runId: "b" as RunId },
      { startedAt: new Date(2000), runId: "a" as RunId },
      { startedAt: new Date(1000), runId: "c" as RunId },
    ];
    expect([...rows].sort(compareRunOrder).map((r) => r.runId)).toEqual([
      "a",
      "c",
      "b",
    ]);
  });

  it("isPastCursor: only rows that sort strictly after the cursor", () => {
    const cursor = { startedAt: new Date(1000), runId: "m" as RunId };
    expect(
      isPastCursor({ startedAt: new Date(1000), runId: "l" as RunId }, cursor),
    ).toBe(true);
    expect(
      isPastCursor({ startedAt: new Date(500), runId: "z" as RunId }, cursor),
    ).toBe(true);
    expect(isPastCursor(cursor, cursor)).toBe(false);
    expect(
      isPastCursor({ startedAt: new Date(1000), runId: "n" as RunId }, cursor),
    ).toBe(false);
    expect(
      isPastCursor({ startedAt: new Date(2000), runId: "a" as RunId }, cursor),
    ).toBe(false);
  });

  it("cursor codec round-trips and stays base64url", () => {
    const c = { startedAt: AT, runId: "run-x/y+z" as RunId };
    const encoded = encodeRunCursor(c);
    expect(encoded).toMatch(/^[A-Za-z0-9_-]+$/);
    expect(decodeRunCursor(encoded)).toEqual(c);
  });

  it("decodeRunCursor rejects garbage and well-formed-but-wrong bodies", () => {
    expect(() => decodeRunCursor("not-a-cursor")).toThrow(/invalid cursor/);
    expect(() =>
      decodeRunCursor(btoa(JSON.stringify({ t: "1", r: 2 }))),
    ).toThrow(/invalid cursor/);
  });
});

describe("jsonContains — reference @> semantics", () => {
  it("objects: needle keys must all be contained", () => {
    expect(jsonContains({ a: 1, b: 2 }, { a: 1 })).toBe(true);
    expect(jsonContains({ a: 1 }, { a: 1, b: 2 })).toBe(false);
    expect(jsonContains({ a: 1 }, {})).toBe(true);
    expect(
      jsonContains({ a: { b: { c: 1, d: 2 } } }, { a: { b: { c: 1 } } }),
    ).toBe(true);
  });

  it("arrays: every needle element contained in some haystack element", () => {
    expect(jsonContains(["a", "b", "c"], ["c", "a"])).toBe(true);
    expect(jsonContains(["a"], ["a", "z"])).toBe(false);
    expect(jsonContains([{ k: 1, x: 2 }], [{ k: 1 }])).toBe(true);
  });

  it("scalars: equality with no coercion", () => {
    expect(jsonContains({ x: "1" }, { x: 1 })).toBe(false);
    expect(jsonContains({ x: null }, { x: null })).toBe(true);
    expect(jsonContains({ x: [1] }, { x: 1 })).toBe(false);
    expect(jsonContains({ x: 1 }, { x: [1] })).toBe(false);
  });
});

describe("selectExpired — expiry filter before limit", () => {
  const now = new Date(1000);
  const item = (id: string, at: number) => ({ id, at: new Date(at) });

  it("skips live items so an expired one behind them is still selected", () => {
    const items = [item("live1", 5000), item("live2", 5000), item("old", 10)];
    const out = selectExpired(items, { now, limit: 2, deadline: (i) => i.at });
    expect(out.map((i) => i.id)).toEqual(["old"]);
  });

  it("applies limit to the expired set and treats deadline === now as live", () => {
    const items = [item("a", 1), item("eq", 1000), item("b", 2), item("c", 3)];
    const out = selectExpired(items, { now, limit: 2, deadline: (i) => i.at });
    expect(out.map((i) => i.id)).toEqual(["a", "b"]);
  });
});

describe("selectPruneBatch — eligibility, order, batch size", () => {
  const run = (
    runId: string,
    status: "running" | "completed" | "failed",
    completedAt: number | null,
  ) => ({
    runId: runId as RunId,
    status,
    completedAt: completedAt === null ? null : new Date(completedAt),
  });

  it("selects only terminal runs in `statuses` completed before olderThan", () => {
    const out = selectPruneBatch(
      [
        run("running", "running", null),
        run("recent", "completed", 5000),
        run("failed", "failed", 10),
        run("old", "completed", 10),
      ],
      { olderThan: new Date(1000), statuses: ["completed"], batchSize: 10 },
    );
    expect(out.map((r) => r.runId)).toEqual(["old"]);
  });

  it("orders oldest first (completedAt ASC, runId ASC) and slices to batchSize", () => {
    const out = selectPruneBatch(
      [
        run("c", "completed", 30),
        run("b2", "completed", 20),
        run("b1", "completed", 20),
        run("a", "completed", 10),
      ],
      { olderThan: new Date(1000), statuses: ["completed"], batchSize: 3 },
    );
    expect(out.map((r) => r.runId)).toEqual(["a", "b1", "b2"]);
  });
});

describe("factEffects — the release table", () => {
  const releaseStep = (fact: Fact, timer: boolean) =>
    expect(factEffects(fact)).toEqual({
      tag: "release-step",
      stepId: "s",
      timer,
    });

  it("terminal step facts release the lease; completion and reset also drop the timer", () => {
    releaseStep(Facts.stepCompleted(R, "s", A1, null, AT), true);
    releaseStep(Facts.stepReset({ runId: R, stepId: "s", at: AT }), true);
    releaseStep(
      Facts.stepFailed(R, "s", A1, { name: "E", message: "" }, AT),
      false,
    );
    releaseStep(Facts.stepCanceled(R, "s", A1, AT), false);
  });

  it("terminal run facts release the concurrency slot", () => {
    expect(factEffects(Facts.flowCompleted(R, null, AT))).toEqual({
      tag: "release-run",
    });
    expect(
      factEffects(Facts.flowFailed(R, { name: "E", message: "" }, AT)),
    ).toEqual({
      tag: "release-run",
    });
    expect(
      factEffects(
        Facts.flowCanceled(R, { cause: "explicit", reason: "r" }, AT),
      ),
    ).toEqual({ tag: "release-run" });
  });

  it("everything else releases nothing", () => {
    const none: Fact[] = [
      Facts.flowStarted({ runId: R, flowId: "f", input: null, at: AT }),
      Facts.stepStarted(R, "s", A1, "task", AT),
      Facts.stepRetried(R, "s", A1, AT, { name: "E", message: "" }, AT),
      Facts.stepSkipped({
        runId: R,
        stepId: "s",
        at: AT,
        reason: "when-false",
      }),
      Facts.stepAbortRequested({
        runId: R,
        stepId: "s",
        attempt: A1,
        at: AT,
        actor: "op",
      }),
      Facts.signalReceived({ runId: R, stepId: "s", payload: null, at: AT }),
      Facts.signalBuffered({ runId: R, stepId: "s", payload: null, at: AT }),
      Facts.matchArmSelected(R, "s", "arm", AT),
      Facts.leaseReaped({
        runId: R,
        stepId: "s",
        attempt: A1,
        at: AT,
        reapedAt: AT,
      }),
    ];
    for (const fact of none) expect(factEffects(fact)).toEqual({ tag: "none" });
  });
});
