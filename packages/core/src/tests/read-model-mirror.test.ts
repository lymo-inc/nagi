import { describe, expect, it } from "vitest";
import {
  type Fact,
  Facts,
  factConsequences,
  foldRun,
  isRunEnd,
} from "../facts";
import {
  nextRunRow,
  nextStepRow,
  type RunRow,
  type StepRow,
} from "../read-model";
import {
  attemptOf,
  errorOf,
  outputOf,
  type RunState,
  stepStateOf,
  stepStatusOf,
} from "../state";
import { admitsRunEnd } from "../store-policy";
import type { AttemptNumber, RunId, StepKind } from "../types";

const runId = "run-mirror" as RunId;
const stepId = "s";
const at = new Date("2026-01-01T00:00:00Z");
const boom = { name: "Error", message: "boom" };

function alphabet(): Fact[] {
  const out: Fact[] = [];
  for (const a of [1, 2, 3] as AttemptNumber[]) {
    for (const k of ["task", "signal", "subflow"] as StepKind[]) {
      out.push(Facts.stepStarted(runId, stepId, a, k, at));
    }
    out.push(Facts.stepCompleted(runId, stepId, a, { a }, at));
    out.push(Facts.stepFailed(runId, stepId, a, boom, at));
    out.push(Facts.stepCanceled(runId, stepId, a, at));
    out.push(Facts.stepCanceled(runId, stepId, a, at, boom));
    out.push(Facts.stepRetried(runId, stepId, a, at, boom, at));
    out.push(
      Facts.stepAbortRequested({ runId, stepId, attempt: a, at, actor: "op" }),
    );
  }
  out.push(Facts.stepSkipped({ runId, stepId, at, reason: "manual" }));
  out.push(Facts.stepReset({ runId, stepId, at }));
  out.push(Facts.flowCompleted(runId, { done: true }, at));
  out.push(Facts.flowFailed(runId, boom, at));
  out.push(Facts.flowCanceled(runId, { cause: "explicit", reason: "r" }, at));
  return out;
}

interface Node {
  readonly facts: readonly Fact[];
  readonly run: RunRow | undefined;
  readonly step: StepRow | undefined;
}

function apply(node: Node, fact: Fact): Node | null {
  // The stores refuse a run end once the run row is settled (writeFact /
  // endRun); every other fact is appended unconditionally.
  if (isRunEnd(fact) && !admitsRunEnd(node.run?.status)) return null;
  const rows = factConsequences(fact).rows;
  return {
    facts: [...node.facts, fact],
    run: rows === null ? node.run : nextRunRow(node.run, rows),
    step:
      rows === null || rows.row === "run"
        ? node.step
        : nextStepRow(node.step, rows),
  };
}

function mismatches(state: RunState, node: Node): string[] {
  const out: string[] = [];
  const s = stepStateOf(state, stepId);
  const row = node.step;
  const status = row?.status ?? "pending";
  if (status !== stepStatusOf(s)) {
    out.push(`step status: row ${status}, fold ${s.tag}`);
  }
  if (row !== undefined && s.tag !== "pending") {
    const attempt = attemptOf(s);
    if (attempt !== 0 && row.attempt !== attempt) {
      out.push(`step attempt: row ${row.attempt}, fold ${attempt}`);
    }
    if (JSON.stringify(row.output) !== JSON.stringify(outputOf(s))) {
      out.push("step output");
    }
    // A run-canceled step keeps the fact's error on its row only: the
    // projection's cancel cause has no slot for it.
    const runCanceled = s.tag === "canceled" && s.cause.kind === "run-canceled";
    if (
      !runCanceled &&
      JSON.stringify(row.error) !== JSON.stringify(errorOf(s) ?? null)
    ) {
      out.push(`step error: row ${row.error !== null}, fold ${s.tag}`);
    }
  }
  const runStatus = node.run?.status ?? "pending";
  if (runStatus !== state.phase.tag) {
    out.push(`run status: row ${runStatus}, fold ${state.phase.tag}`);
  }
  return out;
}

function explore(): { states: number; violations: string[] } {
  const start = apply(
    { facts: [], run: undefined, step: undefined },
    Facts.flowStarted({ runId, flowId: "f", input: null, at }),
  );
  if (start === null) throw new Error("unreachable");
  const letters = alphabet();
  const seen = new Set<string>();
  const violations = new Map<string, string>();
  let frontier: Node[] = [start];
  while (frontier.length > 0) {
    const next: Node[] = [];
    for (const node of frontier) {
      const state = foldRun(runId, node.facts);
      const key = JSON.stringify([
        state.steps,
        state.phase,
        node.run?.status,
        node.step,
      ]);
      if (seen.has(key)) continue;
      seen.add(key);
      const problems = mismatches(state, node).join("; ");
      if (problems !== "" && !violations.has(problems)) {
        const path = node.facts.slice(1).map(describeFact).join(" → ");
        violations.set(problems, `${problems} via ${path}`);
      }
      for (const fact of letters) {
        const child = apply(node, fact);
        if (child !== null) next.push(child);
      }
    }
    frontier = next;
  }
  return { states: seen.size, violations: [...violations.values()] };
}

function describeFact(f: Fact): string {
  const attempt = "attempt" in f ? `(${f.attempt})` : "";
  const kind = f.kind === "step.started" ? `:${f.stepKind}` : "";
  const err = f.kind === "step.canceled" && f.error ? "+err" : "";
  return `${f.kind}${kind}${attempt}${err}`;
}

describe("read model mirrors the fold", () => {
  it("agrees with the projection on every reachable single-step history", () => {
    const { states, violations } = explore();
    expect(violations).toEqual([]);
    expect(states).toBeGreaterThan(100);
  });
});
