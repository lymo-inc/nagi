import { getDef, readSelectedArm, resolveNeeds, type StepDef } from "./internal";
import type { Flow, Json, RunState, StepState } from "./types";

export type SkipReason = "when-false" | "transitive";

export interface ScheduleDecision {
  readonly runnable: readonly string[];
  readonly skip: ReadonlyArray<{ readonly stepId: string; readonly reason: SkipReason }>;
}

export interface ScheduleArgs {
  readonly flow: Flow;
  readonly runState: RunState;
  readonly input: unknown;
}

/**
 * Compute the next set of runnable / skip decisions from current state.
 * Pure function; the caller persists the resulting facts and enqueues messages.
 */
export function nextRunnable(args: ScheduleArgs): ScheduleDecision {
  const { flow, runState, input } = args;
  const runnable: string[] = [];
  const skip: { stepId: string; reason: SkipReason }[] = [];

  for (const [stepId, step] of Object.entries(flow.steps)) {
    const state = runState.steps[stepId];
    if (state !== undefined && state.status !== "pending") continue;

    const def = getDef(step);

    // Gate nested arm steps on parent match arm selection.
    const parentGate = checkParentMatch(def, runState);
    if (parentGate === "blocked") continue;
    if (parentGate === "transitive-skip") {
      skip.push({ stepId, reason: "transitive" });
      continue;
    }

    const upstreamCheck = checkUpstream(def, runState);
    if (upstreamCheck === "blocked") continue;
    if (upstreamCheck === "transitive-skip") {
      skip.push({ stepId, reason: "transitive" });
      continue;
    }

    const when = def.kind === "match" ? undefined : def.when;
    if (when) {
      const needs = resolveNeeds(def, (id) => runState.steps[id]?.output ?? null);
      const shouldRun = when({ input, needs });
      if (!shouldRun) {
        skip.push({ stepId, reason: "when-false" });
        continue;
      }
    }

    runnable.push(stepId);
  }

  return { runnable, skip };
}

type UpstreamStatus = "ready" | "blocked" | "transitive-skip";

function checkUpstream(def: StepDef, runState: RunState): UpstreamStatus {
  for (const upstream of Object.values(def.needs)) {
    if (
      !upstream ||
      typeof upstream !== "object" ||
      !("id" in upstream) ||
      typeof upstream.id !== "string"
    ) {
      continue;
    }
    const upstreamState = runState.steps[upstream.id];
    if (upstreamState === undefined) return "blocked";
    if (upstreamState.status === "completed") continue;
    if (upstreamState.status === "skipped" || upstreamState.status === "failed") {
      return "transitive-skip";
    }
    return "blocked";
  }
  return "ready";
}

/**
 * Gate for nested arm steps. A step with `parentMatch` runs only after its
 * parent match has selected an arm AND that arm matches.
 *
 *   parent missing arm-selected fact   → blocked (parent hasn't routed yet)
 *   parent selected a different arm    → transitive-skip
 *   parent failed/skipped before selecting → transitive-skip
 *   parent selected this arm           → ready
 */
function checkParentMatch(def: StepDef, runState: RunState): UpstreamStatus {
  if (!def.parentMatch) return "ready";
  const { matchId, armId } = def.parentMatch;

  const parentState = runState.steps[matchId];
  if (parentState?.status === "failed" || parentState?.status === "skipped") {
    return "transitive-skip";
  }

  const selected = readSelectedArm(matchId, runState);
  if (selected === null) return "blocked";
  return selected === armId ? "ready" : "transitive-skip";
}

export type MatchAggregation =
  | { readonly kind: "pending" }
  | { readonly kind: "complete"; readonly output: Readonly<Record<string, Json>> }
  | { readonly kind: "fail-fast"; readonly failedStepId: string };

/**
 * Inspect the chosen arm of a `running` match and report whether it can be
 * promoted to a terminal state.
 *
 *   pending    — selected arm has steps still running/pending; do nothing
 *   fail-fast  — at least one chosen-arm step is `failed`; match should fail
 *   complete   — every chosen-arm step is terminal (completed/skipped) and
 *                none failed; match should be completed with assembled output
 *
 * Output is keyed by the *local* step key (the original key from the arm's
 * StepMap), not the namespaced runtime id — that's what the user sees in
 * `needs.matchStep.<key>`. Skipped steps land as `null`, consistent with the
 * "skip is transitive" lock.
 */
export function aggregateMatch(
  matchId: string,
  flow: Flow,
  runState: RunState,
): MatchAggregation {
  const step = flow.steps[matchId];
  if (!step) return { kind: "pending" };
  const def = getDef(step);
  if (def.kind !== "match") return { kind: "pending" };

  const selected = readSelectedArm(matchId, runState);
  if (selected === null) return { kind: "pending" };

  const arm =
    def.mode === "discriminator" ? def.arms[selected] : def.arms.find((a) => a.id === selected);
  if (!arm) return { kind: "pending" };

  const output: Record<string, Json> = {};
  let allTerminal = true;
  const stripPrefix = `${matchId}.${arm.id}.`;

  for (const stepId of arm.stepIds) {
    const state: StepState | undefined = runState.steps[stepId];
    if (state === undefined || state.status === "pending" || state.status === "running") {
      allTerminal = false;
      continue;
    }
    if (state.status === "failed") {
      return { kind: "fail-fast", failedStepId: stepId };
    }
    const localKey = stepId.startsWith(stripPrefix) ? stepId.slice(stripPrefix.length) : stepId;
    output[localKey] = state.status === "completed" ? (state.output ?? null) : null;
  }

  if (!allTerminal) return { kind: "pending" };
  return { kind: "complete", output };
}

export interface FlowTermination {
  readonly done: boolean;
  readonly failed: boolean;
}

/**
 * `done` ⇔ every step is terminal (completed / failed / skipped).
 * `failed` ⇔ at least one step failed terminally.
 * The runtime appends `flow.completed` (or `flow.failed`) once `done`.
 */
export function flowTermination(flow: Flow, runState: RunState): FlowTermination {
  let done = true;
  let failed = false;
  for (const stepId of Object.keys(flow.steps)) {
    const state: StepState | undefined = runState.steps[stepId];
    if (state === undefined || state.status === "pending" || state.status === "running") {
      done = false;
      break;
    }
    if (state.status === "failed") failed = true;
  }
  return { done, failed };
}


/**
 * The flow input is persisted in the `flow.started` fact. Extracted on every
 * dispatch so handler `ctx.input` is consistent with what the run was started with.
 */
export function extractInput(runState: RunState): Json {
  for (const fact of runState.facts) {
    if (fact.kind === "flow.started") return fact.input;
  }
  throw new Error("No flow.started fact in run — was the run initialized via wf.start?");
}
