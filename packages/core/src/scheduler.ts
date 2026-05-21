import {
  asStepMapWithDefs,
  getDef,
  needsStepIds,
  resolveNeeds,
  type StepDef,
} from "./internal";
import {
  attemptOf,
  isStepTerminal,
  outputOf,
  resolvedOf,
  stepStateOf,
} from "./state";
import type {
  AttemptNumber,
  Fact,
  Flow,
  Json,
  RunState,
  SerializedError,
  StepId,
} from "./types";

export { stepStateOf };

export type SkipReason = "when-false" | "transitive";

export interface SkipDecision {
  readonly stepId: string;
  readonly reason: SkipReason;
}

export interface ScheduleDecision {
  readonly runnable: readonly string[];
  readonly skip: readonly SkipDecision[];
}

export interface ScheduleArgs {
  readonly flow: Flow;
  readonly runState: RunState;
  readonly input: unknown;
}

export function nextRunnable({
  flow,
  runState,
  input,
}: ScheduleArgs): ScheduleDecision {
  const runnable: string[] = [];
  const skip: { stepId: string; reason: SkipReason }[] = [];

  for (const [stepId, step] of Object.entries(asStepMapWithDefs(flow.steps))) {
    const state = stepStateOf(runState, stepId);
    if (state.tag !== "pending") continue;

    const def = getDef(step);

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
      const needs = resolveNeeds(def, (id) =>
        resolvedOf(stepStateOf(runState, id)),
      );
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
    const upstreamState = stepStateOf(runState, upstream.id);
    if (upstreamState.tag === "completed") continue;
    if (upstreamState.tag === "skipped") {
      if (upstreamState.cascade === "continue") continue;
      return "transitive-skip";
    }
    if (upstreamState.tag === "failed") {
      return "transitive-skip";
    }
    return "blocked";
  }
  return "ready";
}

function checkParentMatch(def: StepDef, runState: RunState): UpstreamStatus {
  if (!def.parentMatch) return "ready";
  const { matchId, armId } = def.parentMatch;

  const parentState = stepStateOf(runState, matchId);
  if (parentState.tag === "failed" || parentState.tag === "skipped") {
    return "transitive-skip";
  }

  const selected = runState.selectedArms[matchId] ?? null;
  if (selected === null) return "blocked";
  return selected === armId ? "ready" : "transitive-skip";
}

export type MatchAggregation =
  | { readonly kind: "pending" }
  | {
      readonly kind: "complete";
      readonly output: Readonly<Record<string, Json>>;
    }
  | { readonly kind: "fail-fast"; readonly failedStepId: string };

export function aggregateMatch(
  matchId: string,
  flow: Flow,
  runState: RunState,
): MatchAggregation {
  const step = asStepMapWithDefs(flow.steps)[matchId];
  if (!step) return { kind: "pending" };
  const def = getDef(step);
  if (def.kind !== "match") return { kind: "pending" };

  const selected = runState.selectedArms[matchId] ?? null;
  if (selected === null) return { kind: "pending" };

  const arm = def.arms.find((a) => a.id === selected);
  if (!arm) return { kind: "pending" };

  const output: Record<string, Json> = {};
  let allTerminal = true;
  const stripPrefix = `${matchId}.${arm.id}.`;

  for (const stepId of arm.stepIds) {
    const state = stepStateOf(runState, stepId);
    if (!isStepTerminal(state)) {
      allTerminal = false;
      continue;
    }
    if (state.tag === "failed") {
      return { kind: "fail-fast", failedStepId: stepId };
    }
    const localKey = stepId.startsWith(stripPrefix)
      ? stepId.slice(stripPrefix.length)
      : stepId;
    output[localKey] = outputOf(state);
  }

  if (!allTerminal) return { kind: "pending" };
  return { kind: "complete", output };
}

export interface FlowTermination {
  readonly done: boolean;
  readonly failed: boolean;
}

export function flowTermination(
  flow: Flow,
  runState: RunState,
): FlowTermination {
  let done = true;
  let failed = false;
  for (const stepId of Object.keys(flow.steps)) {
    const state = stepStateOf(runState, stepId);
    if (!isStepTerminal(state)) {
      done = false;
      break;
    }
    if (state.tag === "failed") failed = true;
  }
  return { done, failed };
}

export interface MatchPromotion {
  readonly matchId: StepId;
  readonly attempt: AttemptNumber;
  readonly result:
    | { readonly kind: "complete"; readonly output: Json }
    | { readonly kind: "fail"; readonly error: SerializedError };
}

/**
 * One tick of the engine state machine. `nextTransition` returns the single
 * highest-priority action for the current run state; `advance` performs the
 * side effects and loops to re-derive. See docs/rfcs/0011.
 */
export type Transition =
  | {
      readonly kind: "promote-match";
      readonly promotions: readonly MatchPromotion[];
    }
  | { readonly kind: "complete"; readonly output: Json }
  | { readonly kind: "fail"; readonly error: SerializedError }
  | {
      readonly kind: "dispatch";
      readonly runnable: readonly StepId[];
      readonly skip: readonly SkipDecision[];
    }
  | { readonly kind: "skip"; readonly skip: readonly SkipDecision[] }
  | { readonly kind: "settled" }
  | { readonly kind: "waiting" };

export function nextTransition(flow: Flow, runState: RunState): Transition {
  const promotions = readyPromotions(flow, runState);
  if (promotions.length > 0) return { kind: "promote-match", promotions };

  const term = flowTermination(flow, runState);
  if (term.done) {
    if (isFlowTerminal(runState.facts)) return { kind: "settled" };
    if (term.failed) return { kind: "fail", error: flowFailureError(runState) };
    return { kind: "complete", output: computeFlowOutput(flow, runState) };
  }

  const input = extractInput(runState);
  const { runnable, skip } = nextRunnable({ flow, runState, input });
  if (runnable.length > 0) return { kind: "dispatch", runnable, skip };
  if (skip.length > 0) return { kind: "skip", skip };
  return { kind: "waiting" };
}

function readyPromotions(flow: Flow, runState: RunState): MatchPromotion[] {
  const out: MatchPromotion[] = [];
  for (const [matchId, step] of Object.entries(asStepMapWithDefs(flow.steps))) {
    const def = getDef(step);
    if (def.kind !== "match") continue;
    const state = stepStateOf(runState, matchId);
    if (state.tag !== "running") continue;

    const agg = aggregateMatch(matchId, flow, runState);
    if (agg.kind === "pending") continue;

    const attempt: AttemptNumber = attemptOf(state);
    if (agg.kind === "fail-fast") {
      const failedNested = stepStateOf(runState, agg.failedStepId);
      const error: SerializedError =
        failedNested.tag === "failed"
          ? failedNested.error
          : {
              name: "Error",
              message: `match "${matchId}": chosen-arm step "${agg.failedStepId}" failed`,
            };
      out.push({
        matchId: matchId as StepId,
        attempt,
        result: { kind: "fail", error },
      });
    } else {
      out.push({
        matchId: matchId as StepId,
        attempt,
        result: { kind: "complete", output: agg.output },
      });
    }
  }
  return out;
}

function flowFailureError(runState: RunState): SerializedError {
  for (const s of Object.values(runState.steps)) {
    if (s.tag === "failed") return s.error;
  }
  return { name: "Error", message: "step failed" };
}

export function isFlowTerminal(facts: readonly Fact[]): boolean {
  for (let i = facts.length - 1; i >= 0; i--) {
    const f = facts[i];
    if (
      f !== undefined &&
      (f.kind === "flow.completed" ||
        f.kind === "flow.failed" ||
        f.kind === "flow.canceled")
    ) {
      return true;
    }
  }
  return false;
}

export function computeFlowOutput(flow: Flow, runState: RunState): Json {
  if (flow.output === undefined) return null;
  const stepOutputs: Record<string, Json> = {};
  for (const [sid, sstate] of Object.entries(runState.steps)) {
    if (sstate.tag === "completed") stepOutputs[sid] = sstate.output;
  }
  return flow.output(stepOutputs as never) as Json;
}

export function descendantsOf(flow: Flow, stepId: StepId): readonly StepId[] {
  const children = new Map<StepId, StepId[]>();
  const addEdge = (from: StepId, to: StepId) => {
    const bucket = children.get(from);
    if (bucket) bucket.push(to);
    else children.set(from, [to]);
  };
  for (const [id, step] of Object.entries(asStepMapWithDefs(flow.steps))) {
    const def = getDef(step);
    for (const upstreamId of needsStepIds(def)) addEdge(upstreamId, id);
    if (def.kind === "match") {
      for (const arm of def.arms) {
        for (const armStepId of arm.stepIds) addEdge(id, armStepId);
      }
    }
  }

  const out: StepId[] = [stepId];
  const seen = new Set<StepId>([stepId]);
  for (let i = 0; i < out.length; i++) {
    const current = out[i];
    if (current === undefined) continue;
    for (const child of children.get(current) ?? []) {
      if (seen.has(child)) continue;
      if (!(child in flow.steps)) continue;
      seen.add(child);
      out.push(child);
    }
  }
  return out;
}

export function extractInput(runState: RunState): Json {
  for (const fact of runState.facts) {
    if (fact.kind === "flow.started") return fact.input;
  }
  throw new Error(
    "No flow.started fact in run — was the run initialized via wf.start?",
  );
}
