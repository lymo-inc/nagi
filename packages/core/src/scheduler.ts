import {
  asStepMapWithDefs,
  getDef,
  needsStepIds,
  resolveNeeds,
  type StepDef,
} from "./internal";
import {
  attemptOf,
  extractInput,
  isStepTerminal,
  isTerminalRun,
  outputOf,
  resolvedOf,
  stepStateOf,
} from "./state";
import type {
  AttemptNumber,
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

// Each pending step resolves to exactly one outcome. Computing it as data lets
// nextRunnable be a flat partition instead of a continue-driven accumulator.
type StepGate =
  | { readonly kind: "run" }
  | { readonly kind: "skip"; readonly reason: SkipReason }
  | { readonly kind: "block" };

export function nextRunnable({
  flow,
  runState,
  input,
}: ScheduleArgs): ScheduleDecision {
  const runnable: string[] = [];
  const skip: SkipDecision[] = [];

  for (const [stepId, step] of Object.entries(asStepMapWithDefs(flow.steps))) {
    if (stepStateOf(runState, stepId).tag !== "pending") continue;

    const gate = gateStep(getDef(step), runState, input);
    switch (gate.kind) {
      case "run":
        runnable.push(stepId);
        break;
      case "skip":
        skip.push({ stepId, reason: gate.reason });
        break;
      case "block":
        break;
    }
  }

  return { runnable, skip };
}

// Parent-match and upstream gates run before the step's own `when`. A blocked
// upstream leaves the step pending; a skipped/failed one cascades a skip.
function gateStep(def: StepDef, runState: RunState, input: unknown): StepGate {
  const parent = checkParentMatch(def, runState);
  if (parent !== "ready") return gateFor(parent);

  const upstream = checkUpstream(def, runState);
  if (upstream !== "ready") return gateFor(upstream);

  const when = def.kind === "match" ? undefined : def.when;
  if (when) {
    const needs = resolveNeeds(def, (id) =>
      resolvedOf(stepStateOf(runState, id)),
    );
    if (!when({ input, needs })) return { kind: "skip", reason: "when-false" };
  }

  return { kind: "run" };
}

function gateFor(status: "blocked" | "transitive-skip"): StepGate {
  switch (status) {
    case "blocked":
      return { kind: "block" };
    case "transitive-skip":
      return { kind: "skip", reason: "transitive" };
  }
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
  | { readonly kind: "fail-fast"; readonly error: SerializedError };

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

  // Fail-fast: any failed chosen-arm step fails the match, even while siblings
  // are still running — so scan for failure before collecting outputs.
  for (const stepId of arm.stepIds) {
    const state = stepStateOf(runState, stepId);
    if (state.tag === "failed")
      return { kind: "fail-fast", error: state.error };
  }

  const output: Record<string, Json> = {};
  for (const stepId of arm.stepIds) {
    const state = stepStateOf(runState, stepId);
    if (!isStepTerminal(state)) return { kind: "pending" };
    output[stripArmPrefix(matchId, arm.id, stepId)] = outputOf(state);
  }
  return { kind: "complete", output };
}

function stripArmPrefix(
  matchId: string,
  armId: string,
  stepId: string,
): string {
  const prefix = `${matchId}.${armId}.`;
  if (stepId.startsWith(prefix)) return stepId.slice(prefix.length);
  return stepId;
}

export type FlowTermination =
  | { readonly kind: "running" }
  | { readonly kind: "succeeded" }
  | { readonly kind: "failed"; readonly error: SerializedError };

export function flowTermination(
  flow: Flow,
  runState: RunState,
): FlowTermination {
  let failure: SerializedError | undefined;
  for (const stepId of Object.keys(flow.steps)) {
    const state = stepStateOf(runState, stepId);
    if (!isStepTerminal(state)) return { kind: "running" };
    if (state.tag === "failed" && failure === undefined) failure = state.error;
  }
  if (failure !== undefined) return { kind: "failed", error: failure };
  return { kind: "succeeded" };
}

export interface MatchPromotion {
  readonly matchId: StepId;
  readonly attempt: AttemptNumber;
  readonly result:
    | { readonly kind: "complete"; readonly output: Json }
    | { readonly kind: "fail"; readonly error: SerializedError };
}

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
  switch (term.kind) {
    case "succeeded":
    case "failed": {
      if (isTerminalRun(runState)) return { kind: "settled" };
      if (term.kind === "failed") return { kind: "fail", error: term.error };
      return { kind: "complete", output: computeFlowOutput(flow, runState) };
    }
    case "running":
      break;
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
      out.push({
        matchId: matchId as StepId,
        attempt,
        result: { kind: "fail", error: agg.error },
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
