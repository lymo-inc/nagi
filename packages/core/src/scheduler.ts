import { Facts } from "./facts";
import {
  asStepMapWithDefs,
  getDef,
  needsStepIds,
  resolveNeeds,
  type StepDef,
} from "./internal";
import {
  isStepTerminal,
  isTerminalRun,
  type SkipReason,
  type StepState,
  stepStateOf,
} from "./state";
import type {
  Flow,
  Json,
  RunState,
  SerializedError,
  StepId,
  StepResetFact,
} from "./types";

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

// The upstream gate runs before the step's own `when`. A blocked upstream
// leaves the step pending; a skipped/failed one cascades a skip.
function gateStep(def: StepDef, runState: RunState, input: unknown): StepGate {
  const upstream = checkUpstream(def, runState);
  if (upstream !== "ready") return gateFor(upstream);

  if (def.when) {
    const needs = resolveNeeds(def, runState);
    if (!def.when({ input, needs }))
      return { kind: "skip", reason: "when-false" };
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
  for (const ref of Object.values(def.needs)) {
    const upstreamState = stepStateOf(runState, ref.step.id);
    if (upstreamState.tag === "completed") continue;
    // An optional need tolerates a skipped upstream — the step still runs and
    // receives Resolved<skipped>. A bare (required) need cascades the skip (or
    // failure) so its value is always present when the step runs.
    if (ref.optional && upstreamState.tag === "skipped") continue;
    if (upstreamState.tag === "skipped" || upstreamState.tag === "failed") {
      return "transitive-skip";
    }
    return "blocked";
  }
  return "ready";
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
    // A canceled step on a live run is neither success nor an end: it waits
    // for replay({ from }). (A canceled RUN never reaches here — it is settled.)
    if (!isStepTerminal(state) || state.tag === "canceled")
      return { kind: "running" };
    if (state.tag === "failed" && failure === undefined) failure = state.error;
  }
  if (failure !== undefined) return { kind: "failed", error: failure };
  return { kind: "succeeded" };
}

export type Transition =
  | { readonly kind: "complete"; readonly output: Json }
  | { readonly kind: "fail"; readonly error: SerializedError }
  | {
      readonly kind: "dispatch";
      readonly runnable: readonly StepId[];
      readonly skip: readonly SkipDecision[];
    }
  | { readonly kind: "skip"; readonly skip: readonly SkipDecision[] }
  | { readonly kind: "settled" }
  | { readonly kind: "stalled"; readonly canceled: readonly StepId[] }
  | { readonly kind: "waiting" };

export function nextTransition(flow: Flow, runState: RunState): Transition {
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

  const input = runState.input;
  const { runnable, skip } = nextRunnable({ flow, runState, input });
  if (runnable.length > 0) return { kind: "dispatch", runnable, skip };
  if (skip.length > 0) return { kind: "skip", skip };
  const canceled = canceledStepsIfIdle(flow, runState);
  return canceled.length > 0
    ? { kind: "stalled", canceled }
    : { kind: "waiting" };
}

// A step in one of these tags has a handler in flight, or is about to — the
// run is not idle yet, so a canceled step elsewhere cannot be called a stall.
const IN_FLIGHT_TAGS: ReadonlySet<StepState["tag"]> = new Set([
  "running",
  "awaitingSignal",
  "awaitingChild",
  "backoff",
  "aborting",
]);

function canceledStepsIfIdle(
  flow: Flow,
  runState: RunState,
): readonly StepId[] {
  const canceled: StepId[] = [];
  for (const stepId of Object.keys(flow.steps)) {
    const state = stepStateOf(runState, stepId);
    if (state.tag === "canceled") canceled.push(stepId as StepId);
    else if (IN_FLIGHT_TAGS.has(state.tag)) return [];
  }
  return canceled;
}

export function computeFlowOutput(flow: Flow, runState: RunState): Json {
  if (flow.output === undefined) return null;
  const stepOutputs: Record<string, Json> = {};
  for (const [sid, sstate] of Object.entries(runState.steps)) {
    if (sstate.tag === "completed") stepOutputs[sid] = sstate.output;
  }
  return flow.output(stepOutputs as never) as Json;
}

// The facts one reset writes; every non-origin step records where the cascade
// came from. "step" keeps completed descendants and resets the rest, or the
// run would settle without them.
export function resetFactsOf(
  flow: Flow,
  runState: RunState,
  origin: Omit<Parameters<typeof Facts.stepReset>[0], "cascadedFrom">,
): readonly StepResetFact[] {
  const { runId, stepId, at } = origin;
  const all = descendantsOf(flow, stepId);
  const set =
    origin.scope === "step"
      ? all.filter(
          (id) => id === stepId || holdsNoValue(stepStateOf(runState, id)),
        )
      : all;
  return set.map((id) =>
    id === stepId
      ? Facts.stepReset(origin)
      : Facts.stepReset({ runId, stepId: id, at, cascadedFrom: stepId }),
  );
}

function holdsNoValue(s: StepState): boolean {
  return s.tag === "failed" || s.tag === "canceled" || s.tag === "skipped";
}

export function descendantsOf(flow: Flow, stepId: StepId): readonly StepId[] {
  const children = new Map<StepId, StepId[]>();
  const addEdge = (from: StepId, to: StepId) => {
    const bucket = children.get(from);
    if (bucket) bucket.push(to);
    else children.set(from, [to]);
  };
  for (const [id, step] of Object.entries(asStepMapWithDefs(flow.steps))) {
    for (const upstreamId of needsStepIds(getDef(step)))
      addEdge(upstreamId, id);
  }

  const out: StepId[] = [stepId];
  const seen = new Set<StepId>([stepId]);
  for (let i = 0; i < out.length; i++) {
    const current = out[i];
    if (current === undefined) continue;
    for (const child of children.get(current) ?? []) {
      if (seen.has(child)) continue;
      seen.add(child);
      out.push(child);
    }
  }
  return out;
}
