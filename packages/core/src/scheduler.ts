import {
  getDef,
  matchArms,
  needsStepIds,
  readSelectedArm,
  resolveNeeds,
  type StepDef,
} from "./internal";
import type { Flow, Json, RunState, StepId, StepState } from "./types";

export type SkipReason = "when-false" | "transitive";

export interface ScheduleDecision {
  readonly runnable: readonly string[];
  readonly skip: ReadonlyArray<{
    readonly stepId: string;
    readonly reason: SkipReason;
  }>;
}

export interface ScheduleArgs {
  readonly flow: Flow;
  readonly runState: RunState;
  readonly input: unknown;
}

export function nextRunnable(args: ScheduleArgs): ScheduleDecision {
  const { flow, runState, input } = args;
  const runnable: string[] = [];
  const skip: { stepId: string; reason: SkipReason }[] = [];

  for (const [stepId, step] of Object.entries(flow.steps)) {
    const state = runState.steps[stepId];
    if (state !== undefined && state.status !== "pending") continue;

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
      const needs = resolveNeeds(
        def,
        (id) => runState.steps[id]?.output ?? null,
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
    if (upstreamState.status === "skipped") {
      if (manualSkipCascade(upstream.id, runState) === "continue") continue;
      return "transitive-skip";
    }
    if (upstreamState.status === "failed") {
      return "transitive-skip";
    }
    return "blocked";
  }
  return "ready";
}

function manualSkipCascade(
  stepId: StepId,
  runState: RunState,
): "skip" | "continue" {
  for (let i = runState.facts.length - 1; i >= 0; i--) {
    const f = runState.facts[i];
    if (f === undefined) continue;
    if (f.kind !== "step.skipped") continue;
    if (f.stepId !== stepId) continue;
    if (f.reason !== "manual") return "skip";
    return f.cascade ?? "skip";
  }
  return "skip";
}

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
  const step = flow.steps[matchId];
  if (!step) return { kind: "pending" };
  const def = getDef(step);
  if (def.kind !== "match") return { kind: "pending" };

  const selected = readSelectedArm(matchId, runState);
  if (selected === null) return { kind: "pending" };

  const arm =
    def.mode === "discriminator"
      ? def.arms[selected]
      : def.arms.find((a) => a.id === selected);
  if (!arm) return { kind: "pending" };

  const output: Record<string, Json> = {};
  let allTerminal = true;
  const stripPrefix = `${matchId}.${arm.id}.`;

  for (const stepId of arm.stepIds) {
    const state: StepState | undefined = runState.steps[stepId];
    if (
      state === undefined ||
      state.status === "pending" ||
      state.status === "running"
    ) {
      allTerminal = false;
      continue;
    }
    if (state.status === "failed") {
      return { kind: "fail-fast", failedStepId: stepId };
    }
    const localKey = stepId.startsWith(stripPrefix)
      ? stepId.slice(stripPrefix.length)
      : stepId;
    output[localKey] =
      state.status === "completed" ? (state.output ?? null) : null;
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
    const state: StepState | undefined = runState.steps[stepId];
    if (
      state === undefined ||
      state.status === "pending" ||
      state.status === "running"
    ) {
      done = false;
      break;
    }
    if (state.status === "failed") failed = true;
  }
  return { done, failed };
}

export function descendantsOf(flow: Flow, stepId: StepId): readonly StepId[] {
  const children = new Map<StepId, StepId[]>();
  const addEdge = (from: StepId, to: StepId) => {
    const bucket = children.get(from);
    if (bucket) bucket.push(to);
    else children.set(from, [to]);
  };
  for (const [id, step] of Object.entries(flow.steps) as Array<
    [StepId, (typeof flow.steps)[string]]
  >) {
    const def = getDef(step);
    for (const upstreamId of needsStepIds(def)) addEdge(upstreamId, id);
    if (def.kind === "match") {
      for (const arm of matchArms(def)) {
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
