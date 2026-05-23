import type {
  AttemptNumber,
  Fact,
  FlowCanceledFact,
  Json,
  ParentLink,
  RunId,
  RunStatus,
  SerializedError,
  StepAbortRequestedFact,
  StepCanceledFact,
  StepCompletedFact,
  StepFailedFact,
  StepId,
  StepKind,
  StepRetriedFact,
  StepSkippedFact,
  StepStartedFact,
  StepStatus,
} from "./types";

export type SkipReason = "when-false" | "transitive" | "manual";

export type Cascade = "skip" | "continue";

export type StepCancelCause =
  | { readonly kind: "run-canceled" }
  | { readonly kind: "aborted"; readonly error?: SerializedError };

export type RunCancelCause =
  | {
      readonly kind: "concurrency";
      readonly canceledByRunId: RunId;
      readonly concurrencyKey: string;
    }
  | {
      readonly kind: "explicit";
      readonly reason: string;
      readonly note?: string;
    }
  | {
      readonly kind: "operator";
      readonly actor: string;
      readonly reason: string;
      readonly note?: string;
    };

export type StepState =
  | { readonly tag: "pending" }
  | { readonly tag: "running"; readonly attempt: AttemptNumber }
  | { readonly tag: "awaitingSignal"; readonly attempt: AttemptNumber }
  | { readonly tag: "awaitingChild"; readonly attempt: AttemptNumber }
  | {
      readonly tag: "backoff";
      readonly failedAttempt: AttemptNumber;
      readonly retryAt: Date;
      readonly error: SerializedError;
    }
  | { readonly tag: "aborting"; readonly attempt: AttemptNumber }
  | {
      readonly tag: "completed";
      readonly attempt: AttemptNumber;
      readonly output: Json;
    }
  | {
      readonly tag: "failed";
      readonly attempt: AttemptNumber;
      readonly error: SerializedError;
    }
  | {
      readonly tag: "skipped";
      readonly reason: SkipReason;
      readonly cascade: Cascade;
    }
  | { readonly tag: "canceled"; readonly cause: StepCancelCause };

export type Resolved<T = Json> =
  | { readonly tag: "value"; readonly value: T }
  | { readonly tag: "skipped" };

export type RunPhase =
  | { readonly tag: "pending" }
  | { readonly tag: "running" }
  | { readonly tag: "completed"; readonly output: Json }
  | { readonly tag: "failed"; readonly error: SerializedError }
  | { readonly tag: "canceled"; readonly cause: RunCancelCause };

export interface Anomaly {
  readonly at: Date;
  readonly stepId?: StepId;
  readonly from: string;
  readonly fact: Fact["kind"];
}

export interface RunState {
  readonly runId: RunId;
  readonly flowId: string;
  readonly phase: RunPhase;
  readonly steps: Readonly<Record<StepId, StepState>>;
  readonly selectedArms: Readonly<Record<StepId, string>>;
  readonly bufferedSignals: Readonly<
    Record<StepId, { readonly payload: Json; readonly signalName?: string }>
  >;
  readonly anomalies: readonly Anomaly[];
  readonly facts: readonly Fact[];
  readonly parent?: ParentLink;
  readonly flowHash?: string;
  readonly codeVersion?: string;
}

const PENDING: StepState = { tag: "pending" };

export function stepStateOf(runState: RunState, stepId: StepId): StepState {
  return runState.steps[stepId] ?? PENDING;
}

export function runStatusOf(state: RunState): RunStatus {
  return state.phase.tag;
}

export function stepStatusOf(s: StepState): StepStatus {
  switch (s.tag) {
    case "pending":
      return "pending";
    case "running":
    case "awaitingSignal":
    case "awaitingChild":
    case "backoff":
    case "aborting":
      return "running";
    case "completed":
      return "completed";
    case "failed":
      return "failed";
    case "skipped":
      return "skipped";
    case "canceled":
      return "canceled";
  }
}

export function outputOf(s: StepState): Json | null {
  return s.tag === "completed" ? s.output : null;
}

export function errorOf(s: StepState): SerializedError | undefined {
  if (s.tag === "failed" || s.tag === "backoff") return s.error;
  if (s.tag === "canceled" && s.cause.kind === "aborted") return s.cause.error;
  return undefined;
}

export function attemptOf(s: StepState): AttemptNumber {
  switch (s.tag) {
    case "running":
    case "awaitingSignal":
    case "awaitingChild":
    case "aborting":
    case "completed":
    case "failed":
      return s.attempt;
    case "backoff":
      return s.failedAttempt;
    case "pending":
    case "skipped":
    case "canceled":
      return 0 as AttemptNumber;
  }
}

// Only meaningful for terminal upstreams; a non-terminal step resolves to a
// null value (the scheduler resolves needs only once they are settled).
export function resolvedOf(s: StepState): Resolved {
  if (s.tag === "completed") return { tag: "value", value: s.output };
  if (s.tag === "skipped") return { tag: "skipped" };
  return { tag: "value", value: null };
}

export function unwrap<T>(r: Resolved<T>): T {
  if (r.tag === "skipped") {
    throw new Error("nagi: upstream was skipped — its value is unavailable");
  }
  return r.value;
}

export function isTerminalRun(state: RunState): boolean {
  return state.phase.tag !== "pending" && state.phase.tag !== "running";
}

export function extractInput(runState: RunState): Json {
  for (const fact of runState.facts) {
    if (fact.kind === "flow.started") return fact.input;
  }
  throw new Error(
    "No flow.started fact in run — was the run initialized via wf.start?",
  );
}

export function isStepTerminal(s: StepState): boolean {
  return (
    s.tag === "completed" ||
    s.tag === "failed" ||
    s.tag === "skipped" ||
    s.tag === "canceled"
  );
}

type StepScopedFact =
  | StepStartedFact
  | StepCompletedFact
  | StepFailedFact
  | StepCanceledFact
  | StepRetriedFact
  | StepSkippedFact
  | StepAbortRequestedFact;

interface StepTransition {
  readonly next: StepState;
  readonly anomaly: boolean;
}

function keep(prev: StepState): StepTransition {
  return { next: prev, anomaly: true };
}

function startTarget(stepKind: StepKind, attempt: AttemptNumber): StepState {
  switch (stepKind) {
    case "signal":
      return { tag: "awaitingSignal", attempt };
    case "subflow":
      return { tag: "awaitingChild", attempt };
    case "task":
    case "streaming":
    case "match":
      return { tag: "running", attempt };
  }
}

// Total: any pair that isn't a real transition keeps the prior state and is
// flagged anomalous, so the fold never throws.
function stepTransition(prev: StepState, fact: StepScopedFact): StepTransition {
  switch (fact.kind) {
    case "step.started":
      if (prev.tag === "pending" || prev.tag === "backoff")
        return {
          next: startTarget(fact.stepKind, fact.attempt),
          anomaly: false,
        };
      return keep(prev);

    // Terminal facts carry authoritative outcomes, so they settle a step from
    // any non-terminal state; only a terminal→terminal contradiction is flagged.
    case "step.completed":
      if (isStepTerminal(prev)) return keep(prev);
      return {
        next: { tag: "completed", attempt: fact.attempt, output: fact.output },
        anomaly: false,
      };

    case "step.failed":
      if (isStepTerminal(prev)) return keep(prev);
      return {
        next: { tag: "failed", attempt: fact.attempt, error: fact.error },
        anomaly: false,
      };

    case "step.canceled": {
      if (isStepTerminal(prev)) return keep(prev);
      const cause: StepCancelCause =
        prev.tag === "aborting"
          ? {
              kind: "aborted",
              ...(fact.error !== undefined ? { error: fact.error } : {}),
            }
          : { kind: "run-canceled" };
      return { next: { tag: "canceled", cause }, anomaly: false };
    }

    case "step.retried":
      if (prev.tag === "running")
        return {
          next: {
            tag: "backoff",
            failedAttempt: fact.attempt,
            retryAt: fact.nextAttemptAt,
            error: fact.error,
          },
          anomaly: false,
        };
      return keep(prev);

    case "step.skipped":
      // An operator may skip an in-flight step, not just a pending one; only a
      // skip of an already-terminal step is anomalous.
      if (!isStepTerminal(prev))
        return {
          next: {
            tag: "skipped",
            reason: fact.reason,
            cascade: fact.cascade ?? "skip",
          },
          anomaly: false,
        };
      return keep(prev);

    case "step.abort-requested":
      if (
        prev.tag === "running" ||
        prev.tag === "awaitingSignal" ||
        prev.tag === "awaitingChild"
      )
        return {
          next: { tag: "aborting", attempt: fact.attempt },
          anomaly: false,
        };
      return keep(prev);
  }
}

function runCancelCause(fact: FlowCanceledFact): RunCancelCause {
  switch (fact.cause) {
    case "concurrency":
      return {
        kind: "concurrency",
        canceledByRunId: fact.canceledByRunId,
        concurrencyKey: fact.concurrencyKey,
      };
    case "explicit":
      return {
        kind: "explicit",
        reason: fact.reason,
        ...(fact.note !== undefined ? { note: fact.note } : {}),
      };
    case "operator":
      return {
        kind: "operator",
        actor: fact.actor,
        reason: fact.reason,
        ...(fact.note !== undefined ? { note: fact.note } : {}),
      };
  }
}

export function foldRun(runId: RunId, facts: readonly Fact[]): RunState {
  let flowId = "";
  let phase: RunPhase = { tag: "pending" };
  let parent: ParentLink | undefined;
  let flowHash: string | undefined;
  let codeVersion: string | undefined;
  const steps: Record<StepId, StepState> = {};
  const selectedArms: Record<StepId, string> = {};
  const bufferedSignals: Record<
    StepId,
    { readonly payload: Json; readonly signalName?: string }
  > = {};
  const anomalies: Anomaly[] = [];

  for (const fact of facts) {
    switch (fact.kind) {
      case "flow.started":
        flowId = fact.flowId;
        phase = { tag: "running" };
        parent = fact.parent;
        flowHash = fact.flowHash;
        codeVersion = fact.codeVersion;
        break;
      case "flow.completed":
        phase = { tag: "completed", output: fact.output };
        break;
      case "flow.failed":
        phase = { tag: "failed", error: fact.error };
        break;
      case "flow.canceled":
        phase = { tag: "canceled", cause: runCancelCause(fact) };
        break;
      case "match.arm-selected":
        selectedArms[fact.stepId] = fact.arm;
        break;
      case "step.reset":
        steps[fact.stepId] = PENDING;
        delete selectedArms[fact.stepId];
        break;
      case "step.started":
      case "step.completed":
      case "step.failed":
      case "step.canceled":
      case "step.retried":
      case "step.skipped":
      case "step.abort-requested": {
        const prev = steps[fact.stepId] ?? PENDING;
        const t = stepTransition(prev, fact);
        if (t.anomaly) {
          anomalies.push({
            at: fact.at,
            stepId: fact.stepId,
            from: prev.tag,
            fact: fact.kind,
          });
        }
        steps[fact.stepId] = t.next;
        break;
      }
      case "signal.buffered":
        // First buffered signal per step wins, mirroring the happy path where
        // the first delivered signal completes the step and later ones no-op.
        if (!(fact.stepId in bufferedSignals)) {
          bufferedSignals[fact.stepId] = {
            payload: fact.payload,
            ...(fact.signalName !== undefined
              ? { signalName: fact.signalName }
              : {}),
          };
        }
        break;
      case "signal.sent":
      case "signal.received":
      case "once.recorded":
        break;
    }
  }

  return {
    runId,
    flowId,
    phase,
    steps,
    selectedArms,
    bufferedSignals,
    anomalies,
    facts,
    ...(parent !== undefined ? { parent } : {}),
    ...(flowHash !== undefined ? { flowHash } : {}),
    ...(codeVersion !== undefined ? { codeVersion } : {}),
  };
}
