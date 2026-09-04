import type {
  AttemptNumber,
  Fact,
  Json,
  ParentLink,
  RunId,
  RunStatus,
  SerializedError,
  StepId,
  StepStatus,
} from "./types";

export type SkipReason = "when-false" | "transitive" | "manual";

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

export interface RunState {
  readonly runId: RunId;
  readonly flowId: string;
  readonly input: Json;
  readonly phase: RunPhase;
  readonly steps: Readonly<Record<StepId, StepState>>;
  readonly selectedArms: Readonly<Record<StepId, string>>;
  readonly bufferedSignals: Readonly<
    Record<StepId, { readonly payload: Json; readonly signalName?: string }>
  >;
  // The source log this projection folded from, kept for introspection and
  // test/debug assertions; the engine reads the projection above, never this.
  readonly facts: readonly Fact[];
  readonly parent?: ParentLink;
  readonly flowHash?: string;
  readonly codeVersion?: string;
}

export const PENDING: StepState = { tag: "pending" };

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

// Reads the projected `aborting` tag rather than rescanning facts. Sound inside
// the runStep tx and the cancel watcher because the step is still non-terminal
// there, so the tag faithfully reflects an outstanding abort for this attempt.
export function isAbortRequested(
  s: StepState,
  attempt: AttemptNumber,
): boolean {
  return s.tag === "aborting" && s.attempt === attempt;
}

export function isStepTerminal(s: StepState): boolean {
  return (
    s.tag === "completed" ||
    s.tag === "failed" ||
    s.tag === "skipped" ||
    s.tag === "canceled"
  );
}
