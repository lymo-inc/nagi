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

/* ───────────────────────── shared sums ───────────────────────── */

/** Why a step ended up `skipped`. `manual` is operator-driven; the other two
 * are scheduler decisions (a false `when`, or an upstream that didn't run). */
export type SkipReason = "when-false" | "transitive" | "manual";

/** How a skip propagates: `skip` blocks downstream too; `continue` lets it run
 * with the upstream resolved as {@link Resolved} `skipped`. */
export type Cascade = "skip" | "continue";

/** Why a single step is `canceled`. */
export type StepCancelCause =
  | { readonly kind: "run-canceled" }
  | { readonly kind: "aborted"; readonly error?: SerializedError };

/** Why a whole run is `canceled` — mirrors the three `flow.canceled` facts. */
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

/* ─────────────────────── step state machine ─────────────────────── */

/**
 * The per-step state machine. Each tag carries exactly — and only — the data
 * valid in that state, so `output` is unreachable unless `completed`, `error`
 * unreachable unless `failed`/`backoff`, and an `attempt` exists only once a
 * step has actually started. The five "active" tags split what the old flat
 * `status: "running"` overloaded into one string (executing vs parked-on-signal
 * vs awaiting-child vs retry-backoff vs settling-an-abort).
 */
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

/* ─────────────────────── needs resolution ─────────────────────── */

/**
 * The value a downstream step sees for one upstream dependency. `skipped` is now
 * structurally distinct from a genuine `null` output (`{ tag: "value", value:
 * null }`), removing the old `Json | null` sentinel ambiguity.
 */
export type Resolved<T = Json> =
  | { readonly tag: "value"; readonly value: T }
  | { readonly tag: "skipped" };

/* ─────────────────────── run state machine ─────────────────────── */

/** The run-level machine. Terminal data (output/error/cancel-cause) lives here
 * instead of being re-scanned out of the fact log. */
export type RunPhase =
  | { readonly tag: "pending" }
  | { readonly tag: "running" }
  | { readonly tag: "completed"; readonly output: Json }
  | { readonly tag: "failed"; readonly error: SerializedError }
  | { readonly tag: "canceled"; readonly cause: RunCancelCause };

/** Recorded when a fact arrives that cannot apply to the current state (e.g. a
 * `step.skipped` on a `running` step). The fold never throws — it keeps the
 * prior state and appends one of these so drift is observable. */
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
  /** Arm chosen per `match` step. Replaces scanning facts for `match.arm-selected`. */
  readonly selectedArms: Readonly<Record<StepId, string>>;
  readonly anomalies: readonly Anomaly[];
  readonly facts: readonly Fact[];
  /** Set when this run is a subflow child; carried from `flow.started`. */
  readonly parent?: ParentLink;
  readonly flowHash?: string;
  readonly codeVersion?: string;
}

/* ─────────────────────── projections / bridges ─────────────────────── */

const PENDING: StepState = { tag: "pending" };

/** Total read of a step's state: an untouched step reads as `pending`. */
export function stepStateOf(runState: RunState, stepId: StepId): StepState {
  return runState.steps[stepId] ?? PENDING;
}

/** Flat status string for boundary DTOs (RunSummary, query filters). */
export function runStatusOf(state: RunState): RunStatus {
  return state.phase.tag;
}

/** Flat status string for boundary DTOs. The five active tags collapse to
 * `"running"`, matching the legacy six-value `StepStatus`. */
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

/** The step's output if it completed, else `null`. */
export function outputOf(s: StepState): Json | null {
  return s.tag === "completed" ? s.output : null;
}

/** The error associated with a step, if any: a `failed`/`backoff` error, or an
 * aborted `canceled` step's error. `undefined` for every other state. */
export function errorOf(s: StepState): SerializedError | undefined {
  if (s.tag === "failed" || s.tag === "backoff") return s.error;
  if (s.tag === "canceled" && s.cause.kind === "aborted") return s.cause.error;
  return undefined;
}

/** The current/last attempt number, or `0` for states that never started. */
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

/** Resolve a step as an upstream dependency value. Only meaningful for terminal
 * upstreams (the scheduler resolves needs only once they are settled). */
export function resolvedOf(s: StepState): Resolved {
  if (s.tag === "completed") return { tag: "value", value: s.output };
  if (s.tag === "skipped") return { tag: "skipped" };
  return { tag: "value", value: null };
}

/** Extract a resolved upstream's value, throwing if it was skipped. Use when a
 * handler requires the upstream to have produced a value; guard on
 * `r.tag === "skipped"` directly when a `cascade: "continue"` skip is expected. */
export function unwrap<T>(r: Resolved<T>): T {
  if (r.tag === "skipped") {
    throw new Error("nagi: upstream was skipped — its value is unavailable");
  }
  return r.value;
}

export function isTerminalRun(state: RunState): boolean {
  return state.phase.tag !== "pending" && state.phase.tag !== "running";
}

/** A step that will never transition again on its own: completed/failed/
 * skipped/canceled. `pending` and the five active tags are NOT terminal. */
export function isStepTerminal(s: StepState): boolean {
  return (
    s.tag === "completed" ||
    s.tag === "failed" ||
    s.tag === "skipped" ||
    s.tag === "canceled"
  );
}

/* ─────────────────────────── the fold ─────────────────────────── */

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

/**
 * The single (state × fact) → state transition for one step. Total: any pair
 * that isn't a real transition keeps the prior state and is flagged anomalous.
 */
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
    // ANY non-terminal state (a `completed`/`failed`/`canceled` arriving without
    // a recorded start still happened). Only a terminal→terminal contradiction
    // is rejected and flagged.
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
      // Scheduler skips only pending steps, but an operator may skip an
      // in-flight step (running/awaiting/backoff/aborting). Only a skip of an
      // already-terminal step is anomalous.
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

/**
 * Fold the append-only fact log into the projected {@link RunState}. Pure in the
 * facts alone — `step.started` carries its `stepKind`, so the projection needs
 * no flow definition to tell a parked signal from a running task.
 */
export function foldRun(runId: RunId, facts: readonly Fact[]): RunState {
  let flowId = "";
  let phase: RunPhase = { tag: "pending" };
  let parent: ParentLink | undefined;
  let flowHash: string | undefined;
  let codeVersion: string | undefined;
  const steps: Record<StepId, StepState> = {};
  const selectedArms: Record<StepId, string> = {};
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
    anomalies,
    facts,
    ...(parent !== undefined ? { parent } : {}),
    ...(flowHash !== undefined ? { flowHash } : {}),
    ...(codeVersion !== undefined ? { codeVersion } : {}),
  };
}
