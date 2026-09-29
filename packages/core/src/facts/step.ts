import { compact } from "../internal";
import {
  attemptOf,
  isStepTerminal,
  PENDING,
  type SkipReason,
  type StepCancelCause,
  type StepState,
} from "../state";
import type {
  AttemptNumber,
  Json,
  ResetScope,
  RunId,
  SerializedError,
  StepId,
  StepKind,
} from "../types";
import type { FactBase, KindTable, Release } from "./base";
import { foldStep } from "./base";

export interface StepStartedFact extends FactBase {
  readonly kind: "step.started";
  readonly stepId: StepId;
  readonly attempt: AttemptNumber;
  // Lets the projection fold a started step without the flow def.
  readonly stepKind: StepKind;
}

export interface StepCompletedFact extends FactBase {
  readonly kind: "step.completed";
  readonly stepId: StepId;
  readonly attempt: AttemptNumber;
  readonly output: Json;
}

export interface StepFailedFact extends FactBase {
  readonly kind: "step.failed";
  readonly stepId: StepId;
  readonly attempt: AttemptNumber;
  readonly error: SerializedError;
}

export interface StepCanceledFact extends FactBase {
  readonly kind: "step.canceled";
  readonly stepId: StepId;
  readonly attempt: AttemptNumber;
  readonly error?: SerializedError;
}

export interface StepRetriedFact extends FactBase {
  readonly kind: "step.retried";
  readonly stepId: StepId;
  readonly attempt: AttemptNumber;
  readonly nextAttemptAt: Date;
  readonly error: SerializedError;
}

export interface StepSkippedFact extends FactBase {
  readonly kind: "step.skipped";
  readonly stepId: StepId;
  readonly reason: SkipReason;
}

export interface StepResetFact extends FactBase {
  readonly kind: "step.reset";
  readonly stepId: StepId;
  readonly cascadedFrom?: StepId;
  // The caller's INTENT, recorded on the origin step only. Absence of
  // cascadedFrom siblings cannot be read as "isolated": a leaf step has no
  // descendants either, so a cascading reset on a leaf looks identical. Omitted
  // for "cascade" so existing facts keep their shape.
  readonly scope?: ResetScope;
}

export interface StepAbortRequestedFact extends FactBase {
  readonly kind: "step.abort-requested";
  readonly stepId: StepId;
  readonly attempt: AttemptNumber;
}

export type StepFact =
  | StepStartedFact
  | StepCompletedFact
  | StepFailedFact
  | StepCanceledFact
  | StepRetriedFact
  | StepSkippedFact
  | StepResetFact
  | StepAbortRequestedFact;

export const stepFacts = {
  stepStarted(
    runId: RunId,
    stepId: StepId,
    attempt: AttemptNumber,
    stepKind: StepKind,
    at: Date,
  ): StepStartedFact {
    return { kind: "step.started", runId, stepId, attempt, stepKind, at };
  },

  stepCompleted(
    runId: RunId,
    stepId: StepId,
    attempt: AttemptNumber,
    output: Json,
    at: Date,
  ): StepCompletedFact {
    return { kind: "step.completed", runId, stepId, attempt, output, at };
  },

  stepFailed(
    runId: RunId,
    stepId: StepId,
    attempt: AttemptNumber,
    error: SerializedError,
    at: Date,
  ): StepFailedFact {
    return { kind: "step.failed", runId, stepId, attempt, error, at };
  },

  stepCanceled(
    runId: RunId,
    stepId: StepId,
    attempt: AttemptNumber,
    at: Date,
    error?: SerializedError,
  ): StepCanceledFact {
    return {
      kind: "step.canceled",
      runId,
      stepId,
      attempt,
      at,
      ...compact({ error }),
    };
  },

  stepRetried(
    runId: RunId,
    stepId: StepId,
    attempt: AttemptNumber,
    nextAttemptAt: Date,
    error: SerializedError,
    at: Date,
  ): StepRetriedFact {
    return {
      kind: "step.retried",
      runId,
      stepId,
      attempt,
      nextAttemptAt,
      error,
      at,
    };
  },

  stepSkipped(a: {
    readonly runId: RunId;
    readonly stepId: StepId;
    readonly at: Date;
    readonly reason: SkipReason;
  }): StepSkippedFact {
    return {
      kind: "step.skipped",
      runId: a.runId,
      stepId: a.stepId,
      reason: a.reason,
      at: a.at,
    };
  },

  stepReset(a: {
    readonly runId: RunId;
    readonly stepId: StepId;
    readonly at: Date;
    readonly cascadedFrom?: StepId;
    readonly scope?: ResetScope;
  }): StepResetFact {
    return {
      kind: "step.reset",
      runId: a.runId,
      stepId: a.stepId,
      at: a.at,
      ...compact({
        cascadedFrom: a.cascadedFrom,
        scope: a.scope === "cascade" ? undefined : a.scope,
      }),
    };
  },

  stepAbortRequested(a: {
    readonly runId: RunId;
    readonly stepId: StepId;
    readonly attempt: AttemptNumber;
    readonly at: Date;
  }): StepAbortRequestedFact {
    return {
      kind: "step.abort-requested",
      runId: a.runId,
      stepId: a.stepId,
      attempt: a.attempt,
      at: a.at,
    };
  },
} as const;

function startTarget(stepKind: StepKind, attempt: AttemptNumber): StepState {
  switch (stepKind) {
    case "signal":
      return { tag: "awaitingSignal", attempt };
    case "subflow":
      return { tag: "awaitingChild", attempt };
    case "task":
    case "activity":
    case "streaming":
      return { tag: "running", attempt };
  }
}

function releaseStep(stepId: StepId, timer: boolean): Release {
  return { tag: "release-step", stepId, timer };
}

// Attempts only move forward: a start supersedes the step only for a newer
// attempt (a lease reap re-dispatches at attempt+1 while the dead attempt still
// reads as running), and a retry or abort applies only to the attempt in
// flight. Anything else is a stale or duplicate delivery. nextStepRow applies
// the same rules to the read model.
function inFlight(prev: StepState, attempt: AttemptNumber): boolean {
  return (
    (prev.tag === "running" ||
      prev.tag === "awaitingSignal" ||
      prev.tag === "awaitingChild" ||
      prev.tag === "aborting") &&
    prev.attempt === attempt
  );
}

// A settle needs its attempt to have started in this reset generation. A
// higher attempt than the one started, or any attempt on a pending step, can
// only come from an execution that began before a step.reset. Lower attempts
// still settle: a reaped attempt that outlived its lease wins if it finishes
// first.
function settles(prev: StepState, attempt: AttemptNumber): boolean {
  return !isStepTerminal(prev) && attempt <= attemptOf(prev);
}

// Every transition is total: a pair that isn't a real transition keeps the
// prior state, so the fold never throws on a contradictory log.
export const stepKinds = {
  "step.started": {
    fold: (draft, fact) =>
      foldStep(draft, fact.stepId, (prev) =>
        !isStepTerminal(prev) && fact.attempt > attemptOf(prev)
          ? startTarget(fact.stepKind, fact.attempt)
          : prev,
      ),
    rows: (fact) => ({
      row: "step",
      status: "running",
      stepId: fact.stepId,
      attempt: fact.attempt,
      startedAt: fact.at,
    }),
    release: null,
    stream: null,
    event: (fact) => ({
      type: "step.started",
      stepId: fact.stepId,
      attempt: fact.attempt,
    }),
  },
  "step.completed": {
    fold: (draft, fact) =>
      foldStep(draft, fact.stepId, (prev) =>
        settles(prev, fact.attempt)
          ? { tag: "completed", attempt: fact.attempt, output: fact.output }
          : prev,
      ),
    rows: (fact) => ({
      row: "step",
      status: "completed",
      stepId: fact.stepId,
      attempt: fact.attempt,
      output: fact.output,
      completedAt: fact.at,
    }),
    release: (fact) => releaseStep(fact.stepId, true),
    stream: (fact) => ({ tag: "close-ok", stepId: fact.stepId }),
    event: (fact) => ({
      type: "step.completed",
      stepId: fact.stepId,
      attempt: fact.attempt,
    }),
  },
  "step.failed": {
    fold: (draft, fact) =>
      foldStep(draft, fact.stepId, (prev) =>
        settles(prev, fact.attempt)
          ? { tag: "failed", attempt: fact.attempt, error: fact.error }
          : prev,
      ),
    rows: (fact) => ({
      row: "step",
      status: "failed",
      stepId: fact.stepId,
      attempt: fact.attempt,
      error: fact.error,
      completedAt: fact.at,
    }),
    release: (fact) => releaseStep(fact.stepId, false),
    // Only written on terminal failure (retries use step.retried).
    stream: (fact) => ({ tag: "close-error", stepId: fact.stepId }),
    event: (fact) => ({
      type: "step.failed",
      stepId: fact.stepId,
      attempt: fact.attempt,
    }),
  },
  "step.canceled": {
    fold: (draft, fact) =>
      foldStep(draft, fact.stepId, (prev) => {
        if (!settles(prev, fact.attempt)) return prev;
        const cause: StepCancelCause =
          prev.tag === "aborting"
            ? { kind: "aborted", ...compact({ error: fact.error }) }
            : { kind: "run-canceled" };
        return { tag: "canceled", cause };
      }),
    rows: (fact) => ({
      row: "step",
      status: "canceled",
      stepId: fact.stepId,
      attempt: fact.attempt,
      error: fact.error ?? null,
      completedAt: fact.at,
    }),
    release: (fact) => releaseStep(fact.stepId, false),
    stream: null,
    event: null,
  },
  "step.retried": {
    fold: (draft, fact) =>
      foldStep(draft, fact.stepId, (prev) =>
        inFlight(prev, fact.attempt)
          ? {
              tag: "backoff",
              failedAttempt: fact.attempt,
              retryAt: fact.nextAttemptAt,
              error: fact.error,
            }
          : prev,
      ),
    rows: (fact) => ({
      row: "step",
      status: "backoff",
      stepId: fact.stepId,
      attempt: fact.attempt,
      error: fact.error,
    }),
    release: null,
    stream: (fact) => ({
      tag: "retry",
      stepId: fact.stepId,
      nextAttempt: (fact.attempt + 1) as AttemptNumber,
    }),
    // Carries the attempt that FAILED; the next one is attempt + 1.
    event: (fact) => ({
      type: "step.retried",
      stepId: fact.stepId,
      attempt: fact.attempt,
    }),
  },
  "step.skipped": {
    fold: (draft, fact) =>
      foldStep(draft, fact.stepId, (prev) =>
        isStepTerminal(prev) ? prev : { tag: "skipped", reason: fact.reason },
      ),
    rows: (fact) => ({
      row: "step",
      status: "skipped",
      stepId: fact.stepId,
      completedAt: fact.at,
    }),
    release: null,
    stream: null,
    event: (fact) => ({
      type: "step.skipped",
      stepId: fact.stepId,
      reason: fact.reason,
    }),
  },
  "step.reset": {
    fold: (draft, fact) => {
      draft.steps[fact.stepId] = PENDING;
      draft.resetCounts[fact.stepId] =
        (draft.resetCounts[fact.stepId] ?? 0) + 1;
      // A reset step must wait for a fresh signal, never replay the old one.
      delete draft.bufferedSignals[fact.stepId];
      // A run with a pending step is not settled. Reset reopens completed/failed;
      // canceled stays canceled — replay rejects those.
      if (draft.phase.tag === "completed" || draft.phase.tag === "failed") {
        draft.phase = { tag: "running" };
      }
    },
    rows: (fact) => ({ row: "step", status: "reset", stepId: fact.stepId }),
    release: (fact) => releaseStep(fact.stepId, true),
    stream: null,
    event: null,
  },
  "step.abort-requested": {
    fold: (draft, fact) =>
      foldStep(draft, fact.stepId, (prev) =>
        inFlight(prev, fact.attempt) && prev.tag !== "aborting"
          ? { tag: "aborting", attempt: fact.attempt }
          : prev,
      ),
    rows: null,
    release: null,
    stream: null,
    event: null,
  },
} satisfies KindTable<StepFact>;
