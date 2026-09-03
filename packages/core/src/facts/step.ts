import { compact } from "../internal";
import {
  isStepTerminal,
  PENDING,
  type SkipReason,
  type StepCancelCause,
  type StepState,
} from "../state";
import type {
  AttemptNumber,
  Json,
  RunId,
  SerializedError,
  StepId,
  StepKind,
} from "../types";
import type { FactBase, KindTable } from "./base";
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
  readonly actor?: string;
  readonly note?: string;
}

export interface StepResetFact extends FactBase {
  readonly kind: "step.reset";
  readonly stepId: StepId;
  readonly cascadedFrom?: StepId;
  readonly actor?: string;
  readonly note?: string;
}

export interface StepAbortRequestedFact extends FactBase {
  readonly kind: "step.abort-requested";
  readonly stepId: StepId;
  readonly attempt: AttemptNumber;
  readonly actor: string;
  readonly note?: string;
}

export interface MatchArmSelectedFact extends FactBase {
  readonly kind: "match.arm-selected";
  readonly stepId: StepId;
  readonly arm: string;
}

export interface OnceRecordedFact extends FactBase {
  readonly kind: "once.recorded";
  readonly stepId: StepId;
  readonly scope: string;
  readonly value: Json;
}

export type StepFact =
  | StepStartedFact
  | StepCompletedFact
  | StepFailedFact
  | StepCanceledFact
  | StepRetriedFact
  | StepSkippedFact
  | StepResetFact
  | StepAbortRequestedFact
  | MatchArmSelectedFact
  | OnceRecordedFact;

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
    readonly actor?: string;
    readonly note?: string;
  }): StepSkippedFact {
    return {
      kind: "step.skipped",
      runId: a.runId,
      stepId: a.stepId,
      reason: a.reason,
      at: a.at,
      ...compact({ actor: a.actor, note: a.note }),
    };
  },

  stepReset(a: {
    readonly runId: RunId;
    readonly stepId: StepId;
    readonly at: Date;
    readonly cascadedFrom?: StepId;
    readonly actor?: string;
    readonly note?: string;
  }): StepResetFact {
    return {
      kind: "step.reset",
      runId: a.runId,
      stepId: a.stepId,
      at: a.at,
      ...compact({
        cascadedFrom: a.cascadedFrom,
        actor: a.actor,
        note: a.note,
      }),
    };
  },

  stepAbortRequested(a: {
    readonly runId: RunId;
    readonly stepId: StepId;
    readonly attempt: AttemptNumber;
    readonly at: Date;
    readonly actor: string;
    readonly note?: string;
  }): StepAbortRequestedFact {
    return {
      kind: "step.abort-requested",
      runId: a.runId,
      stepId: a.stepId,
      attempt: a.attempt,
      actor: a.actor,
      at: a.at,
      ...compact({ note: a.note }),
    };
  },

  matchArmSelected(
    runId: RunId,
    stepId: StepId,
    arm: string,
    at: Date,
  ): MatchArmSelectedFact {
    return { kind: "match.arm-selected", runId, stepId, arm, at };
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
    case "match":
      return { tag: "running", attempt };
  }
}

// Every transition is total: a pair that isn't a real transition keeps the
// prior state, so the fold never throws on a contradictory log. Terminal facts
// carry authoritative outcomes and settle a step from any non-terminal state.
export const stepKinds = {
  "step.started": {
    fold: (draft, fact) =>
      foldStep(draft, fact.stepId, (prev) =>
        prev.tag === "pending" || prev.tag === "backoff"
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
  },
  "step.completed": {
    fold: (draft, fact) =>
      foldStep(draft, fact.stepId, (prev) =>
        isStepTerminal(prev)
          ? prev
          : { tag: "completed", attempt: fact.attempt, output: fact.output },
      ),
    rows: (fact) => ({
      row: "step",
      status: "completed",
      stepId: fact.stepId,
      attempt: fact.attempt,
      output: fact.output,
    }),
  },
  "step.failed": {
    fold: (draft, fact) =>
      foldStep(draft, fact.stepId, (prev) =>
        isStepTerminal(prev)
          ? prev
          : { tag: "failed", attempt: fact.attempt, error: fact.error },
      ),
    rows: (fact) => ({
      row: "step",
      status: "failed",
      stepId: fact.stepId,
      attempt: fact.attempt,
      error: fact.error,
    }),
  },
  "step.canceled": {
    fold: (draft, fact) =>
      foldStep(draft, fact.stepId, (prev) => {
        if (isStepTerminal(prev)) return prev;
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
    }),
  },
  "step.retried": {
    fold: (draft, fact) =>
      foldStep(draft, fact.stepId, (prev) =>
        prev.tag === "running"
          ? {
              tag: "backoff",
              failedAttempt: fact.attempt,
              retryAt: fact.nextAttemptAt,
              error: fact.error,
            }
          : prev,
      ),
    rows: null,
  },
  "step.skipped": {
    // An operator may skip an in-flight step, not just a pending one.
    fold: (draft, fact) =>
      foldStep(draft, fact.stepId, (prev) =>
        isStepTerminal(prev) ? prev : { tag: "skipped", reason: fact.reason },
      ),
    rows: (fact) => ({ row: "step", status: "skipped", stepId: fact.stepId }),
  },
  "step.reset": {
    fold: (draft, fact) => {
      draft.steps[fact.stepId] = PENDING;
      delete draft.selectedArms[fact.stepId];
    },
    rows: (fact) => ({ row: "step", status: "reset", stepId: fact.stepId }),
  },
  "step.abort-requested": {
    fold: (draft, fact) =>
      foldStep(draft, fact.stepId, (prev) =>
        prev.tag === "running" ||
        prev.tag === "awaitingSignal" ||
        prev.tag === "awaitingChild"
          ? { tag: "aborting", attempt: fact.attempt }
          : prev,
      ),
    rows: null,
  },
  "match.arm-selected": {
    fold: (draft, fact) => {
      draft.selectedArms[fact.stepId] = fact.arm;
    },
    rows: null,
  },
  "once.recorded": {
    fold: () => {},
    rows: (fact) => ({
      row: "once",
      stepId: fact.stepId,
      scope: fact.scope,
      value: fact.value,
    }),
  },
} satisfies KindTable<StepFact>;
