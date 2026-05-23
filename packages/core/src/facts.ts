import { compact } from "./internal";
import type {
  AttemptNumber,
  CancelArgs,
  FlowCanceledByConcurrencyFact,
  FlowCanceledFact,
  FlowCompletedFact,
  FlowFailedFact,
  FlowRefUpdatedFact,
  FlowStartedFact,
  Json,
  MatchArmSelectedFact,
  ParentLink,
  RunId,
  SerializedError,
  SignalBufferedFact,
  SignalReceivedFact,
  StepAbortRequestedFact,
  StepCanceledFact,
  StepCompletedFact,
  StepFailedFact,
  StepId,
  StepKind,
  StepResetFact,
  StepRetriedFact,
  StepSkippedFact,
  StepStartedFact,
} from "./types";

// Sole constructor of every Fact. Keeping construction here (and interpretation
// in projection/foldRun) means the fact schema is owned by two files, not eight.
export const Facts = {
  flowStarted(a: {
    readonly runId: RunId;
    readonly flowId: string;
    readonly input: Json;
    readonly at: Date;
    readonly codeVersion?: string;
    readonly flowHash?: string;
    readonly parent?: ParentLink;
  }): FlowStartedFact {
    return {
      kind: "flow.started",
      runId: a.runId,
      flowId: a.flowId,
      input: a.input,
      at: a.at,
      ...compact({
        codeVersion: a.codeVersion,
        flowHash: a.flowHash,
        parent: a.parent,
      }),
    };
  },

  flowCompleted(runId: RunId, output: Json, at: Date): FlowCompletedFact {
    return { kind: "flow.completed", runId, output, at };
  },

  flowFailed(runId: RunId, error: SerializedError, at: Date): FlowFailedFact {
    return { kind: "flow.failed", runId, error, at };
  },

  flowCanceled(runId: RunId, args: CancelArgs, at: Date): FlowCanceledFact {
    return { ...args, kind: "flow.canceled", runId, at };
  },

  flowCanceledByConcurrency(a: {
    readonly runId: RunId;
    readonly canceledByRunId: RunId;
    readonly concurrencyKey: string;
    readonly at: Date;
  }): FlowCanceledByConcurrencyFact {
    return {
      kind: "flow.canceled",
      cause: "concurrency",
      runId: a.runId,
      at: a.at,
      canceledByRunId: a.canceledByRunId,
      concurrencyKey: a.concurrencyKey,
    };
  },

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
    readonly reason: "when-false" | "transitive" | "manual";
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

  signalReceived(a: {
    readonly runId: RunId;
    readonly stepId: StepId;
    readonly payload: Json;
    readonly at: Date;
    readonly signalName?: string;
  }): SignalReceivedFact {
    return {
      kind: "signal.received",
      runId: a.runId,
      stepId: a.stepId,
      payload: a.payload,
      at: a.at,
      ...compact({ signalName: a.signalName }),
    };
  },

  signalBuffered(a: {
    readonly runId: RunId;
    readonly stepId: StepId;
    readonly payload: Json;
    readonly at: Date;
    readonly signalName?: string;
  }): SignalBufferedFact {
    return {
      kind: "signal.buffered",
      runId: a.runId,
      stepId: a.stepId,
      payload: a.payload,
      at: a.at,
      ...compact({ signalName: a.signalName }),
    };
  },

  flowRefUpdated(a: {
    readonly flowId: string;
    readonly from: string | null;
    readonly to: string;
    readonly at: Date;
  }): FlowRefUpdatedFact {
    return {
      kind: "flow_ref.updated",
      flowId: a.flowId,
      from: a.from,
      to: a.to,
      at: a.at,
    };
  },
} as const;
