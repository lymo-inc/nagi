import { compact } from "../internal";
import type { RunCancelCause } from "../state";
import type { Json, ParentLink, RunId, SerializedError } from "../types";
import type { FactBase, KindTable } from "./base";

export interface FlowStartedFact extends FactBase {
  readonly kind: "flow.started";
  readonly flowId: string;
  readonly input: Json;
  readonly flowHash?: string;
  readonly codeVersion?: string;
  readonly parent?: ParentLink;
}

export interface FlowCompletedFact extends FactBase {
  readonly kind: "flow.completed";
  readonly output: Json;
}

export interface FlowFailedFact extends FactBase {
  readonly kind: "flow.failed";
  readonly error: SerializedError;
}

export interface FlowCanceledByConcurrencyFact extends FactBase {
  readonly kind: "flow.canceled";
  readonly cause: "concurrency";
  readonly canceledByRunId: RunId;
  readonly concurrencyKey: string;
}

export interface FlowCanceledExplicitlyFact extends FactBase {
  readonly kind: "flow.canceled";
  readonly cause: "explicit";
  readonly reason: string;
  readonly note?: string;
}

export interface FlowCanceledByOperatorFact extends FactBase {
  readonly kind: "flow.canceled";
  readonly cause: "operator";
  readonly actor: string;
  readonly reason: string;
  readonly note?: string;
}

export type FlowCanceledFact =
  | FlowCanceledByConcurrencyFact
  | FlowCanceledExplicitlyFact
  | FlowCanceledByOperatorFact;

type DistributiveOmit<T, K extends PropertyKey> = T extends unknown
  ? Omit<T, K>
  : never;

// Derived from the fact types so cancel intent can't drift from the recorded
// fact. Concurrency cancellation is system-internal and intentionally excluded.
export type CancelArgs = DistributiveOmit<
  FlowCanceledExplicitlyFact | FlowCanceledByOperatorFact,
  "kind" | "runId" | "at"
>;

export type RunEndFact = FlowCompletedFact | FlowFailedFact | FlowCanceledFact;

export type FlowFact = FlowStartedFact | RunEndFact;

export function isRunEnd(fact: { readonly kind: string }): fact is RunEndFact {
  return (
    fact.kind === "flow.completed" ||
    fact.kind === "flow.failed" ||
    fact.kind === "flow.canceled"
  );
}

// Lives in the global log, not a run's; it never folds into a RunState.
export interface FlowRefUpdatedFact {
  readonly kind: "flow_ref.updated";
  readonly flowId: string;
  readonly from: string | null;
  readonly to: string;
  readonly at: Date;
}

export type GlobalFact = FlowRefUpdatedFact;

export const flowFacts = {
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

export function runCancelCause(
  fact: DistributiveOmit<FlowCanceledFact, "kind" | "runId" | "at">,
): RunCancelCause {
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
        ...compact({ note: fact.note }),
      };
    case "operator":
      return {
        kind: "operator",
        actor: fact.actor,
        reason: fact.reason,
        ...compact({ note: fact.note }),
      };
  }
}

const RELEASE_RUN = { tag: "release-run" } as const;
const CLOSE_RUN = { tag: "close-run" } as const;

export const flowKinds = {
  "flow.started": {
    fold: (draft, fact) => {
      draft.flowId = fact.flowId;
      draft.input = fact.input;
      draft.phase = { tag: "running" };
      draft.parent = fact.parent;
      draft.flowHash = fact.flowHash;
      draft.codeVersion = fact.codeVersion;
    },
    rows: (fact) => ({
      row: "run",
      status: "running",
      flowId: fact.flowId,
      input: fact.input,
      startedAt: fact.at,
      flowHash: fact.flowHash ?? null,
      codeVersion: fact.codeVersion ?? null,
      parent: fact.parent ?? null,
    }),
    release: null,
    stream: null,
    event: (fact) => ({ type: "flow.started", flowId: fact.flowId }),
  },
  "flow.completed": {
    fold: (draft, fact) => {
      draft.phase = { tag: "completed", output: fact.output };
    },
    rows: (fact) => ({
      row: "run",
      status: "completed",
      output: fact.output,
      completedAt: fact.at,
    }),
    release: () => RELEASE_RUN,
    // Closes every still-open channel of the run, so a subscriber to a skipped
    // or never-emitting step never hangs.
    stream: () => CLOSE_RUN,
    event: (fact) => ({ type: "flow.completed", output: fact.output }),
  },
  "flow.failed": {
    fold: (draft, fact) => {
      draft.phase = { tag: "failed", error: fact.error };
    },
    rows: (fact) => ({
      row: "run",
      status: "failed",
      error: fact.error,
      completedAt: fact.at,
    }),
    release: () => RELEASE_RUN,
    stream: () => CLOSE_RUN,
    event: (fact) => ({ type: "flow.failed", error: fact.error }),
  },
  "flow.canceled": {
    fold: (draft, fact) => {
      draft.phase = { tag: "canceled", cause: runCancelCause(fact) };
    },
    rows: (fact) => ({
      row: "run",
      status: "canceled",
      canceledByRunId:
        fact.cause === "concurrency" ? fact.canceledByRunId : null,
      completedAt: fact.at,
    }),
    release: () => RELEASE_RUN,
    stream: () => CLOSE_RUN,
    event: (fact) =>
      fact.cause === "concurrency"
        ? {
            type: "flow.canceled",
            cause: "concurrency",
            canceledByRunId: fact.canceledByRunId,
          }
        : { type: "flow.canceled", cause: fact.cause },
  },
} satisfies KindTable<FlowFact>;
