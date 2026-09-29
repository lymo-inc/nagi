import { PENDING, type RunPhase, type StepState } from "../state";
import type {
  AttemptNumber,
  Json,
  ParentLink,
  RunEvent,
  RunId,
  SerializedError,
  StepId,
} from "../types";

export interface FactBase {
  readonly runId: RunId;
  readonly at: Date;
}

export interface BufferedSignal {
  readonly payload: Json;
  readonly signalName?: string;
}

export interface RunDraft {
  flowId: string;
  input: Json;
  phase: RunPhase;
  parent: ParentLink | undefined;
  flowHash: string | undefined;
  codeVersion: string | undefined;
  readonly steps: Record<StepId, StepState>;
  readonly bufferedSignals: Record<StepId, BufferedSignal>;
  readonly resetCounts: Record<StepId, number>;
}

export function foldStep(
  draft: RunDraft,
  stepId: StepId,
  next: (prev: StepState) => StepState,
): void {
  draft.steps[stepId] = next(draft.steps[stepId] ?? PENDING);
}

// The change a fact makes to the materialized read model (RunView / StepView
// rows). Declared per kind next to the fold so a store adapter interprets this
// small closed vocabulary instead of re-deriving fact semantics; nextRunRow /
// nextStepRow (read-model.ts) are the reference interpretation. Every timestamp
// is the fact's `at`, never the store's clock.
export type RowDelta =
  | {
      readonly row: "run";
      readonly status: "running";
      readonly flowId: string;
      readonly input: Json;
      readonly startedAt: Date;
      readonly flowHash: string | null;
      readonly codeVersion: string | null;
      readonly parent: ParentLink | null;
    }
  | {
      readonly row: "run";
      readonly status: "completed";
      readonly output: Json;
      readonly completedAt: Date;
    }
  | {
      readonly row: "run";
      readonly status: "failed";
      readonly error: SerializedError;
      readonly completedAt: Date;
    }
  | {
      readonly row: "run";
      readonly status: "canceled";
      readonly canceledByRunId: RunId | null;
      readonly completedAt: Date;
    }
  | {
      readonly row: "step";
      readonly status: "running";
      readonly stepId: StepId;
      readonly attempt: AttemptNumber;
      readonly startedAt: Date;
    }
  | {
      readonly row: "step";
      readonly status: "backoff";
      readonly stepId: StepId;
      readonly attempt: AttemptNumber;
      readonly error: SerializedError;
    }
  | {
      readonly row: "step";
      readonly status: "completed";
      readonly stepId: StepId;
      readonly attempt: AttemptNumber;
      readonly output: Json;
      readonly completedAt: Date;
    }
  | {
      readonly row: "step";
      readonly status: "failed";
      readonly stepId: StepId;
      readonly attempt: AttemptNumber;
      readonly error: SerializedError;
      readonly completedAt: Date;
    }
  | {
      readonly row: "step";
      readonly status: "canceled";
      readonly stepId: StepId;
      readonly attempt: AttemptNumber;
      readonly error: SerializedError | null;
      readonly completedAt: Date;
    }
  | {
      readonly row: "step";
      readonly status: "skipped";
      readonly stepId: StepId;
      readonly completedAt: Date;
    }
  | { readonly row: "step"; readonly status: "reset"; readonly stepId: StepId };

// What a fact releases besides being appended. `timer` is the signal-timeout
// deadline: released when the step resolves by delivery or is restarted. A
// deadline that fires consumes its own row (sweepSignalTimeouts), and the other
// terminal paths leave a stale timer to fire as a no-op rather than contend
// with the sweeper's lock order.
export type Release =
  | {
      readonly tag: "release-step";
      readonly stepId: StepId;
      readonly timer: boolean;
    }
  | {
      readonly tag: "release-lease";
      readonly stepId: StepId;
      readonly attempt: AttemptNumber;
    }
  | { readonly tag: "release-run" };

// Fires for every step, streaming or not: a close on a step that never opened
// a channel is a no-op, and closed-ness is authoritative in the facts.
export type StreamEffect =
  | { readonly tag: "close-ok"; readonly stepId: StepId }
  | { readonly tag: "close-error"; readonly stepId: StepId }
  | {
      readonly tag: "retry";
      readonly stepId: StepId;
      readonly nextAttempt: AttemptNumber;
    }
  | { readonly tag: "reopen"; readonly stepId: StepId }
  | { readonly tag: "close-run" };

type Consequence<F, T> = ((fact: F) => T) | null;

// One entry per kind: how it folds into the projection, and everything else
// writing it implies (`null` = none). A kind missing any field fails to compile
// at the `satisfies` site, so a new kind is exactly one table entry.
export type KindTable<F extends { readonly kind: string }> = {
  readonly [K in F["kind"]]: {
    readonly fold: (draft: RunDraft, fact: Extract<F, { kind: K }>) => void;
    readonly rows: Consequence<Extract<F, { kind: K }>, RowDelta>;
    readonly release: Consequence<Extract<F, { kind: K }>, Release>;
    readonly stream: Consequence<Extract<F, { kind: K }>, StreamEffect>;
    readonly event: Consequence<Extract<F, { kind: K }>, RunEvent>;
  };
};
