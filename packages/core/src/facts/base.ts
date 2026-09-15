import { PENDING, type RunPhase, type StepState } from "../state";
import type {
  AttemptNumber,
  Json,
  ParentLink,
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
  readonly selectedArms: Record<StepId, string>;
  readonly bufferedSignals: Record<StepId, BufferedSignal>;
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
// small closed vocabulary instead of re-deriving fact semantics.
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
      readonly status: "completed";
      readonly stepId: StepId;
      readonly attempt: AttemptNumber;
      readonly output: Json;
    }
  | {
      readonly row: "step";
      readonly status: "failed";
      readonly stepId: StepId;
      readonly attempt: AttemptNumber;
      readonly error: SerializedError;
    }
  | {
      readonly row: "step";
      readonly status: "canceled";
      readonly stepId: StepId;
      readonly attempt: AttemptNumber;
      readonly error: SerializedError | null;
    }
  | {
      readonly row: "step";
      readonly status: "skipped";
      readonly stepId: StepId;
    }
  | { readonly row: "step"; readonly status: "reset"; readonly stepId: StepId }
  | {
      readonly row: "once";
      readonly stepId: StepId;
      readonly scope: string;
      readonly value: Json;
    };

// One entry per kind: how it folds into the projection and what it changes in
// the read model (`rows: null` = log-only). A kind missing either fails to
// compile at the `satisfies` site.
export type KindTable<F extends { readonly kind: string }> = {
  readonly [K in F["kind"]]: {
    readonly fold: (draft: RunDraft, fact: Extract<F, { kind: K }>) => void;
    readonly rows: ((fact: Extract<F, { kind: K }>) => RowDelta) | null;
  };
};
