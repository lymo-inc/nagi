import type {
  AttemptNumber,
  Json,
  RunId,
  RunStatus,
  StepId,
  StepStatus,
} from "./types";

// Step-level run status — exposes the durable lifecycle states an inspector
// cares about. `pending` is reserved on the materialized column but never
// projected here; surfaced statuses match RunSummary's RunStatus shape.
export type StepRunStatus = StepStatus;

export interface RunView {
  readonly runId: RunId;
  readonly flowId: string;
  readonly flowHash: string;
  readonly status: RunStatus;
  readonly startedAt: Date;
  readonly completedAt?: Date;
  readonly input: Json;
  readonly output?: Json;
  readonly error?: Json;
  readonly canceledByRunId?: RunId;
  readonly concurrencyKey?: string;
  readonly parent?: { readonly runId: RunId; readonly stepId: StepId };
  readonly children: readonly RunId[];
}

export interface StepView {
  readonly stepId: StepId;
  readonly attempt: AttemptNumber;
  readonly status: StepRunStatus;
  readonly startedAt?: Date;
  readonly completedAt?: Date;
  readonly output?: Json;
  readonly error?: Json;
  readonly lease?: { readonly expiresAt: Date };
}

export type RunDescription = {
  readonly run: RunView;
  readonly steps: readonly StepView[];
} | null;
