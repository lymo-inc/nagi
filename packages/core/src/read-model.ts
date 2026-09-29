import type { RowDelta } from "./facts";
import type {
  AttemptNumber,
  Json,
  ParentLink,
  RunId,
  RunStatus,
  SerializedError,
  StepStatus,
} from "./types";

// The materialized read model behind describe / pruneFacts: one row per run
// and one per (run, step). The functions below are the reference
// interpretation of RowDelta; a SQL adapter mirrors them statement for
// statement, so two stores can only disagree by not applying a delta.

export interface RunRow {
  readonly flowId: string;
  readonly status: RunStatus;
  readonly input: Json;
  readonly output: Json | null;
  readonly error: SerializedError | null;
  readonly startedAt: Date;
  readonly completedAt: Date | null;
  readonly flowHash: string | null;
  readonly codeVersion: string | null;
  readonly parent: ParentLink | null;
  readonly canceledByRunId: RunId | null;
}

export type StepRowStatus = Exclude<StepStatus, "pending">;

// The step's LATEST attempt, updated in place: a retry supersedes the row
// rather than adding one. Attempt 0 = settled without ever starting (skipped).
export interface StepRow {
  readonly attempt: AttemptNumber;
  readonly status: StepRowStatus;
  readonly startedAt: Date | null;
  readonly completedAt: Date | null;
  readonly output: Json | null;
  readonly error: SerializedError | null;
}

const SETTLED: ReadonlySet<StepRowStatus> = new Set([
  "completed",
  "failed",
  "canceled",
  "skipped",
]);

// `undefined` = no row. A step `reset` reopens a completed/failed run, the same
// transition the fold makes; a canceled run stays canceled.
export function nextRunRow(
  prev: RunRow | undefined,
  delta: RowDelta,
): RunRow | undefined {
  if (delta.row === "step") {
    return delta.status === "reset" &&
      prev !== undefined &&
      (prev.status === "completed" || prev.status === "failed")
      ? {
          ...prev,
          status: "running",
          output: null,
          error: null,
          completedAt: null,
        }
      : prev;
  }
  switch (delta.status) {
    case "running":
      return (
        prev ?? {
          flowId: delta.flowId,
          status: "running",
          input: delta.input,
          output: null,
          error: null,
          startedAt: delta.startedAt,
          completedAt: null,
          flowHash: delta.flowHash,
          codeVersion: delta.codeVersion,
          parent: delta.parent,
          canceledByRunId: null,
        }
      );
    case "completed":
      return (
        prev && {
          ...prev,
          status: "completed",
          output: delta.output,
          error: null,
          completedAt: delta.completedAt,
        }
      );
    case "failed":
      return (
        prev && {
          ...prev,
          status: "failed",
          output: null,
          error: delta.error,
          completedAt: delta.completedAt,
        }
      );
    case "canceled":
      return (
        prev && {
          ...prev,
          status: "canceled",
          output: null,
          error: null,
          canceledByRunId: delta.canceledByRunId,
          completedAt: delta.completedAt,
        }
      );
  }
}

// Mirrors the fold: a settled row changes only by reset, a start applies only
// for a newer attempt, a retry only to the attempt in flight (error null: not
// already backing off), and a settle only for an attempt the row has started.
export function nextStepRow(
  prev: StepRow | undefined,
  delta: Extract<RowDelta, { row: "step" }>,
): StepRow | undefined {
  if (delta.status === "reset") return undefined;
  if (prev !== undefined && SETTLED.has(prev.status)) return prev;
  switch (delta.status) {
    case "running":
      if (prev !== undefined && prev.attempt >= delta.attempt) return prev;
      return {
        attempt: delta.attempt,
        status: "running",
        startedAt: delta.startedAt,
        completedAt: null,
        output: null,
        error: null,
      };
    case "backoff":
      if (
        prev === undefined ||
        prev.error !== null ||
        prev.attempt !== delta.attempt
      ) {
        return prev;
      }
      return { ...prev, error: delta.error };
    case "completed":
    case "failed":
    case "canceled":
      if (prev === undefined || prev.attempt < delta.attempt) return prev;
      return settle(
        prev,
        delta.attempt,
        delta,
        delta.status === "completed" ? delta.output : null,
        delta.status === "completed" ? null : delta.error,
      );
    case "skipped":
      return settle(
        prev,
        prev?.attempt ?? (0 as AttemptNumber),
        delta,
        null,
        null,
      );
  }
}

function settle(
  prev: StepRow | undefined,
  attempt: AttemptNumber,
  delta: { readonly status: StepRowStatus; readonly completedAt: Date },
  output: Json | null,
  error: SerializedError | null,
): StepRow {
  return {
    attempt,
    status: delta.status,
    startedAt: prev?.startedAt ?? null,
    completedAt: delta.completedAt,
    output,
    error,
  };
}
