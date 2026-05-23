import { compact } from "./internal";
import type { RunId, SerializedError, StandardSchemaV1 } from "./types";

export class NagiValidationError extends Error {
  readonly issues: ReadonlyArray<StandardSchemaV1.Issue>;
  constructor(issues: ReadonlyArray<StandardSchemaV1.Issue>) {
    super(issues.map((i) => i.message).join("; "));
    this.name = "NagiValidationError";
    this.issues = issues;
  }
}

export function validationError(
  message: string,
  path: ReadonlyArray<PropertyKey>,
): NagiValidationError {
  return new NagiValidationError([{ message, path }]);
}

export class NagiRuntimeError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "NagiRuntimeError";
  }
}

export class NagiCanceledError extends Error {
  readonly runId: RunId;
  readonly canceledByRunId: RunId;
  readonly concurrencyKey: string;
  constructor({
    runId,
    canceledByRunId,
    concurrencyKey,
  }: {
    readonly runId: RunId;
    readonly canceledByRunId: RunId;
    readonly concurrencyKey: string;
  }) {
    super(
      `Run ${runId} was canceled (superseded by run ${canceledByRunId} for concurrency key "${concurrencyKey}").`,
    );

    this.name = "NagiCanceledError";
    this.runId = runId;
    this.canceledByRunId = canceledByRunId;
    this.concurrencyKey = concurrencyKey;
    this.cause = {
      canceledByRunId: canceledByRunId,
      concurrencyKey: concurrencyKey,
    };
  }
}

export class NagiSnapshotDriftError extends Error {
  readonly runId: RunId;
  readonly expected: string;
  readonly actual: string;
  constructor({
    runId,
    expected,
    actual,
  }: {
    readonly runId: RunId;
    readonly expected: string;
    readonly actual: string;
  }) {
    super(
      `Run ${runId} was pinned to flow hash ${expected.slice(0, 12)}… ` +
        `but the live flow's hash is ${actual.slice(0, 12)}…. ` +
        `Pass replayOpts.allowDrift = true to replay against the live code anyway.`,
    );
    this.name = "NagiSnapshotDriftError";
    this.runId = runId;
    this.expected = expected;
    this.actual = actual;
  }
}

export function serializeError(err: unknown): SerializedError {
  if (err instanceof Error) {
    return {
      name: err.name,
      message: err.message,
      ...compact({ stack: err.stack }),
    };
  }
  return { name: "Error", message: String(err) };
}
