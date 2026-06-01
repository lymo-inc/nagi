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

// A signal step's timeoutMs elapsed before any of its signals arrived. The
// timer-sweep settles the awaiting step as failed with this error, which the
// scheduler then propagates to flow.failed. `name` is the canonical
// discriminator a consumer can pattern-match in the step/flow error (the fold
// only persists name + message), but step identity is the cheaper signal: a
// signal step otherwise only ever ends `canceled`, never `failed`.
export class NagiSignalTimeoutError extends Error {
  readonly runId: RunId;
  readonly stepId: string;
  readonly fireAt: Date;
  constructor({
    runId,
    stepId,
    fireAt,
  }: {
    readonly runId: RunId;
    readonly stepId: string;
    readonly fireAt: Date;
  }) {
    super(
      `Signal step "${stepId}" (run ${runId}) timed out — no signal arrived before its deadline ${fireAt.toISOString()}.`,
    );
    this.name = "NagiSignalTimeoutError";
    this.runId = runId;
    this.stepId = stepId;
    this.fireAt = fireAt;
  }
}

// A run was started against a flowHash that is no longer in this process's
// registry — typically a forward-incompatible deploy retired the old hash. We
// raise loudly at dispatch (not silently against the new code) so an admin can
// pin a frozen-version worker on the dead runs. `currentHash` is null when the
// run's flowId itself isn't registered (the consumer dropped the flow); the
// distinction matters for the operator's recovery decision.
export class NagiFlowSnapshotGoneError extends Error {
  readonly runId: RunId;
  readonly flowId: string;
  readonly pinnedHash: string;
  readonly currentHash: string | null;
  constructor({
    runId,
    flowId,
    pinnedHash,
    currentHash,
  }: {
    readonly runId: RunId;
    readonly flowId: string;
    readonly pinnedHash: string;
    readonly currentHash: string | null;
  }) {
    super(
      currentHash === null
        ? `Run ${runId} is pinned to flow "${flowId}" hash ${pinnedHash.slice(0, 12)}…, ` +
            `but flow "${flowId}" is not registered with the current nagi(). ` +
            `Pin a worker on the prior code version or migrate the run.`
        : `Run ${runId} is pinned to flow "${flowId}" hash ${pinnedHash.slice(0, 12)}…, ` +
            `but the current registry has hash ${currentHash.slice(0, 12)}…. ` +
            `Pin a worker on the prior code version or migrate the run.`,
    );
    this.name = "NagiFlowSnapshotGoneError";
    this.runId = runId;
    this.flowId = flowId;
    this.pinnedHash = pinnedHash;
    this.currentHash = currentHash;
  }
}

// Thrown by Store.tryStartRunOnTx when a unique-violation on (flow_id,
// concurrency_key) recurs across two consecutive attempts under the caller's
// tx. The first violation is racy and absorbed by re-reading the prior set;
// a second one means a concurrent committer landed mid-retry, and we surface
// it to the caller (rather than blocking on an advisory lock that could
// deadlock with their own).
export class NagiConcurrencyConflictError extends Error {
  readonly runId: RunId;
  readonly flowId: string;
  readonly concurrencyKey: string;
  constructor({
    runId,
    flowId,
    concurrencyKey,
  }: {
    readonly runId: RunId;
    readonly flowId: string;
    readonly concurrencyKey: string;
  }) {
    super(
      `Run ${runId}: concurrent start lost the race on flow "${flowId}" concurrency key "${concurrencyKey}" — retry the caller transaction.`,
    );
    this.name = "NagiConcurrencyConflictError";
    this.runId = runId;
    this.flowId = flowId;
    this.concurrencyKey = concurrencyKey;
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
