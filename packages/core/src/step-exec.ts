import { NagiCanceledError, NagiNonRetryableError } from "./errors";
import { Facts } from "./facts";
import { makeIdempotencyKey, makeOnce } from "./idempotency";
import type { EmitLog } from "./internal";
import { stepBackoff } from "./retry";
import { isAbortRequested, isTerminalRun, stepStateOf } from "./state";
import type {
  ActivityCtx,
  AttemptNumber,
  Clock,
  Json,
  Logger,
  LogLevel,
  Millis,
  Queue,
  RetryPolicy,
  RunId,
  RunState,
  StepCanceledFact,
  StepCompletedFact,
  StepCtx,
  Store,
  Tx,
} from "./types";

export const CANCEL_POLL_INTERVAL_MS = 250;

export type StepOutcome =
  | {
      readonly tag: "canceled";
      readonly advanceAfter: boolean;
      readonly includeError: boolean;
    }
  | { readonly tag: "retry"; readonly delayMs: Millis }
  | { readonly tag: "failed" }
  | {
      // NagiCanceledError surfaced from a handler: the run was superseded by
      // another. Reclassify as flow.canceled (cause: concurrency) so the
      // canonical run status matches reality.
      readonly tag: "flowCanceled";
      readonly canceledByRunId: RunId;
      readonly concurrencyKey: string;
    };

// name="AbortError" so WHATWG-allowlisting fetch SDKs treat this as a real abort, not a failure.
export class NagiAbortError extends Error {
  readonly runId: RunId;
  readonly kind: "run" | "step";
  constructor(runId: RunId, kind: "run" | "step") {
    super(
      kind === "run"
        ? `Run ${runId} was canceled — ctx.signal aborted.`
        : `Step in run ${runId} was aborted by operator.retry() — ctx.signal aborted.`,
    );
    this.name = "AbortError";
    this.runId = runId;
    this.kind = kind;
  }
}

// Computed inside the runStep tx so the fact commits atomically with the step's
// writes. An operator abort reports abortedHere so the caller skips advancing —
// the abort re-enqueues the step elsewhere.
export function resolveExecutionFact(args: {
  readonly postState: RunState;
  readonly runId: RunId;
  readonly stepId: string;
  readonly attempt: number;
  readonly output: Json;
  readonly at: Date;
}): {
  readonly fact: StepCompletedFact | StepCanceledFact;
  readonly abortedHere: boolean;
} {
  const { postState, runId, stepId, attempt, output, at } = args;
  if (postState.phase.tag === "canceled") {
    return {
      fact: Facts.stepCanceled(runId, stepId, attempt, at),
      abortedHere: false,
    };
  }
  if (isAbortRequested(stepStateOf(postState, stepId), attempt)) {
    return {
      fact: Facts.stepCanceled(runId, stepId, attempt, at),
      abortedHere: true,
    };
  }
  return {
    fact: Facts.stepCompleted(runId, stepId, attempt, output, at),
    abortedHere: false,
  };
}

export function startCancelWatcher(args: {
  readonly store: Store;
  readonly runId: RunId;
  readonly stepId: string;
  readonly attempt: number;
  readonly ac: AbortController;
  readonly intervalMs: Millis;
}): { readonly stop: () => void } {
  const { store, runId, stepId, attempt, ac, intervalMs } = args;
  let stopped = false;
  void (async () => {
    while (!stopped) {
      await new Promise((r) => setTimeout(r, intervalMs));
      if (stopped) return;
      try {
        const s = await store.loadRunState(runId);
        if (isTerminalRun(s)) {
          if (!ac.signal.aborted) ac.abort(new NagiAbortError(runId, "run"));
          return;
        }
        if (isAbortRequested(stepStateOf(s, stepId), attempt)) {
          if (!ac.signal.aborted) ac.abort(new NagiAbortError(runId, "step"));
          return;
        }
      } catch {
        // Transient store read error: skip this tick and re-check on the next
        // interval rather than tearing down the watcher (which would strand the
        // abort and let a canceled handler run to completion).
      }
    }
  })();
  return {
    stop: () => {
      stopped = true;
    },
  };
}

export const DEFAULT_HEARTBEAT_LEASE_MS: Millis = 120_000;
export const DEFAULT_HEARTBEAT_INTERVAL_MS: Millis = 40_000;
// A step body holding a worker slot this long is either a legitimately slow
// handler or an in-step wait on an external fact (a poll loop) that should be
// a b.signal step — parked signals hold no slot. Warn on every crossing so a
// wedged pool announces itself long before it exhausts worker concurrency.
export const DEFAULT_LEASE_HOLD_WARN_MS: Millis = 300_000;

// Keeps an in-flight step's queue message invisible while its handler runs. A
// long handler (e.g. a multi-minute LLM call) otherwise outlives the queue's
// visibility timeout, so the broker redelivers the message and the step body
// runs a second time concurrently — duplicating side effects. Every intervalMs
// we push the lease out by leaseMs; the caller stops the heartbeat once the
// step settles, just before ack. A crashed worker simply stops extending, so
// the message redelivers after at most one leaseMs, preserving crash recovery.
// intervalMs must be shorter than the queue's initial visibility timeout, or
// the first redelivery happens before the first extension lands.
//
// Also extends the store-side lease row in lock-step with the queue VT: the
// lease reaper (sweepLeases) uses the store lease as ground truth for "is a
// worker still alive on this step", so a heartbeat that only extends the queue
// would race the reaper into double-dispatching live work. Each tick fires
// both extensions in parallel; either failing is logged but never re-thrown so
// a transient store outage doesn't tear down a still-healthy handler.
export function startHeartbeat(args: {
  readonly queue: Queue;
  readonly store: Store;
  readonly runId: RunId;
  readonly stepId: string;
  readonly attempt: number;
  readonly receipt: string;
  readonly intervalMs: Millis;
  readonly leaseMs: Millis;
  // Warn each time a step has held its worker slot for another multiple of
  // this. 0 disables (tests). See DEFAULT_LEASE_HOLD_WARN_MS.
  readonly holdWarnMs: Millis;
  readonly emitLog: EmitLog;
}): { readonly stop: () => void } {
  const {
    queue,
    store,
    runId,
    stepId,
    attempt,
    receipt,
    intervalMs,
    leaseMs,
    holdWarnMs,
    emitLog,
  } = args;
  let stopped = false;
  let timer: ReturnType<typeof setTimeout> | undefined;
  const startedAtMs = Date.now();
  let warnedCrossings = 0;

  const schedule = (): void => {
    timer = setTimeout(() => {
      void (async () => {
        if (holdWarnMs > 0) {
          const heldMs = Date.now() - startedAtMs;
          const crossings = Math.floor(heldMs / holdWarnMs);
          if (crossings > warnedCrossings) {
            warnedCrossings = crossings;
            emitLog({
              level: "warn",
              msg: "heartbeat: step is holding a worker slot unusually long — slow handler, or an in-step wait that should be a b.signal step?",
              attrs: { runId, stepId, attempt, heldMs },
            });
          }
        }
        await Promise.all([
          queue.extend(receipt, leaseMs).catch((err: unknown) => {
            // A missed extension only risks an early redelivery, which admit()'s
            // claimStep dedupes — never tear the loop down, the next tick may win.
            emitLog({
              level: "warn",
              msg: "heartbeat: failed to extend message visibility",
              attrs: { receipt, error: String(err) },
            });
          }),
          store
            .extendLease(runId, stepId, attempt as AttemptNumber, leaseMs)
            .catch((err: unknown) => {
              // Same logic as queue extend: a missed store-lease extension only
              // risks an early sweep, and the sweeper itself re-checks step
              // status before reaping.
              emitLog({
                level: "warn",
                msg: "heartbeat: failed to extend store lease",
                attrs: { runId, stepId, attempt, error: String(err) },
              });
            }),
        ]);
        if (!stopped) schedule();
      })();
    }, intervalMs);
  };
  schedule();

  return {
    // Clear the pending timer so a settled step strands nothing on the event
    // loop — without this every fast step would leak a timer for up to
    // intervalMs, keeping the process alive and delaying clean shutdown.
    stop: () => {
      stopped = true;
      if (timer !== undefined) clearTimeout(timer);
    },
  };
}

// Cancellation/abort wins over retry. An operator-aborted step is re-enqueued
// elsewhere so it must not advance, whereas a run-cancellation must advance so
// the run can finalize; only an abort/cancel error is recorded on the fact.
// A handler that throws NagiCanceledError reclassifies the whole run to
// flow.canceled (cause: concurrency) — the run was superseded; calling it
// "failed" would mis-discriminate the canonical status.
export function classifyFailure(args: {
  readonly attempt: number;
  readonly policy: RetryPolicy;
  readonly err: unknown;
  readonly runIsCanceled: boolean;
  readonly stepAborted: boolean;
}): StepOutcome {
  const { attempt, policy, err, runIsCanceled, stepAborted } = args;
  if (runIsCanceled || stepAborted) {
    const isAbort =
      err instanceof NagiAbortError ||
      (err instanceof Error && err.name === "AbortError");
    return {
      tag: "canceled",
      advanceAfter: !stepAborted,
      includeError: isAbort,
    };
  }
  const cancel = findInCauseChain(err, NagiCanceledError);
  if (cancel !== undefined) {
    return {
      tag: "flowCanceled",
      canceledByRunId: cancel.canceledByRunId,
      concurrencyKey: cancel.concurrencyKey,
    };
  }
  if (findInCauseChain(err, NagiNonRetryableError) !== undefined) {
    return { tag: "failed" };
  }
  if (attempt < policy.maxAttempts && retryAllows(policy, err)) {
    return { tag: "retry", delayMs: stepBackoff(policy)(attempt) };
  }
  return { tag: "failed" };
}

// Walk the error cause chain looking for a real instance of the given class.
// Requiring the class (not just shape) defends against forged JSONB cause data
// re-thrown as plain Errors.
function findInCauseChain<T extends Error>(
  err: unknown,
  ctor: new (...args: never[]) => T,
): T | undefined {
  let cursor: unknown = err;
  const seen = new Set<unknown>();
  while (cursor instanceof Error && !seen.has(cursor)) {
    if (cursor instanceof ctor) return cursor;
    seen.add(cursor);
    cursor = (cursor as { cause?: unknown }).cause;
  }
  return undefined;
}

function retryAllows(policy: RetryPolicy, err: unknown): boolean {
  if (!policy.retryOn) return true;
  return policy.retryOn(err);
}

// Every handler-kind ctx is identical except for `tx` (a real transaction for a
// task, absent for an activity), so the common fields are built once here. The
// concrete builders only attach the tx slot.
function makeBaseCtx(args: {
  runId: RunId;
  stepId: string;
  attempt: number;
  input: unknown;
  store: Store;
  clock: Clock;
  signal: AbortSignal;
  emitLog: EmitLog;
}): ActivityCtx<unknown> {
  const { runId, stepId, attempt, input, store, clock, signal, emitLog } = args;

  // Spread caller attrs first, then the correlation keys, so the runtime wins on
  // collision — a handler can't clobber the real runId/stepId/attempt.
  const log =
    (level: LogLevel) =>
    (msg: string, attrs?: Record<string, unknown>): void =>
      emitLog({ level, msg, attrs: { ...attrs, runId, stepId, attempt } });

  const logger: Logger = {
    debug: log("debug"),
    info: log("info"),
    warn: log("warn"),
    error: log("error"),
  };

  return {
    input,
    runId,
    stepId,
    attempt,
    signal,
    now: () => clock.now(),
    logger,
    once: makeOnce({ runId, stepId, store }),
    idempotencyKey: makeIdempotencyKey(runId, stepId),
  };
}

export function makeStepCtx(args: {
  runId: RunId;
  stepId: string;
  attempt: number;
  input: unknown;
  store: Store;
  clock: Clock;
  tx: Tx;
  signal: AbortSignal;
  emitLog: EmitLog;
}): StepCtx<unknown> {
  const { tx, ...base } = args;
  return { ...makeBaseCtx(base), tx };
}

// Identical to a task's ctx except there is no durable transaction — an activity
// runs OUTSIDE the step tx by design (RFC 0013), so ctx.tx is absent on the
// public ActivityCtx type. The throwing getter is the runtime backstop for any
// internal caller that reaches for it through the looser StepCtx type.
//
// CAUTION: `tx` is a getter that throws, so spreading this ctx (`{ ...ctx }`)
// EAGERLY invokes the getter and throws — even when the spreader never touches
// tx. Handlers and internal callers must read fields off the ctx directly and
// never spread it. (The streaming path builds its ctx from a real task ctx, not
// this one, so it is unaffected.)
export function makeActivityCtx(args: {
  runId: RunId;
  stepId: string;
  attempt: number;
  input: unknown;
  store: Store;
  clock: Clock;
  signal: AbortSignal;
  emitLog: EmitLog;
}): StepCtx<unknown> {
  return {
    ...makeBaseCtx(args),
    get tx(): Tx {
      throw new Error(
        "nagi: activity steps have no ctx.tx — they run outside the durable " +
          "transaction. Write idempotently via your own client, or use a task.",
      );
    },
  };
}
