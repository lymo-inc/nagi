import { Facts } from "./facts";
import { makeIdempotencyKey, makeOnce } from "./idempotency";
import type { EmitLog } from "./internal";
import { isAbortRequested, isTerminalRun, stepStateOf } from "./state";
import type {
  ActivityCtx,
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

export const DEFAULT_RETRY: RetryPolicy = {
  maxAttempts: 3,
  backoff: "exponential",
  initialDelayMs: 1_000,
  maxDelayMs: 60_000,
};

export const CANCEL_POLL_INTERVAL_MS = 250;

export type StepOutcome =
  | {
      readonly tag: "canceled";
      readonly advanceAfter: boolean;
      readonly includeError: boolean;
    }
  | { readonly tag: "retry"; readonly delayMs: Millis }
  | { readonly tag: "failed" };

class NagiAbortError extends Error {
  readonly runId: RunId;
  readonly scope: "run" | "step";
  constructor(runId: RunId, scope: "run" | "step") {
    super(
      scope === "run"
        ? `Run ${runId} was canceled — ctx.signal aborted.`
        : `Step in run ${runId} was aborted by operator.retry() — ctx.signal aborted.`,
    );
    this.name = "NagiAbortError";
    this.runId = runId;
    this.scope = scope;
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

// Keeps an in-flight step's queue message invisible while its handler runs. A
// long handler (e.g. a multi-minute LLM call) otherwise outlives the queue's
// visibility timeout, so the broker redelivers the message and the step body
// runs a second time concurrently — duplicating side effects. Every intervalMs
// we push the lease out by leaseMs; the caller stops the heartbeat once the
// step settles, just before ack. A crashed worker simply stops extending, so
// the message redelivers after at most one leaseMs, preserving crash recovery.
// intervalMs must be shorter than the queue's initial visibility timeout, or
// the first redelivery happens before the first extension lands.
export function startHeartbeat(args: {
  readonly queue: Queue;
  readonly receipt: string;
  readonly intervalMs: Millis;
  readonly leaseMs: Millis;
  readonly emitLog: EmitLog;
}): { readonly stop: () => void } {
  const { queue, receipt, intervalMs, leaseMs, emitLog } = args;
  let stopped = false;
  let timer: ReturnType<typeof setTimeout> | undefined;

  const schedule = (): void => {
    timer = setTimeout(() => {
      void (async () => {
        try {
          await queue.extend(receipt, leaseMs);
        } catch (err) {
          // A missed extension only risks an early redelivery, which admit()'s
          // claimStep dedupes — never tear the loop down, the next tick may win.
          emitLog({
            level: "warn",
            msg: "heartbeat: failed to extend message visibility",
            attrs: { receipt, error: String(err) },
          });
        }
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
  if (attempt < policy.maxAttempts && retryAllows(policy, err)) {
    return { tag: "retry", delayMs: computeBackoff(policy, attempt) };
  }
  return { tag: "failed" };
}

export function computeBackoff(policy: RetryPolicy, attempt: number): Millis {
  const initial = policy.initialDelayMs ?? 1_000;
  const max = policy.maxDelayMs ?? 60_000;
  switch (policy.backoff) {
    case "exponential":
      return Math.min(initial * 2 ** Math.max(0, attempt - 1), max);
    case "linear":
      return Math.min(initial * Math.max(1, attempt), max);
    case "fixed":
      return Math.min(initial, max);
  }
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
