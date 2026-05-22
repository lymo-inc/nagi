import { makeIdempotencyKey, makeOnce } from "./idempotency";
import {
  asStepMapWithDefs,
  compact,
  type EmitLog,
  getDef,
  type HandlerDef,
  handlerDef,
  type MatchDef,
  resolveNeeds,
  type StepDef,
  type StreamingTaskDef,
  type SubflowDef,
  selectArm,
  type TaskDef,
} from "./internal";
import {
  extractInput,
  isFlowTerminal,
  type MatchPromotion,
  nextTransition,
  type SkipDecision,
  stepStateOf,
} from "./scheduler";
import { isTerminalRun, resolvedOf, runStatusOf, stepStatusOf } from "./state";
import type {
  AttemptNumber,
  Clock,
  Fact,
  Flow,
  FlowHooks,
  FlowStartedFact,
  Json,
  Logger,
  Millis,
  ParentRef,
  Queue,
  QueueMessage,
  RetryPolicy,
  RunId,
  RunState,
  SerializedError,
  StepCanceledFact,
  StepCompletedFact,
  StepCtx,
  StepId,
  Store,
  StreamingStepCtx,
  Tx,
} from "./types";

export interface DispatchDeps {
  readonly flowFor: (runId: RunId) => Promise<Flow>;
  readonly lookupFlow: (flowId: string) => Flow | undefined;
  readonly startChildRun: (args: {
    readonly child: Flow;
    readonly childInput: unknown;
    readonly parent: ParentRef;
  }) => Promise<RunId>;
  readonly store: Store;
  readonly queue: Queue;
  readonly clock: Clock;
  readonly hooks?: FlowHooks;
  readonly emitLog: EmitLog;
  readonly defaultRetry?: RetryPolicy;
  readonly fireHooks?: boolean;
  readonly cancelPollIntervalMs?: Millis;
}

// The deep module's tight interface: every operation takes only domain values,
// never `deps` — that is bound once by makeDispatcher and hidden inside.
export interface Dispatcher {
  dispatchMessage(message: QueueMessage): Promise<void>;
  advance(runId: RunId): Promise<void>;
  propagateToParent(
    childRunId: RunId,
    outcome: SubflowChildOutcome,
  ): Promise<void>;
  fireHook<E>(
    hook: ((event: E) => void | Promise<void>) | undefined,
    event: E,
    hookName: string,
  ): Promise<void>;
}

const DEFAULT_RETRY: RetryPolicy = {
  maxAttempts: 3,
  backoff: "exponential",
  initialDelayMs: 1_000,
  maxDelayMs: 60_000,
};

const CANCEL_POLL_INTERVAL_MS = 250;
const MAX_ADVANCE_ITERS = 1024;

type Dispatched =
  | { readonly tag: "completed"; readonly output: Json }
  | { readonly tag: "advance" }
  | { readonly tag: "parked" };

type Admission =
  | { readonly tag: "skip" }
  | { readonly tag: "run"; readonly def: StepDef };

type StepOutcome =
  | {
      readonly tag: "canceled";
      readonly advanceAfter: boolean;
      readonly includeError: boolean;
    }
  | { readonly tag: "retry"; readonly delayMs: Millis }
  | { readonly tag: "failed" };

interface ExecuteTaskResult {
  readonly output: Json;
  readonly skipAdvance: boolean;
}

export type SubflowChildOutcome =
  | { readonly kind: "completed"; readonly output: Json }
  | { readonly kind: "failed"; readonly error: SerializedError }
  | { readonly kind: "canceled"; readonly error: SerializedError };

// Computed inside the runStep tx so the fact commits atomically with the step's
// writes. An operator abort reports abortedHere so the caller skips advancing —
// the abort re-enqueues the step elsewhere.
function resolveExecutionFact(args: {
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
  const canceled: StepCanceledFact = {
    kind: "step.canceled",
    runId,
    stepId,
    attempt,
    at,
  };
  if (postState.phase.tag === "canceled") {
    return { fact: canceled, abortedHere: false };
  }
  if (hasAbortRequest(postState.facts, stepId, attempt)) {
    return { fact: canceled, abortedHere: true };
  }
  const completed: StepCompletedFact = {
    kind: "step.completed",
    runId,
    stepId,
    attempt,
    output,
    at,
  };
  return { fact: completed, abortedHere: false };
}

interface CancelWatcher {
  stop(): void;
}

function startCancelWatcher(args: {
  readonly store: Store;
  readonly runId: RunId;
  readonly stepId: string;
  readonly attempt: number;
  readonly ac: AbortController;
  readonly intervalMs: Millis;
}): CancelWatcher {
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
        if (hasAbortRequest(s.facts, stepId, attempt)) {
          if (!ac.signal.aborted) ac.abort(new NagiAbortError(runId, "step"));
          return;
        }
      } catch {}
    }
  })();
  return {
    stop: () => {
      stopped = true;
    },
  };
}

function hasAbortRequest(
  facts: ReadonlyArray<Fact>,
  stepId: string,
  attempt: number,
): boolean {
  for (let i = facts.length - 1; i >= 0; i--) {
    const f = facts[i];
    if (f === undefined) continue;
    if (f.kind === "step.reset" && f.stepId === stepId) return false;
    if (
      f.kind === "step.abort-requested" &&
      f.stepId === stepId &&
      f.attempt === attempt
    ) {
      return true;
    }
  }
  return false;
}

export class NagiAbortError extends Error {
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

// Cancellation/abort wins over retry. An operator-aborted step is re-enqueued
// elsewhere so it must not advance, whereas a run-cancellation must advance so
// the run can finalize; only an abort/cancel error is recorded on the fact.
function classifyFailure(args: {
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

function makeStepCtx(args: {
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
  const { runId, stepId, attempt, input, store, clock, tx, signal, emitLog } =
    args;

  // Spread caller `attrs` first, then the correlation keys, so the runtime wins
  // on collision — a handler can't clobber the real runId/stepId/attempt.
  const stepLogger: Logger = {
    debug: (m, a) =>
      emitLog({
        level: "debug",
        msg: m,
        attrs: { ...a, runId, stepId, attempt },
      }),
    info: (m, a) =>
      emitLog({
        level: "info",
        msg: m,
        attrs: { ...a, runId, stepId, attempt },
      }),
    warn: (m, a) =>
      emitLog({
        level: "warn",
        msg: m,
        attrs: { ...a, runId, stepId, attempt },
      }),
    error: (m, a) =>
      emitLog({
        level: "error",
        msg: m,
        attrs: { ...a, runId, stepId, attempt },
      }),
  };

  return {
    input,
    runId,
    stepId,
    attempt,
    signal,
    now: () => clock.now(),
    tx,
    logger: stepLogger,
    once: makeOnce({ runId, stepId, store }),
    idempotencyKey: makeIdempotencyKey(runId, stepId),
  };
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

export function makeDispatcher(deps: DispatchDeps): Dispatcher {
  async function fireHook<E>(
    hook: ((event: E) => void | Promise<void>) | undefined,
    event: E,
    hookName: string,
  ): Promise<void> {
    if (deps.fireHooks === false) return;
    if (hook === undefined) return;
    try {
      await hook(event);
    } catch (err) {
      const { message, stack } = serializeError(err);
      deps.emitLog({
        level: "error",
        msg: `nagi hook "${hookName}" threw — swallowed`,
        attrs: { error: message, ...(stack !== undefined ? { stack } : {}) },
      });
    }
  }

  async function fireStepLifecycle<E>(
    stepHook: ((event: E) => void | Promise<void>) | undefined,
    flowHook: ((event: E) => void | Promise<void>) | undefined,
    [stepName, flowName]: readonly [step: string, flow: string],
    event: E,
  ): Promise<void> {
    await fireHook(stepHook, event, stepName);
    await fireHook(flowHook, event, flowName);
  }

  async function dispatchMessage(message: QueueMessage): Promise<void> {
    const { queue } = deps;
    const flow = await deps.flowFor(message.runId);

    const admission = await admit({ flow, message });
    if (admission.tag === "skip") {
      await queue.ack(message.receipt);
      return;
    }
    const { def } = admission;

    await recordStarted({ flow, message, def });

    const startedAt = Date.now();
    let outcome: Dispatched;
    try {
      outcome = await execute({ message, def });
    } catch (err) {
      outcome = await handleStepError({ flow, message, def, err });
    }

    await queue.ack(message.receipt);
    await interpret({ flow, message, def, startedAt, outcome });
  }

  async function admit(args: {
    flow: Flow;
    message: QueueMessage;
  }): Promise<Admission> {
    const { flow, message } = args;
    const { store } = deps;
    const { runId, stepId, attempt } = message;

    const step = asStepMapWithDefs(flow.steps)[stepId];
    if (!step) {
      deps.emitLog({
        level: "warn",
        msg: `dispatch: step "${stepId}" not in flow "${flow.id}"; ack and skip`,
      });
      return { tag: "skip" };
    }

    const preState = await store.loadRunState(runId);
    if (preState.phase.tag === "canceled") return { tag: "skip" };

    const preStep = stepStateOf(preState, stepId);
    if (
      preStep.tag === "completed" ||
      preStep.tag === "failed" ||
      preStep.tag === "skipped"
    ) {
      return { tag: "skip" };
    }

    const claim = await store.claimStep(runId, stepId, attempt);
    if (claim === null) return { tag: "skip" };

    return { tag: "run", def: getDef(step) };
  }

  async function recordStarted(args: {
    flow: Flow;
    message: QueueMessage;
    def: StepDef;
  }): Promise<void> {
    const { flow, message, def } = args;
    const { store, clock } = deps;
    const { runId, stepId, attempt } = message;
    const handler = handlerDef(def);

    await store.appendFact(runId, {
      kind: "step.started",
      runId,
      stepId,
      attempt,
      stepKind: def.kind,
      at: clock.now(),
    });
    const input: Json =
      handler !== undefined || def.kind === "subflow"
        ? extractInput(await store.loadRunState(runId))
        : null;
    await fireStepLifecycle(
      handler?.onStart,
      deps.hooks?.onStepStart,
      ["step.onStart", "onStepStart"],
      {
        runId,
        flowId: flow.id,
        stepId,
        attempt,
        kind: def.kind,
        input,
        at: clock.now(),
      },
    );
  }

  async function execute(args: {
    message: QueueMessage;
    def: StepDef;
  }): Promise<Dispatched> {
    const { message, def } = args;
    const { runId, stepId, attempt } = message;

    switch (def.kind) {
      case "task":
      case "streaming": {
        const { output, skipAdvance } = await executeTask({
          def,
          runId,
          stepId,
          attempt,
        });
        return skipAdvance ? { tag: "parked" } : { tag: "completed", output };
      }
      case "signal":
        return { tag: "parked" };
      case "match":
        await executeMatch({ def, runId, stepId });
        return { tag: "advance" };
      case "subflow":
        await executeSubflow({ def, runId, stepId, attempt });
        return { tag: "parked" };
    }
  }

  async function interpret(args: {
    flow: Flow;
    message: QueueMessage;
    def: StepDef;
    startedAt: number;
    outcome: Dispatched;
  }): Promise<void> {
    const { flow, message, def, startedAt, outcome } = args;
    const { runId, stepId, attempt } = message;

    switch (outcome.tag) {
      case "parked":
        return;
      case "advance":
        await advance(runId);
        return;
      case "completed":
        await fireStepLifecycle(
          handlerDef(def)?.onComplete,
          deps.hooks?.onStepComplete,
          ["step.onComplete", "onStepComplete"],
          {
            runId,
            flowId: flow.id,
            stepId,
            attempt,
            kind: def.kind,
            output: outcome.output,
            durationMs: Date.now() - startedAt,
            at: deps.clock.now(),
          },
        );
        await advance(runId);
        return;
    }
  }

  async function executeTask(args: {
    def: HandlerDef;
    runId: RunId;
    stepId: string;
    attempt: number;
  }): Promise<ExecuteTaskResult> {
    const { def, runId, stepId, attempt } = args;
    const { store, clock } = deps;
    const runState = await store.loadRunState(runId);
    const input = extractInput(runState);
    const needs = resolveNeeds(def, (id) =>
      resolvedOf(stepStateOf(runState, id)),
    );

    const ac = new AbortController();
    const watcher = startCancelWatcher({
      store,
      runId,
      stepId,
      attempt,
      ac,
      intervalMs: deps.cancelPollIntervalMs ?? CANCEL_POLL_INTERVAL_MS,
    });

    try {
      let stepAbortedHere = false;
      const output = await store.runStep<Json>(
        runId,
        stepId,
        attempt,
        async (tx) => {
          const ctx = makeStepCtx({
            runId,
            stepId,
            attempt,
            input,
            store,
            clock,
            tx,
            signal: ac.signal,
            emitLog: deps.emitLog,
          });
          let out: Json;
          if (def.kind === "streaming") {
            // emit publishes out-of-band via the store's write-side, never
            // through `tx`: chunks must be visible before commit and never enter
            // the fact log. emitActive makes any emit after the handler returns a
            // no-op.
            let emitActive = true;
            const streamingCtx: StreamingStepCtx<unknown> = {
              ...ctx,
              emit: async (chunk: Json) => {
                if (emitActive) store.publishChunk?.(runId, stepId, chunk);
              },
            };
            const run = def.run as StreamingTaskDef["run"];
            try {
              out = (await run({ input, needs, ctx: streamingCtx })) as Json;
            } finally {
              emitActive = false;
            }
          } else {
            const run = def.run as TaskDef["run"];
            out = (await run({ input, needs, ctx })) as Json;
          }
          const postState = await store.loadRunState(runId);
          const { fact, abortedHere } = resolveExecutionFact({
            postState,
            runId,
            stepId,
            attempt,
            output: out,
            at: clock.now(),
          });
          stepAbortedHere = abortedHere;
          return { output: out, fact };
        },
      );
      return { output, skipAdvance: stepAbortedHere };
    } finally {
      watcher.stop();
    }
  }

  async function executeSubflow(args: {
    def: SubflowDef;
    runId: RunId;
    stepId: string;
    attempt: number;
  }): Promise<void> {
    const { def, runId, stepId, attempt } = args;
    const { store } = deps;
    const child = deps.lookupFlow(def.childFlowId);
    if (child === undefined) {
      throw new Error(
        `Subflow step "${stepId}" references child flow "${def.childFlowId}" which is not registered with nagi(). Pass it to flows[].`,
      );
    }
    const runState = await store.loadRunState(runId);
    const parentInput = extractInput(runState);
    const needs = resolveNeeds(def, (id) =>
      resolvedOf(stepStateOf(runState, id)),
    );
    const childInput = def.buildInput({ input: parentInput, needs });
    await deps.startChildRun({
      child,
      childInput,
      parent: { runId, stepId, attempt },
    });
  }

  async function executeMatch(args: {
    def: MatchDef;
    runId: RunId;
    stepId: string;
  }): Promise<void> {
    const { def, runId, stepId } = args;
    const { store, clock } = deps;

    const runState = await store.loadRunState(runId);
    const input = extractInput(runState);
    const needs = resolveNeeds(def, (id) =>
      resolvedOf(stepStateOf(runState, id)),
    );

    const armId = selectArm(def, { input, needs });

    await store.appendFact(runId, {
      kind: "match.arm-selected",
      runId,
      stepId,
      arm: armId,
      at: clock.now(),
    });
  }

  async function handleStepError(args: {
    flow: Flow;
    message: QueueMessage;
    def: StepDef;
    err: unknown;
  }): Promise<Dispatched> {
    const { flow, message, def, err } = args;
    const { store, queue, clock } = deps;
    const { runId, stepId, attempt } = message;
    const error = serializeError(err);
    const handler = handlerDef(def);

    const postState = await store.loadRunState(runId);
    const policy = handler?.retry ?? deps.defaultRetry ?? DEFAULT_RETRY;
    const outcome = classifyFailure({
      attempt,
      policy,
      err,
      runIsCanceled: postState.phase.tag === "canceled",
      stepAborted: hasAbortRequest(postState.facts, stepId, attempt),
    });

    const base = { runId, flowId: flow.id, stepId, attempt, kind: def.kind };

    switch (outcome.tag) {
      case "canceled": {
        await store.appendFact(runId, {
          kind: "step.canceled",
          runId,
          stepId,
          attempt,
          ...(outcome.includeError ? { error } : {}),
          at: clock.now(),
        });
        return outcome.advanceAfter ? { tag: "advance" } : { tag: "parked" };
      }
      case "retry": {
        const at = clock.now();
        const nextAttemptAt = new Date(Date.now() + outcome.delayMs);
        await store.appendFact(runId, {
          kind: "step.retried",
          runId,
          stepId,
          attempt,
          nextAttemptAt,
          error,
          at,
        });
        await fireStepLifecycle(
          handler?.onRetry,
          deps.hooks?.onStepRetry,
          ["step.onRetry", "onStepRetry"],
          { ...base, error, nextAttemptAt, at },
        );
        await queue.enqueue(runId, stepId, {
          attempt: attempt + 1,
          delayMs: outcome.delayMs,
        });
        return { tag: "parked" };
      }
      case "failed": {
        const at = clock.now();
        await store.failStep(runId, stepId, error, {
          kind: "step.failed",
          runId,
          stepId,
          attempt,
          error,
          at,
        });
        await fireStepLifecycle(
          handler?.onError,
          deps.hooks?.onStepError,
          ["step.onError", "onStepError"],
          { ...base, error, at },
        );
        return { tag: "advance" };
      }
    }
  }

  async function advance(runId: RunId): Promise<void> {
    const { store, queue } = deps;
    const flow = await deps.flowFor(runId);

    for (let iter = 0; iter < MAX_ADVANCE_ITERS; iter++) {
      const runState = await store.loadRunState(runId);
      const t = nextTransition(flow, runState);

      switch (t.kind) {
        case "settled":
        case "waiting":
          return;
        case "complete":
          await finalizeFlowCompletion({ flow, runId, output: t.output });
          return;
        case "fail":
          await finalizeFlowFailure({ flow, runId, error: t.error });
          return;
        case "dispatch":
          await recordSkips(runId, t.skip);
          for (const stepId of t.runnable) await queue.enqueue(runId, stepId);
          return;
        case "skip":
          await recordSkips(runId, t.skip);
          continue;
        case "promote-match":
          await applyPromotions(flow, runState, t.promotions);
          continue;
      }
    }

    const cycleError: SerializedError = {
      name: "NagiCycleError",
      message: `advance exceeded ${MAX_ADVANCE_ITERS} iterations — likely a cycle or infinite skip loop in flow "${flow.id}"`,
    };
    const facts = (await store.loadRunState(runId)).facts;
    if (!isFlowTerminal(facts)) {
      await finalizeFlowFailure({ flow, runId, error: cycleError });
    }
  }

  async function recordSkips(
    runId: RunId,
    skip: readonly SkipDecision[],
  ): Promise<void> {
    const { store, clock } = deps;
    for (const { stepId, reason } of skip) {
      await store.appendFact(runId, {
        kind: "step.skipped",
        runId,
        stepId,
        reason,
        at: clock.now(),
      });
    }
  }

  // Match/subflow terminal completion. The task path writes its fact inside the
  // runStep tx and does not route through here.
  async function markStepComplete(args: {
    readonly flow: Flow;
    readonly runId: RunId;
    readonly stepId: StepId;
    readonly attempt: AttemptNumber;
    readonly kind: "match" | "subflow";
    readonly output: Json;
  }): Promise<void> {
    const { flow, runId, stepId, attempt, kind, output } = args;
    const at = deps.clock.now();
    const fact: Fact = {
      kind: "step.completed",
      runId,
      stepId,
      attempt,
      output,
      at,
    };
    await deps.store.completeStep(runId, stepId, output, fact);
    await fireHook(
      deps.hooks?.onStepComplete,
      {
        runId,
        flowId: flow.id,
        stepId,
        attempt,
        kind,
        output,
        durationMs: 0,
        at,
      },
      "onStepComplete",
    );
  }

  async function markStepFail(args: {
    readonly flow: Flow;
    readonly runId: RunId;
    readonly stepId: StepId;
    readonly attempt: AttemptNumber;
    readonly kind: "match" | "subflow";
    readonly error: SerializedError;
  }): Promise<void> {
    const { flow, runId, stepId, attempt, kind, error } = args;
    const at = deps.clock.now();
    const fact: Fact = {
      kind: "step.failed",
      runId,
      stepId,
      attempt,
      error,
      at,
    };
    await deps.store.failStep(runId, stepId, error, fact);
    await fireHook(
      deps.hooks?.onStepError,
      { runId, flowId: flow.id, stepId, attempt, kind, error, at },
      "onStepError",
    );
  }

  async function applyPromotions(
    flow: Flow,
    runState: RunState,
    promotions: readonly MatchPromotion[],
  ): Promise<void> {
    for (const { matchId, attempt, result } of promotions) {
      if (result.kind === "fail") {
        await markStepFail({
          flow,
          runId: runState.runId,
          stepId: matchId,
          attempt,
          kind: "match",
          error: result.error,
        });
      } else {
        await markStepComplete({
          flow,
          runId: runState.runId,
          stepId: matchId,
          attempt,
          kind: "match",
          output: result.output,
        });
      }
    }
  }

  async function finalizeFlowFailure(args: {
    readonly flow: Flow;
    readonly runId: RunId;
    readonly error: SerializedError;
  }): Promise<void> {
    const { flow, runId, error } = args;
    const { store, clock } = deps;
    await store.appendFact(runId, {
      kind: "flow.failed",
      runId,
      error,
      at: clock.now(),
    });
    const event = { runId, flowId: flow.id, error, at: clock.now() };
    await fireHook(flow.onError, event, "flow.onError");
    await fireHook(deps.hooks?.onFlowError, event, "onFlowError");
    await propagateToParent(runId, { kind: "failed", error });
  }

  async function finalizeFlowCompletion(args: {
    readonly flow: Flow;
    readonly runId: RunId;
    readonly output: Json;
  }): Promise<void> {
    const { flow, runId, output } = args;
    const { store, clock } = deps;
    await store.appendFact(runId, {
      kind: "flow.completed",
      runId,
      output,
      at: clock.now(),
    });
    const event = { runId, flowId: flow.id, output, at: clock.now() };
    await fireHook(flow.onComplete, event, "flow.onComplete");
    await fireHook(deps.hooks?.onFlowComplete, event, "onFlowComplete");
    await propagateToParent(runId, { kind: "completed", output });
  }

  async function propagateToParent(
    childRunId: RunId,
    outcome: SubflowChildOutcome,
  ): Promise<void> {
    const { store } = deps;
    const childState = await store.loadRunState(childRunId);
    const startedFact = childState.facts.find(
      (f) => f.kind === "flow.started",
    ) as FlowStartedFact | undefined;
    if (startedFact?.parent === undefined) {
      return;
    }
    const { runId: parentRunId, stepId: parentStepId } = startedFact.parent;

    const parentState = await store.loadRunState(parentRunId);
    if (isTerminalRun(parentState)) {
      deps.emitLog({
        level: "info",
        msg: "nagi: subflow wake skipped — parent run already terminal",
        attrs: {
          parentRunId,
          parentStepId,
          childRunId,
          parentStatus: runStatusOf(parentState),
        },
      });
      return;
    }
    const parentStep = stepStateOf(parentState, parentStepId);
    if (parentStep.tag !== "awaitingChild") {
      deps.emitLog({
        level: "info",
        msg: "nagi: subflow wake skipped — parent step not awaiting child",
        attrs: {
          parentRunId,
          parentStepId,
          childRunId,
          parentStepStatus: stepStatusOf(parentStep),
        },
      });
      return;
    }
    const attempt = parentStep.attempt;

    const parentFlow = await deps.flowFor(parentRunId);

    if (outcome.kind === "completed") {
      const subflowOutput: Json = {
        childRunId,
        output: outcome.output,
      };
      await markStepComplete({
        flow: parentFlow,
        runId: parentRunId,
        stepId: parentStepId,
        attempt,
        kind: "subflow",
        output: subflowOutput,
      });
    } else {
      await markStepFail({
        flow: parentFlow,
        runId: parentRunId,
        stepId: parentStepId,
        attempt,
        kind: "subflow",
        error: outcome.error,
      });
    }

    await advance(parentRunId);
  }

  return { dispatchMessage, advance, propagateToParent, fireHook };
}
