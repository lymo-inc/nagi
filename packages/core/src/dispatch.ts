import { makeIdempotencyKey, makeOnce } from "./idempotency";
import {
  getDef,
  type MatchDef,
  resolveNeeds,
  type StepDef,
  type SubflowDef,
  selectArm,
} from "./internal";
import {
  extractInput,
  isFlowTerminal,
  type MatchPromotion,
  nextTransition,
  type SkipDecision,
} from "./scheduler";
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
  Queue,
  QueueMessage,
  RetryPolicy,
  RunId,
  RunState,
  SerializedError,
  StepCtx,
  StepId,
  Store,
  Tx,
} from "./types";

export interface DispatchDeps {
  readonly flowFor: (runId: RunId) => Promise<Flow>;
  readonly lookupFlow: (flowId: string) => Flow | undefined;
  readonly startChildRun: (args: {
    readonly child: Flow;
    readonly childInput: unknown;
    readonly parentRunId: RunId;
    readonly parentStepId: StepId;
    readonly parentStepAttempt: AttemptNumber;
  }) => Promise<RunId>;
  readonly store: Store;
  readonly queue: Queue;
  readonly clock: Clock;
  readonly hooks?: FlowHooks;
  readonly logger?: Logger;
  readonly defaultRetry?: RetryPolicy;
  readonly fireHooks?: boolean;
  readonly cancelPollIntervalMs?: Millis;
}

export async function fireHook<E>(
  hook: ((event: E) => void | Promise<void>) | undefined,
  event: E,
  hookName: string,
  deps: DispatchDeps,
): Promise<void> {
  if (deps.fireHooks === false) return;
  if (hook === undefined) return;
  try {
    await hook(event);
  } catch (err) {
    const { message, stack } = serializeError(err);
    deps.logger?.error(`nagi hook "${hookName}" threw — swallowed`, {
      error: message,
      ...(stack !== undefined ? { stack } : {}),
    });
  }
}

const DEFAULT_RETRY: RetryPolicy = {
  maxAttempts: 3,
  backoff: "exponential",
  initialDelayMs: 1_000,
  maxDelayMs: 60_000,
};

export async function dispatchMessage(
  deps: DispatchDeps,
  message: QueueMessage,
): Promise<void> {
  const { store, queue, clock } = deps;
  const { runId, stepId, attempt } = message;
  const flow = await deps.flowFor(runId);

  const step = flow.steps[stepId];
  if (!step) {
    deps.logger?.warn(
      `dispatch: step "${stepId}" not in flow "${flow.id}"; ack and skip`,
    );
    await queue.ack(message.receipt);
    return;
  }
  const def = getDef(step);

  const preState = await store.loadRunState(runId);
  if (preState.status === "canceled") {
    await queue.ack(message.receipt);
    return;
  }
  const preStep = preState.steps[stepId];
  if (
    preStep &&
    (preStep.status === "completed" ||
      preStep.status === "failed" ||
      preStep.status === "skipped")
  ) {
    await queue.ack(message.receipt);
    return;
  }

  const claim = await store.claimStep(runId, stepId, attempt);
  if (claim === null) {
    await queue.ack(message.receipt);
    return;
  }

  await store.appendFact(runId, {
    kind: "step.started",
    runId,
    stepId,
    attempt,
    at: clock.now(),
  });
  const startEventInput: Json =
    def.kind === "task" || def.kind === "subflow"
      ? extractInput(await store.loadRunState(runId))
      : null;
  const startEvent = {
    runId,
    flowId: flow.id,
    stepId,
    attempt,
    kind: def.kind,
    input: startEventInput,
    at: clock.now(),
  };
  const stepOnStart = def.kind === "task" ? def.onStart : undefined;
  await fireHook(stepOnStart, startEvent, "step.onStart", deps);
  await fireHook(deps.hooks?.onStepStart, startEvent, "onStepStart", deps);

  const startedAt = Date.now();

  try {
    if (def.kind === "task") {
      const { output, skipAdvance } = await executeTask({
        deps,
        message,
        def,
        runId,
        stepId,
        attempt,
      });
      if (skipAdvance) return;
      const completeEvent = {
        runId,
        flowId: flow.id,
        stepId,
        attempt,
        kind: "task" as const,
        output,
        durationMs: Date.now() - startedAt,
        at: clock.now(),
      };
      await fireHook(def.onComplete, completeEvent, "step.onComplete", deps);
      await fireHook(
        deps.hooks?.onStepComplete,
        completeEvent,
        "onStepComplete",
        deps,
      );
      await advance(deps, runId);
    } else if (def.kind === "signal") {
      await queue.ack(message.receipt);
      return;
    } else if (def.kind === "match") {
      await executeMatch({ deps, message, def, runId, stepId, attempt });
      await advance(deps, runId);
    } else if (def.kind === "subflow") {
      await executeSubflow({ deps, message, def, runId, stepId });
      await queue.ack(message.receipt);
      return;
    }
  } catch (err) {
    await handleStepError({
      deps,
      flow,
      message,
      def,
      runId,
      stepId,
      attempt,
      err,
    });
  }
}

interface ExecuteTaskResult {
  readonly output: Json;
  readonly skipAdvance: boolean;
}

async function executeTask(args: {
  deps: DispatchDeps;
  message: QueueMessage;
  def: StepDef;
  runId: RunId;
  stepId: string;
  attempt: number;
}): Promise<ExecuteTaskResult> {
  const { deps, message, def, runId, stepId, attempt } = args;
  if (def.kind !== "task") {
    throw new Error(`executeTask called with non-task def (kind: ${def.kind})`);
  }

  const { store, queue, clock } = deps;
  const runState = await store.loadRunState(runId);
  const input = extractInput(runState);
  const needs = resolveNeeds(def, (id) => runState.steps[id]?.output ?? null);

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
          ...(deps.logger !== undefined ? { logger: deps.logger } : {}),
        });
        const out = (await def.run({ input, needs, ctx })) as Json;
        const postState = await store.loadRunState(runId);
        if (postState.status === "canceled") {
          const canceledFact: Fact = {
            kind: "step.canceled",
            runId,
            stepId,
            attempt,
            at: clock.now(),
          };
          return { output: out, fact: canceledFact };
        }
        if (hasAbortRequest(postState.facts, stepId, attempt)) {
          stepAbortedHere = true;
          const canceledFact: Fact = {
            kind: "step.canceled",
            runId,
            stepId,
            attempt,
            at: clock.now(),
          };
          return { output: out, fact: canceledFact };
        }
        const fact: Fact = {
          kind: "step.completed",
          runId,
          stepId,
          attempt,
          output: out,
          at: clock.now(),
        };
        return { output: out, fact };
      },
    );
    await queue.ack(message.receipt);
    return { output, skipAdvance: stepAbortedHere };
  } finally {
    watcher.stop();
  }
}

const CANCEL_POLL_INTERVAL_MS = 250;

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
        if (
          s.status === "canceled" ||
          s.status === "failed" ||
          s.status === "completed"
        ) {
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

async function executeSubflow(args: {
  deps: DispatchDeps;
  message: QueueMessage;
  def: SubflowDef;
  runId: RunId;
  stepId: string;
}): Promise<void> {
  const { deps, message, def, runId, stepId } = args;
  const { store } = deps;
  const child = deps.lookupFlow(def.childFlowId);
  if (child === undefined) {
    throw new Error(
      `Subflow step "${stepId}" references child flow "${def.childFlowId}" which is not registered with nagi(). Pass it to flows[].`,
    );
  }
  const runState = await store.loadRunState(runId);
  const parentInput = extractInput(runState);
  const needs = resolveNeeds(def, (id) => runState.steps[id]?.output ?? null);
  const childInput = def.buildInput({ input: parentInput, needs });
  await deps.startChildRun({
    child,
    childInput,
    parentRunId: runId,
    parentStepId: stepId,
    parentStepAttempt: message.attempt,
  });
}

async function executeMatch(args: {
  deps: DispatchDeps;
  message: QueueMessage;
  def: MatchDef;
  runId: RunId;
  stepId: string;
  attempt: number;
}): Promise<void> {
  const { deps, message, def, runId, stepId } = args;
  const { store, queue, clock } = deps;

  const runState = await store.loadRunState(runId);
  const input = extractInput(runState);
  const needs = resolveNeeds(def, (id) => runState.steps[id]?.output ?? null);

  const armId = selectArm(def, { input, needs });

  await store.appendFact(runId, {
    kind: "match.arm-selected",
    runId,
    stepId,
    arm: armId,
    at: clock.now(),
  });
  await queue.ack(message.receipt);
}

async function handleStepError(args: {
  deps: DispatchDeps;
  flow: Flow;
  message: QueueMessage;
  def: StepDef;
  runId: RunId;
  stepId: string;
  attempt: number;
  err: unknown;
}): Promise<void> {
  const { deps, flow, message, def, runId, stepId, attempt, err } = args;
  const { store, queue, clock } = deps;
  const error = serializeError(err);

  const postState = await store.loadRunState(runId);
  const runIsCanceled = postState.status === "canceled";
  const stepAborted = hasAbortRequest(postState.facts, stepId, attempt);
  if (runIsCanceled || stepAborted) {
    const isAbort =
      err instanceof NagiAbortError ||
      (err instanceof Error && err.name === "AbortError");
    const canceledFact: Fact = {
      kind: "step.canceled",
      runId,
      stepId,
      attempt,
      ...(isAbort ? { error } : {}),
      at: clock.now(),
    };
    await store.appendFact(runId, canceledFact);
    await queue.ack(message.receipt);
    if (!stepAborted) await advance(deps, runId);
    return;
  }

  const policy =
    def.kind === "task" && def.retry
      ? def.retry
      : (deps.defaultRetry ?? DEFAULT_RETRY);
  const shouldRetry = attempt < policy.maxAttempts && retryAllows(policy, err);

  if (shouldRetry) {
    const delayMs = computeBackoff(policy, attempt);
    await store.appendFact(runId, {
      kind: "step.retried",
      runId,
      stepId,
      attempt,
      nextAttemptAt: new Date(Date.now() + delayMs),
      at: clock.now(),
    });
    const retryEvent = {
      runId,
      flowId: flow.id,
      stepId,
      attempt,
      kind: def.kind,
      error,
      nextAttemptAt: new Date(Date.now() + delayMs),
      at: clock.now(),
    };
    const stepOnRetry = def.kind === "task" ? def.onRetry : undefined;
    await fireHook(stepOnRetry, retryEvent, "step.onRetry", deps);
    await fireHook(deps.hooks?.onStepRetry, retryEvent, "onStepRetry", deps);
    await queue.enqueue(runId, stepId, { attempt: attempt + 1, delayMs });
    await queue.ack(message.receipt);
    return;
  }

  const fact: Fact = {
    kind: "step.failed",
    runId,
    stepId,
    attempt,
    error,
    at: clock.now(),
  };
  await store.failStep(runId, stepId, error, fact);
  const errorEvent = {
    runId,
    flowId: flow.id,
    stepId,
    attempt,
    kind: def.kind,
    error,
    at: clock.now(),
  };
  const stepOnError = def.kind === "task" ? def.onError : undefined;
  await fireHook(stepOnError, errorEvent, "step.onError", deps);
  await fireHook(deps.hooks?.onStepError, errorEvent, "onStepError", deps);
  await queue.ack(message.receipt);
  await advance(deps, runId);
}

const MAX_ADVANCE_ITERS = 1024;

export async function advance(deps: DispatchDeps, runId: RunId): Promise<void> {
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
        await finalizeFlowCompletion({ deps, flow, runId, output: t.output });
        return;
      case "fail":
        await finalizeFlowFailure({ deps, flow, runId, error: t.error });
        return;
      case "dispatch":
        await recordSkips(deps, runId, t.skip);
        for (const stepId of t.runnable) await queue.enqueue(runId, stepId);
        return;
      case "skip":
        await recordSkips(deps, runId, t.skip);
        continue;
      case "promote-match":
        await applyPromotions(deps, flow, runState, t.promotions);
        continue;
    }
  }

  const cycleError: SerializedError = {
    name: "NagiCycleError",
    message: `advance exceeded ${MAX_ADVANCE_ITERS} iterations — likely a cycle or infinite skip loop in flow "${flow.id}"`,
  };
  const facts = (await store.loadRunState(runId)).facts;
  if (!isFlowTerminal(facts)) {
    await finalizeFlowFailure({ deps, flow, runId, error: cycleError });
  }
}

async function recordSkips(
  deps: DispatchDeps,
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

/**
 * Mark a match/subflow step terminal: append the fact, persist, and fire the
 * runtime-level hook. Shared by match promotion and subflow wake. The task
 * path differs (the fact is written inside the runStep transaction) and does
 * not route through here.
 */
async function markStepComplete(args: {
  readonly deps: DispatchDeps;
  readonly flow: Flow;
  readonly runId: RunId;
  readonly stepId: StepId;
  readonly attempt: AttemptNumber;
  readonly kind: "match" | "subflow";
  readonly output: Json;
}): Promise<void> {
  const { deps, flow, runId, stepId, attempt, kind, output } = args;
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
    deps,
  );
}

async function markStepFail(args: {
  readonly deps: DispatchDeps;
  readonly flow: Flow;
  readonly runId: RunId;
  readonly stepId: StepId;
  readonly attempt: AttemptNumber;
  readonly kind: "match" | "subflow";
  readonly error: SerializedError;
}): Promise<void> {
  const { deps, flow, runId, stepId, attempt, kind, error } = args;
  const at = deps.clock.now();
  const fact: Fact = { kind: "step.failed", runId, stepId, attempt, error, at };
  await deps.store.failStep(runId, stepId, error, fact);
  await fireHook(
    deps.hooks?.onStepError,
    { runId, flowId: flow.id, stepId, attempt, kind, error, at },
    "onStepError",
    deps,
  );
}

async function applyPromotions(
  deps: DispatchDeps,
  flow: Flow,
  runState: RunState,
  promotions: readonly MatchPromotion[],
): Promise<void> {
  for (const { matchId, attempt, result } of promotions) {
    if (result.kind === "fail") {
      await markStepFail({
        deps,
        flow,
        runId: runState.runId,
        stepId: matchId,
        attempt,
        kind: "match",
        error: result.error,
      });
    } else {
      await markStepComplete({
        deps,
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

function makeStepCtx(args: {
  runId: RunId;
  stepId: string;
  attempt: number;
  input: unknown;
  store: Store;
  clock: Clock;
  tx: Tx;
  signal: AbortSignal;
  logger?: Logger;
}): StepCtx<unknown> {
  const { runId, stepId, attempt, input, store, clock, tx, signal } = args;

  return {
    input,
    runId,
    stepId,
    attempt,
    signal,
    now: () => clock.now(),
    tx,
    logger: args.logger ?? consoleLogger(),
    once: makeOnce({ runId, stepId, store }),
    idempotencyKey: makeIdempotencyKey(runId, stepId),
  };
}

function consoleLogger(): Logger {
  return {
    debug: (m, a) => console.debug(m, a),
    info: (m, a) => console.info(m, a),
    warn: (m, a) => console.warn(m, a),
    error: (m, a) => console.error(m, a),
  };
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

async function finalizeFlowFailure(args: {
  readonly deps: DispatchDeps;
  readonly flow: Flow;
  readonly runId: RunId;
  readonly error: SerializedError;
}): Promise<void> {
  const { deps, flow, runId, error } = args;
  const { store, clock } = deps;
  await store.appendFact(runId, {
    kind: "flow.failed",
    runId,
    error,
    at: clock.now(),
  });
  const event = { runId, flowId: flow.id, error, at: clock.now() };
  await fireHook(flow.onError, event, "flow.onError", deps);
  await fireHook(deps.hooks?.onFlowError, event, "onFlowError", deps);
  await propagateToParent(deps, runId, { kind: "failed", error });
}

async function finalizeFlowCompletion(args: {
  readonly deps: DispatchDeps;
  readonly flow: Flow;
  readonly runId: RunId;
  readonly output: Json;
}): Promise<void> {
  const { deps, flow, runId, output } = args;
  const { store, clock } = deps;
  await store.appendFact(runId, {
    kind: "flow.completed",
    runId,
    output,
    at: clock.now(),
  });
  const event = { runId, flowId: flow.id, output, at: clock.now() };
  await fireHook(flow.onComplete, event, "flow.onComplete", deps);
  await fireHook(deps.hooks?.onFlowComplete, event, "onFlowComplete", deps);
  await propagateToParent(deps, runId, { kind: "completed", output });
}

export type SubflowChildOutcome =
  | { readonly kind: "completed"; readonly output: Json }
  | { readonly kind: "failed"; readonly error: SerializedError }
  | { readonly kind: "canceled"; readonly error: SerializedError };

export async function propagateToParent(
  deps: DispatchDeps,
  childRunId: RunId,
  outcome: SubflowChildOutcome,
): Promise<void> {
  const { store } = deps;
  const childState = await store.loadRunState(childRunId);
  const startedFact = childState.facts.find((f) => f.kind === "flow.started") as
    | FlowStartedFact
    | undefined;
  if (
    startedFact?.parentRunId === undefined ||
    startedFact?.parentStepId === undefined
  ) {
    return;
  }
  const parentRunId = startedFact.parentRunId;
  const parentStepId = startedFact.parentStepId;

  const parentState = await store.loadRunState(parentRunId);
  if (
    parentState.status === "completed" ||
    parentState.status === "failed" ||
    parentState.status === "canceled"
  ) {
    deps.logger?.info(
      "nagi: subflow wake skipped — parent run already terminal",
      {
        parentRunId,
        parentStepId,
        childRunId,
        parentStatus: parentState.status,
      },
    );
    return;
  }
  const parentStep = parentState.steps[parentStepId];
  if (parentStep?.status !== "running") {
    deps.logger?.info("nagi: subflow wake skipped — parent step not running", {
      parentRunId,
      parentStepId,
      childRunId,
      parentStepStatus: parentStep?.status ?? "missing",
    });
    return;
  }
  const attempt = parentStep.attempts > 0 ? parentStep.attempts : 1;

  const parentFlow = await deps.flowFor(parentRunId);

  if (outcome.kind === "completed") {
    const subflowOutput: Json = {
      childRunId,
      output: outcome.output,
    };
    await markStepComplete({
      deps,
      flow: parentFlow,
      runId: parentRunId,
      stepId: parentStepId,
      attempt,
      kind: "subflow",
      output: subflowOutput,
    });
  } else {
    await markStepFail({
      deps,
      flow: parentFlow,
      runId: parentRunId,
      stepId: parentStepId,
      attempt,
      kind: "subflow",
      error: outcome.error,
    });
  }

  await advance(deps, parentRunId);
}

export function serializeError(err: unknown): SerializedError {
  if (err instanceof Error) {
    return {
      name: err.name,
      message: err.message,
      ...(err.stack !== undefined ? { stack: err.stack } : {}),
    };
  }
  return { name: "Error", message: String(err) };
}
