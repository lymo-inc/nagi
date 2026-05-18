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
  aggregateMatch,
  extractInput,
  flowTermination,
  nextRunnable,
} from "./scheduler";
import type {
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
  /**
   * Resolves the flow for a given run. The runtime supplies a closure over its
   * flow registry; adapters may override to load flows lazily. Called once at
   * the top of `dispatchMessage` and `advance` — per-step lookups reuse the
   * resolved value.
   */
  readonly flowFor: (runId: RunId) => Promise<Flow>;
  /**
   * Synchronous flow lookup by flow id. Used by the subflow dispatch path to
   * resolve a referenced child flow without round-tripping through the store.
   * Returns `undefined` for unregistered flows; the caller throws an
   * actionable error. Optional only to keep adapters that don't supply it
   * compilable — calling a subflow step without it is a runtime error.
   */
  readonly lookupFlow?: (flowId: string) => Flow | undefined;
  /**
   * Start a child run linked to a parent. Honors the child flow's
   * `concurrency` config (cancel-in-progress, etc.) via the same atomic
   * `tryStartRun` path used by `wf.start()`. Writes a `flow.started` fact
   * with `parentRunId`/`parentStepId` so the runtime can wake the parent's
   * subflow step on terminal completion.
   *
   * Returns the minted child runId.
   */
  readonly startChildRun?: (args: {
    readonly child: Flow;
    readonly childInput: unknown;
    readonly parentRunId: RunId;
    readonly parentStepId: StepId;
  }) => Promise<RunId>;
  readonly store: Store;
  readonly queue: Queue;
  readonly clock: Clock;
  readonly hooks?: FlowHooks;
  readonly logger?: Logger;
  readonly defaultRetry?: RetryPolicy;
  /**
   * When `false`, neither step-local nor flow-local hooks fire, and the
   * cross-cutting `hooks` (FlowHooks) are also suppressed. Set by
   * `wf.replay({ fireHooks: false })` for backfills where webhooks
   * must not re-publish. Default behavior (undefined / `true`): hooks fire.
   */
  readonly fireHooks?: boolean;
}

/**
 * Invoke a lifecycle hook, swallowing thrown errors so a hook bug cannot fail
 * the run. Errors are logged via `deps.logger` — operators see the failure
 * without the workflow caring. When `deps.fireHooks === false`, the call is a
 * no-op (used by replay to suppress re-emission).
 */
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
  // Run was canceled by a concurrency-group supersede before this message
  // was claimed. Ack and skip — the dispatcher stops scheduling new work
  // for canceled runs; in-flight steps already executing run to completion
  // (their facts persist), but anything that hadn't started yet is dropped.
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
  // Resolve the hook's `input` field by step kind:
  //  - task:    the same value passed to `run({ input, ... })` (flow input).
  //  - subflow: the parent's flow input (the resolved child input is
  //             derived inside the dispatch branch, not visible to hooks).
  //  - signal:  `null` — no pre-execution input.
  //  - match:   `null` — the discriminator value is computed inside the
  //             match handler; the start event fires before that resolves.
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
      const output = await executeTask({
        deps,
        message,
        def,
        runId,
        stepId,
        attempt,
      });
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
      // Signals don't run; they wait. Mark `started` (already done above) and ack.
      // Completion arrives via `wf.signal()`.
      await queue.ack(message.receipt);
      return;
    } else if (def.kind === "match") {
      await executeMatch({ deps, message, def, runId, stepId, attempt });
      await advance(deps, runId);
    } else if (def.kind === "subflow") {
      // Subflows don't run inline; they spawn a child run on the same
      // store/queue and wait. `step.started` is already written above; the
      // parent step stays in `running` until the child reaches a terminal
      // state — at which point `finalizeFlowCompletion` /
      // `finalizeFlowFailure` writes the parent's `step.completed` /
      // `step.failed` directly (mirrors how `wf.signal` resolves a signal).
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

async function executeTask(args: {
  deps: DispatchDeps;
  message: QueueMessage;
  def: StepDef;
  runId: RunId;
  stepId: string;
  attempt: number;
}): Promise<Json> {
  const { deps, message, def, runId, stepId, attempt } = args;
  if (def.kind !== "task") {
    throw new Error(`executeTask called with non-task def (kind: ${def.kind})`);
  }

  const { store, queue, clock } = deps;
  const runState = await store.loadRunState(runId);
  const input = extractInput(runState);
  const needs = resolveNeeds(def, (id) => runState.steps[id]?.output ?? null);

  // `ctx.signal` aborts when the run reaches a terminal canceled status. The
  // wakeup that fires the abort is a follow-up feature (see issue tracker);
  // for now the signal is wired structurally and observable to handlers that
  // compose it with their own AbortSignals (e.g. `AbortSignal.any([ctx.signal,
  // AbortSignal.timeout(60_000)])`). The boundary check after the handler
  // returns is what reclassifies cancellation today.
  const ac = new AbortController();

  // `runStep` owns the atomic scope: adapter-managed tx, then atomic write of
  // step output + `step.completed` fact + lease release. The handler's
  // `ctx.tx` is the same tx so domain writes commit together with the step.
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
      // Boundary check: if the run reached canceled status while the handler
      // was in flight (e.g. a newer `wf.start()` superseded), record
      // `step.canceled` instead of `step.completed`. The handler's domain
      // writes still commit atomically with this fact — the read-side just
      // sees the truthful classification rather than a stale "completed"
      // output on a canceled run.
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
  return output;
}

/**
 * Subflow dispatch: derive the child input from `(parentInput, needs)`, start
 * a child run on the same store/queue, and park the parent step in `running`.
 * The parent step's `step.completed` is written by `finalizeFlowCompletion`
 * on the child run's terminal transition — not via a queue re-dispatch.
 *
 * Throws if the child flow is unregistered or its input fails schema
 * validation. `handleStepError` then applies the parent step's retry policy
 * (or surfaces the failure). Sibling cancellation triggered by the child's
 * own `concurrency` config is propagated to those siblings' parents inside
 * `startChildRun` itself.
 */
async function executeSubflow(args: {
  deps: DispatchDeps;
  message: QueueMessage;
  def: SubflowDef;
  runId: RunId;
  stepId: string;
}): Promise<void> {
  const { deps, def, runId, stepId } = args;
  const { store } = deps;
  if (deps.lookupFlow === undefined || deps.startChildRun === undefined) {
    throw new Error(
      `subflow dispatch requires DispatchDeps.lookupFlow + .startChildRun (step "${stepId}" referenced child "${def.childFlowId}")`,
    );
  }
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
  });
}

/**
 * Match dispatch: select an arm, persist the `match.arm-selected` fact, ack
 * the message. The match step stays in `running` status — it transitions to
 * `completed` (or `failed`) inside `advance()` once the chosen arm's nested
 * steps are terminal. Throws if `selectArm` throws so `handleStepError`
 * applies the match's retry policy / surfaces the failure.
 */
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

  // If the run reached canceled status mid-handler (e.g. a newer wf.start()
  // superseded via cancel-in-progress), record `step.canceled` instead of
  // `step.failed`. The handler's transaction already rolled back via
  // `runStep`; the canceled fact lands on its own and onStepError stays
  // silent — `flow.onError` already fired against the canceling run.
  const postState = await store.loadRunState(runId);
  if (postState.status === "canceled") {
    const isAbort = err instanceof Error && err.name === "AbortError";
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
    await advance(deps, runId);
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
  const { store, queue, clock } = deps;
  const flow = await deps.flowFor(runId);

  for (let iter = 0; iter < MAX_ADVANCE_ITERS; iter++) {
    const runState = await store.loadRunState(runId);

    // Promote any `running` match whose chosen arm has reached a terminal
    // state. This must precede termination + scheduling: a match that just
    // completed releases its downstream needs in the same iteration.
    const promoted = await promoteMatches({ deps, flow, runState });
    if (promoted) continue;

    const termination = flowTermination(flow, runState);

    if (termination.done) {
      if (isFlowTerminal(runState.facts)) return;
      if (termination.failed) {
        const failedStep = Object.values(runState.steps).find(
          (s) => s.status === "failed",
        );
        const flowError = failedStep?.error ?? {
          name: "Error",
          message: "step failed",
        };
        await finalizeFlowFailure({ deps, flow, runId, error: flowError });
      } else {
        await finalizeFlowCompletion({ deps, flow, runId, runState });
      }
      return;
    }

    const input = extractInput(runState);
    const decision = nextRunnable({ flow, runState, input });

    if (decision.skip.length === 0 && decision.runnable.length === 0) return;

    for (const { stepId, reason } of decision.skip) {
      await store.appendFact(runId, {
        kind: "step.skipped",
        runId,
        stepId,
        reason,
        at: clock.now(),
      });
    }

    for (const stepId of decision.runnable) {
      await queue.enqueue(runId, stepId);
    }

    if (decision.runnable.length > 0) {
      // Workers will pick up these messages; advance loop exits and resumes
      // when those steps complete.
      return;
    }
    // Only skips happened; loop to recompute.
  }

  // Cycle guard: rather than crash the worker, mark the flow as failed with a
  // structured error so callers can query the terminal state.
  const cycleError: SerializedError = {
    name: "NagiCycleError",
    message: `advance exceeded ${MAX_ADVANCE_ITERS} iterations — likely a cycle or infinite skip loop in flow "${flow.id}"`,
  };
  const facts = (await store.loadRunState(runId)).facts;
  if (!isFlowTerminal(facts)) {
    await finalizeFlowFailure({ deps, flow, runId, error: cycleError });
  }
}

/**
 * Walk every match step in `running` status and promote any whose chosen arm
 * has reached a terminal state.
 *
 *   fail-fast — a chosen-arm step terminally failed. Mark the match `failed`
 *               with an error that points back to the failing nested step.
 *               Sibling running arm steps will still complete on their workers,
 *               but the match is already terminal — downstream cascades skip.
 *   complete  — every chosen-arm step terminated cleanly. Mark the match
 *               `completed` with the assembled `{ stepKey: stepOutput }` map.
 *
 * Returns true if any match was promoted (caller should reload + re-loop).
 */
async function promoteMatches(args: {
  deps: DispatchDeps;
  flow: Flow;
  runState: RunState;
}): Promise<boolean> {
  const { deps, flow, runState } = args;
  const { store, clock } = deps;
  let promoted = false;

  for (const [matchId, step] of Object.entries(flow.steps)) {
    const def = getDef(step);
    if (def.kind !== "match") continue;
    const state = runState.steps[matchId];
    if (state?.status !== "running") continue;

    const agg = aggregateMatch(matchId, flow, runState);
    if (agg.kind === "pending") continue;

    const attempt = state.attempts > 0 ? state.attempts : 1;

    if (agg.kind === "fail-fast") {
      const failedNested = runState.steps[agg.failedStepId];
      const error: SerializedError = failedNested?.error ?? {
        name: "Error",
        message: `match "${matchId}": chosen-arm step "${agg.failedStepId}" failed`,
      };
      const fact: Fact = {
        kind: "step.failed",
        runId: runState.runId,
        stepId: matchId,
        attempt,
        error,
        at: clock.now(),
      };
      await store.failStep(runState.runId, matchId, error, fact);
      await fireHook(
        deps.hooks?.onStepError,
        {
          runId: runState.runId,
          flowId: flow.id,
          stepId: matchId,
          attempt,
          kind: "match",
          error,
          at: clock.now(),
        },
        "onStepError",
        deps,
      );
      promoted = true;
      continue;
    }

    const fact: Fact = {
      kind: "step.completed",
      runId: runState.runId,
      stepId: matchId,
      attempt,
      output: agg.output,
      at: clock.now(),
    };
    await store.completeStep(runState.runId, matchId, agg.output, fact);
    await fireHook(
      deps.hooks?.onStepComplete,
      {
        runId: runState.runId,
        flowId: flow.id,
        stepId: matchId,
        attempt,
        kind: "match",
        output: agg.output,
        durationMs: 0,
        at: clock.now(),
      },
      "onStepComplete",
      deps,
    );
    promoted = true;
  }

  return promoted;
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
    // `tx` is supplied by `Store.runStep` — for adapters with no real
    // transaction (in-memory) it is `undefined as Tx`, and handlers that
    // touch `ctx.tx` will throw on the first call. Adapters that need
    // typed transactional clients (e.g. `@nagi-js/postgres`) augment
    // `Register.tx` and pass a real Kysely transaction here.
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

function isFlowTerminal(facts: readonly Fact[]): boolean {
  // Walk back from the tail. `flow.canceled` is appended to a prior run by a
  // concurrent `wf.start()`, which may interleave with the prior run's own
  // step facts — so the canceled fact is not always the literal last entry.
  for (let i = facts.length - 1; i >= 0; i--) {
    const f = facts[i];
    if (
      f !== undefined &&
      (f.kind === "flow.completed" ||
        f.kind === "flow.failed" ||
        f.kind === "flow.canceled")
    ) {
      return true;
    }
  }
  return false;
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
  readonly runState: RunState;
}): Promise<void> {
  const { deps, flow, runId, runState } = args;
  const { store, clock } = deps;
  const output = computeFlowOutput(flow, runState);
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

/**
 * Read the parent linkage from the just-finalized child run's `flow.started`
 * fact. If linked, resolve the parent's subflow step:
 *
 * - child completed → write parent step.completed with `{ childRunId, output }`,
 *   matching `SubflowStepOutput`.
 * - child failed     → write parent step.failed with the child's error.
 * - child canceled   → write parent step.failed with a structured cancel error.
 *
 * Then call `advance(parent)` so downstream parent steps unblock. The
 * mechanism mirrors `wf.signal()`: the dispatcher does NOT re-enqueue the
 * parent step; completion is written directly.
 *
 * Idempotent: if the parent's subflow step is already terminal (or the
 * parent itself is canceled / completed), this is a logged no-op rather than
 * a throw. Race-safe because the child only transitions to terminal once.
 */
export type SubflowChildOutcome =
  | { readonly kind: "completed"; readonly output: Json }
  | { readonly kind: "failed"; readonly error: SerializedError }
  | { readonly kind: "canceled"; readonly error: SerializedError };

export async function propagateToParent(
  deps: DispatchDeps,
  childRunId: RunId,
  outcome: SubflowChildOutcome,
): Promise<void> {
  const { store, clock } = deps;
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
    const completedFact: Fact = {
      kind: "step.completed",
      runId: parentRunId,
      stepId: parentStepId,
      attempt,
      output: subflowOutput,
      at: clock.now(),
    };
    await store.completeStep(
      parentRunId,
      parentStepId,
      subflowOutput,
      completedFact,
    );
    await fireHook(
      deps.hooks?.onStepComplete,
      {
        runId: parentRunId,
        flowId: parentFlow.id,
        stepId: parentStepId,
        attempt,
        kind: "subflow" as const,
        output: subflowOutput,
        durationMs: 0,
        at: clock.now(),
      },
      "onStepComplete",
      deps,
    );
  } else {
    const failedFact: Fact = {
      kind: "step.failed",
      runId: parentRunId,
      stepId: parentStepId,
      attempt,
      error: outcome.error,
      at: clock.now(),
    };
    await store.failStep(parentRunId, parentStepId, outcome.error, failedFact);
    await fireHook(
      deps.hooks?.onStepError,
      {
        runId: parentRunId,
        flowId: parentFlow.id,
        stepId: parentStepId,
        attempt,
        kind: "subflow" as const,
        error: outcome.error,
        at: clock.now(),
      },
      "onStepError",
      deps,
    );
  }

  await advance(deps, parentRunId);
}

function computeFlowOutput(flow: Flow, runState: RunState): Json {
  if (flow.output === undefined) return null;
  const stepOutputs: Record<string, Json> = {};
  for (const [sid, sstate] of Object.entries(runState.steps)) {
    if (sstate.output !== undefined) stepOutputs[sid] = sstate.output;
  }
  return flow.output(stepOutputs as never) as Json;
}

function serializeError(err: unknown): SerializedError {
  if (err instanceof Error) {
    return {
      name: err.name,
      message: err.message,
      ...(err.stack !== undefined ? { stack: err.stack } : {}),
    };
  }
  return { name: "Error", message: String(err) };
}
