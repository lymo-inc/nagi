import { makeIdempotencyKey, makeOnce } from "./idempotency";
import {
  getDef,
  type MatchDef,
  resolveNeeds,
  type StepDef,
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
  //  - task:   the same value passed to `run({ input, ... })` (flow input).
  //  - signal: `null` — no pre-execution input.
  //  - match:  `null` — the discriminator value is computed inside the
  //            match handler; the start event fires before that resolves.
  const startEventInput: Json =
    def.kind === "task" ? extractInput(await store.loadRunState(runId)) : null;
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
        ...(deps.logger !== undefined ? { logger: deps.logger } : {}),
      });
      const out = (await def.run({ input, needs, ctx })) as Json;
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
  logger?: Logger;
}): StepCtx<unknown> {
  const { runId, stepId, attempt, input, store, clock, tx } = args;
  const ac = new AbortController();

  return {
    input,
    runId,
    stepId,
    attempt,
    signal: ac.signal,
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
