import { serializeError } from "./errors";
import {
  asStepMapWithDefs,
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
  type MatchPromotion,
  nextTransition,
  type SkipDecision,
  stepStateOf,
} from "./scheduler";
import {
  extractInput,
  isTerminalRun,
  resolvedOf,
  runStatusOf,
  stepStatusOf,
} from "./state";
import {
  CANCEL_POLL_INTERVAL_MS,
  classifyFailure,
  DEFAULT_RETRY,
  hasAbortRequest,
  makeStepCtx,
  resolveExecutionFact,
  startCancelWatcher,
} from "./step-exec";
import type {
  AttemptNumber,
  Clock,
  Fact,
  Flow,
  FlowHooks,
  FlowStartedFact,
  Json,
  Millis,
  ParentRef,
  Queue,
  QueueMessage,
  RetryPolicy,
  RunId,
  RunState,
  SerializedError,
  StepCtx,
  StepId,
  Store,
  StreamingStepCtx,
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

const MAX_ADVANCE_ITERS = 1024;

type Dispatched =
  | { readonly tag: "completed"; readonly output: Json }
  | { readonly tag: "advance" }
  | { readonly tag: "parked" };

type Admission =
  | { readonly tag: "skip" }
  | { readonly tag: "run"; readonly def: StepDef };

interface ExecuteTaskResult {
  readonly output: Json;
  readonly skipAdvance: boolean;
}

export type SubflowChildOutcome =
  | { readonly kind: "completed"; readonly output: Json }
  | { readonly kind: "failed"; readonly error: SerializedError }
  | { readonly kind: "canceled"; readonly error: SerializedError };

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
      outcome = await execute({ flow, message, def });
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
    flow: Flow;
    message: QueueMessage;
    def: StepDef;
  }): Promise<Dispatched> {
    const { flow, message, def } = args;
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
      case "signal": {
        // recordStarted just moved this signal step into awaitingSignal. If a
        // signal arrived early (the start/await race) it was parked as a
        // signal.buffered fact; apply it now, atomically under the store's
        // per-run lock. With nothing buffered this no-ops and the step stays
        // parked until wf.signal delivers.
        const settled = await deps.store.settleSignal({
          runId,
          stepId,
          at: deps.clock.now(),
        });
        if (settled.tag === "delivered") {
          await fireHook(
            deps.hooks?.onSignalReceived,
            {
              runId,
              flowId: flow.id,
              stepId,
              attempt: settled.attempt,
              kind: "signal",
              payload: settled.payload,
              at: deps.clock.now(),
            },
            "onSignalReceived",
          );
          return { tag: "advance" };
        }
        return { tag: "parked" };
      }
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

  async function runHandler(
    def: HandlerDef,
    args: {
      input: unknown;
      needs: Record<string, unknown>;
      ctx: StepCtx<unknown>;
      runId: RunId;
      stepId: string;
    },
  ): Promise<Json> {
    const { input, needs, ctx, runId, stepId } = args;
    if (def.kind !== "streaming") {
      return (await (def.run as TaskDef["run"])({ input, needs, ctx })) as Json;
    }
    // emit publishes out-of-band via the store's write-side, never through `tx`:
    // chunks must be visible before commit and never enter the fact log.
    // emitActive makes any emit after the handler returns a no-op.
    let emitActive = true;
    const streamingCtx: StreamingStepCtx<unknown> = {
      ...ctx,
      emit: async (chunk: Json) => {
        if (emitActive) deps.store.publishChunk?.(runId, stepId, chunk);
      },
    };
    try {
      return (await (def.run as StreamingTaskDef["run"])({
        input,
        needs,
        ctx: streamingCtx,
      })) as Json;
    } finally {
      emitActive = false;
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
          const out = await runHandler(def, {
            input,
            needs,
            ctx,
            runId,
            stepId,
          });
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
        const nextAttemptAt = new Date(at.getTime() + outcome.delayMs);
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
    const finalState = await store.loadRunState(runId);
    if (!isTerminalRun(finalState)) {
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

  // The task path writes its fact inside the runStep tx and does not route through here.
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
