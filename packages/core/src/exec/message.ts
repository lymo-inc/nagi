import type { DispatchDeps } from "../dispatch";
import { serializeError } from "../errors";
import { Facts } from "../facts";
import {
  asStepMapWithDefs,
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
} from "../internal";
import { stepStateOf } from "../scheduler";
import { isAbortRequested, resolvedOf } from "../state";
import {
  CANCEL_POLL_INTERVAL_MS,
  classifyFailure,
  DEFAULT_RETRY,
  makeStepCtx,
  resolveExecutionFact,
  startCancelWatcher,
} from "../step-exec";
import type {
  Flow,
  Json,
  QueueMessage,
  RunId,
  RunState,
  StepCtx,
  StreamingStepCtx,
} from "../types";
import type { Hooks } from "./hooks";
import type { Progression } from "./progression";

type Dispatched =
  | { readonly tag: "completed"; readonly output: Json }
  | { readonly tag: "advance" }
  | { readonly tag: "parked" };

type Admission =
  | { readonly tag: "skip" }
  | { readonly tag: "run"; readonly def: StepDef; readonly state: RunState };

interface ExecuteTaskResult {
  readonly output: Json;
  readonly skipAdvance: boolean;
}

export interface MessageHandler {
  dispatchMessage(message: QueueMessage): Promise<void>;
}

// Handles one queue message: admit (claim + dedupe), record start, execute by
// kind, classify failure, then interpret the outcome (fire completion hook +
// advance, or park). Drives the run forward via progression.advance.
export function makeMessage(
  deps: DispatchDeps,
  hooks: Hooks,
  progression: Progression,
): MessageHandler {
  const { fireHook, fireStepLifecycle } = hooks;
  const { advance } = progression;

  async function dispatchMessage(message: QueueMessage): Promise<void> {
    const { queue } = deps;
    const flow = await deps.flowFor(message.runId);

    const admission = await admit({ flow, message });
    if (admission.tag === "skip") {
      await queue.ack(message.receipt);
      return;
    }
    const { def, state } = admission;

    await recordStarted({ flow, message, def, state });

    const startedAt = Date.now();
    let outcome: Dispatched;
    try {
      outcome = await execute({ flow, message, def, state });
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

    // preState carries the immutable flow input and the (terminal) upstream
    // outputs this step needs, so the rest of the dispatch reuses it instead of
    // re-folding the history. The signal branch and the post-handler abort check
    // still load fresh, since those depend on facts written after admission.
    return { tag: "run", def: getDef(step), state: preState };
  }

  async function recordStarted(args: {
    flow: Flow;
    message: QueueMessage;
    def: StepDef;
    state: RunState;
  }): Promise<void> {
    const { flow, message, def, state } = args;
    const { store, clock } = deps;
    const { runId, stepId, attempt } = message;
    const handler = handlerDef(def);

    await store.appendFact(
      runId,
      Facts.stepStarted(runId, stepId, attempt, def.kind, clock.now()),
    );
    // Start events surface the flow input only where the start is an
    // input-processing moment; signal/match start with null by contract. Input
    // is immutable after flow.started, so the admission snapshot is current.
    const startCarriesInput =
      def.kind === "task" || def.kind === "streaming" || def.kind === "subflow";
    const input: Json = startCarriesInput ? state.input : null;
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
    state: RunState;
  }): Promise<Dispatched> {
    const { flow, message, def, state } = args;
    const { runId, stepId, attempt } = message;

    switch (def.kind) {
      case "task":
      case "streaming": {
        const { output, skipAdvance } = await executeTask({
          def,
          runId,
          stepId,
          attempt,
          state,
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
        await executeMatch({ def, runId, stepId, state });
        return { tag: "advance" };
      case "subflow":
        await executeSubflow({ def, runId, stepId, attempt, state });
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
    // emit publishes out-of-band via the stream transport, never through `tx`:
    // chunks must be visible before commit and never enter the fact log.
    // emitActive makes any emit after the handler returns a no-op.
    let emitActive = true;
    const streamingCtx: StreamingStepCtx<unknown> = {
      ...ctx,
      emit: async (chunk: Json) => {
        if (emitActive)
          deps.streamTransport?.publishChunk(runId, stepId, chunk);
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
    state: RunState;
  }): Promise<ExecuteTaskResult> {
    const { def, runId, stepId, attempt, state } = args;
    const { store, clock } = deps;
    const input = state.input;
    const needs = resolveNeeds(def, (id) => resolvedOf(stepStateOf(state, id)));

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
    state: RunState;
  }): Promise<void> {
    const { def, runId, stepId, attempt, state } = args;
    const child = deps.lookupFlow(def.childFlowId);
    if (child === undefined) {
      throw new Error(
        `Subflow step "${stepId}" references child flow "${def.childFlowId}" which is not registered with nagi(). Pass it to flows[].`,
      );
    }
    const parentInput = state.input;
    const needs = resolveNeeds(def, (id) => resolvedOf(stepStateOf(state, id)));
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
    state: RunState;
  }): Promise<void> {
    const { def, runId, stepId, state } = args;
    const { store, clock } = deps;

    const input = state.input;
    const needs = resolveNeeds(def, (id) => resolvedOf(stepStateOf(state, id)));

    const armId = selectArm(def, { input, needs });

    await store.appendFact(
      runId,
      Facts.matchArmSelected(runId, stepId, armId, clock.now()),
    );
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
      stepAborted: isAbortRequested(stepStateOf(postState, stepId), attempt),
    });

    const base = { runId, flowId: flow.id, stepId, attempt, kind: def.kind };

    switch (outcome.tag) {
      case "canceled": {
        await store.appendFact(
          runId,
          Facts.stepCanceled(
            runId,
            stepId,
            attempt,
            clock.now(),
            outcome.includeError ? error : undefined,
          ),
        );
        return outcome.advanceAfter ? { tag: "advance" } : { tag: "parked" };
      }
      case "retry": {
        const at = clock.now();
        const nextAttemptAt = new Date(at.getTime() + outcome.delayMs);
        await store.appendFact(
          runId,
          Facts.stepRetried(runId, stepId, attempt, nextAttemptAt, error, at),
        );
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
        await store.settleStep(
          runId,
          stepId,
          Facts.stepFailed(runId, stepId, attempt, error, at),
        );
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

  return { dispatchMessage };
}
