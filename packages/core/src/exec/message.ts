import type { DispatchDeps, DispatchResult } from "../dispatch";
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
  type SubflowDef,
  selectArm,
} from "../internal";
import { resolveRetry } from "../retry";
import { deriveChildRunId } from "../run-id";
import { isAbortRequested, isTerminalRun, stepStateOf } from "../state";
import {
  CANCEL_POLL_INTERVAL_MS,
  classifyFailure,
  makeActivityCtx,
  makeStepCtx,
  resolveExecutionFact,
  startCancelWatcher,
  startDeadline,
  startHeartbeat,
  unwrapDeadline,
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
  // Step already settled but the run is still live: the advance that should
  // have followed the settle may have been lost (crash between settle and
  // enqueue). Re-drive instead of dropping the redelivery.
  | { readonly tag: "recover" }
  | { readonly tag: "run"; readonly def: StepDef; readonly state: RunState };

export interface MessageHandler {
  dispatchMessage(message: QueueMessage): Promise<DispatchResult>;
}

// Start events surface the flow input only where the start is an
// input-processing moment; signal/match start with null by contract.
function startInput(def: StepDef, input: Json): Json {
  switch (def.kind) {
    case "task":
    case "activity":
    case "streaming":
    case "subflow":
      return input;
    case "signal":
    case "match":
      return null;
  }
}

export function makeMessage(
  deps: DispatchDeps,
  hooks: Hooks,
  progression: Progression,
): MessageHandler {
  const { fireHook, fireStepLifecycle } = hooks;
  const { advance } = progression;

  async function dispatchMessage(
    message: QueueMessage,
  ): Promise<DispatchResult> {
    const { queue } = deps;
    const resolution = await deps.flowOf(message.runId);
    let flow: Flow;
    switch (resolution.kind) {
      case "current":
        flow = resolution.flow;
        break;
      // A terminal run's message must not outlive it: a run canceled AFTER
      // its snapshot went gone would otherwise nack-loop forever, since the
      // hash check fires before admit()'s canceled-phase check ever runs.
      case "gone-terminal":
        await queue.ack(message.receipt);
        return { kind: "done" };
      case "gone-live":
        return { kind: "snapshot-gone", error: resolution.error };
    }

    const admission = await admit({ flow, message });
    if (admission.tag === "skip") {
      await queue.ack(message.receipt);
      return { kind: "done" };
    }
    if (admission.tag === "recover") {
      await advance(message.runId);
      await queue.ack(message.receipt);
      return { kind: "done" };
    }
    const { def, state } = admission;

    await recordStarted({ flow, message, def, state });

    const startedAt = Date.now();
    // Hold the message lease for the whole handler run so a slow step (e.g. a
    // multi-minute LLM call) isn't redelivered and re-executed concurrently.
    // The store lease is extended in lock-step so the reaper sees a live lease.
    const heartbeat = startHeartbeat({
      queue,
      store: deps.store,
      runId: message.runId,
      stepId: message.stepId,
      attempt: message.attempt,
      receipt: message.receipt,
      intervalMs: deps.heartbeat.intervalMs,
      leaseMs: deps.heartbeat.leaseMs,
      holdWarnMs: deps.heartbeat.holdWarnMs,
      emitLog: deps.emitLog,
    });
    let outcome: Dispatched;
    try {
      outcome = await execute({ flow, message, def, state });
    } catch (err) {
      outcome = await handleStepError({ flow, message, def, err });
    } finally {
      await heartbeat.stop();
    }

    await interpret({ flow, message, def, startedAt, outcome });
    // Ack last: if interpret's advance throws, dispatchSafely nacks and the
    // redelivery takes the recover path above.
    await queue.ack(message.receipt);
    return { kind: "done" };
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
      // Settled step on a live run: re-drive. On a settled run there is nothing
      // to drive — but do NOT skip non-terminal steps of a completed/failed run:
      // replay({ from }) resets steps on such runs and re-dispatches them.
      return isTerminalRun(preState) ? { tag: "skip" } : { tag: "recover" };
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

    const startedAt = clock.now();
    await store.appendFact(
      runId,
      Facts.stepStarted(runId, stepId, attempt, def.kind, startedAt),
    );
    // Arm the signal timeout off the same start instant the step.started fact
    // records, so the deadline is derived from a persisted fact (replay-safe),
    // never from a fresh clock read. upsertTimer keeps the earliest deadline, so
    // a lease-reap re-dispatch of a still-parked signal doesn't push it out.
    // timeoutMs: "unbounded" is the (explicit, config-level) opt-in to parking
    // forever — the only way to get a timer-less wait.
    if (def.kind === "signal" && typeof def.timeoutMs === "number") {
      await store.upsertTimer(
        runId,
        stepId,
        new Date(startedAt.getTime() + def.timeoutMs),
      );
    }
    // Input is immutable after flow.started, so the admission snapshot is current.
    const input = startInput(def, state.input);
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
      case "activity":
      case "streaming":
        return await executeHandler({ def, runId, stepId, attempt, state });
      case "signal": {
        // recordStarted just moved this signal step into awaitingSignal. If a
        // signal arrived early (the start/await race) it was parked as a
        // signal.buffered fact; apply it now, atomically under the store's
        // per-run lock.
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
        return await executeSubflow({ def, runId, stepId, state });
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

  // emit publishes out-of-band via the stream transport, never through `tx`:
  // chunks must be visible before commit and never enter the fact log. Any
  // emit after the body returns is a no-op.
  async function withEmit(
    runId: RunId,
    stepId: string,
    base: StepCtx<unknown>,
    body: (ctx: StreamingStepCtx<unknown>) => Promise<Json>,
  ): Promise<Json> {
    let emitActive = true;
    try {
      return await body({
        ...base,
        emit: async (chunk) => {
          if (emitActive)
            deps.streamTransport?.publishChunk(runId, stepId, chunk);
        },
      });
    } finally {
      emitActive = false;
    }
  }

  async function executeHandler(args: {
    def: HandlerDef;
    runId: RunId;
    stepId: string;
    attempt: number;
    state: RunState;
  }): Promise<Dispatched> {
    const { def, runId, stepId, attempt, state } = args;
    const { store, clock } = deps;
    const input = state.input;
    const needs = resolveNeeds(def, state);

    const ac = new AbortController();
    const watcher = startCancelWatcher({
      store,
      runId,
      stepId,
      attempt,
      ac,
      intervalMs: CANCEL_POLL_INTERVAL_MS,
    });
    // Shares the watcher's controller: one signal, two reasons.
    const deadline =
      def.timeoutMs === undefined
        ? undefined
        : startDeadline({
            runId,
            stepId,
            attempt,
            timeoutMs: def.timeoutMs,
            ac,
          });
    const ctxArgs = {
      runId,
      stepId,
      attempt,
      input,
      store,
      clock,
      signal: ac.signal,
      emitLog: deps.emitLog,
    };

    let abortedHere = false;
    const settle = async (output: Json) => {
      const resolved = resolveExecutionFact({
        postState: await store.loadRunState(runId),
        runId,
        stepId,
        attempt,
        output,
        at: clock.now(),
      });
      abortedHere = resolved.abortedHere;
      return { output, fact: resolved.fact };
    };

    const inTx = (body: (ctx: StepCtx<unknown>) => Promise<Json>) =>
      store.runStep<Json>(runId, stepId, attempt, async (tx) =>
        settle(await body(makeStepCtx({ ...ctxArgs, tx }))),
      );

    try {
      let output: Json;
      switch (def.kind) {
        case "task":
          output = await inTx((ctx) => def.run({ input, needs, ctx }));
          break;
        case "streaming":
          output = await inTx((base) =>
            withEmit(runId, stepId, base, (ctx) =>
              def.run({ input, needs, ctx }),
            ),
          );
          break;
        // The body runs OUTSIDE the durable transaction (RFC 0013): an external
        // effect must not hold a tx for minutes. Only the terminal fact commits,
        // in a short runStep tx, through the same settle as a task.
        case "activity": {
          const ctx = makeActivityCtx(ctxArgs);
          const out = await def.run({ input, needs, ctx });
          output = await store.runStep<Json>(runId, stepId, attempt, () =>
            settle(out),
          );
          break;
        }
      }
      return abortedHere ? { tag: "parked" } : { tag: "completed", output };
    } catch (err) {
      throw unwrapDeadline(err, ac.signal);
    } finally {
      await watcher.stop();
      deadline?.stop();
    }
  }

  async function executeSubflow(args: {
    def: SubflowDef;
    runId: RunId;
    stepId: string;
    state: RunState;
  }): Promise<Dispatched> {
    const { def, runId, stepId, state } = args;
    const child = deps.lookupFlow(def.childFlowId);
    if (child === undefined) {
      throw new Error(
        `Subflow step "${stepId}" references child flow "${def.childFlowId}" which is not registered with nagi(). Pass it to flows[].`,
      );
    }
    // generation = how many times this step has been reset (replayed). It is
    // unchanged by lease-reap / redelivery, so every re-dispatch of the same
    // logical spawn re-derives the SAME child id (idempotent re-attach); a
    // replay (nagi#6) bumps it and gets a fresh child. See deriveChildRunId.
    const generation = state.resetCounts[stepId] ?? 0;
    const childRunId = await deriveChildRunId({ runId, stepId, generation });

    // Re-entrant: a re-dispatch (lease-reap, or recovery after a lost wake) of a
    // parked subflow step whose child has ALREADY finished settles the parent
    // from the child's outcome instead of re-spawning — which would park forever
    // waiting on a wake that already fired. Idempotent with the in-process wake.
    const childState = await deps.store.loadRunState(childRunId);
    if (isTerminalRun(childState)) {
      await progression.wakeParentFromChild(childRunId);
      return { tag: "parked" };
    }

    // First spawn, or re-attach to an in-flight child (deterministic id +
    // tryStartRun existence-check ⇒ idempotent), then park.
    const parentInput = state.input;
    const needs = resolveNeeds(def, state);
    const childInput = def.buildInput({ input: parentInput, needs });
    await deps.startChildRun({
      child,
      childInput,
      parent: { runId, stepId },
      generation,
    });
    return { tag: "parked" };
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
    const needs = resolveNeeds(def, state);

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
    const policy = resolveRetry(handler?.retry, deps.defaultRetry);
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
          flowId: flow.id,
        });
        return { tag: "parked" };
      }
      case "failed": {
        const at = clock.now();
        await store.appendFact(
          runId,
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
      case "flowCanceled": {
        await store.appendFact(
          runId,
          Facts.stepCanceled(runId, stepId, attempt, clock.now(), error),
        );
        await progression.terminate(
          { kind: "resolved", runId, flow },
          {
            tag: "canceled",
            cause: {
              kind: "concurrency",
              canceledByRunId: outcome.canceledByRunId,
              concurrencyKey: outcome.concurrencyKey,
            },
          },
        );
        return { tag: "parked" };
      }
    }
  }

  return { dispatchMessage };
}
