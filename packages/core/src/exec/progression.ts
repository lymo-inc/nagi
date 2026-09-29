import type { DispatchDeps, EndingRun, RunEnd } from "../dispatch";
import { NagiCanceledError } from "../errors";
import { Facts, runCancelCause } from "../facts";
import { type FlowResolution, requireCurrent } from "../flows";
import { nextTransition, type SkipDecision } from "../scheduler";
import {
  isTerminalRun,
  type RunCancelCause,
  runStatusOf,
  stepStateOf,
  stepStatusOf,
  type TerminalPhase,
} from "../state";
import type {
  AttemptNumber,
  CancelArgs,
  Flow,
  FlowCanceledByConcurrencyFact,
  FlowCompletedFact,
  FlowFailedFact,
  Json,
  RunId,
  SerializedError,
  StepId,
} from "../types";
import type { Hooks } from "./hooks";

const MAX_ADVANCE_ITERS = 1024;

type StepSettlement =
  | { readonly kind: "complete"; readonly output: Json }
  | { readonly kind: "fail"; readonly error: SerializedError };

export interface Progression {
  advance(runId: RunId): Promise<void>;
  terminate(run: EndingRun, end: RunEnd): Promise<void>;
  settleSuperseded(
    run: EndingRun,
    fact: FlowCanceledByConcurrencyFact,
  ): Promise<void>;
  cancel(runId: RunId, args: CancelArgs): Promise<void>;
  wakeParentFromChild(childRunId: RunId): Promise<void>;
}

// The one cause→error conversion: the flow-error hooks and a woken parent's
// subflow step see the same error whichever path ended the run.
function terminalErrorOf(
  runId: RunId,
  end: Exclude<TerminalPhase, { readonly tag: "completed" }>,
): SerializedError {
  return end.tag === "failed"
    ? end.error
    : cancelCauseToError(runId, end.cause);
}

function cancelCauseToError(
  runId: RunId,
  cause: RunCancelCause,
): SerializedError {
  switch (cause.kind) {
    case "concurrency":
      return {
        name: "NagiCanceledError",
        message: new NagiCanceledError({ runId, ...cause }).message,
        cause: {
          canceledByRunId: cause.canceledByRunId,
          concurrencyKey: cause.concurrencyKey,
        },
      };
    case "explicit":
      return {
        name: "NagiCanceledError",
        message: `Run ${runId} was canceled: ${cause.reason}`,
      };
  }
}

function endFact(
  runId: RunId,
  end: RunEnd,
  at: Date,
): FlowCompletedFact | FlowFailedFact | FlowCanceledByConcurrencyFact {
  switch (end.tag) {
    case "completed":
      return Facts.flowCompleted(runId, end.output, at);
    case "failed":
      return Facts.flowFailed(runId, end.error, at);
    case "canceled":
      return Facts.flowCanceledByConcurrency({
        runId,
        at,
        canceledByRunId: end.cause.canceledByRunId,
        concurrencyKey: end.cause.concurrencyKey,
      });
  }
}

export function endingRunOf(
  runId: RunId,
  resolution: FlowResolution,
): EndingRun {
  return resolution.kind === "current"
    ? { kind: "resolved", runId, flow: resolution.flow }
    : { kind: "gone", runId, flowId: resolution.error.flowId };
}

export function makeProgression(deps: DispatchDeps, hooks: Hooks): Progression {
  const { fireHook } = hooks;

  async function advance(runId: RunId): Promise<void> {
    const { store, queue } = deps;
    const flow = requireCurrent(await deps.flowOf(runId));

    for (let iter = 0; iter < MAX_ADVANCE_ITERS; iter++) {
      const runState = await store.loadRunState(runId);
      const t = nextTransition(flow, runState);

      switch (t.kind) {
        case "settled":
        case "waiting":
          return;
        case "complete":
          await terminate(
            { kind: "resolved", runId, flow },
            { tag: "completed", output: t.output },
          );
          return;
        case "fail":
          await terminate(
            { kind: "resolved", runId, flow },
            { tag: "failed", error: t.error },
          );
          return;
        case "dispatch":
          // A skip can unblock an optional() dependent: settle skips, then re-scan,
          // and enqueue only from a pass with nothing left to skip.
          if (t.skip.length > 0) {
            await recordSkips(runId, t.skip);
            continue;
          }
          for (const stepId of t.runnable)
            await queue.enqueue(runId, stepId, { flowId: flow.id });
          return;
        case "skip":
          await recordSkips(runId, t.skip);
          continue;
      }
    }

    const cycleError: SerializedError = {
      name: "NagiCycleError",
      message: `advance exceeded ${MAX_ADVANCE_ITERS} iterations — likely a cycle or infinite skip loop in flow "${flow.id}"`,
    };
    await terminate(
      { kind: "resolved", runId, flow },
      { tag: "failed", error: cycleError },
    );
  }

  async function recordSkips(
    runId: RunId,
    skip: readonly SkipDecision[],
  ): Promise<void> {
    const { store, clock } = deps;
    for (const { stepId, reason } of skip) {
      await store.appendFact(
        runId,
        Facts.stepSkipped({ runId, stepId, reason, at: clock.now() }),
      );
    }
  }

  // The task path writes its own settle fact inside the runStep tx; a subflow
  // step settles here, when its child run ends.
  async function settleSubflowStep(args: {
    readonly flow: Flow;
    readonly runId: RunId;
    readonly stepId: StepId;
    readonly attempt: AttemptNumber;
    readonly settlement: StepSettlement;
  }): Promise<void> {
    const { flow, runId, stepId, attempt, settlement } = args;
    const at = deps.clock.now();
    const base = {
      runId,
      flowId: flow.id,
      stepId,
      attempt,
      kind: "subflow" as const,
      at,
    };
    if (settlement.kind === "complete") {
      await deps.store.appendFact(
        runId,
        Facts.stepCompleted(runId, stepId, attempt, settlement.output, at),
      );
      await fireHook(
        deps.hooks?.onStepComplete,
        { ...base, output: settlement.output },
        "onStepComplete",
      );
    } else {
      await deps.store.appendFact(
        runId,
        Facts.stepFailed(runId, stepId, attempt, settlement.error, at),
      );
      await fireHook(
        deps.hooks?.onStepError,
        { ...base, error: settlement.error },
        "onStepError",
      );
    }
  }

  // Every run end is fact → flow hooks → parent wake, and only the writer
  // whose fact the store admitted goes on to the hooks and the wake. terminate
  // owns all three; settleSuperseded and cancel differ only in who writes the
  // fact and (cancel) the child cascade before the wake.
  async function terminate(run: EndingRun, end: RunEnd): Promise<void> {
    const at = deps.clock.now();
    if (!(await deps.store.endRun(run.runId, endFact(run.runId, end, at)))) {
      deps.emitLog({
        level: "info",
        msg: "nagi: run end skipped — run already terminal",
        attrs: { runId: run.runId, end: end.tag },
      });
      return;
    }
    await fireTerminalHooks(run, end, at);
    await propagateToParent(run.runId, end);
  }

  async function settleSuperseded(
    run: EndingRun,
    fact: FlowCanceledByConcurrencyFact,
  ): Promise<void> {
    const end: TerminalPhase = { tag: "canceled", cause: runCancelCause(fact) };
    await fireTerminalHooks(run, end, fact.at);
    await propagateToParent(run.runId, end);
  }

  async function cancel(runId: RunId, args: CancelArgs): Promise<void> {
    const { store, clock } = deps;
    const run = endingRunOf(runId, await deps.flowOf(runId));
    const at = clock.now();
    if (!(await store.endRun(runId, Facts.flowCanceled(runId, args, at)))) {
      deps.emitLog({
        level: "info",
        msg: "nagi: cancel skipped — run already terminal",
        attrs: { runId, status: runStatusOf(await store.loadRunState(runId)) },
      });
      return;
    }
    const end: TerminalPhase = { tag: "canceled", cause: runCancelCause(args) };
    await fireTerminalHooks(run, end, at);

    // Before the parent wake: this run is already terminal, so each child's
    // own wake is a no-op, and a throwing wake cannot strand live children.
    for (const childId of await store.listChildren(runId)) {
      await cancel(childId, {
        cause: "explicit",
        reason: `parent ${runId} canceled: ${args.reason}`,
        note: `cascade from parent ${runId}`,
      });
    }

    await propagateToParent(runId, end);
  }

  async function fireTerminalHooks(
    run: EndingRun,
    end: TerminalPhase,
    at: Date,
  ): Promise<void> {
    const { runId } = run;
    const flow = run.kind === "resolved" ? run.flow : undefined;
    const flowId = run.kind === "resolved" ? run.flow.id : run.flowId;
    if (end.tag === "completed") {
      const event = { runId, flowId, output: end.output, at };
      await fireHook(flow?.onComplete, event, "flow.onComplete");
      await fireHook(deps.hooks?.onFlowComplete, event, "onFlowComplete");
      return;
    }
    const event = { runId, flowId, error: terminalErrorOf(runId, end), at };
    await fireHook(flow?.onError, event, "flow.onError");
    await fireHook(deps.hooks?.onFlowError, event, "onFlowError");
  }

  // Idempotent: only a parent step still awaitingChild is settled, so a
  // second wake (re-entrant wakeParentFromChild, lease-reap) is a no-op.
  async function propagateToParent(
    childRunId: RunId,
    end: TerminalPhase,
  ): Promise<void> {
    const { store } = deps;
    const childState = await store.loadRunState(childRunId);
    if (childState.parent === undefined) {
      return;
    }
    const { runId: parentRunId, stepId: parentStepId } = childState.parent;

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

    const parentFlow = requireCurrent(await deps.flowOf(parentRunId));

    await settleSubflowStep({
      flow: parentFlow,
      runId: parentRunId,
      stepId: parentStepId,
      attempt,
      settlement:
        end.tag === "completed"
          ? { kind: "complete", output: { childRunId, output: end.output } }
          : { kind: "fail", error: terminalErrorOf(childRunId, end) },
    });

    await advance(parentRunId);
  }

  // Re-entrant wake: a parked subflow step was re-dispatched (lease-reap, or a
  // recovery after a lost in-process wake) and its child is ALREADY terminal.
  // Wakes from the child's persisted phase through the same guarded
  // propagateToParent as the in-process wake.
  async function wakeParentFromChild(childRunId: RunId): Promise<void> {
    const { phase } = await deps.store.loadRunState(childRunId);
    if (phase.tag === "pending" || phase.tag === "running") return;
    await propagateToParent(childRunId, phase);
  }

  return { advance, terminate, settleSuperseded, cancel, wakeParentFromChild };
}
