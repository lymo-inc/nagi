import type { DispatchDeps, SubflowChildOutcome } from "../dispatch";
import { Facts } from "../facts";
import {
  type MatchPromotion,
  nextTransition,
  type SkipDecision,
  stepStateOf,
} from "../scheduler";
import {
  isTerminalRun,
  type RunCancelCause,
  runStatusOf,
  stepStatusOf,
} from "../state";
import type {
  AttemptNumber,
  Flow,
  Json,
  RunId,
  RunState,
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
  propagateToParent(
    childRunId: RunId,
    outcome: SubflowChildOutcome,
  ): Promise<void>;
  wakeParentFromChild(childRunId: RunId): Promise<void>;
}

// Reconstruct a child's subflow outcome from its PERSISTED terminal phase, for
// the re-entrant wake path (a re-dispatched subflow step whose child already
// finished). undefined ⇒ the child is not terminal yet, so there is nothing to
// propagate. Mirrors the outcomes the in-process finalize/cancel paths pass.
function subflowOutcomeOf(
  childRunId: RunId,
  child: RunState,
): SubflowChildOutcome | undefined {
  switch (child.phase.tag) {
    case "completed":
      return { kind: "completed", output: child.phase.output };
    case "failed":
      return { kind: "failed", error: child.phase.error };
    case "canceled":
      return {
        kind: "canceled",
        error: cancelCauseToError(childRunId, child.phase.cause),
      };
    default:
      return undefined;
  }
}

function cancelCauseToError(
  runId: RunId,
  cause: RunCancelCause,
): SerializedError {
  switch (cause.kind) {
    case "concurrency":
      return {
        name: "NagiCanceledError",
        message: `Run ${runId} was canceled (superseded by ${cause.canceledByRunId})`,
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
    case "operator":
      return {
        name: "NagiCanceledError",
        message: `Run ${runId} was canceled by ${cause.actor}: ${cause.reason}`,
      };
  }
}

export function makeProgression(deps: DispatchDeps, hooks: Hooks): Progression {
  const { fireHook } = hooks;

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
          for (const stepId of t.runnable)
            await queue.enqueue(runId, stepId, { flowId: flow.id });
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
      await store.appendFact(
        runId,
        Facts.stepSkipped({ runId, stepId, reason, at: clock.now() }),
      );
    }
  }

  // match/subflow settlements route through here; the task path writes its own
  // fact inside the runStep tx and does not.
  async function markStepSettled(args: {
    readonly flow: Flow;
    readonly runId: RunId;
    readonly stepId: StepId;
    readonly attempt: AttemptNumber;
    readonly stepKind: "match" | "subflow";
    readonly settlement: StepSettlement;
  }): Promise<void> {
    const { flow, runId, stepId, attempt, stepKind, settlement } = args;
    const at = deps.clock.now();
    const base = {
      runId,
      flowId: flow.id,
      stepId,
      attempt,
      kind: stepKind,
      at,
    };
    if (settlement.kind === "complete") {
      await deps.store.settleStep(
        runId,
        stepId,
        Facts.stepCompleted(runId, stepId, attempt, settlement.output, at),
      );
      await fireHook(
        deps.hooks?.onStepComplete,
        { ...base, output: settlement.output },
        "onStepComplete",
      );
    } else {
      await deps.store.settleStep(
        runId,
        stepId,
        Facts.stepFailed(runId, stepId, attempt, settlement.error, at),
      );
      await fireHook(
        deps.hooks?.onStepError,
        { ...base, error: settlement.error },
        "onStepError",
      );
    }
  }

  async function applyPromotions(
    flow: Flow,
    runState: RunState,
    promotions: readonly MatchPromotion[],
  ): Promise<void> {
    for (const { matchId, attempt, result } of promotions) {
      await markStepSettled({
        flow,
        runId: runState.runId,
        stepId: matchId,
        attempt,
        stepKind: "match",
        settlement: result,
      });
    }
  }

  async function finalizeFlowFailure(args: {
    readonly flow: Flow;
    readonly runId: RunId;
    readonly error: SerializedError;
  }): Promise<void> {
    const { flow, runId, error } = args;
    const { store, clock } = deps;
    await store.appendFact(runId, Facts.flowFailed(runId, error, clock.now()));
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
    await store.appendFact(
      runId,
      Facts.flowCompleted(runId, output, clock.now()),
    );
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

    const parentFlow = await deps.flowFor(parentRunId);

    await markStepSettled({
      flow: parentFlow,
      runId: parentRunId,
      stepId: parentStepId,
      attempt,
      stepKind: "subflow",
      settlement:
        outcome.kind === "completed"
          ? { kind: "complete", output: { childRunId, output: outcome.output } }
          : { kind: "fail", error: outcome.error },
    });

    await advance(parentRunId);
  }

  // Re-entrant wake: a parked subflow step was re-dispatched (lease-reap, or a
  // recovery after a lost in-process wake) and its child is ALREADY terminal.
  // Derive the outcome from the child's persisted phase and route it through
  // the same guarded propagateToParent — idempotent with the in-process wake
  // (the awaitingChild guard makes the loser a no-op).
  async function wakeParentFromChild(childRunId: RunId): Promise<void> {
    const child = await deps.store.loadRunState(childRunId);
    const outcome = subflowOutcomeOf(childRunId, child);
    if (outcome === undefined) return;
    await propagateToParent(childRunId, outcome);
  }

  return { advance, propagateToParent, wakeParentFromChild };
}
