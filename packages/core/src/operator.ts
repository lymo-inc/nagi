import type { Dispatcher } from "./dispatch";
import { NagiRuntimeError, validationError } from "./errors";
import { Facts } from "./facts";
import type { FlowRegistry } from "./flows";
import { compact, type EmitLog } from "./internal";
import { descendantsOf, stepStateOf } from "./scheduler";
import {
  attemptOf,
  isStepTerminal,
  isTerminalRun,
  runStatusOf,
  stepStatusOf,
} from "./state";
import type {
  CancelArgs,
  Clock,
  Operator,
  OperatorAuditOpts,
  RunId,
  StepId,
  Store,
} from "./types";

export interface OperatorDeps {
  readonly dispatcher: Dispatcher;
  readonly store: Store;
  readonly clock: Clock;
  readonly registry: FlowRegistry;
  readonly cancelRunRecursive: (
    runId: RunId,
    args: CancelArgs,
  ) => Promise<void>;
  readonly emitLog: EmitLog;
}

// Wall-clock bounds (not the injected clock): operator.retry waits on a real
// handler that may be ignoring ctx.signal, so these must elapse in real time.
const ABORT_SETTLE_DEADLINE_MS = 30_000;
const ABORT_SETTLE_POLL_MS = 50;

function requireActor(method: string, actor: string): void {
  if (typeof actor !== "string" || actor.length === 0) {
    throw validationError(`${method}: opts.actor must be a non-empty string`, [
      "actor",
    ]);
  }
}

export function makeOperator(o: OperatorDeps): Operator {
  const { dispatcher, store, clock, registry, cancelRunRecursive, emitLog } = o;

  async function skip(
    runId: RunId,
    stepId: StepId,
    opts: OperatorAuditOpts,
  ): Promise<void> {
    requireActor("operator.skip", opts.actor);
    const state = await store.loadRunState(runId);
    const flow = registry.require(state.flowId);
    if (!(stepId in flow.steps)) {
      throw validationError(
        `operator.skip: step "${stepId}" is not a step in flow "${flow.id}".`,
        ["stepId"],
      );
    }
    const stepState = stepStateOf(state, stepId);
    if (isStepTerminal(stepState)) {
      emitLog({
        level: "info",
        msg: "nagi: operator.skip noop — step already terminal",
        attrs: { runId, stepId, status: stepStatusOf(stepState) },
      });
      return;
    }
    if (isTerminalRun(state)) {
      throw new NagiRuntimeError(
        `operator.skip: run ${runId} is already terminal (${runStatusOf(state)}); cannot skip step "${stepId}".`,
      );
    }
    await store.appendFact(
      runId,
      Facts.stepSkipped({
        runId,
        stepId,
        at: clock.now(),
        reason: "manual",
        actor: opts.actor,
        ...compact({ note: opts.note }),
      }),
    );
    await dispatcher.advance(runId);
  }

  async function waitForStepToSettle(
    runId: RunId,
    stepId: StepId,
    attempt: number,
    deadlineMs: number,
  ): Promise<void> {
    const start = Date.now();
    while (Date.now() - start < deadlineMs) {
      const s = await store.loadRunState(runId);
      const ss = s.steps[stepId];
      if (ss === undefined) return;
      if (isStepTerminal(ss)) return;
      if (isTerminalRun(s)) return;
      if (attemptOf(ss) > attempt) return;
      await new Promise((r) => setTimeout(r, ABORT_SETTLE_POLL_MS));
    }
    throw new NagiRuntimeError(
      `operator.retry: timed out after ${deadlineMs}ms waiting for step "${stepId}" ` +
        `(attempt ${attempt}) to honor abort signal. Handler may be ignoring ctx.signal.`,
    );
  }

  async function retry(
    runId: RunId,
    stepId: StepId,
    opts: OperatorAuditOpts,
  ): Promise<void> {
    requireActor("operator.retry", opts.actor);
    const state = await store.loadRunState(runId);
    if (state.phase.tag === "canceled") {
      throw new NagiRuntimeError(
        `operator.retry: run ${runId} is canceled; cannot retry. Start a new run instead.`,
      );
    }
    const flow = registry.require(state.flowId);
    if (!(stepId in flow.steps)) {
      throw validationError(
        `operator.retry: step "${stepId}" is not a step in flow "${flow.id}".`,
        ["stepId"],
      );
    }
    const stepState = stepStateOf(state, stepId);

    if (stepState.tag === "running") {
      await store.appendFact(
        runId,
        Facts.stepAbortRequested({
          runId,
          stepId,
          attempt: stepState.attempt,
          at: clock.now(),
          actor: opts.actor,
          ...compact({ note: opts.note }),
        }),
      );
      await waitForStepToSettle(
        runId,
        stepId,
        stepState.attempt,
        ABORT_SETTLE_DEADLINE_MS,
      );
    }

    const cascade = descendantsOf(flow, stepId);
    const at = clock.now();
    for (const id of cascade) {
      const fact =
        id === stepId
          ? Facts.stepReset({
              runId,
              stepId: id,
              at,
              actor: opts.actor,
              ...compact({ note: opts.note }),
            })
          : Facts.stepReset({ runId, stepId: id, at, cascadedFrom: stepId });
      await store.appendFact(runId, fact);
    }
    await dispatcher.advance(runId);
  }

  async function abort(runId: RunId, opts: OperatorAuditOpts): Promise<void> {
    requireActor("operator.abort", opts.actor);
    await cancelRunRecursive(runId, {
      cause: "operator",
      reason: opts.note ?? `aborted by operator ${opts.actor}`,
      actor: opts.actor,
      ...compact({ note: opts.note }),
    });
  }

  return { skip, retry, abort };
}
