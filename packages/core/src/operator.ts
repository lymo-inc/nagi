import type { Dispatcher } from "./dispatch";
import { NagiRuntimeError, validationError } from "./errors";
import type { FlowRegistry } from "./flow-registry";
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
  Fact,
  Operator,
  OperatorAuditOpts,
  OperatorSkipOpts,
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
    opts: OperatorSkipOpts,
  ): Promise<void> {
    requireActor("operator.skip", opts.actor);
    const cascade = opts.cascade ?? "skip";
    const state = await store.loadRunState(runId);
    const flow = registry.requireForRun(state.flowId, runId);
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
    const fact: Fact = {
      kind: "step.skipped",
      runId,
      at: clock.now(),
      stepId,
      reason: "manual",
      actor: opts.actor,
      cascade,
      ...compact({ note: opts.note }),
    };
    await store.appendFact(runId, fact);
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
    const flow = registry.requireForRun(state.flowId, runId);
    if (!(stepId in flow.steps)) {
      throw validationError(
        `operator.retry: step "${stepId}" is not a step in flow "${flow.id}".`,
        ["stepId"],
      );
    }
    const stepState = stepStateOf(state, stepId);

    if (stepState.tag === "running") {
      const abortFact: Fact = {
        kind: "step.abort-requested",
        runId,
        at: clock.now(),
        stepId,
        attempt: stepState.attempt,
        actor: opts.actor,
        ...compact({ note: opts.note }),
      };
      await store.appendFact(runId, abortFact);
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
      const fact: Fact =
        id === stepId
          ? {
              kind: "step.reset",
              runId,
              at,
              stepId: id,
              actor: opts.actor,
              ...compact({ note: opts.note }),
            }
          : {
              kind: "step.reset",
              runId,
              at,
              stepId: id,
              cascadedFrom: stepId,
            };
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
