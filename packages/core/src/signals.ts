import type { Dispatcher } from "./dispatch";
import {
  NagiRuntimeError,
  NagiSignalTimeoutError,
  serializeError,
} from "./errors";
import type { Hooks } from "./exec/hooks";
import { Facts } from "./facts";
import type { FlowRegistry } from "./flow-registry";
import {
  asStepMapWithDefs,
  compact,
  type EmitLog,
  getDef,
  type SignalDef,
} from "./internal";
import { isStepTerminal, stepStateOf } from "./state";
import type {
  AttemptNumber,
  Clock,
  Flow,
  FlowHooks,
  Json,
  RunId,
  RunState,
  SettleSignalResult,
  SignalBufferedFact,
  SignalReceivedFact,
  StepCompletedFact,
  StepFailedFact,
  StepId,
  Store,
} from "./types";
import { validate } from "./validate";

// The signal-reconciliation policy, owned by core instead of duplicated into
// every Store. An adapter loads run state under its per-run lock, calls this,
// persists the returned fact(s), and returns `result` — owning only the
// transaction boundary, never the deliver/buffer/noop decision.
export type SignalDecision =
  | { readonly kind: "noop"; readonly result: SettleSignalResult }
  | {
      readonly kind: "buffer";
      // null when an earlier signal is already parked (first one wins): the
      // adapter persists nothing but still reports "buffered".
      readonly fact: SignalBufferedFact | null;
      readonly result: SettleSignalResult;
    }
  | {
      readonly kind: "deliver";
      readonly attempt: AttemptNumber;
      readonly received: SignalReceivedFact;
      readonly completed: StepCompletedFact;
      readonly result: SettleSignalResult;
    };

const NOOP: SignalDecision = { kind: "noop", result: { tag: "noop" } };

export function decideSignal(args: {
  readonly runState: RunState;
  readonly stepId: StepId;
  readonly at: Date;
  // Absent/undefined is the worker-consume path (apply a buffered signal if one
  // exists); present is a wf.signal() delivery. Adapters pass it straight from
  // Store.settleSignal, so undefined is a first-class input, not just optional.
  readonly incoming?:
    | {
        readonly payload: Json;
        readonly signalName?: string;
      }
    | undefined;
}): SignalDecision {
  const { runState, stepId, at, incoming } = args;
  const { runId } = runState;
  const step = stepStateOf(runState, stepId);

  if (isStepTerminal(step)) return NOOP;

  if (step.tag === "awaitingSignal") {
    const source = incoming ?? runState.bufferedSignals[stepId];
    if (source === undefined) return NOOP;
    const { attempt } = step;
    return {
      kind: "deliver",
      attempt,
      received: Facts.signalReceived({
        runId,
        stepId,
        payload: source.payload,
        at,
        ...compact({ signalName: source.signalName }),
      }),
      completed: Facts.stepCompleted(
        runId,
        stepId,
        attempt,
        source.payload,
        at,
      ),
      result: {
        tag: "delivered",
        attempt,
        payload: source.payload,
        ...compact({ signalName: source.signalName }),
      },
    };
  }

  // Pre-awaiting (pending/running/backoff/aborting): park an incoming signal; a
  // consume call (no incoming) has nothing to apply yet.
  if (incoming === undefined) return NOOP;
  if (runState.bufferedSignals[stepId] !== undefined) {
    return { kind: "buffer", fact: null, result: { tag: "buffered" } };
  }
  return {
    kind: "buffer",
    fact: Facts.signalBuffered({
      runId,
      stepId,
      payload: incoming.payload,
      at,
      ...compact({ signalName: incoming.signalName }),
    }),
    result: { tag: "buffered" },
  };
}

// The timeout counterpart to decideSignal, owned by core so both stores share
// one decision (each owns only its tx/lock). A signal step resolves either by
// delivery (decideSignal → step.completed) or by deadline (here → step.failed);
// the scheduler then propagates the failure to flow.failed. Stale timers (the
// signal already landed, the run was canceled, or the step was reset) fold to a
// non-awaiting state → noop, and the caller deletes the row regardless.
export type TimeoutDecision =
  | { readonly kind: "noop" }
  | {
      readonly kind: "fail";
      readonly attempt: AttemptNumber;
      readonly fact: StepFailedFact;
    };

export function decideTimeout(args: {
  readonly runState: RunState;
  readonly stepId: StepId;
  readonly fireAt: Date;
  readonly at: Date;
}): TimeoutDecision {
  const { runState, stepId, fireAt, at } = args;
  const step = stepStateOf(runState, stepId);
  if (step.tag !== "awaitingSignal") return { kind: "noop" };
  const error = serializeError(
    new NagiSignalTimeoutError({ runId: runState.runId, stepId, fireAt }),
  );
  return {
    kind: "fail",
    attempt: step.attempt,
    fact: Facts.stepFailed(runState.runId, stepId, step.attempt, error, at),
  };
}

export interface SignalsDeps {
  readonly dispatcher: Dispatcher;
  readonly store: Store;
  readonly clock: Clock;
  readonly registry: FlowRegistry;
  readonly hooks: Hooks;
  readonly flowHooks?: FlowHooks;
  readonly emitLog: EmitLog;
}

export function makeSignals(deps: SignalsDeps): {
  signal(runId: RunId, signalName: string, payload: unknown): Promise<void>;
} {
  const { dispatcher, store, clock, registry, hooks, flowHooks, emitLog } =
    deps;

  async function signal(
    runId: RunId,
    signalName: string,
    payload: unknown,
  ): Promise<void> {
    const runState = await store.loadRunState(runId);
    const flow = registry.requireForRun(runState.flowId, runId);
    const resolved = resolveSignalStep(flow, signalName);
    if (resolved === null) {
      throw new NagiRuntimeError(
        `Flow "${flow.id}" has no signal step accepting "${signalName}".`,
      );
    }
    const { stepId, def } = resolved;
    const validated = (await validate(def.schema, payload)) as Json;
    const carriesAlias = signalName !== stepId;

    // Atomic under the store's per-run lock; parks a signal.buffered fact when
    // the step isn't claimed yet (the start/await race). The worker applies a
    // buffered signal when it claims the step — see dispatch's signal branch.
    const result = await store.settleSignal({
      runId,
      stepId,
      at: clock.now(),
      incoming: {
        payload: validated,
        ...(carriesAlias ? { signalName } : {}),
      },
    });

    switch (result.tag) {
      case "delivered":
        await hooks.fireHook(
          flowHooks?.onSignalReceived,
          {
            runId,
            flowId: flow.id,
            stepId,
            attempt: result.attempt,
            kind: "signal",
            payload: validated,
            at: clock.now(),
          },
          "onSignalReceived",
        );
        await dispatcher.advance(runId);
        return;
      case "buffered":
        emitLog({
          level: "info",
          msg: "nagi: signal buffered before step ready; will apply on dispatch",
          attrs: { runId, stepId, signalName },
        });
        return;
      case "noop":
        emitLog({
          level: "info",
          msg: "nagi: signal arrived after step resolved",
          attrs: { runId, stepId, signalName },
        });
        return;
    }
  }

  return { signal };
}

function resolveSignalStep(
  flow: Flow,
  signalName: string,
): { readonly stepId: StepId; readonly def: SignalDef } | null {
  const steps = asStepMapWithDefs(flow.steps);

  const direct = steps[signalName];
  if (direct !== undefined) {
    const d = getDef(direct);
    if (d.kind === "signal" && d.names === undefined) {
      return { stepId: signalName, def: d };
    }
  }

  for (const [id, s] of Object.entries(steps)) {
    const d = getDef(s);
    if (d.kind === "signal" && d.names?.includes(signalName)) {
      return { stepId: id, def: d };
    }
  }

  return null;
}
