import type { Dispatcher } from "./dispatch";
import {
  NagiCanceledError,
  NagiRuntimeError,
  serializeError,
  validationError,
} from "./errors";
import type { Hooks } from "./exec/hooks";
import { Facts } from "./facts";
import type { FlowRegistry } from "./flow-registry";
import { compact, type EmitLog } from "./internal";
import { deriveChildRunId } from "./run-id";
import { nextTransition } from "./scheduler";
import { foldRun, isTerminalRun, runStatusOf } from "./state";
import type {
  CancelArgs,
  Clock,
  ConcurrencyMode,
  Flow,
  FlowCanceledByConcurrencyFact,
  FlowErrorEvent,
  FlowHooks,
  FlowStartEvent,
  FlowStartedFact,
  Json,
  ParentRef,
  Queue,
  RunId,
  SerializedError,
  StepId,
  Store,
  Tx,
} from "./types";
import { validate } from "./validate";

export interface RunLifecycleDeps {
  readonly store: Store;
  readonly queue: Queue;
  readonly clock: Clock;
  readonly registry: FlowRegistry;
  readonly codeVersion: string;
  readonly hashFor: (flowId: string) => string | undefined;
  readonly queueForTx: (tx: Tx) => Queue;
  readonly hooks: Hooks;
  readonly flowHooks: FlowHooks | undefined;
  readonly dispatcher: Pick<Dispatcher, "advance" | "propagateToParent">;
  readonly emitLog: EmitLog;
}

// The one fork: the run row + flow.started fact either commit on their own or
// ride the caller's transaction. Resolved once in resolveBoundary; nothing
// past it looks at the boundary again.
export type TxBoundary =
  | { readonly kind: "own" }
  | { readonly kind: "caller"; readonly tx: Tx };

// How the initial step dispatch is settled. "enqueue" is still owed (own tx:
// fired after the start hooks, matching onFlowStart-before-onStepStart);
// "enqueued" already rode the caller's tx; "advance" means the seed state was
// not a plain dispatch (when-false roots, empty flow) and needs the
// store-driven progression loop once the row is committed.
export type InitialDispatch =
  | { readonly kind: "enqueue"; readonly steps: readonly StepId[] }
  | { readonly kind: "enqueued"; readonly steps: readonly StepId[] }
  | { readonly kind: "advance" };

// Post-commit effects of a started run, as data: applied exactly once by
// applyEffects, immediately (own tx) or from the caller's applyOnCommit.
export interface StartEffects {
  readonly superseded: readonly FlowErrorEvent[];
  readonly started: FlowStartEvent;
  readonly dispatch: InitialDispatch;
}

export type StagedStart =
  | { readonly kind: "exists" }
  | { readonly kind: "started"; readonly effects: StartEffects };

export interface StageArgs {
  readonly flow: Flow;
  readonly validatedInput: Json;
  readonly runId: RunId;
  readonly parent: ParentRef | undefined;
  readonly boundary: TxBoundary;
}

export interface RunLifecycle {
  start(args: {
    readonly flowId: string;
    readonly input: unknown;
    readonly runId: RunId | undefined;
    readonly boundary: TxBoundary;
  }): Promise<{ readonly runId: RunId; readonly staged: StagedStart }>;
  stage(args: StageArgs): Promise<StagedStart>;
  applyEffects(effects: StartEffects): Promise<void>;
  startChildRun(args: {
    readonly child: Flow;
    readonly childInput: unknown;
    readonly parent: ParentRef;
    readonly generation: number;
  }): Promise<RunId>;
  cancelRunRecursive(runId: RunId, args: CancelArgs): Promise<void>;
}

type Concurrency = { readonly key: string; readonly mode: ConcurrencyMode };
type TryStartResult = Awaited<ReturnType<Store["tryStartRun"]>>;

const EXISTS: StagedStart = { kind: "exists" };
const ADVANCE: InitialDispatch = { kind: "advance" };

export function makeRunLifecycle(deps: RunLifecycleDeps): RunLifecycle {
  const { store, queue, clock, registry, hooks, flowHooks, dispatcher } = deps;

  function resolveBoundary(boundary: TxBoundary): {
    tryStart(
      runId: RunId,
      fact: FlowStartedFact,
      concurrency: Concurrency | undefined,
    ): Promise<TryStartResult>;
    seed(
      runId: RunId,
      flowId: string,
      steps: readonly StepId[],
    ): Promise<InitialDispatch>;
  } {
    switch (boundary.kind) {
      case "own":
        return {
          tryStart: (runId, fact, concurrency) =>
            store.tryStartRun(runId, fact, concurrency),
          seed: async (_runId, _flowId, steps) => ({ kind: "enqueue", steps }),
        };
      case "caller":
        return {
          tryStart: (runId, fact, concurrency) =>
            store.tryStartRunOnTx(boundary.tx, runId, fact, concurrency),
          seed: async (runId, flowId, steps) => {
            const txQueue = deps.queueForTx(boundary.tx);
            for (const stepId of steps) {
              await txQueue.enqueue(runId, stepId, { flowId });
            }
            return { kind: "enqueued", steps };
          },
        };
    }
  }

  async function stage(args: StageArgs): Promise<StagedStart> {
    const { flow, validatedInput, runId, parent } = args;
    const { tryStart, seed } = resolveBoundary(args.boundary);

    const startedAt = clock.now();
    const fact = Facts.flowStarted({
      runId,
      flowId: flow.id,
      input: validatedInput,
      at: startedAt,
      codeVersion: deps.codeVersion,
      ...compact({
        flowHash: deps.hashFor(flow.id),
        parent:
          parent !== undefined
            ? { runId: parent.runId, stepId: parent.stepId }
            : undefined,
      }),
    });
    const concurrency = concurrencyOf(flow, validatedInput);

    const { started, canceled } = await tryStart(runId, fact, concurrency);
    if (!started) return EXISTS;

    // The fresh run is not visible outside its tx yet, so the initial
    // transition is computed off the in-memory [fact] — the same answer the
    // dispatcher would fold post-commit.
    const t = nextTransition(flow, foldRun(runId, [fact]));
    const dispatch =
      t.kind === "dispatch" && t.skip.length === 0
        ? await seed(runId, flow.id, t.runnable)
        : ADVANCE;

    return {
      kind: "started",
      effects: {
        superseded: canceled.map((c) => supersededEvent(flow.id, runId, c)),
        started: {
          runId,
          flowId: flow.id,
          input: validatedInput,
          at: startedAt,
          ...compact({ parent }),
        },
        dispatch,
      },
    };
  }

  async function applyEffects(effects: StartEffects): Promise<void> {
    const { superseded, started, dispatch } = effects;
    const flow = registry.require(started.flowId);

    for (const event of superseded) {
      await hooks.fireHook(flow.onError, event, "flow.onError");
      await hooks.fireHook(flowHooks?.onFlowError, event, "onFlowError");
      await dispatcher.propagateToParent(event.runId, {
        kind: "canceled",
        error: event.error,
      });
    }

    await hooks.fireHook(flow.onStart, started, "flow.onStart");
    await hooks.fireHook(flowHooks?.onFlowStart, started, "onFlowStart");

    switch (dispatch.kind) {
      case "enqueue":
        for (const stepId of dispatch.steps) {
          await queue.enqueue(started.runId, stepId, { flowId: flow.id });
        }
        return;
      case "enqueued":
        return;
      case "advance":
        await dispatcher.advance(started.runId);
        return;
    }
  }

  async function start(args: {
    readonly flowId: string;
    readonly input: unknown;
    readonly runId: RunId | undefined;
    readonly boundary: TxBoundary;
  }): Promise<{ readonly runId: RunId; readonly staged: StagedStart }> {
    const flow = registry.require(args.flowId);
    const runId = resolveRunId(args.runId);
    const validatedInput = (await validate(flow.input, args.input)) as Json;
    const staged = await stage({
      flow,
      validatedInput,
      runId,
      parent: undefined,
      boundary: args.boundary,
    });
    return { runId, staged };
  }

  async function startChildRun(args: {
    readonly child: Flow;
    readonly childInput: unknown;
    readonly parent: ParentRef;
    readonly generation: number;
  }): Promise<RunId> {
    const { child, childInput, parent, generation } = args;
    if (!registry.has(child.id)) {
      throw new NagiRuntimeError(
        `Subflow child "${child.id}" not registered with nagi(). Pass it to flows[].`,
      );
    }
    const validatedInput = (await validate(child.input, childInput)) as Json;
    // Deterministic per (parentRunId, stepId, generation) — INVARIANT under
    // attempt — so any at-least-once re-dispatch of this subflow step (redelivery,
    // lease-reap at attempt+1, durable child-wake) re-attaches to the existing
    // child instead of spawning a duplicate that cancel-in-progress would then
    // self-supersede (failing the parent). A replay (new generation) gets a
    // fresh child. See deriveChildRunId.
    const runId = await deriveChildRunId({
      runId: parent.runId,
      stepId: parent.stepId,
      generation,
    });
    // "exists" ⇒ this (parentRunId, stepId, generation) already spawned the
    // child on a prior dispatch. tryStartRun checks run existence before its
    // concurrency-cancel pass, so re-attaching cancels nothing and leaves the
    // original child (and its parent link) intact. Idempotent.
    const staged = await stage({
      flow: child,
      validatedInput,
      runId,
      parent,
      boundary: { kind: "own" },
    });
    if (staged.kind === "started") await applyEffects(staged.effects);
    return runId;
  }

  async function cancelRunRecursive(
    runId: RunId,
    args: CancelArgs,
  ): Promise<void> {
    const state = await store.loadRunState(runId);
    if (isTerminalRun(state)) {
      deps.emitLog({
        level: "info",
        msg: "nagi: cancel skipped — run already terminal",
        attrs: { runId, status: runStatusOf(state) },
      });
      return;
    }
    const flow = registry.get(state.flowId);
    await store.appendFact(runId, Facts.flowCanceled(runId, args, clock.now()));
    const error: SerializedError = {
      name: "NagiCanceledError",
      message: `Run ${runId} was canceled: ${args.reason}`,
    };
    if (flow !== undefined) {
      const event = { runId, flowId: flow.id, error, at: clock.now() };
      await hooks.fireHook(flow.onError, event, "flow.onError");
      await hooks.fireHook(flowHooks?.onFlowError, event, "onFlowError");
    }

    for (const childId of await store.listChildren(runId)) {
      await cancelRunRecursive(childId, {
        cause: "explicit",
        reason: `parent ${runId} canceled: ${args.reason}`,
        note:
          args.cause === "operator"
            ? `cascade from operator ${args.actor} aborting parent ${runId}`
            : `cascade from parent ${runId}`,
      });
    }

    await dispatcher.propagateToParent(runId, { kind: "canceled", error });
  }

  return { start, stage, applyEffects, startChildRun, cancelRunRecursive };
}

function resolveRunId(runId: RunId | undefined): RunId {
  if (runId === undefined) return `run-${crypto.randomUUID()}` as RunId;
  if (typeof runId !== "string" || runId.length === 0) {
    throw validationError("opts.runId must be a non-empty string", ["runId"]);
  }
  return runId;
}

function concurrencyOf(flow: Flow, input: Json): Concurrency | undefined {
  if (flow.concurrency === undefined) return undefined;
  const key = flow.concurrency.keyFn(input);
  if (typeof key !== "string" || key.length === 0) {
    throw validationError(
      `flow.concurrency.keyFn must return a non-empty string (got ${typeof key === "string" ? '""' : typeof key})`,
      ["concurrency", "keyFn"],
    );
  }
  return { key, mode: flow.concurrency.mode };
}

function supersededEvent(
  flowId: string,
  canceledByRunId: RunId,
  c: { readonly runId: RunId; readonly fact: FlowCanceledByConcurrencyFact },
): FlowErrorEvent {
  const cause = { canceledByRunId, concurrencyKey: c.fact.concurrencyKey };
  const error: SerializedError = {
    ...serializeError(new NagiCanceledError({ runId: c.runId, ...cause })),
    cause,
  };
  return { runId: c.runId, flowId, error, at: c.fact.at };
}
