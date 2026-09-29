import type { Dispatcher } from "./dispatch";
import { NagiRuntimeError, validationError } from "./errors";
import type { Hooks } from "./exec/hooks";
import { endingRunOf } from "./exec/progression";
import { Facts, foldRun } from "./facts";
import type { FlowOf, FlowRegistry } from "./flows";
import { compact } from "./internal";
import { deriveChildRunId } from "./run-id";
import { nextTransition, type SkipDecision } from "./scheduler";
import { stepStateOf } from "./state";
import type {
  Clock,
  ConcurrencyMode,
  Flow,
  FlowCanceledByConcurrencyFact,
  FlowHooks,
  FlowStartEvent,
  FlowStartedFact,
  Json,
  ParentLink,
  Queue,
  RunId,
  StepId,
  Store,
  Tx,
} from "./types";
import { validate } from "./validate";

export interface RunLifecycleDeps {
  readonly store: Store;
  readonly queue: Queue;
  readonly clock: Clock;
  // By flow id: the live code a new run starts on.
  readonly registry: FlowRegistry;
  readonly flowOf: FlowOf;
  readonly codeVersion: string;
  readonly queueForTx: (tx: Tx) => Queue;
  readonly hooks: Hooks;
  readonly flowHooks: FlowHooks | undefined;
  readonly dispatcher: Pick<Dispatcher, "advance" | "settleSuperseded">;
}

// The one fork: the run row + flow.started fact either commit on their own or
// ride the caller's transaction. Resolved once in resolveBoundary; nothing
// past it looks at the boundary again.
export type TxBoundary =
  | { readonly kind: "own" }
  | { readonly kind: "caller"; readonly tx: Tx };

// How the initial step dispatch is settled. "enqueued" already rode the
// start's own transaction (own tx: Store.tryStartRun's seed; caller tx:
// queueForTx) — only the when-false siblings remain to be recorded
// post-commit. "advance" hands the seed to the store-driven progression loop
// once the row is committed: own tx with when-false siblings (advance records
// the skips, then enqueues — once), or no runnable root at all on either
// boundary.
export type InitialDispatch =
  | {
      readonly kind: "enqueued";
      readonly steps: readonly StepId[];
      readonly skip: readonly SkipDecision[];
    }
  | { readonly kind: "advance" };

// Post-commit effects of a started run, as data: applied exactly once by
// applyEffects, immediately (own tx) or from the caller's applyOnCommit.
export interface SupersededRun {
  readonly runId: RunId;
  readonly fact: FlowCanceledByConcurrencyFact;
}

export interface StartEffects {
  readonly flow: Flow;
  readonly superseded: readonly SupersededRun[];
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
  readonly parent: ParentLink | undefined;
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
    readonly parent: ParentLink;
    readonly generation: number;
  }): Promise<RunId>;
}

type Concurrency = { readonly key: string; readonly mode: ConcurrencyMode };
type TryStartResult = Awaited<ReturnType<Store["tryStartRun"]>>;

const EXISTS: StagedStart = { kind: "exists" };
const ADVANCE: InitialDispatch = { kind: "advance" };

export function makeRunLifecycle(deps: RunLifecycleDeps): RunLifecycle {
  const {
    store,
    queue,
    clock,
    registry,
    flowOf,
    hooks,
    flowHooks,
    dispatcher,
  } = deps;

  function resolveBoundary(boundary: TxBoundary): {
    tryStart(
      runId: RunId,
      fact: FlowStartedFact,
      concurrency: Concurrency | undefined,
      steps: readonly StepId[],
    ): Promise<TryStartResult>;
  } {
    switch (boundary.kind) {
      case "own":
        return {
          tryStart: (runId, fact, concurrency, steps) =>
            store.tryStartRun(
              runId,
              fact,
              concurrency,
              steps.length > 0
                ? { queue, flowId: fact.flowId, steps }
                : undefined,
            ),
        };
      case "caller":
        return {
          tryStart: async (runId, fact, concurrency, steps) => {
            const result = await store.tryStartRunOnTx(
              boundary.tx,
              runId,
              fact,
              concurrency,
            );
            if (result.started && steps.length > 0) {
              const txQueue = deps.queueForTx(boundary.tx);
              for (const stepId of steps) {
                await txQueue.enqueue(runId, stepId, { flowId: fact.flowId });
              }
            }
            return result;
          },
        };
    }
  }

  async function stage(args: StageArgs): Promise<StagedStart> {
    const { flow, validatedInput, runId, parent } = args;
    const { tryStart } = resolveBoundary(args.boundary);

    const startedAt = clock.now();
    const fact = Facts.flowStarted({
      runId,
      flowId: flow.id,
      input: validatedInput,
      at: startedAt,
      codeVersion: deps.codeVersion,
      flowHash: registry.hashOf(flow.id),
      ...compact({
        parent,
      }),
    });
    const concurrency = concurrencyOf(flow, validatedInput);

    // The fresh run is not visible outside its tx yet, so the initial
    // transition is computed off the in-memory [fact] — the same answer the
    // dispatcher would fold post-commit. Pure, and fact already exists, so
    // this runs before tryStart: the seed steps ride the same transaction.
    const t = nextTransition(flow, foldRun(runId, [fact]));
    const steps = t.kind === "dispatch" ? t.runnable : [];

    const { started, canceled } = await tryStart(
      runId,
      fact,
      concurrency,
      steps,
    );
    if (!started) return EXISTS;

    const dispatch: InitialDispatch =
      t.kind === "dispatch"
        ? { kind: "enqueued", steps: t.runnable, skip: t.skip }
        : ADVANCE;

    return {
      kind: "started",
      effects: {
        flow,
        superseded: canceled,
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
    // The staged run's flow is known, and its row may not yet be visible
    // outside the caller's tx, so it is never re-resolved by runId here.
    const { flow, superseded, started, dispatch } = effects;

    // A superseded run may be pinned to an older hash than the run replacing it.
    for (const { runId, fact } of superseded) {
      await dispatcher.settleSuperseded(
        endingRunOf(runId, await flowOf(runId)),
        fact,
      );
    }

    await hooks.fireHook(flow.onStart, started, "flow.onStart");
    await hooks.fireHook(flowHooks?.onFlowStart, started, "onFlowStart");

    switch (dispatch.kind) {
      case "enqueued":
        await recordSkips(started.runId, dispatch.skip);
        // The roots already rode the start's own tx; the skips may unblock more.
        if (dispatch.skip.length > 0) await dispatcher.advance(started.runId);
        return;
      case "advance":
        await dispatcher.advance(started.runId);
        return;
    }
  }

  // The runnable roots were visible to workers from the start's own commit,
  // so a worker may already have advanced the run and recorded these skips;
  // append only for steps still pending.
  async function recordSkips(
    runId: RunId,
    skip: readonly SkipDecision[],
  ): Promise<void> {
    if (skip.length === 0) return;
    const state = await store.loadRunState(runId);
    for (const { stepId, reason } of skip) {
      if (stepStateOf(state, stepId).tag !== "pending") continue;
      await store.appendFact(
        runId,
        Facts.stepSkipped({ runId, stepId, reason, at: clock.now() }),
      );
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
    readonly parent: ParentLink;
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
    if (staged.kind === "started") {
      await applyEffects(staged.effects);
    } else {
      // Re-attach: re-seed a child whose first messages may have been lost; idempotent.
      await dispatcher.advance(runId);
    }
    return runId;
  }

  return { start, stage, applyEffects, startChildRun };
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
