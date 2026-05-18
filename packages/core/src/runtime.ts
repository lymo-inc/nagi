import {
  type CanonicalDag,
  canonicalize,
  fingerprintFlows,
  sha256Canonical,
} from "./canonicalize";
import {
  advance,
  type DispatchDeps,
  dispatchMessage,
  fireHook as fireRuntimeHook,
  propagateToParent,
} from "./dispatch";
import { attachDef, getDef, type SignalDef, type StepDef } from "./internal";
import { InMemoryClock } from "./memory";
import { descendantsOf } from "./scheduler";
import type {
  Clock,
  Fact,
  Flow,
  FlowCanceledFact,
  FlowHooks,
  FlowInput,
  FlowStartedFact,
  Json,
  Logger,
  PrunableStatus,
  PruneOpts,
  PruneResult,
  QueryRunsOpts,
  QueryRunsResult,
  Queue,
  ReplayOpts,
  RetryPolicy,
  RunId,
  SerializedError,
  StandardSchemaV1,
  StepId,
  Store,
  Trigger,
  Worker,
  WorkerConfig,
} from "./types";
import { makeWorker } from "./worker";

export interface NagiConfig {
  readonly flows: ReadonlyArray<Flow>;
  readonly store: Store;
  readonly queue: Queue;
  readonly clock?: Clock;
  readonly trigger?: Trigger;
  readonly hooks?: FlowHooks;
  readonly logger?: Logger;
  readonly defaultRetry?: RetryPolicy;
  /**
   * Process-wide identifier persisted on `workflow_run.code_version` and on
   * every `flow.started` fact for runs started by this process.
   *
   * When omitted (default), nagi computes a structural fingerprint over the
   * registered flows via {@link fingerprintFlows} and uses that — so the
   * audit field is meaningful by default and shifts only when flow topology
   * changes, not on every deploy. Supply an explicit string to override
   * (e.g. for a forced cutover: `"cutover-2026-05-15"`).
   *
   * Drift detection at replay uses each flow's per-flow `flowHash`, not this
   * value. See RFC 0001 "Topology vs handler code" and RFC 0003 "Auto-compute
   * codeVersion."
   */
  readonly codeVersion?: string;
}

export interface StartOpts {
  /**
   * Caller-supplied runId for idempotent kickoff. If provided and a run with
   * this ID already exists, `start()` is a no-op and returns the same ID
   * without re-appending `flow.started`, re-dispatching, or re-validating the
   * input. Two concurrent `start()` calls with the same `runId` produce
   * exactly one run (enforced at the Store layer).
   *
   * If omitted, the runtime mints a fresh ID via `crypto.randomUUID()`.
   *
   * Typical usage: a content hash of the input, so callers de-duplicate
   * kickoffs without coordinating.
   */
  readonly runId?: RunId;
}

export interface CancelOpts {
  /**
   * Caller-supplied reason recorded on the `flow.canceled` fact. Surfaces in
   * the cascaded `step.failed` error message for any parent waiting on this
   * run via `b.subflow()`. Default: "explicit wf.cancel()".
   */
  readonly reason?: string;
}

export interface Wf {
  /** Begin a new run. Returns the runId. */
  start<F extends Flow>(
    flow: F,
    input: FlowInput<F>,
    opts?: StartOpts,
  ): Promise<RunId>;

  /** Resolve a `b.signal()` step waiting on external input. */
  signal(runId: RunId, stepName: string, payload: unknown): Promise<void>;

  /**
   * Cancel a pending or running run. Writes `flow.canceled` to the run's
   * fact log and cascades transitively to every child run spawned by
   * `b.subflow()`. Idempotent: cancelling a run that's already terminal
   * (`completed` / `failed` / `canceled`) is a logged no-op.
   *
   * For canceled child runs, the parent's subflow step (if still running)
   * transitions to `failed` with a structured cancel error — the parent
   * doesn't hang waiting for a child that will never finish.
   */
  cancel(runId: RunId, opts?: CancelOpts): Promise<void>;

  /** Construct a worker; call `run` / `runOnce` / `runUntilEmpty` on it. */
  worker(config?: WorkerConfig): Worker;

  /**
   * Re-dispatch from the first incomplete step. `mode: "continue"` runs side
   * effects (idempotency protects); `mode: "inspect"` is a no-op probe.
   */
  replay(runId: RunId, opts?: ReplayOpts): Promise<void>;

  /**
   * Discover runs by flow / status / input. Read-only — does not create or
   * mutate runs. Use this to power read-side surfaces like "current run for
   * video X" without coupling consumers to nagi's storage schema.
   *
   * Filtering on `input` is JSONB containment: the stored input is a
   * superset of the filter object (recursive on nested objects).
   *
   * Pagination is keyset on `(startedAt, runId)` DESC. Pass the returned
   * `cursor` to the next call. `latest: true` returns at most one run and
   * forbids `limit` / `cursor` at the type level.
   */
  queryRuns(opts?: QueryRunsOpts): Promise<QueryRunsResult>;

  /**
   * Retention sweep over terminal runs. Deletes facts (and per-step rows,
   * leases, timers, dedupes) for runs whose `completedAt < olderThan` and
   * whose status is in `statuses` (default `["completed"]`). `pending` /
   * `running` runs are excluded at compile time via {@link PrunableStatus}
   * and re-validated at runtime.
   *
   * `keepSummary: true` (default) retains a summary row so `queryRuns` still
   * lists the pruned run; the postgres adapter keeps the `workflow_run` row
   * and the in-memory adapter keeps a shadow {@link RunSummary}. After a
   * prune, `loadRunState` and `replay` for that run return an empty state —
   * documented trade-off: you traded fact-fidelity for storage.
   *
   * Idempotent and safe under concurrent callers (postgres uses
   * `FOR UPDATE SKIP LOCKED` on the victim set).
   */
  pruneFacts(opts: PruneOpts): Promise<PruneResult>;
}

export class NagiValidationError extends Error {
  readonly issues: ReadonlyArray<StandardSchemaV1.Issue>;
  constructor(issues: ReadonlyArray<StandardSchemaV1.Issue>) {
    super(issues.map((i) => i.message).join("; "));
    this.name = "NagiValidationError";
    this.issues = issues;
  }
}

export class NagiRuntimeError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "NagiRuntimeError";
  }
}

/**
 * Carried by `onFlowError` (both flow-local and cross-cutting) when a run is
 * canceled because a newer `wf.start()` for the same concurrency group
 * superseded it. The serialized form preserves `name` and a structured
 * `cause` so observers can discriminate via `error.name === "NagiCanceledError"`
 * and read `error.cause.canceledByRunId` / `error.cause.concurrencyKey`.
 */
export class NagiCanceledError extends Error {
  readonly runId: RunId;
  readonly canceledByRunId: RunId;
  readonly concurrencyKey: string;
  constructor(args: {
    readonly runId: RunId;
    readonly canceledByRunId: RunId;
    readonly concurrencyKey: string;
  }) {
    super(
      `Run ${args.runId} was canceled (superseded by run ${args.canceledByRunId} for concurrency key "${args.concurrencyKey}").`,
    );
    this.name = "NagiCanceledError";
    this.runId = args.runId;
    this.canceledByRunId = args.canceledByRunId;
    this.concurrencyKey = args.concurrencyKey;
    this.cause = {
      canceledByRunId: args.canceledByRunId,
      concurrencyKey: args.concurrencyKey,
    };
  }
}

/**
 * Thrown by `wf.replay(runId)` when the live flow's canonical hash differs
 * from the snapshot the run was pinned to. The default replay behavior is
 * fail-loud — pass `replayOpts.allowDrift = true` to proceed using a
 * synthesized flow (topology from snapshot, handlers/predicates from live).
 */
export class NagiSnapshotDriftError extends Error {
  readonly runId: RunId;
  readonly expected: string;
  readonly actual: string;
  constructor(args: {
    readonly runId: RunId;
    readonly expected: string;
    readonly actual: string;
  }) {
    super(
      `Run ${args.runId} was pinned to flow hash ${args.expected.slice(0, 12)}… ` +
        `but the live flow's hash is ${args.actual.slice(0, 12)}…. ` +
        `Pass replayOpts.allowDrift = true to replay against the live code anyway.`,
    );
    this.name = "NagiSnapshotDriftError";
    this.runId = args.runId;
    this.expected = args.expected;
    this.actual = args.actual;
  }
}

export async function nagi(config: NagiConfig): Promise<Wf> {
  const clock = config.clock ?? new InMemoryClock();
  const flowsById = new Map<string, Flow>();
  const flowHashById = new Map<string, string>();
  for (const f of config.flows) {
    if (flowsById.has(f.id)) {
      throw new NagiRuntimeError(
        `Duplicate flow id "${f.id}" passed to nagi()`,
      );
    }
    flowsById.set(f.id, f);

    // Canonicalize + content-address the flow. The snapshot store dedupes
    // by hash, so repeated boots of the same code version are a no-op.
    const dag = await canonicalize(f);
    const flowHash = await sha256Canonical(dag);
    flowHashById.set(f.id, flowHash);
    await config.store.upsertSnapshot({
      flowHash,
      flowId: f.id,
      dag: dag as unknown as Json,
    });

    // Mutable ref: rotate the published hash if the topology changed since
    // the last boot. The audit trail of changes lives in `global_fact` as
    // `flow_ref.updated`.
    const previousHash = await config.store.getRef(f.id);
    if (previousHash !== flowHash) {
      await config.store.setRef(f.id, flowHash);
      await config.store.appendGlobalFact({
        kind: "flow_ref.updated",
        flowId: f.id,
        from: previousHash,
        to: flowHash,
        at: clock.now(),
      });
    }
  }

  // Resolve codeVersion once. If the caller omitted it, derive a structural
  // fingerprint over the registered flows so the audit field is meaningful
  // by default. Stable across deploys that don't change topology; moves
  // when any structural change (added step, edge, flipped `when`, etc.) lands.
  const codeVersion =
    config.codeVersion ?? (await fingerprintFlows(config.flows));

  async function flowFor(runId: RunId): Promise<Flow> {
    const runState = await config.store.loadRunState(runId);
    const flow = flowsById.get(runState.flowId);
    if (!flow) {
      throw new NagiRuntimeError(
        `Run ${runId} references flow "${runState.flowId}" which is not registered with nagi().`,
      );
    }
    return flow;
  }

  function lookupFlow(flowId: string): Flow | undefined {
    return flowsById.get(flowId);
  }

  /**
   * Atomically insert `flow.started` + handle concurrency-supersede cancels +
   * fire onStart/onError hooks + kick off `advance`. Shared between
   * `wf.start()` (caller-driven) and `startChildRun` (subflow-driven).
   *
   * Returns `started: false` if a run with the same id already exists — the
   * caller decides whether that's idempotent success (wf.start) or a hard
   * error (startChildRun, where the runId was freshly minted).
   */
  async function startRunInternal(args: {
    readonly flow: Flow;
    readonly validatedInput: Json;
    readonly runId: RunId;
    readonly parentRunId?: RunId;
    readonly parentStepId?: StepId;
  }): Promise<{ readonly started: boolean }> {
    const { flow, validatedInput, runId, parentRunId, parentStepId } = args;
    const startedAt = clock.now();
    const flowHash = flowHashById.get(flow.id);
    const fact: FlowStartedFact = {
      kind: "flow.started",
      runId,
      flowId: flow.id,
      input: validatedInput,
      at: startedAt,
      ...(flowHash !== undefined ? { flowHash } : {}),
      codeVersion,
      ...(parentRunId !== undefined ? { parentRunId } : {}),
      ...(parentStepId !== undefined ? { parentStepId } : {}),
    };

    let concurrencyArg:
      | { readonly key: string; readonly mode: "cancel-in-progress" }
      | undefined;
    if (flow.concurrency !== undefined) {
      const derived = flow.concurrency.keyFn(validatedInput);
      if (typeof derived !== "string" || derived.length === 0) {
        throw new NagiValidationError([
          {
            message: `flow.concurrency.keyFn must return a non-empty string (got ${typeof derived === "string" ? '""' : typeof derived})`,
            path: ["concurrency", "keyFn"],
          },
        ]);
      }
      concurrencyArg = { key: derived, mode: flow.concurrency.mode };
    }

    const { started, canceled } = await config.store.tryStartRun(
      runId,
      fact,
      concurrencyArg,
    );
    if (!started) return { started: false };

    for (const c of canceled) {
      const cancelError = new NagiCanceledError({
        runId: c.runId,
        canceledByRunId: runId,
        concurrencyKey: c.fact.concurrencyKey,
      });
      const serialized: SerializedError = {
        name: cancelError.name,
        message: cancelError.message,
        ...(cancelError.stack !== undefined
          ? { stack: cancelError.stack }
          : {}),
        cause: {
          canceledByRunId: runId,
          concurrencyKey: c.fact.concurrencyKey,
        },
      };
      const errorEvent = {
        runId: c.runId,
        flowId: flow.id,
        error: serialized,
        at: c.fact.at,
      };
      await fireRuntimeHook(
        flow.onError,
        errorEvent,
        "flow.onError",
        dispatchDeps,
      );
      await fireRuntimeHook(
        config.hooks?.onFlowError,
        errorEvent,
        "onFlowError",
        dispatchDeps,
      );
      // If the canceled sibling was itself a subflow child of some parent,
      // surface the cancellation as that parent's subflow step.failed so the
      // parent doesn't hang waiting forever.
      await propagateToParent(dispatchDeps, c.runId, {
        kind: "canceled",
        error: serialized,
      });
    }

    const startEvent = {
      runId,
      flowId: flow.id,
      input: validatedInput,
      at: startedAt,
    };
    await fireRuntimeHook(
      flow.onStart,
      startEvent,
      "flow.onStart",
      dispatchDeps,
    );
    await fireRuntimeHook(
      config.hooks?.onFlowStart,
      startEvent,
      "onFlowStart",
      dispatchDeps,
    );

    await advance(dispatchDeps, runId);
    return { started: true };
  }

  /**
   * Spawn a child run from a parent's `b.subflow()` step. The child runs
   * independently on the same store/queue; its eventual terminal state
   * wakes the parent via `finalizeFlowCompletion` / `finalizeFlowFailure`
   * (see dispatch.ts).
   */
  async function startChildRun(args: {
    readonly child: Flow;
    readonly childInput: unknown;
    readonly parentRunId: RunId;
    readonly parentStepId: StepId;
  }): Promise<RunId> {
    const { child, childInput, parentRunId, parentStepId } = args;
    if (!flowsById.has(child.id)) {
      throw new NagiRuntimeError(
        `Subflow child "${child.id}" not registered with nagi(). Pass it to flows[].`,
      );
    }
    const validated = (await validate(child.input, childInput)) as Json;
    const childRunId = mintRunId();
    const { started } = await startRunInternal({
      flow: child,
      validatedInput: validated,
      runId: childRunId,
      parentRunId,
      parentStepId,
    });
    if (!started) {
      // mintRunId() collisions are vanishingly unlikely; treat as a runtime
      // invariant break rather than silently retrying.
      throw new NagiRuntimeError(
        `startChildRun: minted child runId collided with an existing run (${childRunId})`,
      );
    }
    return childRunId;
  }

  /**
   * Cancel a run and all of its descendants. Order matters:
   *
   * 1. Write `flow.canceled` to self FIRST. This flips self's projected
   *    status to `canceled`, so any cascade-driven `propagateToParent`
   *    calls bubbling up from children see self as terminal and short-circuit.
   * 2. Cascade depth-first through children via the indexed
   *    `store.listChildren` lookup. Each child's cancellation in turn
   *    walks its own children.
   * 3. Surface self-cancellation to a higher parent (if this run was itself
   *    a subflow child). That parent's subflow step transitions to `failed`.
   *
   * Idempotent: skips runs that are already terminal.
   */
  async function cancelRunRecursive(
    runId: RunId,
    reason: string,
  ): Promise<void> {
    const state = await config.store.loadRunState(runId);
    if (
      state.status === "completed" ||
      state.status === "failed" ||
      state.status === "canceled"
    ) {
      config.logger?.info("nagi: cancel skipped — run already terminal", {
        runId,
        status: state.status,
      });
      return;
    }
    const flow = flowsById.get(state.flowId);
    const canceledFact: FlowCanceledFact = {
      kind: "flow.canceled",
      runId,
      at: clock.now(),
      // For explicit cancels we record the SAME runId as the canceler — the
      // existing fact shape requires `canceledByRunId`. Operators distinguish
      // explicit cancels from concurrency-supersede by `canceledByRunId === runId`.
      canceledByRunId: runId,
      concurrencyKey: reason,
    };
    await config.store.appendFact(runId, canceledFact);
    if (flow !== undefined) {
      const cancelError: SerializedError = {
        name: "NagiCanceledError",
        message: `Run ${runId} was canceled: ${reason}`,
      };
      const event = {
        runId,
        flowId: flow.id,
        error: cancelError,
        at: clock.now(),
      };
      await fireRuntimeHook(flow.onError, event, "flow.onError", dispatchDeps);
      await fireRuntimeHook(
        config.hooks?.onFlowError,
        event,
        "onFlowError",
        dispatchDeps,
      );
    }

    const children = await config.store.listChildren(runId);
    for (const childId of children) {
      await cancelRunRecursive(childId, `parent ${runId} canceled: ${reason}`);
    }

    const cancelError: SerializedError = {
      name: "NagiCanceledError",
      message: `Run ${runId} was canceled: ${reason}`,
    };
    await propagateToParent(dispatchDeps, runId, {
      kind: "canceled",
      error: cancelError,
    });
  }

  const dispatchDeps: DispatchDeps = {
    flowFor,
    lookupFlow,
    startChildRun,
    store: config.store,
    queue: config.queue,
    clock,
    ...(config.hooks !== undefined ? { hooks: config.hooks } : {}),
    ...(config.logger !== undefined ? { logger: config.logger } : {}),
    ...(config.defaultRetry !== undefined
      ? { defaultRetry: config.defaultRetry }
      : {}),
  };

  const wf: Wf = {
    async start<F extends Flow>(
      flow: F,
      input: FlowInput<F>,
      opts?: StartOpts,
    ): Promise<RunId> {
      if (!flowsById.has(flow.id)) {
        throw new NagiRuntimeError(
          `Flow "${flow.id}" not registered with nagi(). Pass it to flows[].`,
        );
      }

      // Caller-supplied runId. We accept it, validate shape, and rely on the
      // Store's atomic `tryStartRun` for race safety — never check-then-insert
      // at this layer.
      let runId: RunId;
      if (opts?.runId !== undefined) {
        if (typeof opts.runId !== "string" || opts.runId.length === 0) {
          throw new NagiValidationError([
            {
              message: "opts.runId must be a non-empty string",
              path: ["runId"],
            },
          ]);
        }
        runId = opts.runId;
      } else {
        runId = mintRunId();
      }

      const validated = (await validate(flow.input, input)) as Json;
      // `startRunInternal` returns `started: false` if the runId already
      // exists. For wf.start() that's idempotent success — per contract we
      // do NOT re-append the fact, re-dispatch, or re-validate against the
      // prior input. Caller asked for "use this runId" semantics.
      await startRunInternal({ flow, validatedInput: validated, runId });
      return runId;
    },

    async signal(
      runId: RunId,
      signalName: string,
      payload: unknown,
    ): Promise<void> {
      const runState = await config.store.loadRunState(runId);
      const flow = flowsById.get(runState.flowId);
      if (!flow) {
        throw new NagiRuntimeError(
          `Run ${runId} references flow "${runState.flowId}" which is not registered with nagi().`,
        );
      }
      // Resolve signalName → (stepId, def).
      //   Fast path: signalName === step id and the step is a signal with NO
      //     explicit `names` (so the step id is the implicit accepted name).
      //   Slow path: scan for a signal step whose explicit `names` includes
      //     signalName. A step that opted into `names` rejects its own step id
      //     unless the id is also listed in `names`.
      // Construction-time uniqueness (`assertSignalNameUniqueness` in
      // builder.ts) guarantees at most one match across both paths. O(steps)
      // is fine — webhook frequency is single-digit per run.
      let stepId: string | undefined;
      let def: SignalDef | undefined;
      const direct = flow.steps[signalName];
      if (direct !== undefined) {
        const d = getDef(direct);
        if (d.kind === "signal" && d.names === undefined) {
          stepId = signalName;
          def = d;
        }
      }
      if (stepId === undefined) {
        for (const [id, s] of Object.entries(flow.steps)) {
          const d = getDef(s);
          if (d.kind === "signal" && d.names?.includes(signalName)) {
            stepId = id;
            def = d;
            break;
          }
        }
      }
      if (stepId === undefined || def === undefined) {
        throw new NagiRuntimeError(
          `Flow "${flow.id}" has no signal step accepting "${signalName}".`,
        );
      }
      const stepState = runState.steps[stepId];
      if (stepState?.status !== "running") {
        // Late loser: the step already resolved via another alias (or the
        // same name on a second delivery). With multi-name signals this is
        // operationally normal — log it and ack, don't throw.
        if (stepState?.status === "completed") {
          config.logger?.info("nagi: signal arrived after step resolved", {
            runId,
            stepId,
            signalName,
          });
          return;
        }
        throw new NagiRuntimeError(
          `Step "${stepId}" is not waiting for signal (status: ${stepState?.status ?? "pending"}).`,
        );
      }

      const validated = (await validate(def.schema, payload)) as Json;
      const attempt = stepState.attempts > 0 ? stepState.attempts : 1;
      const carriesAlias = signalName !== stepId;

      await config.store.appendFact(runId, {
        kind: "signal.received",
        runId,
        stepId,
        payload: validated,
        at: clock.now(),
        ...(carriesAlias ? { signalName } : {}),
      });

      const completedFact: Fact = {
        kind: "step.completed",
        runId,
        stepId,
        attempt,
        output: validated,
        at: clock.now(),
      };
      await config.store.completeStep(runId, stepId, validated, completedFact);

      await fireRuntimeHook(
        config.hooks?.onSignalReceived,
        {
          runId,
          flowId: flow.id,
          stepId,
          attempt,
          kind: "signal",
          payload: validated,
          at: clock.now(),
        },
        "onSignalReceived",
        dispatchDeps,
      );

      await advance(dispatchDeps, runId);
    },

    async cancel(runId: RunId, opts?: CancelOpts): Promise<void> {
      await cancelRunRecursive(runId, opts?.reason ?? "explicit wf.cancel()");
    },

    worker(workerConfig?: WorkerConfig): Worker {
      if (config.flows.length === 0) {
        throw new NagiRuntimeError(
          "nagi(): no flows registered — cannot create a worker.",
        );
      }
      return makeWorker({ ...dispatchDeps, clock }, workerConfig);
    },

    async replay(
      runId: RunId,
      opts: ReplayOpts = { mode: "continue" },
    ): Promise<void> {
      const runState = await config.store.loadRunState(runId);
      const liveFlow = flowsById.get(runState.flowId);
      if (!liveFlow) {
        throw new NagiRuntimeError(
          `Run ${runId} references flow "${runState.flowId}" which is not registered with nagi().`,
        );
      }
      if (runState.status === "canceled") {
        throw new NagiRuntimeError(
          `Run ${runId} was canceled (superseded by a newer run with the same concurrency key). ` +
            `Replay is not supported for canceled runs — start a new run instead.`,
        );
      }
      if (opts.mode === "inspect") return;

      // When `fireHooks: false`, every hook call (step-local, flow-local,
      // cross-cutting `FlowHooks`) short-circuits inside `fireHook` —
      // preventing dual-publish on backfills.
      const fireHooks = opts.fireHooks !== false;
      const baseDeps: DispatchDeps = fireHooks
        ? dispatchDeps
        : { ...dispatchDeps, fireHooks: false };

      // On allowed drift, swap in a synthesized flow (topology from the
      // pinned snapshot, handlers from live code). Without drift, the
      // run replays against baseDeps unchanged.
      let replayDeps = baseDeps;
      let effectiveFlow: Flow = liveFlow;
      const pinned = runState.flowHash;
      if (pinned !== undefined) {
        const liveHash = flowHashById.get(liveFlow.id);
        if (liveHash !== undefined && liveHash !== pinned) {
          if (!opts.allowDrift) {
            throw new NagiSnapshotDriftError({
              runId,
              expected: pinned,
              actual: liveHash,
            });
          }
          const snapshot = await config.store.loadSnapshot(pinned);
          if (snapshot === null) {
            throw new NagiRuntimeError(
              `Run ${runId} pinned to flow hash ${pinned.slice(0, 12)}… but ` +
                `no snapshot with that hash was found. Cannot replay with allowDrift.`,
            );
          }
          const synthesized = synthesizeReplayFlow(
            snapshot.dag as unknown as CanonicalDag,
            liveFlow,
          );
          replayDeps = { ...baseDeps, flowFor: async () => synthesized };
          effectiveFlow = synthesized;
        }
      }

      // Step-scoped replay: append `step.reset` facts for `from` and every
      // transitive descendant so the projector forgets their state and
      // `nextRunnable` re-picks them. `from` resolves against the effective
      // flow (snapshot topology under drift + allowDrift, else live) so
      // validation stays consistent with what advance() will actually run.
      if (opts.from !== undefined) {
        if (runState.status === "running") {
          throw new NagiRuntimeError(
            `Run ${runId} is still running — replay({ from }) would race in-flight workers. ` +
              `Wait for the run to settle (completed / failed) before resetting from a step.`,
          );
        }
        if (!(opts.from in effectiveFlow.steps)) {
          throw new NagiValidationError([
            {
              message: `replay({ from }): step "${opts.from}" is not a step in flow "${effectiveFlow.id}".`,
              path: ["from"],
            },
          ]);
        }
        const cascade = descendantsOf(effectiveFlow, opts.from);
        const at = clock.now();
        for (const stepId of cascade) {
          const fact: Fact =
            stepId === opts.from
              ? { kind: "step.reset", runId, at, stepId }
              : {
                  kind: "step.reset",
                  runId,
                  at,
                  stepId,
                  cascadedFrom: opts.from,
                };
          await config.store.appendFact(runId, fact);
        }
      }

      await advance(replayDeps, runId);
      // Drain inline so the hook-suppression scope covers every dispatch
      // this replay initiates. Without it, advance() only enqueues work
      // and a worker would later run those dispatches with worker-scoped
      // deps (no suppression) — defeating the flag's purpose.
      if (!fireHooks) await drainInline(replayDeps);
    },

    async queryRuns(opts: QueryRunsOpts = {}): Promise<QueryRunsResult> {
      // The discriminated-union type rejects `{ latest: true, limit, cursor }`
      // at compile time. This runtime check catches JS-only callers and the
      // `as any` escape hatch — same contract, defense in depth.
      if (opts.latest === true) {
        if (opts.limit !== undefined || opts.cursor !== undefined) {
          throw new NagiValidationError([
            {
              message:
                "queryRuns: `latest: true` is incompatible with `limit` / `cursor` — `latest` returns at most one row.",
              path: ["latest"],
            },
          ]);
        }
      }
      return config.store.queryRuns(opts);
    },

    async pruneFacts(opts: PruneOpts): Promise<PruneResult> {
      // `olderThan` is the only required field; everything else has a
      // sensible default applied here so adapters receive a fully-specified
      // opts object — same delegation pattern as `queryRuns`.
      if (
        !(opts.olderThan instanceof Date) ||
        Number.isNaN(opts.olderThan.getTime())
      ) {
        throw new NagiValidationError([
          {
            message: "pruneFacts: `olderThan` must be a valid Date.",
            path: ["olderThan"],
          },
        ]);
      }
      // `PrunableStatus` blocks `'pending'` / `'running'` at compile time.
      // Re-validate at runtime for JS-only callers / `as any` escapes.
      const statuses: ReadonlyArray<PrunableStatus> = opts.statuses ?? [
        "completed",
      ];
      for (const s of statuses) {
        if (s !== "completed" && s !== "failed" && s !== "canceled") {
          throw new NagiValidationError([
            {
              message: `pruneFacts: status "${s}" is not prunable. Allowed: "completed" | "failed" | "canceled".`,
              path: ["statuses"],
            },
          ]);
        }
      }
      const batchSize = opts.batchSize ?? 1000;
      if (!Number.isInteger(batchSize) || batchSize < 1) {
        throw new NagiValidationError([
          {
            message: "pruneFacts: `batchSize` must be a positive integer.",
            path: ["batchSize"],
          },
        ]);
      }
      const keepSummary = opts.keepSummary ?? true;
      return config.store.pruneFacts({
        olderThan: opts.olderThan,
        statuses,
        batchSize,
        keepSummary,
      });
    },
  };
  // Internal hook for the in-process test harness (`makeHarness`) so it
  // can drive `dispatchMessage` directly with the same deps that the
  // runtime's own worker uses. Non-enumerable to keep it out of the public
  // API surface; not part of the `Wf` interface.
  Object.defineProperty(wf, "__dispatchDeps", {
    value: dispatchDeps,
    enumerable: false,
    writable: false,
    configurable: false,
  });
  return wf;
}

function mintRunId(): RunId {
  return `run-${crypto.randomUUID()}` as RunId;
}

/**
 * Drain the queue under the given dispatch deps until empty. Used by
 * `wf.replay({ fireHooks: false })` to keep every replayed dispatch under
 * the hook-suppressed scope. Bounded to avoid runaway loops on broken
 * stores; if hit, the caller can re-invoke replay to pick up where this
 * left off.
 */
const MAX_REPLAY_DISPATCHES = 4096;
async function drainInline(deps: DispatchDeps): Promise<void> {
  for (let i = 0; i < MAX_REPLAY_DISPATCHES; i++) {
    const messages = await deps.queue.dequeue({ count: 1 });
    if (messages.length === 0) return;
    for (const msg of messages) await dispatchMessage(deps, msg);
  }
}

async function validate<S extends StandardSchemaV1>(
  schema: S,
  value: unknown,
): Promise<unknown> {
  const result = await schema["~standard"].validate(value);
  if ("issues" in result && result.issues !== undefined) {
    throw new NagiValidationError(result.issues);
  }
  return (result as { value: unknown }).value;
}

/**
 * Build a Flow for drift-allowed replay. The step set and `needs` edges come
 * from the pinned snapshot's canonical DAG; the actual step definitions
 * (`run` handlers, `when` predicates, schemas, match details) come from the
 * live flow.
 *
 * The synthesized step's def is the live flow's def with `needs` rewired to
 * point at the synthesized step shells for the snapshot's listed upstreams.
 * This way `needsStepIds(def)` returns the snapshot's needs, but the
 * dispatcher executes the live handler.
 *
 * Steps listed in the snapshot but missing from the live flow throw at
 * synthesis time — drift-allowed replay still requires the live code to
 * recognize every step name from the snapshot.
 */
function synthesizeReplayFlow(dag: CanonicalDag, liveFlow: Flow): Flow {
  const synthesized: Record<string, ReturnType<typeof attachDef>> = {};

  // Phase 1: create step shells so `needs` arrays can refer to them by
  // object identity.
  for (const canonStep of dag.steps) {
    synthesized[canonStep.id] = attachDef(
      { kind: canonStep.kind, id: canonStep.id },
      { kind: canonStep.kind } as unknown as StepDef,
    );
  }

  // Phase 2: rewire each step's def. We take the live flow's def (which has
  // the runnable handler, predicate, schema, etc.) and overwrite `needs`
  // with the snapshot's edges pointing at our synthesized shells.
  for (const canonStep of dag.steps) {
    const liveStep = liveFlow.steps[canonStep.id];
    if (liveStep === undefined) {
      throw new NagiRuntimeError(
        `Drift-allowed replay: step "${canonStep.id}" exists in snapshot but ` +
          `is missing from the live flow "${liveFlow.id}". Cannot synthesize a handler.`,
      );
    }
    const liveDef = getDef(liveStep);

    const synthesizedNeeds: Record<string, unknown> = {};
    for (const upstreamId of canonStep.needs) {
      const upstreamShell = synthesized[upstreamId];
      if (upstreamShell === undefined) {
        throw new NagiRuntimeError(
          `Drift-allowed replay: step "${canonStep.id}" needs upstream ` +
            `"${upstreamId}" which the snapshot does not declare.`,
        );
      }
      synthesizedNeeds[upstreamId] = upstreamShell;
    }

    const rewiredDef = {
      ...liveDef,
      needs: synthesizedNeeds,
    } as StepDef;
    const shell = synthesized[canonStep.id];
    if (shell === undefined) continue;
    (shell as unknown as { __def: StepDef }).__def = rewiredDef;
  }

  return {
    id: liveFlow.id,
    input: liveFlow.input,
    steps: synthesized,
    ...(liveFlow.output !== undefined ? { output: liveFlow.output } : {}),
  };
}
