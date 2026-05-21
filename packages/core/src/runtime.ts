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
  serializeError,
} from "./dispatch";
import {
  asStepMapWithDefs,
  attachDef,
  compact,
  type EmitLog,
  getDef,
  makeEmit,
  type SignalDef,
  type StepDef,
  setDef,
} from "./internal";
import { InMemoryClock } from "./memory";
import { descendantsOf, stepStateOf } from "./scheduler";
import type {
  Clock,
  Fact,
  Flow,
  FlowCanceledByOperatorFact,
  FlowCanceledExplicitlyFact,
  FlowCanceledFact,
  FlowHooks,
  FlowIdOf,
  FlowInput,
  FlowStartedFact,
  Json,
  LogEntry,
  Operator,
  OperatorAuditOpts,
  OperatorSkipOpts,
  ParentRef,
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
  readonly onLog?: (entry: LogEntry) => void;
  readonly defaultRetry?: RetryPolicy;
  readonly codeVersion?: string;
}

export interface StartOpts {
  readonly runId?: RunId;
}

export interface CancelOpts {
  readonly reason?: string;
}

export interface Wf<TFlows extends ReadonlyArray<Flow> = ReadonlyArray<Flow>> {
  start<F extends TFlows[number]>(
    flow: F,
    input: FlowInput<F>,
    opts?: StartOpts,
  ): Promise<RunId>;

  /**
   * Start a flow by id with a runtime-typed input. Intended for callers
   * holding a serialized payload (transactional-outbox reconcilers, queue
   * consumers replaying DLQs, admin CLIs replaying a runId): the input
   * is validated against the registered flow's schema before the run is
   * created, mirroring `start`'s runtime contract without requiring a
   * compile-time-typed input.
   *
   * Throws `NagiRuntimeError` when `flowId` is not registered with
   * `nagi()`, and `NagiValidationError` when the input fails the flow's
   * schema or when `opts.runId` is invalid.
   */
  startById(flowId: string, input: unknown, opts?: StartOpts): Promise<RunId>;

  signal(runId: RunId, stepName: string, payload: unknown): Promise<void>;

  cancel(runId: RunId, opts?: CancelOpts): Promise<void>;

  worker(config?: WorkerConfig): Worker;

  replay(runId: RunId, opts?: ReplayOpts): Promise<void>;

  queryRuns(
    opts?: QueryRunsOpts<FlowIdOf<TFlows>>,
  ): Promise<QueryRunsResult<FlowIdOf<TFlows>>>;

  operator(): Operator;

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

export class NagiCanceledError extends Error {
  readonly runId: RunId;
  readonly canceledByRunId: RunId;
  readonly concurrencyKey: string;
  constructor({
    runId,
    canceledByRunId,
    concurrencyKey,
  }: {
    readonly runId: RunId;
    readonly canceledByRunId: RunId;
    readonly concurrencyKey: string;
  }) {
    super(
      `Run ${runId} was canceled (superseded by run ${canceledByRunId} for concurrency key "${concurrencyKey}").`,
    );

    this.name = "NagiCanceledError";
    this.runId = runId;
    this.canceledByRunId = canceledByRunId;
    this.concurrencyKey = concurrencyKey;
    this.cause = {
      canceledByRunId: canceledByRunId,
      concurrencyKey: concurrencyKey,
    };
  }
}

type DistributiveOmit<T, K extends PropertyKey> = T extends unknown
  ? Omit<T, K>
  : never;

// The human-initiated cancel causes minus the persisted-fact envelope, derived
// from the facts so cancel intent and the recorded fact can't drift.
// Concurrency cancellation is system-internal and intentionally excluded.
type CancelArgs = DistributiveOmit<
  FlowCanceledExplicitlyFact | FlowCanceledByOperatorFact,
  "kind" | "runId" | "at"
>;

export class NagiSnapshotDriftError extends Error {
  readonly runId: RunId;
  readonly expected: string;
  readonly actual: string;
  constructor({
    runId,
    expected,
    actual,
  }: {
    readonly runId: RunId;
    readonly expected: string;
    readonly actual: string;
  }) {
    super(
      `Run ${runId} was pinned to flow hash ${expected.slice(0, 12)}… ` +
        `but the live flow's hash is ${actual.slice(0, 12)}…. ` +
        `Pass replayOpts.allowDrift = true to replay against the live code anyway.`,
    );
    this.name = "NagiSnapshotDriftError";
    this.runId = runId;
    this.expected = expected;
    this.actual = actual;
  }
}

async function nagiImpl<const TFlows extends ReadonlyArray<Flow>>(
  config: NagiConfig & { flows: TFlows },
): Promise<Wf<TFlows>> {
  const clock = config.clock ?? new InMemoryClock();
  // Single diagnostic choke point (RFC 0020): built once from config.onLog and
  // threaded everywhere a log is produced. A no-op when onLog is absent (silent
  // by default), so call sites never branch on its presence.
  const emitLog = makeEmit(config.onLog);
  // One-shot queue provisioning (RFC 0013): eager + fail-fast at construction,
  // before any run can enqueue. A no-op for adapters without the hook.
  await config.queue.ensureSchema?.();

  const flowsById = new Map<string, Flow>();
  const flowHashById = new Map<string, string>();
  for (const f of config.flows) {
    if (flowsById.has(f.id)) {
      throw new NagiRuntimeError(
        `Duplicate flow id "${f.id}" passed to nagi()`,
      );
    }
    flowsById.set(f.id, f);

    const dag = await canonicalize(f);
    const flowHash = await sha256Canonical(dag);
    flowHashById.set(f.id, flowHash);
    await config.store.upsertSnapshot({
      flowHash,
      flowId: f.id,
      dag: dag as unknown as Json,
    });

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

  async function startRunInternal({
    flow,
    validatedInput,
    runId,
    parent,
  }: {
    readonly flow: Flow;
    readonly validatedInput: Json;
    readonly runId: RunId;
    readonly parent?: ParentRef;
  }): Promise<{ readonly started: boolean }> {
    const startedAt = clock.now();
    const flowHash = flowHashById.get(flow.id);

    const fact: FlowStartedFact = {
      kind: "flow.started",
      runId,
      flowId: flow.id,
      input: validatedInput,
      at: startedAt,
      codeVersion,
      ...compact({
        flowHash,
        parent:
          parent !== undefined
            ? { runId: parent.runId, stepId: parent.stepId }
            : undefined,
      }),
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
        ...serializeError(cancelError),
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
      await propagateToParent(dispatchDeps, c.runId, {
        kind: "canceled",
        error: serialized,
      });
    }

    const startEvent =
      parent !== undefined
        ? {
            runId,
            flowId: flow.id,
            input: validatedInput,
            at: startedAt,
            parent,
          }
        : {
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

  async function startChildRun(args: {
    readonly child: Flow;
    readonly childInput: unknown;
    readonly parent: ParentRef;
  }): Promise<RunId> {
    const { child, childInput, parent } = args;
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
      parent,
    });
    if (!started) {
      throw new NagiRuntimeError(
        `startChildRun: minted child runId collided with an existing run (${childRunId})`,
      );
    }
    return childRunId;
  }

  async function cancelRunRecursive(
    runId: RunId,
    args: CancelArgs,
  ): Promise<void> {
    const state = await config.store.loadRunState(runId);
    if (
      state.status === "completed" ||
      state.status === "failed" ||
      state.status === "canceled"
    ) {
      emitLog({
        level: "info",
        msg: "nagi: cancel skipped — run already terminal",
        attrs: { runId, status: state.status },
      });
      return;
    }
    const flow = flowsById.get(state.flowId);
    const canceledFact: FlowCanceledFact = {
      ...args,
      kind: "flow.canceled",
      runId,
      at: clock.now(),
    };
    await config.store.appendFact(runId, canceledFact);
    const cancelError: SerializedError = {
      name: "NagiCanceledError",
      message: `Run ${runId} was canceled: ${args.reason}`,
    };
    if (flow !== undefined) {
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
      await cancelRunRecursive(childId, {
        cause: "explicit",
        reason: `parent ${runId} canceled: ${args.reason}`,
        note:
          args.cause === "operator"
            ? `cascade from operator ${args.actor} aborting parent ${runId}`
            : `cascade from parent ${runId}`,
      });
    }

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
    emitLog,
    ...compact({
      hooks: config.hooks,
      defaultRetry: config.defaultRetry,
    }),
  };

  const wf: Wf = {
    async start<F extends Flow>(
      flow: F,
      input: FlowInput<F>,
      opts?: StartOpts,
    ): Promise<RunId> {
      return wf.startById(flow.id, input, opts);
    },

    async startById(
      flowId: string,
      input: unknown,
      opts?: StartOpts,
    ): Promise<RunId> {
      const flow = flowsById.get(flowId);
      if (!flow) {
        throw new NagiRuntimeError(
          `Flow "${flowId}" not registered with nagi(). Pass it to flows[].`,
        );
      }

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
      const resolved = resolveSignalStep(flow, signalName);
      if (resolved === null) {
        throw new NagiRuntimeError(
          `Flow "${flow.id}" has no signal step accepting "${signalName}".`,
        );
      }
      const { stepId, def } = resolved;
      const stepState = stepStateOf(runState, stepId);
      if (stepState.status !== "running") {
        if (stepState.status === "completed") {
          emitLog({
            level: "info",
            msg: "nagi: signal arrived after step resolved",
            attrs: { runId, stepId, signalName },
          });
          return;
        }
        throw new NagiRuntimeError(
          `Step "${stepId}" is not waiting for signal (status: ${stepState.status}).`,
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
      await cancelRunRecursive(runId, {
        cause: "explicit",
        reason: opts?.reason ?? "explicit wf.cancel()",
      });
    },

    operator(): Operator {
      return makeOperator({
        deps: dispatchDeps,
        clock,
        flowsById,
        cancelRunRecursive,
        emitLog,
      });
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

      const fireHooks = opts.fireHooks !== false;
      const baseDeps: DispatchDeps = fireHooks
        ? dispatchDeps
        : { ...dispatchDeps, fireHooks: false };

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
      if (!fireHooks) await drainInline(replayDeps);
    },

    async queryRuns(opts: QueryRunsOpts = {}): Promise<QueryRunsResult> {
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
  Object.defineProperty(wf, "__dispatchDeps", {
    value: dispatchDeps,
    enumerable: false,
    writable: false,
    configurable: false,
  });
  // Trust boundary: persisted flow_id values were registered flow ids at write
  // time. The text column erases the literal type, so queryRuns returns
  // QueryRunsResult<string>; assert it back to the registered union here, the
  // single read-side boundary (RFC 0012 D7). All other Wf<TFlows> members are
  // structurally identical to the bare Wf, so this one cast covers the widening.
  return wf as unknown as Wf<TFlows>;
}

/**
 * Config for {@link nagi.run}: a `NagiConfig` plus optional worker tuning and an
 * external shutdown signal. The signal is merged with an internal controller, so
 * aborting either source drains the worker.
 */
export interface NagiRunConfig extends NagiConfig {
  readonly worker?: Omit<WorkerConfig, "signal">;
  readonly signal?: AbortSignal;
}

/**
 * Handle returned by {@link nagi.run}. `stop()` aborts the internal controller,
 * awaits the worker loop, and is idempotent (safe to call twice or concurrently).
 * It resolves cleanly even if the loop crashed — a true crash is logged once via
 * the configured `onLog`.
 */
export interface RuntimeHandle<
  TFlows extends ReadonlyArray<Flow> = ReadonlyArray<Flow>,
> {
  readonly wf: Wf<TFlows>;
  stop(): Promise<void>;
}

async function nagiRun<const TFlows extends ReadonlyArray<Flow>>(
  config: NagiRunConfig & { flows: TFlows },
): Promise<RuntimeHandle<TFlows>> {
  const wf = await nagiImpl(config);
  const internal = new AbortController();
  const signal: AbortSignal = config.signal
    ? AbortSignal.any([internal.signal, config.signal])
    : internal.signal;
  const worker = wf.worker({ ...config.worker, signal });

  // The loop promise is held privately: it resolves on graceful drain and
  // rejects only on a true loop crash (e.g. queue.dequeue throws while the
  // runtime is not shutting down).
  const emitLog = makeEmit(config.onLog);
  const loop = worker.run();
  loop.catch((err: unknown) => {
    if (signal.aborted) return; // graceful shutdown — not a crash
    emitLog({
      level: "error",
      msg: "nagi.run: worker exited unexpectedly",
      attrs: { error: String(err) },
    });
  });

  let stopping: Promise<void> | undefined;
  const stop = (): Promise<void> => {
    stopping ??= (async () => {
      internal.abort();
      try {
        await loop;
      } catch {
        // graceful abort, or a crash already logged above — stop() never throws.
      }
    })();
    return stopping;
  };

  return { wf, stop };
}

/**
 * The nagi runtime factory. Call `nagi(config)` for fine-grained control, or
 * `nagi.run(config)` for a turnkey worker lifecycle returning `{ wf, stop }`.
 */
export const nagi: typeof nagiImpl & { run: typeof nagiRun } = Object.assign(
  nagiImpl,
  { run: nagiRun },
);

function mintRunId(): RunId {
  return `run-${crypto.randomUUID()}` as RunId;
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

const MAX_REPLAY_DISPATCHES = 4096;
async function drainInline(deps: DispatchDeps): Promise<void> {
  for (let i = 0; i < MAX_REPLAY_DISPATCHES; i++) {
    const messages = await deps.queue.dequeue({ count: 1 });
    if (messages.length === 0) return;
    for (const msg of messages) await dispatchMessage(deps, msg);
  }
}

interface OperatorDeps {
  readonly deps: DispatchDeps;
  readonly clock: Clock;
  readonly flowsById: ReadonlyMap<string, Flow>;
  readonly cancelRunRecursive: (
    runId: RunId,
    args: CancelArgs,
  ) => Promise<void>;
  readonly emitLog: EmitLog;
}

function makeOperator(o: OperatorDeps): Operator {
  const { deps, clock, flowsById, cancelRunRecursive, emitLog } = o;
  const store = deps.store;

  function resolveFlow(flowId: string, runId: RunId): Flow {
    const flow = flowsById.get(flowId);
    if (!flow) {
      throw new NagiRuntimeError(
        `Run ${runId} references flow "${flowId}" which is not registered with nagi().`,
      );
    }
    return flow;
  }

  async function skip(
    runId: RunId,
    stepId: StepId,
    opts: OperatorSkipOpts,
  ): Promise<void> {
    if (typeof opts.actor !== "string" || opts.actor.length === 0) {
      throw new NagiValidationError([
        {
          message: "operator.skip: opts.actor must be a non-empty string",
          path: ["actor"],
        },
      ]);
    }
    const cascade = opts.cascade ?? "skip";
    const state = await store.loadRunState(runId);
    const flow = resolveFlow(state.flowId, runId);
    if (!(stepId in flow.steps)) {
      throw new NagiValidationError([
        {
          message: `operator.skip: step "${stepId}" is not a step in flow "${flow.id}".`,
          path: ["stepId"],
        },
      ]);
    }
    const stepState = stepStateOf(state, stepId);
    if (
      stepState.status === "completed" ||
      stepState.status === "failed" ||
      stepState.status === "skipped" ||
      stepState.status === "canceled"
    ) {
      emitLog({
        level: "info",
        msg: "nagi: operator.skip noop — step already terminal",
        attrs: { runId, stepId, status: stepState.status },
      });
      return;
    }
    if (
      state.status === "completed" ||
      state.status === "failed" ||
      state.status === "canceled"
    ) {
      throw new NagiRuntimeError(
        `operator.skip: run ${runId} is already terminal (${state.status}); cannot skip step "${stepId}".`,
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
    await advance(deps, runId);
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
      if (
        ss.status === "completed" ||
        ss.status === "failed" ||
        ss.status === "skipped" ||
        ss.status === "canceled"
      ) {
        return;
      }
      if (
        s.status === "completed" ||
        s.status === "failed" ||
        s.status === "canceled"
      ) {
        return;
      }
      if (ss.attempts > attempt) return;
      await new Promise((r) => setTimeout(r, 50));
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
    if (typeof opts.actor !== "string" || opts.actor.length === 0) {
      throw new NagiValidationError([
        {
          message: "operator.retry: opts.actor must be a non-empty string",
          path: ["actor"],
        },
      ]);
    }
    const state = await store.loadRunState(runId);
    if (state.status === "canceled") {
      throw new NagiRuntimeError(
        `operator.retry: run ${runId} is canceled; cannot retry. Start a new run instead.`,
      );
    }
    const flow = resolveFlow(state.flowId, runId);
    if (!(stepId in flow.steps)) {
      throw new NagiValidationError([
        {
          message: `operator.retry: step "${stepId}" is not a step in flow "${flow.id}".`,
          path: ["stepId"],
        },
      ]);
    }
    const stepState = stepStateOf(state, stepId);

    if (stepState.status === "running") {
      const abortFact: Fact = {
        kind: "step.abort-requested",
        runId,
        at: clock.now(),
        stepId,
        attempt: stepState.attempts,
        actor: opts.actor,
        ...compact({ note: opts.note }),
      };
      await store.appendFact(runId, abortFact);
      await waitForStepToSettle(runId, stepId, stepState.attempts, 30_000);
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
    await advance(deps, runId);
  }

  async function abort(runId: RunId, opts: OperatorAuditOpts): Promise<void> {
    if (typeof opts.actor !== "string" || opts.actor.length === 0) {
      throw new NagiValidationError([
        {
          message: "operator.abort: opts.actor must be a non-empty string",
          path: ["actor"],
        },
      ]);
    }
    await cancelRunRecursive(runId, {
      cause: "operator",
      reason: opts.note ?? `aborted by operator ${opts.actor}`,
      actor: opts.actor,
      ...compact({ note: opts.note }),
    });
  }

  return { skip, retry, abort };
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

function synthesizeReplayFlow(dag: CanonicalDag, liveFlow: Flow): Flow {
  const liveSteps = asStepMapWithDefs(liveFlow.steps);
  const synthesized: Record<string, ReturnType<typeof attachDef>> = {};

  // Phase 1: a shell per canonical step, seeded with its live def. needs still
  // point at the live upstream identities; phase 2 rewires them to the shells.
  for (const canonStep of dag.steps) {
    const liveStep = liveSteps[canonStep.id];
    if (liveStep === undefined) {
      throw new NagiRuntimeError(
        `Drift-allowed replay: step "${canonStep.id}" exists in snapshot but ` +
          `is missing from the live flow "${liveFlow.id}". Cannot synthesize a handler.`,
      );
    }
    synthesized[canonStep.id] = attachDef(
      { kind: canonStep.kind, id: canonStep.id },
      getDef(liveStep),
    );
  }

  // Phase 2: rewire each step's needs to reference the synthesized shells.
  for (const canonStep of dag.steps) {
    const shell = synthesized[canonStep.id];
    if (shell === undefined) continue;

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

    setDef(shell, { ...getDef(shell), needs: synthesizedNeeds } as StepDef);
  }

  return {
    id: liveFlow.id,
    input: liveFlow.input,
    steps: synthesized,
    ...compact({ output: liveFlow.output }),
  };
}
