import {
  canonicalize,
  fingerprintFlows,
  sha256Canonical,
} from "./canonicalize";
import { type DispatchDeps, makeDispatcher } from "./dispatch";
import {
  NagiCanceledError,
  NagiRuntimeError,
  serializeError,
  validationError,
} from "./errors";
import { makeHooks } from "./exec/hooks";
import { Facts } from "./facts";
import { makeFlowRegistry } from "./flow-registry";
import { asStepMapWithDefs, compact, getDef, makeEmit } from "./internal";
import { InMemoryClock } from "./memory";
import { makeOperator } from "./operator";
import { makeReplay } from "./replay";
import { makeSignals } from "./signals";
import { isTerminalRun, runStatusOf } from "./state";
import {
  DEFAULT_HEARTBEAT_INTERVAL_MS,
  DEFAULT_HEARTBEAT_LEASE_MS,
} from "./step-exec";
import type {
  CancelArgs,
  Clock,
  Flow,
  FlowHooks,
  FlowIdOf,
  FlowInput,
  Json,
  LogEntry,
  Millis,
  Operator,
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
  StepId,
  Store,
  StreamEvent,
  StreamTransport,
  Trigger,
  Worker,
  WorkerConfig,
} from "./types";
import { validate } from "./validate";
import { makeWorker } from "./worker";

export interface NagiConfig {
  readonly flows: ReadonlyArray<Flow>;
  readonly store: Store;
  readonly streamTransport?: StreamTransport;
  readonly queue: Queue;
  readonly clock?: Clock;
  readonly trigger?: Trigger;
  readonly hooks?: FlowHooks;
  readonly onLog?: (entry: LogEntry) => void;
  readonly defaultRetry?: RetryPolicy;
  readonly codeVersion?: string;
  // heartbeatIntervalMs must stay below the queue's initial visibility timeout,
  // or a slow step's message is redelivered before the first lease extension.
  readonly heartbeatIntervalMs?: Millis;
  readonly heartbeatLeaseMs?: Millis;
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

  startById(flowId: string, input: unknown, opts?: StartOpts): Promise<RunId>;

  signal(runId: RunId, stepName: string, payload: unknown): Promise<void>;

  cancel(runId: RunId, opts?: CancelOpts): Promise<void>;

  worker(config?: WorkerConfig): Worker;

  replay(runId: RunId, opts?: ReplayOpts): Promise<void>;

  queryRuns(
    opts?: QueryRunsOpts<FlowIdOf<TFlows>>,
  ): Promise<QueryRunsResult<FlowIdOf<TFlows>>>;

  subscribe<C = Json>(
    runId: RunId,
    stepId: StepId,
    opts?: { readonly replayBuffered?: boolean },
  ): AsyncIterable<StreamEvent<C>>;

  operator(): Operator;

  pruneFacts(opts: PruneOpts): Promise<PruneResult>;
}

async function nagiImpl<const TFlows extends ReadonlyArray<Flow>>(
  config: NagiConfig & { flows: TFlows },
): Promise<Wf<TFlows>> {
  const clock = config.clock ?? new InMemoryClock();
  const emitLog = makeEmit(config.onLog);
  await config.queue.ensureSchema?.();

  const registry = makeFlowRegistry(config.flows);

  // Falls back to the store when it also implements StreamTransport (the
  // in-memory reference does); real deployments inject a dedicated transport.
  const streamTransport =
    config.streamTransport ?? asStreamTransport(config.store);

  // Streaming steps publish ephemeral chunks out-of-band, so without a transport
  // they cannot be carried. Only scanned on the failure path.
  if (streamTransport === undefined) {
    for (const f of registry.all) {
      for (const [stepId, step] of Object.entries(asStepMapWithDefs(f.steps))) {
        if (getDef(step).kind !== "streaming") continue;
        throw new NagiRuntimeError(
          `Flow "${f.id}" has a streaming step "${stepId}" (b.streamingTask), ` +
            `but no StreamTransport is configured — it cannot transport ` +
            `ephemeral chunks. Pass streamTransport (or a store that implements ` +
            `it, e.g. the in-memory store) or remove the streaming step.`,
        );
      }
    }
  }

  const flowHashById = new Map<string, string>();
  for (const f of registry.all) {
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
      await config.store.appendGlobalFact(
        Facts.flowRefUpdated({
          flowId: f.id,
          from: previousHash,
          to: flowHash,
          at: clock.now(),
        }),
      );
    }
  }

  const codeVersion =
    config.codeVersion ?? (await fingerprintFlows(config.flows));

  async function flowFor(runId: RunId): Promise<Flow> {
    const runState = await config.store.loadRunState(runId);
    return registry.requireForRun(runState.flowId, runId);
  }

  function lookupFlow(flowId: string): Flow | undefined {
    return registry.get(flowId);
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

    const fact = Facts.flowStarted({
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
    });

    let concurrencyArg:
      | { readonly key: string; readonly mode: "cancel-in-progress" }
      | undefined;
    if (flow.concurrency !== undefined) {
      const derived = flow.concurrency.keyFn(validatedInput);
      if (typeof derived !== "string" || derived.length === 0) {
        throw validationError(
          `flow.concurrency.keyFn must return a non-empty string (got ${typeof derived === "string" ? '""' : typeof derived})`,
          ["concurrency", "keyFn"],
        );
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
      await hooks.fireHook(flow.onError, errorEvent, "flow.onError");
      await hooks.fireHook(
        config.hooks?.onFlowError,
        errorEvent,
        "onFlowError",
      );
      await dispatcher.propagateToParent(c.runId, {
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
    await hooks.fireHook(flow.onStart, startEvent, "flow.onStart");
    await hooks.fireHook(config.hooks?.onFlowStart, startEvent, "onFlowStart");

    await dispatcher.advance(runId);
    return { started: true };
  }

  async function startChildRun(args: {
    readonly child: Flow;
    readonly childInput: unknown;
    readonly parent: ParentRef;
  }): Promise<RunId> {
    const { child, childInput, parent } = args;
    if (!registry.has(child.id)) {
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
    if (isTerminalRun(state)) {
      emitLog({
        level: "info",
        msg: "nagi: cancel skipped — run already terminal",
        attrs: { runId, status: runStatusOf(state) },
      });
      return;
    }
    const flow = registry.get(state.flowId);
    const canceledFact = Facts.flowCanceled(runId, args, clock.now());
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
      await hooks.fireHook(flow.onError, event, "flow.onError");
      await hooks.fireHook(config.hooks?.onFlowError, event, "onFlowError");
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

    await dispatcher.propagateToParent(runId, {
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
    // The one fork point for heartbeat tuning: collapse the public optionals to
    // a single required config here so the dispatch internals never re-check it.
    heartbeat: {
      intervalMs: config.heartbeatIntervalMs ?? DEFAULT_HEARTBEAT_INTERVAL_MS,
      leaseMs: config.heartbeatLeaseMs ?? DEFAULT_HEARTBEAT_LEASE_MS,
    },
    ...compact({
      hooks: config.hooks,
      defaultRetry: config.defaultRetry,
      streamTransport,
    }),
  };
  const dispatcher = makeDispatcher(dispatchDeps);
  // Runtime fires flow-level hooks directly (concurrency-cancel on start, and
  // cancelRunRecursive) — outside the dispatch path, so it owns its own Hooks.
  const hooks = makeHooks(dispatchDeps);

  const signals = makeSignals({
    dispatcher,
    store: config.store,
    clock,
    registry,
    hooks,
    emitLog,
    ...compact({ flowHooks: config.hooks }),
  });
  const replayer = makeReplay({
    ...dispatchDeps,
    registry,
    hashFor: (id) => flowHashById.get(id),
  });

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
      const flow = registry.require(flowId);

      let runId: RunId;
      if (opts?.runId !== undefined) {
        if (typeof opts.runId !== "string" || opts.runId.length === 0) {
          throw validationError("opts.runId must be a non-empty string", [
            "runId",
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

    signal: signals.signal,

    async cancel(runId: RunId, opts?: CancelOpts): Promise<void> {
      await cancelRunRecursive(runId, {
        cause: "explicit",
        reason: opts?.reason ?? "explicit wf.cancel()",
      });
    },

    operator(): Operator {
      return makeOperator({
        dispatcher,
        store: config.store,
        clock,
        registry,
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

    replay: replayer.replay,

    async queryRuns(opts: QueryRunsOpts = {}): Promise<QueryRunsResult> {
      if (opts.latest === true) {
        if (opts.limit !== undefined || opts.cursor !== undefined) {
          throw validationError(
            "queryRuns: `latest: true` is incompatible with `limit` / `cursor` — `latest` returns at most one row.",
            ["latest"],
          );
        }
      }
      return config.store.queryRuns(opts);
    },

    subscribe<C = Json>(
      runId: RunId,
      stepId: StepId,
      opts?: { readonly replayBuffered?: boolean },
    ): AsyncIterable<StreamEvent<C>> {
      // Registration guarantees a transport exists when a streaming step does,
      // so the `!` below is sound once this check passes.
      if (!registry.isStreaming(stepId)) {
        throw new NagiRuntimeError(
          `wf.subscribe: step "${stepId}" is not a streaming step ` +
            `(b.streamingTask) in any registered flow. ` +
            `Only streaming steps can be subscribed to.`,
        );
      }
      // C is caller-asserted; the transport yields StreamEvent<Json>. This cast
      // bridges the Json→C assertion.
      return streamTransport!.subscribeStream(
        runId,
        stepId,
        opts,
      ) as AsyncIterable<StreamEvent<C>>;
    },

    async pruneFacts(opts: PruneOpts): Promise<PruneResult> {
      if (
        !(opts.olderThan instanceof Date) ||
        Number.isNaN(opts.olderThan.getTime())
      ) {
        throw validationError("pruneFacts: `olderThan` must be a valid Date.", [
          "olderThan",
        ]);
      }
      const statuses: ReadonlyArray<PrunableStatus> = opts.statuses ?? [
        "completed",
      ];
      for (const s of statuses) {
        if (s !== "completed" && s !== "failed" && s !== "canceled") {
          throw validationError(
            `pruneFacts: status "${s}" is not prunable. Allowed: "completed" | "failed" | "canceled".`,
            ["statuses"],
          );
        }
      }
      const batchSize = opts.batchSize ?? 1000;
      if (!Number.isInteger(batchSize) || batchSize < 1) {
        throw validationError(
          "pruneFacts: `batchSize` must be a positive integer.",
          ["batchSize"],
        );
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
  // time, so assert the erased string back to the registered union here.
  return wf as unknown as Wf<TFlows>;
}

export interface NagiRunConfig extends NagiConfig {
  readonly worker?: Omit<WorkerConfig, "signal">;
  readonly signal?: AbortSignal;
}

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

export const nagi: typeof nagiImpl & { run: typeof nagiRun } = Object.assign(
  nagiImpl,
  { run: nagiRun },
);

function mintRunId(): RunId {
  return `run-${crypto.randomUUID()}` as RunId;
}

function asStreamTransport(store: Store): StreamTransport | undefined {
  const s = store as Partial<StreamTransport>;
  return typeof s.subscribeStream === "function" &&
    typeof s.publishChunk === "function"
    ? (s as StreamTransport)
    : undefined;
}
