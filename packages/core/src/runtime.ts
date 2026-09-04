import {
  canonicalize,
  fingerprintFlows,
  sha256Canonical,
} from "./canonicalize";
import { type DispatchDeps, makeDispatcher } from "./dispatch";
import { NagiRuntimeError, validationError } from "./errors";
import { makeHooks } from "./exec/hooks";
import { Facts } from "./facts";
import { makeFlowRegistry } from "./flow-registry";
import { asStepMapWithDefs, compact, getDef, makeEmit } from "./internal";
import { DEFAULT_REAPER_INTERVAL_MS } from "./lease-reaper";
import { InMemoryClock } from "./memory";
import { makeOperator } from "./operator";
import { makeReplay } from "./replay";
import { makeRunLifecycle } from "./run-lifecycle";
import type { RunDescription } from "./run-view";
import { makeSignals } from "./signals";
import {
  DEFAULT_HEARTBEAT_INTERVAL_MS,
  DEFAULT_HEARTBEAT_LEASE_MS,
  DEFAULT_LEASE_HOLD_WARN_MS,
} from "./step-exec";
import type {
  Clock,
  Flow,
  FlowHooks,
  FlowIdOf,
  FlowInput,
  Json,
  LogEntry,
  Millis,
  Operator,
  PrunableStatus,
  PruneOpts,
  PruneResult,
  QueryRunsOpts,
  QueryRunsResult,
  Queue,
  QueueInspectEntry,
  ReplayOpts,
  RetryPolicy,
  RunId,
  StepId,
  Store,
  StreamEvent,
  StreamTransport,
  Trigger,
  Tx,
  Worker,
  WorkerConfig,
} from "./types";
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
  // Warn each time a step body has held a worker slot for another multiple of
  // this (default 5 min; 0 disables). A firing watchdog usually means an
  // in-step wait on an external fact — model those as b.signal steps, which
  // park WITHOUT holding a slot. Timeout-less in-step waits have starved
  // entire worker pools in production.
  readonly leaseHoldWarnMs?: Millis;
  // Lease-reaper sweep cadence. Default 30s — stay ≤ ½ the store lease TTL so
  // a crashed worker is reaped within one TTL. 0 disables the reaper (tests,
  // or external/cron-driven reaping).
  readonly reaperIntervalMs?: Millis;
}

export interface StartOpts {
  readonly runId?: RunId;
}

// Two-phase staged start (D5=B). The caller supplies a transaction that the
// store run-row insert + flow.started fact + initial-step queue enqueue all
// commit (or roll back) atomically with the caller's own writes. Hooks and
// child-of-parent propagation are deferred to applyOnCommit so they only run
// after the caller's tx commits — never against a half-canceled-now-rolled-
// back prior run.
export interface StartStagedOpts {
  readonly tx: Tx;
  readonly runId?: RunId;
}

// `started` is false when an existing runId blocked the insert. `canceled`
// lists the prior runs that were superseded under concurrency-cancel (their
// row + fact commit with the caller tx; their hooks fire from applyOnCommit).
// applyOnCommit is idempotent: a second call after the first is a no-op.
export interface StartStagedResult {
  readonly started: boolean;
  readonly canceled: readonly RunId[];
  readonly applyOnCommit: () => Promise<void>;
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

  // Staged transactional start (D5=B). The store run-row insert, flow.started
  // fact, and initial-step queue enqueue commit (or roll back) on the
  // caller's `opts.tx`. Concurrency-supersession cancel hooks AND
  // dispatcher.propagateToParent fire from applyOnCommit, which the caller
  // MUST invoke after their tx commits. Idempotent: a second applyOnCommit
  // call is a no-op.
  startStaged<F extends TFlows[number]>(
    flow: F,
    input: FlowInput<F>,
    opts: StartStagedOpts,
  ): Promise<StartStagedResult & { readonly runId: RunId }>;

  startStagedById(
    flowId: string,
    input: unknown,
    opts: StartStagedOpts,
  ): Promise<StartStagedResult & { readonly runId: RunId }>;

  signal(runId: RunId, stepName: string, payload: unknown): Promise<void>;

  cancel(runId: RunId, opts?: CancelOpts): Promise<void>;

  worker(config?: WorkerConfig): Worker;

  replay(runId: RunId, opts?: ReplayOpts): Promise<void>;

  queryRuns(
    opts?: QueryRunsOpts<FlowIdOf<TFlows>>,
  ): Promise<QueryRunsResult<FlowIdOf<TFlows>>>;

  // Returns the canonical projection of a single run + its steps. Returns null
  // for an unknown runId (never throws). Parent/children are nested on the
  // returned RunView; the outer envelope is `{run, steps}` only.
  describe(runId: RunId): Promise<RunDescription>;

  // Read-only triage view of the run's in-queue messages, complementing
  // describe(): a non-terminal run with pending steps and NO queue entries was
  // never scheduled (worker starvation); a future visibleAt is leased/delayed;
  // a high readCount is a redelivery loop. Throws NagiRuntimeError when the
  // queue adapter doesn't implement inspect().
  inspectQueue(runId: RunId): Promise<readonly QueueInspectEntry[]>;

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

  // dispatch path: when the run was pinned to a flowHash, fail loud if the
  // registry no longer matches (NagiFlowSnapshotGoneError). Legacy runs with
  // no pinned hash continue to resolve by flowId.
  async function flowFor(runId: RunId): Promise<Flow> {
    const runState = await config.store.loadRunState(runId);
    return registry.requireForRun(
      runState.flowId,
      runId,
      runState.flowHash,
      (id) => flowHashById.get(id),
    );
  }

  function lookupFlow(flowId: string): Flow | undefined {
    return registry.get(flowId);
  }

  const dispatchDeps: DispatchDeps = {
    flowFor,
    lookupFlow,
    startChildRun: (args) => lifecycle.startChildRun(args),
    store: config.store,
    queue: config.queue,
    clock,
    emitLog,
    // The one fork point for heartbeat tuning: collapse the public optionals to
    // a single required config here so the dispatch internals never re-check it.
    heartbeat: {
      intervalMs: config.heartbeatIntervalMs ?? DEFAULT_HEARTBEAT_INTERVAL_MS,
      leaseMs: config.heartbeatLeaseMs ?? DEFAULT_HEARTBEAT_LEASE_MS,
      holdWarnMs: config.leaseHoldWarnMs ?? DEFAULT_LEASE_HOLD_WARN_MS,
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
  const lifecycle = makeRunLifecycle({
    store: config.store,
    queue: config.queue,
    clock,
    registry,
    codeVersion,
    hashFor: (id) => flowHashById.get(id),
    queueForTx: (tx) => bindQueueToTx(config.queue, tx),
    hooks,
    flowHooks: config.hooks,
    dispatcher,
    emitLog,
  });

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
      const { runId, staged } = await lifecycle.start({
        flowId,
        input,
        runId: opts?.runId,
        boundary: { kind: "own" },
      });
      if (staged.kind === "started")
        await lifecycle.applyEffects(staged.effects);
      return runId;
    },

    async startStaged<F extends Flow>(
      flow: F,
      input: FlowInput<F>,
      opts: StartStagedOpts,
    ): Promise<StartStagedResult & { readonly runId: RunId }> {
      return wf.startStagedById(flow.id, input, opts);
    },

    async startStagedById(
      flowId: string,
      input: unknown,
      opts: StartStagedOpts,
    ): Promise<StartStagedResult & { readonly runId: RunId }> {
      if (opts === undefined || opts.tx === undefined) {
        throw validationError(
          "startStaged: opts.tx is required (the caller-supplied transaction the run row, flow.started fact, and initial enqueue commit on).",
          ["tx"],
        );
      }
      const { runId, staged } = await lifecycle.start({
        flowId,
        input,
        runId: opts.runId,
        boundary: { kind: "caller", tx: opts.tx },
      });
      if (staged.kind === "exists") {
        return { runId, started: false, canceled: [], applyOnCommit: noop };
      }
      let applied: Promise<void> | undefined;
      return {
        runId,
        started: true,
        canceled: staged.effects.superseded.map((e) => e.runId),
        applyOnCommit: () =>
          (applied ??= lifecycle.applyEffects(staged.effects)),
      };
    },

    signal: signals.signal,

    async cancel(runId: RunId, opts?: CancelOpts): Promise<void> {
      await lifecycle.cancelRunRecursive(runId, {
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
        cancelRunRecursive: lifecycle.cancelRunRecursive,
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

    async describe(runId: RunId): Promise<RunDescription> {
      return config.store.describe(runId);
    },

    async inspectQueue(runId: RunId): Promise<readonly QueueInspectEntry[]> {
      if (config.queue.inspect === undefined) {
        throw new NagiRuntimeError(
          "inspectQueue: the configured queue adapter does not implement inspect().",
        );
      }
      return config.queue.inspect(runId);
    },

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
  const clock = config.clock ?? new InMemoryClock();
  const loop = worker.run();
  loop.catch((err: unknown) => {
    if (signal.aborted) return; // graceful shutdown — not a crash
    emitLog({
      level: "error",
      msg: "nagi.run: worker exited unexpectedly",
      attrs: { error: String(err) },
    });
  });

  // Lease-reaper loop. Driven by clock.sleep so fake clocks (tests) can step
  // it deterministically; aborts on stop() via the shared signal so process
  // shutdown drains cleanly without orphaning a pending timer.
  const reaperIntervalMs =
    config.reaperIntervalMs ?? DEFAULT_REAPER_INTERVAL_MS;
  const reaper: Promise<void> =
    reaperIntervalMs > 0
      ? (async () => {
          while (!signal.aborted) {
            try {
              await clock.sleep(reaperIntervalMs, signal);
            } catch {
              return; // aborted via signal — graceful shutdown
            }
            if (signal.aborted) return;
            try {
              await config.store.sweepLeases({
                now: clock.now(),
                queue: config.queue,
              });
            } catch (err) {
              emitLog({
                level: "warn",
                msg: "nagi.run: lease reaper sweep failed",
                attrs: { error: String(err) },
              });
            }
          }
        })()
      : Promise.resolve();
  reaper.catch(() => {
    // sweep errors are already logged; this catch only guards against an
    // uncaught rejection from the loop's own teardown.
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
      try {
        await reaper;
      } catch {
        // reaper rejections are already logged above.
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

async function noop(): Promise<void> {}

function asStreamTransport(store: Store): StreamTransport | undefined {
  const s = store as Partial<StreamTransport>;
  return typeof s.subscribeStream === "function" &&
    typeof s.publishChunk === "function"
    ? (s as StreamTransport)
    : undefined;
}

// Adapters that expose `withTx` (e.g. pgmq) join the supplied tx so the
// initial-step enqueue commits atomically with the store run-row insert +
// flow.started fact. Plain queues (in-memory) ignore the tx — there is no
// atomicity to inherit there anyway.
interface QueueWithTx extends Queue {
  withTx(tx: Tx): Queue;
}
function bindQueueToTx(queue: Queue, tx: Tx): Queue {
  const q = queue as Partial<QueueWithTx>;
  if (typeof q.withTx === "function") return q.withTx(tx);
  return queue;
}
