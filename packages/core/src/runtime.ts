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
  Operator,
  OperatorAuditOpts,
  OperatorSkipOpts,
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
  readonly codeVersion?: string;
}

export interface StartOpts {
  readonly runId?: RunId;
}

export interface CancelOpts {
  readonly reason?: string;
}

export interface Wf {
  start<F extends Flow>(
    flow: F,
    input: FlowInput<F>,
    opts?: StartOpts,
  ): Promise<RunId>;

  signal(runId: RunId, stepName: string, payload: unknown): Promise<void>;

  cancel(runId: RunId, opts?: CancelOpts): Promise<void>;

  worker(config?: WorkerConfig): Worker;

  replay(runId: RunId, opts?: ReplayOpts): Promise<void>;

  queryRuns(opts?: QueryRunsOpts): Promise<QueryRunsResult>;

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
      throw new NagiRuntimeError(
        `startChildRun: minted child runId collided with an existing run (${childRunId})`,
      );
    }
    return childRunId;
  }

  async function cancelRunRecursive(
    runId: RunId,
    args: {
      readonly cause: "explicit" | "operator";
      readonly reason: string;
      readonly actor?: string;
      readonly note?: string;
    },
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
      cause: args.cause,
      runId,
      at: clock.now(),
      canceledByRunId: runId,
      concurrencyKey: args.cause === "explicit" ? args.reason : "",
      ...(args.actor !== undefined ? { actor: args.actor } : {}),
      ...(args.note !== undefined ? { note: args.note } : {}),
    };
    await config.store.appendFact(runId, canceledFact);
    if (flow !== undefined) {
      const cancelError: SerializedError = {
        name: "NagiCanceledError",
        message: `Run ${runId} was canceled: ${args.reason}`,
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
      await cancelRunRecursive(childId, {
        cause: "explicit",
        reason: `parent ${runId} canceled: ${args.reason}`,
        note:
          args.cause === "operator" && args.actor !== undefined
            ? `cascade from operator ${args.actor} aborting parent ${runId}`
            : `cascade from parent ${runId}`,
      });
    }

    const cancelError: SerializedError = {
      name: "NagiCanceledError",
      message: `Run ${runId} was canceled: ${args.reason}`,
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
        ...(config.logger !== undefined ? { logger: config.logger } : {}),
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
  return wf;
}

function mintRunId(): RunId {
  return `run-${crypto.randomUUID()}` as RunId;
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
    args: {
      readonly cause: "explicit" | "operator";
      readonly reason: string;
      readonly actor?: string;
      readonly note?: string;
    },
  ) => Promise<void>;
  readonly logger?: Logger;
}

function makeOperator(o: OperatorDeps): Operator {
  const { deps, clock, flowsById, cancelRunRecursive } = o;
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
    const stepState = state.steps[stepId];
    if (
      stepState !== undefined &&
      (stepState.status === "completed" ||
        stepState.status === "failed" ||
        stepState.status === "skipped" ||
        stepState.status === "canceled")
    ) {
      o.logger?.info("nagi: operator.skip noop — step already terminal", {
        runId,
        stepId,
        status: stepState.status,
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
      ...(opts.note !== undefined ? { note: opts.note } : {}),
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
    const stepState = state.steps[stepId];

    if (stepState !== undefined && stepState.status === "running") {
      const abortFact: Fact = {
        kind: "step.abort-requested",
        runId,
        at: clock.now(),
        stepId,
        attempt: stepState.attempts,
        actor: opts.actor,
        ...(opts.note !== undefined ? { note: opts.note } : {}),
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
              ...(opts.note !== undefined ? { note: opts.note } : {}),
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
      ...(opts.note !== undefined ? { note: opts.note } : {}),
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
  const synthesized: Record<string, ReturnType<typeof attachDef>> = {};

  for (const canonStep of dag.steps) {
    synthesized[canonStep.id] = attachDef(
      { kind: canonStep.kind, id: canonStep.id },
      { kind: canonStep.kind } as unknown as StepDef,
    );
  }

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
