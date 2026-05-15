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
} from "./dispatch";
import { attachDef, getDef, type SignalDef, type StepDef } from "./internal";
import { InMemoryClock } from "./memory";
import type {
  Clock,
  Fact,
  Flow,
  FlowHooks,
  FlowInput,
  Json,
  Logger,
  Queue,
  ReplayOpts,
  RetryPolicy,
  RunId,
  SerializedError,
  StandardSchemaV1,
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

export interface Wf {
  /** Begin a new run. Returns the runId. */
  start<F extends Flow>(
    flow: F,
    input: FlowInput<F>,
    opts?: StartOpts,
  ): Promise<RunId>;

  /** Resolve a `b.signal()` step waiting on external input. */
  signal(runId: RunId, stepName: string, payload: unknown): Promise<void>;

  /** Construct a worker; call `run` / `runOnce` / `runUntilEmpty` on it. */
  worker(config?: WorkerConfig): Worker;

  /**
   * Re-dispatch from the first incomplete step. `mode: "continue"` runs side
   * effects (idempotency protects); `mode: "inspect"` is a no-op probe.
   */
  replay(runId: RunId, opts?: ReplayOpts): Promise<void>;
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

  const dispatchDeps: DispatchDeps = {
    flowFor,
    store: config.store,
    queue: config.queue,
    clock,
    ...(config.hooks !== undefined ? { hooks: config.hooks } : {}),
    ...(config.logger !== undefined ? { logger: config.logger } : {}),
    ...(config.defaultRetry !== undefined
      ? { defaultRetry: config.defaultRetry }
      : {}),
  };

  return {
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
      const startedAt = clock.now();
      const flowHash = flowHashById.get(flow.id);
      const fact = {
        kind: "flow.started" as const,
        runId,
        flowId: flow.id,
        input: validated,
        at: startedAt,
        ...(flowHash !== undefined ? { flowHash } : {}),
        codeVersion,
      };

      // Derive the concurrency group key from validated input, if configured.
      // `keyFn` is synchronous and pure per the FlowConcurrency contract.
      // Non-string / empty return → validation error (catches typos like
      // `(input) => input.dealId` where `dealId` doesn't exist on the schema).
      let concurrencyArg:
        | { readonly key: string; readonly mode: "cancel-in-progress" }
        | undefined;
      if (flow.concurrency !== undefined) {
        const derived = flow.concurrency.keyFn(validated);
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
      if (!started) {
        // Idempotent no-op: a run with this ID already exists. Per the contract
        // we do NOT re-append the fact, do NOT re-dispatch, and do NOT
        // re-validate against the prior input (we don't have it, and even if
        // we did, callers asked for "use this runId" semantics — not "verify
        // the same input").
        return runId;
      }

      // Fire onError hooks for runs that were canceled atomically inside
      // tryStartRun. We do this before the new run's onStart so observers
      // see the supersede/start pair in the natural cause→effect order.
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
      }

      const startEvent = {
        runId,
        flowId: flow.id,
        input: validated,
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
        }
      }

      await advance(replayDeps, runId);
      // Drain inline so the hook-suppression scope covers every dispatch
      // this replay initiates. Without it, advance() only enqueues work
      // and a worker would later run those dispatches with worker-scoped
      // deps (no suppression) — defeating the flag's purpose.
      if (!fireHooks) await drainInline(replayDeps);
    },
  };
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
