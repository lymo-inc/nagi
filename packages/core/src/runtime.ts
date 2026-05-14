import {
  type CanonicalDag,
  canonicalize,
  sha256Canonical,
} from "./canonicalize";
import { advance, type DispatchDeps } from "./dispatch";
import { attachDef, getDef, type StepDef } from "./internal";
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
   * Handler-code identifier — typically a git SHA from `process.env.GIT_SHA`
   * or your build's bundle hash. Persisted on `workflow_run.code_version` and
   * on every `flow.started` fact for runs started by this process. Captures
   * handler-body drift orthogonally to the topology hash. See RFC 0001
   * "Topology vs handler code."
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
        ...(config.codeVersion !== undefined
          ? { codeVersion: config.codeVersion }
          : {}),
      };

      const { started } = await config.store.tryStartRun(runId, fact);
      if (!started) {
        // Idempotent no-op: a run with this ID already exists. Per the contract
        // we do NOT re-append the fact, do NOT re-dispatch, and do NOT
        // re-validate against the prior input (we don't have it, and even if
        // we did, callers asked for "use this runId" semantics — not "verify
        // the same input").
        return runId;
      }

      await config.hooks?.onFlowStart?.({
        runId,
        flowId: flow.id,
        input: validated,
        at: startedAt,
      });

      await advance(dispatchDeps, runId);
      return runId;
    },

    async signal(
      runId: RunId,
      stepName: string,
      payload: unknown,
    ): Promise<void> {
      const runState = await config.store.loadRunState(runId);
      const flow = flowsById.get(runState.flowId);
      if (!flow) {
        throw new NagiRuntimeError(
          `Run ${runId} references flow "${runState.flowId}" which is not registered with nagi().`,
        );
      }
      const step = flow.steps[stepName];
      if (!step) {
        throw new NagiRuntimeError(
          `Flow "${flow.id}" has no step named "${stepName}".`,
        );
      }
      const def = getDef(step);
      if (def.kind !== "signal") {
        throw new NagiRuntimeError(
          `Step "${stepName}" is a ${def.kind}, not a signal.`,
        );
      }
      const stepState = runState.steps[stepName];
      if (stepState?.status !== "running") {
        throw new NagiRuntimeError(
          `Step "${stepName}" is not waiting for signal (status: ${stepState?.status ?? "pending"}).`,
        );
      }

      const validated = (await validate(def.schema, payload)) as Json;
      const attempt = stepState.attempts > 0 ? stepState.attempts : 1;

      await config.store.appendFact(runId, {
        kind: "signal.received",
        runId,
        stepId: stepName,
        payload: validated,
        at: clock.now(),
      });

      const completedFact: Fact = {
        kind: "step.completed",
        runId,
        stepId: stepName,
        attempt,
        output: validated,
        at: clock.now(),
      };
      await config.store.completeStep(
        runId,
        stepName,
        validated,
        completedFact,
      );

      await config.hooks?.onSignalReceived?.({
        runId,
        flowId: flow.id,
        stepId: stepName,
        attempt,
        kind: "signal",
        payload: validated,
        at: clock.now(),
      });

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
      if (opts.mode === "inspect") return;

      // Drift detection. Runs started before snapshot tracking landed have
      // `flowHash === undefined`; for those we proceed without a check —
      // best-effort legacy behavior. Runs with a pinned hash get validated
      // against the live flow's hash.
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
          // allowDrift: build a synthesized flow whose topology comes from the
          // pinned snapshot and whose handler bodies come from the live flow.
          // Steps present in the snapshot but missing from the live flow
          // throw at synthesis time (clear failure rather than silent skip).
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
          const driftDeps: DispatchDeps = {
            ...dispatchDeps,
            flowFor: async () => synthesized,
          };
          await advance(driftDeps, runId);
          return;
        }
      }

      await advance(dispatchDeps, runId);
    },
  };
}

function mintRunId(): RunId {
  return `run-${crypto.randomUUID()}` as RunId;
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
