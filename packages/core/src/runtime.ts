import { advance, type DispatchDeps } from "./dispatch";
import { getDef } from "./internal";
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
}

export interface Wf {
  /** Begin a new run. Returns the runId. */
  start<F extends Flow>(flow: F, input: FlowInput<F>): Promise<RunId>;

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

export function nagi(config: NagiConfig): Wf {
  const clock = config.clock ?? new InMemoryClock();
  const flowsById = new Map<string, Flow>();
  for (const f of config.flows) {
    if (flowsById.has(f.id)) {
      throw new NagiRuntimeError(
        `Duplicate flow id "${f.id}" passed to nagi()`,
      );
    }
    flowsById.set(f.id, f);
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
    async start<F extends Flow>(flow: F, input: FlowInput<F>): Promise<RunId> {
      if (!flowsById.has(flow.id)) {
        throw new NagiRuntimeError(
          `Flow "${flow.id}" not registered with nagi(). Pass it to flows[].`,
        );
      }
      const validated = (await validate(flow.input, input)) as Json;
      const runId = mintRunId();

      await config.store.appendFact(runId, {
        kind: "flow.started",
        runId,
        flowId: flow.id,
        input: validated,
        at: clock.now(),
      });
      await config.hooks?.onFlowStart?.({
        runId,
        flowId: flow.id,
        input: validated,
        at: clock.now(),
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
      const flow = flowsById.get(runState.flowId);
      if (!flow) {
        throw new NagiRuntimeError(
          `Run ${runId} references flow "${runState.flowId}" which is not registered with nagi().`,
        );
      }
      if (opts.mode === "inspect") return;
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
