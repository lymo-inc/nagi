// Shared test fixtures + harnesses.
//
// Two modes:
// - `runFlow(flow, input)` — fire-and-forget end-to-end with a worker.
// - `makeHarness(flow)` — gives the test direct control of the dispatcher
//   (no worker), the queue, the store, and a `Result` snapshot.

import { type DispatchDeps, dispatchMessage } from "./dispatch";
import { InMemoryClock, InMemoryQueue, InMemoryStore } from "./memory";
import { type NagiConfig, nagi, type Wf } from "./runtime";
import type {
  Fact,
  Flow,
  FlowHooks,
  FlowInput,
  Json,
  RetryPolicy,
  RunId,
  RunState,
  RunStatus,
  SerializedError,
  StandardSchemaV1,
  StepStatus,
  WorkerConfig,
} from "./types";

export function passthroughSchema<T>(): StandardSchemaV1<T, T> {
  return {
    "~standard": {
      version: 1,
      vendor: "nagi-test",
      validate: (value: unknown) => ({ value: value as T }),
    },
  };
}

export function emptySchema(): StandardSchemaV1<
  Record<string, never>,
  Record<string, never>
> {
  return passthroughSchema<Record<string, never>>();
}

export interface Result {
  readonly status: RunStatus;
  readonly raw: RunState;

  /** Asserts the step exists and is completed; returns its output. */
  output(stepName: string): Json;
  /** Returns the step's status, or "pending" if not in run state. */
  stepStatus(stepName: string): StepStatus;
  /** Asserts the step failed; returns its serialized error. */
  error(stepName: string): SerializedError;

  factCount(kind: Fact["kind"]): number;
  factsOf<K extends Fact["kind"]>(
    kind: K,
  ): readonly Extract<Fact, { kind: K }>[];
}

function makeResult(state: RunState): Result {
  return {
    status: state.status,
    raw: state,
    output(stepName) {
      const step = state.steps[stepName];
      if (!step) throw new Error(`Result.output: no step "${stepName}" in run`);
      if (step.status !== "completed") {
        throw new Error(
          `Result.output: step "${stepName}" status is "${step.status}", not "completed"`,
        );
      }
      return step.output ?? null;
    },
    stepStatus(stepName) {
      return state.steps[stepName]?.status ?? "pending";
    },
    error(stepName) {
      const step = state.steps[stepName];
      if (!step?.error) {
        throw new Error(`Result.error: step "${stepName}" did not fail`);
      }
      return step.error;
    },
    factCount(kind) {
      return state.facts.filter((f) => f.kind === kind).length;
    },
    factsOf<K extends Fact["kind"]>(kind: K) {
      return state.facts.filter((f) => f.kind === kind) as Extract<
        Fact,
        { kind: K }
      >[];
    },
  };
}

export interface HarnessOpts {
  readonly defaultRetry?: RetryPolicy;
  readonly logger?: NagiConfig["logger"];
  readonly hooks?: FlowHooks;
}

export interface Harness {
  readonly wf: Wf;
  readonly store: InMemoryStore;
  readonly queue: InMemoryQueue;
  readonly clock: InMemoryClock;

  /** DispatchDeps for driver-style tests that call `dispatchMessage` directly. */
  readonly deps: DispatchDeps;

  /** Start a worker; returns a `stop()` that aborts and waits for drain. */
  startWorker(config?: WorkerConfig): { stop: () => Promise<void> };

  /** Dequeue at most `count` messages and dispatch each sequentially. */
  drainOnce(count?: number): Promise<number>;
  /** Repeat `drainOnce` until the queue is empty. */
  drain(opts?: { maxIter?: number }): Promise<number>;

  /** Block until `flow.completed` or `flow.failed` is the last fact. */
  waitForEnd(runId: RunId, timeoutMs?: number): Promise<Result>;
  /** Block until a specific step reaches the target status. */
  waitForStep(
    runId: RunId,
    stepName: string,
    status: StepStatus,
    timeoutMs?: number,
  ): Promise<RunState>;

  /** Snapshot the current run state into a `Result`. */
  result(runId: RunId): Promise<Result>;
}

export function makeHarness(
  flows: Flow | ReadonlyArray<Flow>,
  opts?: HarnessOpts,
): Harness {
  const flowList = Array.isArray(flows) ? flows : [flows as Flow];
  const store = new InMemoryStore();
  const queue = new InMemoryQueue();
  const clock = new InMemoryClock();

  const wf = nagi({
    flows: flowList,
    store,
    queue,
    clock,
    ...(opts?.defaultRetry !== undefined
      ? { defaultRetry: opts.defaultRetry }
      : {}),
    ...(opts?.logger !== undefined ? { logger: opts.logger } : {}),
    ...(opts?.hooks !== undefined ? { hooks: opts.hooks } : {}),
  });

  if (flowList.length === 0) throw new Error("makeHarness: no flows provided");

  const flowsById = new Map(flowList.map((f) => [f.id, f]));

  const deps: DispatchDeps = {
    flowFor: async (runId) => {
      const runState = await store.loadRunState(runId);
      const flow = flowsById.get(runState.flowId);
      if (!flow) {
        throw new Error(
          `makeHarness: run ${runId} references flow "${runState.flowId}" which is not registered.`,
        );
      }
      return flow;
    },
    store,
    queue,
    clock,
    ...(opts?.defaultRetry !== undefined
      ? { defaultRetry: opts.defaultRetry }
      : {}),
    ...(opts?.logger !== undefined ? { logger: opts.logger } : {}),
    ...(opts?.hooks !== undefined ? { hooks: opts.hooks } : {}),
  };

  return {
    wf,
    store,
    queue,
    clock,
    deps,

    startWorker(config) {
      const ac = new AbortController();
      const merged: WorkerConfig = {
        pollIntervalMs: config?.pollIntervalMs ?? 5,
        ...(config?.concurrency !== undefined
          ? { concurrency: config.concurrency }
          : {}),
        signal: config?.signal ?? ac.signal,
      };
      const worker = wf.worker(merged);
      const done = worker.run();
      return {
        stop: async () => {
          ac.abort();
          await done;
        },
      };
    },

    async drainOnce(count = 32) {
      const messages = await queue.dequeue({ count });
      for (const msg of messages) {
        await dispatchMessage(deps, msg);
      }
      return messages.length;
    },

    async drain(opts) {
      const max = opts?.maxIter ?? 256;
      let total = 0;
      for (let i = 0; i < max; i++) {
        const n = await this.drainOnce();
        if (n === 0) return total;
        total += n;
      }
      throw new Error(`drain: exceeded ${max} iterations`);
    },

    async waitForEnd(runId, timeoutMs = 3_000) {
      const start = Date.now();
      while (Date.now() - start < timeoutMs) {
        const state = await store.loadRunState(runId);
        const last = state.facts[state.facts.length - 1];
        if (last?.kind === "flow.completed" || last?.kind === "flow.failed") {
          return makeResult(state);
        }
        await new Promise((r) => setTimeout(r, 5));
      }
      throw new Error(`waitForEnd: timeout after ${timeoutMs}ms`);
    },

    async waitForStep(runId, stepName, status, timeoutMs = 3_000) {
      const start = Date.now();
      while (Date.now() - start < timeoutMs) {
        const state = await store.loadRunState(runId);
        if (state.steps[stepName]?.status === status) return state;
        await new Promise((r) => setTimeout(r, 5));
      }
      throw new Error(
        `waitForStep("${stepName}", "${status}"): timeout after ${timeoutMs}ms`,
      );
    },

    async result(runId) {
      return makeResult(await store.loadRunState(runId));
    },
  };
}

export async function runFlow<F extends Flow>(
  flow: F,
  input: FlowInput<F>,
  opts?: HarnessOpts & { timeoutMs?: number; pollIntervalMs?: number },
): Promise<Result> {
  const harness = makeHarness(flow, opts);
  const worker = harness.startWorker(
    opts?.pollIntervalMs !== undefined
      ? { pollIntervalMs: opts.pollIntervalMs }
      : {},
  );
  try {
    const runId = await harness.wf.start(flow, input);
    return await harness.waitForEnd(runId, opts?.timeoutMs);
  } finally {
    await worker.stop();
  }
}
