import { type MockInstance, vi } from "vitest";
import { InMemoryClock, InMemoryQueue, InMemoryStore } from "../memory";
import { type NagiConfig, nagi, type Wf } from "../runtime";
import { errorOf, isTerminalRun, runStatusOf, stepStatusOf } from "../state";
import type {
  Fact,
  Flow,
  FlowHooks,
  FlowInput,
  Json,
  LogEntry,
  RetryPolicy,
  RunId,
  RunState,
  RunStatus,
  SerializedError,
  StandardSchemaV1,
  StepStatus,
  Worker,
  WorkerConfig,
} from "../types";

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

  output(stepName: string): Json;
  stepStatus(stepName: string): StepStatus;
  error(stepName: string): SerializedError;

  factCount(kind: Fact["kind"]): number;
  factsOf<K extends Fact["kind"]>(
    kind: K,
  ): readonly Extract<Fact, { kind: K }>[];
}

function makeResult(state: RunState): Result {
  return {
    status: runStatusOf(state),
    raw: state,
    output(stepName) {
      const step = state.steps[stepName];
      if (!step) throw new Error(`Result.output: no step "${stepName}" in run`);
      if (step.tag !== "completed") {
        throw new Error(
          `Result.output: step "${stepName}" status is "${stepStatusOf(step)}", not "completed"`,
        );
      }
      return step.output;
    },
    stepStatus(stepName) {
      const step = state.steps[stepName];
      return step ? stepStatusOf(step) : "pending";
    },
    error(stepName) {
      const step = state.steps[stepName];
      const err = step ? errorOf(step) : undefined;
      if (!err) {
        throw new Error(`Result.error: step "${stepName}" did not fail`);
      }
      return err;
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
  readonly onLog?: NagiConfig["onLog"];
  readonly hooks?: FlowHooks;
}

export function spyOnLog(): {
  onLog: NonNullable<NagiConfig["onLog"]>;
  entries: LogEntry[];
} {
  const entries: LogEntry[] = [];
  return {
    onLog: (entry) => entries.push(entry),
    entries,
  };
}

export interface Harness {
  readonly wf: Wf;
  readonly store: InMemoryStore;
  readonly queue: InMemoryQueue;
  readonly clock: InMemoryClock;

  startWorker(config?: WorkerConfig): { stop: () => Promise<void> };

  // Bounded drains through the real Worker at concurrency 1, so dispatch order
  // is queue order. Both stop at the first empty dequeue.
  drainOnce(maxSteps?: number): Promise<number>;
  drain(opts?: { maxSteps?: number }): Promise<number>;

  waitForEnd(runId: RunId, timeoutMs?: number): Promise<Result>;
  waitForStep(
    runId: RunId,
    stepName: string,
    status: StepStatus,
    timeoutMs?: number,
  ): Promise<RunState>;

  result(runId: RunId): Promise<Result>;
}

export async function makeHarness(
  flows: Flow | ReadonlyArray<Flow>,
  opts?: HarnessOpts,
): Promise<Harness> {
  const flowList = Array.isArray(flows) ? flows : [flows as Flow];
  const store = new InMemoryStore();
  const queue = new InMemoryQueue();
  const clock = new InMemoryClock();

  const wf = await nagi({
    flows: flowList,
    store,
    queue,
    clock,
    ...(opts?.defaultRetry !== undefined
      ? { defaultRetry: opts.defaultRetry }
      : {}),
    ...(opts?.onLog !== undefined ? { onLog: opts.onLog } : {}),
    ...(opts?.hooks !== undefined ? { hooks: opts.hooks } : {}),
  });

  if (flowList.length === 0) throw new Error("makeHarness: no flows provided");

  const worker: Worker = wf.worker({ concurrency: 1 });

  return {
    wf,
    store,
    queue,
    clock,

    startWorker(config) {
      const ac = new AbortController();
      const done = wf
        .worker({
          pollIntervalMs: 5,
          ...config,
          signal: config?.signal ?? ac.signal,
        })
        .run();
      return {
        stop: async () => {
          ac.abort();
          await done;
        },
      };
    },

    async drainOnce(maxSteps = 1) {
      const { processed } = await worker.runOnce({ maxSteps });
      return processed;
    },

    async drain(opts) {
      const maxSteps = opts?.maxSteps ?? 4096;
      const { processed } = await worker.runOnce({ maxSteps });
      if (processed >= maxSteps) {
        throw new Error(`drain: exceeded ${maxSteps} steps`);
      }
      return processed;
    },

    async waitForEnd(runId, timeoutMs = 3_000) {
      const start = Date.now();
      while (Date.now() - start < timeoutMs) {
        const state = await store.loadRunState(runId);
        if (isTerminalRun(state)) {
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
        const step = state.steps[stepName];
        if (step !== undefined && stepStatusOf(step) === status) return state;
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

export function leasePorts(): {
  store: InMemoryStore;
  queue: InMemoryQueue;
  extendLease: MockInstance<InMemoryStore["extendLease"]>;
  extend: MockInstance<InMemoryQueue["extend"]>;
} {
  const store = new InMemoryStore();
  const queue = new InMemoryQueue();
  return {
    store,
    queue,
    extendLease: vi.spyOn(store, "extendLease").mockResolvedValue(undefined),
    extend: vi.spyOn(queue, "extend").mockResolvedValue(undefined),
  };
}

export async function runFlow<F extends Flow>(
  flow: F,
  input: FlowInput<F>,
  opts?: HarnessOpts & { timeoutMs?: number; pollIntervalMs?: number },
): Promise<Result> {
  const harness = await makeHarness(flow, opts);
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
