import { type DispatchDeps, dispatchMessage } from "../dispatch";
import { InMemoryClock, InMemoryQueue, InMemoryStore } from "../memory";
import { type NagiConfig, nagi, type Wf } from "../runtime";
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
  readonly onLog?: NagiConfig["onLog"];
  readonly hooks?: FlowHooks;
}

/**
 * Capture helper for the RFC 0020 `onLog` sink: returns an `onLog` callback and
 * the array it appends every {@link LogEntry} into, for assertions on
 * `{ level, msg, attrs }`.
 */
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

  readonly deps: DispatchDeps;

  startWorker(config?: WorkerConfig): { stop: () => Promise<void> };

  drainOnce(count?: number): Promise<number>;
  drain(opts?: { maxIter?: number }): Promise<number>;

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

  const deps = (wf as unknown as { __dispatchDeps: DispatchDeps })
    .__dispatchDeps;

  async function drainOnce(count = 32): Promise<number> {
    const messages = await queue.dequeue({ count });
    for (const msg of messages) {
      await dispatchMessage(deps, msg);
    }
    return messages.length;
  }

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

    drainOnce,

    async drain(opts) {
      const max = opts?.maxIter ?? 256;
      let total = 0;
      for (let i = 0; i < max; i++) {
        const n = await drainOnce();
        if (n === 0) return total;
        total += n;
      }
      throw new Error(`drain: exceeded ${max} iterations`);
    },

    async waitForEnd(runId, timeoutMs = 3_000) {
      const start = Date.now();
      while (Date.now() - start < timeoutMs) {
        const state = await store.loadRunState(runId);
        // Check via projected status so post-cancellation step facts (which
        // may interleave after `flow.canceled`) don't trip the check.
        if (
          state.status === "completed" ||
          state.status === "failed" ||
          state.status === "canceled"
        ) {
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
