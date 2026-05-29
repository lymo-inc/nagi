import type { ReapedLease } from "./lease-reaper";
import type { RunDescription } from "./run-view";
import type { Resolved, RunState, StepState } from "./state";

export type Json =
  | string
  | number
  | boolean
  | null
  | Json[]
  | { [key: string]: Json };

export type Millis = number;
export type RunId = string & { readonly __brand: "RunId" };
export type StepId = string;
export type AttemptNumber = number;

export interface StandardSchemaV1<Input = unknown, Output = Input> {
  readonly "~standard": StandardSchemaV1.Props<Input, Output>;
}

export namespace StandardSchemaV1 {
  export interface Props<Input = unknown, Output = Input> {
    readonly version: 1;
    readonly vendor: string;
    readonly validate: (
      value: unknown,
    ) => Result<Output> | Promise<Result<Output>>;
    readonly types?: Types<Input, Output>;
  }

  export type Result<Output> = SuccessResult<Output> | FailureResult;

  export interface SuccessResult<Output> {
    readonly value: Output;
    readonly issues?: undefined;
  }

  export interface FailureResult {
    readonly issues: ReadonlyArray<Issue>;
  }

  export interface Issue {
    readonly message: string;
    readonly path?: ReadonlyArray<PropertyKey | PathSegment>;
  }

  export interface PathSegment {
    readonly key: PropertyKey;
  }

  export interface Types<Input = unknown, Output = Input> {
    readonly input: Input;
    readonly output: Output;
  }
}

export type InferSchemaInput<S> =
  S extends StandardSchemaV1<infer I, unknown> ? I : never;
export type InferSchemaOutput<S> =
  S extends StandardSchemaV1<unknown, infer O> ? O : never;

// biome-ignore lint/suspicious/noEmptyInterface: intentional augmentation slot
export interface Register {}

export type Tx = Register extends { tx: infer T } ? T : unknown;

export type StepKind =
  | "task"
  | "activity"
  | "signal"
  | "match"
  | "subflow"
  | "streaming";

export interface Step<Output = unknown> {
  readonly kind: StepKind;
  readonly id: StepId;
  readonly __output?: Output;
}

export type StepOutput<S> = S extends Step<infer O> ? O : never;
export type StepMap = Readonly<Record<string, Step<unknown>>>;

export interface Optional<S extends Step = Step> {
  readonly __optional: S;
}
export type NeedRef = Step | Optional<Step>;
export type NeedsMap = Readonly<Record<string, NeedRef>>;

export type NeedsOutputs<N extends NeedsMap> = {
  readonly [K in keyof N]: StepOutput<N[K]>;
};

export type ResolvedNeeds<N extends NeedsMap> = {
  readonly [K in keyof N]: N[K] extends Optional<infer S>
    ? Resolved<StepOutput<S>>
    : N[K] extends Step
      ? StepOutput<N[K]>
      : never;
};

export type LogLevel = "debug" | "info" | "warn" | "error";

export interface LogEntry {
  readonly level: LogLevel;
  readonly msg: string;
  readonly attrs?: Record<string, unknown>;
}

export interface Logger {
  debug(message: string, attrs?: Record<string, unknown>): void;
  info(message: string, attrs?: Record<string, unknown>): void;
  warn(message: string, attrs?: Record<string, unknown>): void;
  error(message: string, attrs?: Record<string, unknown>): void;
}

export interface StepCtx<Input = unknown> {
  readonly input: Input;
  readonly tx: Tx;
  readonly runId: RunId;
  readonly stepId: StepId;
  readonly attempt: AttemptNumber;
  readonly signal: AbortSignal;
  readonly now: () => Date;
  readonly logger: Logger;

  once<T extends Json>(scope: string, fn: () => Promise<T>): Promise<T>;

  idempotencyKey(scope: string): string;
}

// An activity step runs its body OUTSIDE the durable transaction (RFC 0013), so
// it has no `tx`.
export type ActivityCtx<Input = unknown> = Omit<StepCtx<Input>, "tx">;

export type StreamEvent<C = Json> =
  | { readonly kind: "chunk"; readonly chunk: C }
  | { readonly kind: "dropped"; readonly count: number }
  | { readonly kind: "retry"; readonly attempt: AttemptNumber }
  | { readonly kind: "error"; readonly error: SerializedError };

export interface StreamingStepCtx<Input = unknown, Chunk = Json>
  extends StepCtx<Input> {
  readonly emit: (chunk: Chunk) => Promise<void>;
}

export type BackoffStrategy = "exponential" | "linear" | "fixed";

export interface RetryPolicy {
  readonly maxAttempts: number;
  readonly backoff: BackoffStrategy;
  readonly initialDelayMs?: Millis;
  readonly maxDelayMs?: Millis;
  readonly retryOn?: (error: unknown) => boolean;
}

interface StepConfigBase<Input, N extends NeedsMap> {
  readonly needs?: N;
  readonly when?: (args: {
    readonly input: NoInfer<Input>;
    readonly needs: NoInfer<ResolvedNeeds<N>>;
  }) => boolean;
  readonly timeoutMs?: Millis;
}

export interface StepLifecycleHooks<Output> {
  readonly onStart?: (event: StepStartEvent) => void | Promise<void>;
  readonly onComplete?: (
    event: StepCompleteEvent & { readonly output: NoInfer<Output> },
  ) => void | Promise<void>;
  readonly onError?: (event: StepErrorEvent) => void | Promise<void>;
  readonly onRetry?: (event: StepRetryEvent) => void | Promise<void>;
}

export interface TaskConfig<Input, N extends NeedsMap, Output>
  extends StepConfigBase<Input, N>,
    StepLifecycleHooks<Output> {
  readonly retry?: RetryPolicy;
  readonly run: (args: {
    readonly input: NoInfer<Input>;
    readonly needs: NoInfer<ResolvedNeeds<N>>;
    readonly ctx: StepCtx<NoInfer<Input>>;
  }) => Promise<Output>;
}

export interface ActivityConfig<Input, N extends NeedsMap, Output>
  extends StepConfigBase<Input, N>,
    StepLifecycleHooks<Output> {
  readonly retry?: RetryPolicy;
  readonly run: (args: {
    readonly input: NoInfer<Input>;
    readonly needs: NoInfer<ResolvedNeeds<N>>;
    readonly ctx: ActivityCtx<NoInfer<Input>>;
  }) => Promise<Output>;
}

export interface StreamingTaskConfig<Input, N extends NeedsMap, Output, Chunk>
  extends StepConfigBase<Input, N>,
    StepLifecycleHooks<Output> {
  readonly retry?: RetryPolicy;
  readonly run: (args: {
    readonly input: NoInfer<Input>;
    readonly needs: NoInfer<ResolvedNeeds<N>>;
    readonly ctx: StreamingStepCtx<NoInfer<Input>, Chunk>;
  }) => Promise<Output>;
}

export interface SignalConfig<
  Input,
  N extends NeedsMap,
  Schema extends StandardSchemaV1,
> extends StepConfigBase<Input, N> {
  readonly schema: Schema;
  readonly names?: readonly [string, ...string[]];
}

export interface MatchArmGuard<Input, N extends NeedsMap, M extends StepMap> {
  readonly when: (args: {
    readonly input: NoInfer<Input>;
    readonly needs: NoInfer<ResolvedNeeds<N>>;
  }) => boolean;
  readonly otherwise?: never;
  readonly build: (b: Builder<Input>) => M;
}

export interface MatchArmOtherwise<Input, M extends StepMap> {
  readonly otherwise: true;
  readonly when?: never;
  readonly build: (b: Builder<Input>) => M;
}

export type MatchArm<Input, N extends NeedsMap, M extends StepMap> =
  | MatchArmGuard<Input, N, M>
  | MatchArmOtherwise<Input, M>;

export type MatchArmShape<Input, N extends NeedsMap> =
  | {
      readonly when: (args: {
        readonly input: NoInfer<Input>;
        readonly needs: NoInfer<ResolvedNeeds<N>>;
      }) => boolean;
      readonly otherwise?: never;
      readonly build: (b: Builder<Input>) => StepMap;
    }
  | {
      readonly otherwise: true;
      readonly when?: never;
      readonly build: (b: Builder<Input>) => StepMap;
    };

export interface MatchGuardConfig<
  Input,
  N extends NeedsMap,
  M extends StepMap,
> {
  readonly needs?: N;
  readonly arms: ReadonlyArray<MatchArm<Input, N, M>>;
}

export type MatchArmOutput<M extends StepMap> = {
  readonly [K in keyof M]: StepOutput<M[K]>;
};

export interface SubflowConfig<Input, N extends NeedsMap, Child extends Flow>
  extends StepConfigBase<Input, N> {
  readonly input: (args: {
    readonly input: NoInfer<Input>;
    readonly needs: NoInfer<ResolvedNeeds<N>>;
  }) => FlowInput<Child>;
}

export interface SubflowStepOutput<ChildOutput> {
  readonly childRunId: RunId;
  readonly output: ChildOutput;
}

export interface Builder<Input = unknown> {
  task<N extends NeedsMap, Output>(
    config: TaskConfig<Input, N, Output>,
  ): Step<Output>;

  activity<N extends NeedsMap, Output>(
    config: ActivityConfig<Input, N, Output>,
  ): Step<Output>;

  streamingTask<N extends NeedsMap, O, C = Json>(
    config: StreamingTaskConfig<Input, N, O, C>,
  ): Step<O>;

  signal<N extends NeedsMap, S extends StandardSchemaV1>(
    config: SignalConfig<Input, N, S>,
  ): Step<InferSchemaOutput<S>>;

  subflow<N extends NeedsMap, Child extends Flow>(
    child: Child,
    config: SubflowConfig<Input, N, Child>,
  ): Step<SubflowStepOutput<FlowOutput<Child>>>;

  match<
    N extends NeedsMap,
    Arms extends ReadonlyArray<MatchArmShape<Input, N>>,
  >(config: {
    readonly needs?: N;
    readonly arms: Arms;
  }): Step<MatchArmOutput<ReturnType<Arms[number]["build"]>>>;
}

export type ConcurrencyMode = "cancel-in-progress";

export type StringKeyOf<Input> = {
  [K in keyof Input]-?: Input[K] extends string ? K : never;
}[keyof Input];

export type FlowConcurrency<Input = Json> =
  | StringKeyOf<Input>
  | {
      readonly keyFn: (input: Input) => string;
      readonly mode?: ConcurrencyMode;
    };

export interface ResolvedConcurrency {
  readonly keyFn: (input: Json) => string;
  readonly mode: ConcurrencyMode;
}

export interface FlowConfig<
  Id extends string,
  InputSchema extends StandardSchemaV1,
  R extends StepMap,
  Output = unknown,
> {
  readonly id: Id;
  readonly input: InputSchema;
  readonly build: (b: Builder<InferSchemaOutput<InputSchema>>) => R;
  output?(steps: NeedsOutputs<R>): Output;

  readonly concurrency?: FlowConcurrency<InferSchemaOutput<InputSchema>>;

  readonly onStart?: (event: FlowStartEvent) => void | Promise<void>;
  readonly onComplete?: (
    event: FlowCompleteEvent & { readonly output: NoInfer<Output> },
  ) => void | Promise<void>;
  readonly onError?: (event: FlowErrorEvent) => void | Promise<void>;
}

export interface Flow<
  Id extends string = string,
  InputSchema extends StandardSchemaV1 = StandardSchemaV1,
  M extends StepMap = StepMap,
  Output = unknown,
> {
  readonly id: Id;
  readonly input: InputSchema;
  readonly steps: M;
  output?(steps: NeedsOutputs<M>): Output;

  readonly onStart?: (event: FlowStartEvent) => void | Promise<void>;
  readonly onComplete?: (event: FlowCompleteEvent) => void | Promise<void>;
  readonly onError?: (event: FlowErrorEvent) => void | Promise<void>;
  readonly concurrency?: ResolvedConcurrency;
}

export type FlowInput<F> =
  F extends Flow<string, infer S, StepMap, unknown>
    ? InferSchemaOutput<S>
    : never;

export type FlowOutput<F> =
  F extends Flow<string, StandardSchemaV1, StepMap, infer O> ? O : never;

export type FlowIdOf<T extends ReadonlyArray<Flow>> =
  T[number] extends Flow<infer Id> ? Id : never;

export interface ParentLink {
  readonly runId: RunId;
  readonly stepId: StepId;
}

export interface ParentRef extends ParentLink {
  readonly attempt: AttemptNumber;
}

export interface FlowEvent {
  readonly runId: RunId;
  readonly flowId: string;
  readonly at: Date;
}

export interface FlowStartEvent extends FlowEvent {
  readonly input: Json;
  readonly parent?: ParentRef;
}

export interface FlowCompleteEvent extends FlowEvent {
  readonly output: Json;
}

export interface FlowErrorEvent extends FlowEvent {
  readonly error: SerializedError;
}

export interface StepEvent extends FlowEvent {
  readonly stepId: StepId;
  readonly attempt: AttemptNumber;
  readonly kind: StepKind;
}

export interface StepStartEvent extends StepEvent {
  readonly input: Json;
}

export interface StepCompleteEvent extends StepEvent {
  readonly output: Json;
  // Absent for match/subflow completions, which have no handler wall-time;
  // consumers that need it recompute from the step's start.
  readonly durationMs?: Millis;
}

export interface StepErrorEvent extends StepEvent {
  readonly error: SerializedError;
}

export interface StepRetryEvent extends StepEvent {
  readonly error: SerializedError;
  readonly nextAttemptAt: Date;
}

export interface SignalSentEvent extends StepEvent {
  readonly payload: Json;
}

export interface SignalReceivedEvent extends StepEvent {
  readonly payload: Json;
}

export interface FlowHooks {
  readonly onFlowStart?: (event: FlowStartEvent) => void | Promise<void>;
  readonly onFlowComplete?: (event: FlowCompleteEvent) => void | Promise<void>;
  readonly onFlowError?: (event: FlowErrorEvent) => void | Promise<void>;
  readonly onStepStart?: (event: StepStartEvent) => void | Promise<void>;
  readonly onStepComplete?: (event: StepCompleteEvent) => void | Promise<void>;
  readonly onStepError?: (event: StepErrorEvent) => void | Promise<void>;
  readonly onStepRetry?: (event: StepRetryEvent) => void | Promise<void>;
  readonly onSignalSent?: (event: SignalSentEvent) => void | Promise<void>;
  readonly onSignalReceived?: (
    event: SignalReceivedEvent,
  ) => void | Promise<void>;
}

export interface WorkerConfig {
  readonly concurrency?: number;
  readonly pollIntervalMs?: Millis;
  readonly signal?: AbortSignal;
}

export interface WorkerRunOnceOpts {
  readonly maxSteps?: number;
}

export interface WorkerRunUntilEmptyOpts {
  readonly deadline?: number;
}

export interface WorkerRunResult {
  readonly processed: number;
}

export interface Worker {
  run(): Promise<void>;
  runOnce(opts?: WorkerRunOnceOpts): Promise<WorkerRunResult>;
  runUntilEmpty(opts?: WorkerRunUntilEmptyOpts): Promise<WorkerRunResult>;
}

export type ClaimToken = string & { readonly __brand: "ClaimToken" };

export interface SerializedError {
  readonly name: string;
  readonly message: string;
  readonly stack?: string;
  readonly cause?: Json;
}

export type SettleSignalResult =
  | {
      readonly tag: "delivered";
      readonly attempt: AttemptNumber;
      readonly payload: Json;
      readonly signalName?: string;
    }
  | { readonly tag: "buffered" }
  | { readonly tag: "noop" };

export interface Store {
  appendFact(runId: RunId, fact: Fact): Promise<void>;
  loadRunState(runId: RunId): Promise<RunState>;

  // Atomically reconcile a signal with its target step under a per-run lock.
  // With `incoming` (a wf.signal call): deliver when the step is awaitingSignal,
  // buffer when it has not started yet, no-op when it already resolved. Without
  // `incoming` (the worker, right after the step entered awaitingSignal): deliver
  // a previously buffered signal if one exists, else no-op. MUST be atomic so a
  // signal is never lost to the start/await race.
  settleSignal(args: {
    readonly runId: RunId;
    readonly stepId: StepId;
    readonly at: Date;
    readonly incoming?: {
      readonly payload: Json;
      readonly signalName?: string;
    };
  }): Promise<SettleSignalResult>;

  // MUST be atomic: concurrent calls with the same runId produce exactly one
  // flow.started fact. When concurrency is supplied, MUST atomically cancel
  // prior active runs sharing (flowId, key) and serialize concurrent starts.
  tryStartRun(
    runId: RunId,
    fact: FlowStartedFact,
    concurrency?: {
      readonly key: string;
      readonly mode: ConcurrencyMode;
    },
  ): Promise<{
    readonly started: boolean;
    readonly canceled: ReadonlyArray<{
      readonly runId: RunId;
      readonly fact: FlowCanceledByConcurrencyFact;
    }>;
  }>;

  // Same semantics as tryStartRun, but operates on the caller's tx — the run
  // row and flow.started fact commit (or roll back) with whatever else the
  // caller writes. MUST NOT open its own tx, fire hooks, or propagate to
  // parents: those are post-commit policy and belong to wf.startStaged's
  // applyOnCommit closure. Under shared tx the advisory lock is skipped; the
  // partial unique index on (flow_id, concurrency_key) WHERE status IN
  // (pending, running) is the actual invariant, and a unique-violation race
  // is retried once before throwing NagiConcurrencyConflictError.
  tryStartRunOnTx(
    tx: Tx,
    runId: RunId,
    fact: FlowStartedFact,
    concurrency?: {
      readonly key: string;
      readonly mode: ConcurrencyMode;
    },
  ): Promise<{
    readonly started: boolean;
    readonly canceled: ReadonlyArray<{
      readonly runId: RunId;
      readonly fact: FlowCanceledByConcurrencyFact;
    }>;
  }>;

  claimStep(
    runId: RunId,
    stepId: StepId,
    attempt: AttemptNumber,
  ): Promise<ClaimToken | null>;

  // Heartbeat extension for the lease row, called alongside Queue.extend so the
  // lease state and the queue VT advance together. Idempotent: a 0-row update
  // (lease already reaped, step settled, or a stale receipt) MUST not throw —
  // the next heartbeat tick re-checks.
  extendLease(
    runId: RunId,
    stepId: StepId,
    attempt: AttemptNumber,
    leaseMs: Millis,
  ): Promise<void>;

  // Periodic lease-reaper sweep. The adapter SELECTs expired leases (with row
  // locking against concurrent reapers), feeds each through
  // decideExpiredLeaseAction, and on "reap" atomically deletes the lease,
  // writes a lease.reaped fact, and re-enqueues at attempt+1 via the supplied
  // queue — all inside one tx so a crash mid-sweep leaves no half-reaped state.
  // Returns the lease identifiers that actually re-dispatched.
  sweepLeases(args: {
    readonly now: Date;
    readonly queue: Queue;
    readonly limit?: number;
  }): Promise<readonly ReapedLease[]>;

  // Append a terminal step fact for a step that did not run under `runStep`
  // (match/subflow promotions, operator-driven settles). The output travels on
  // the StepCompletedFact, so there is no separate output store to keep in sync.
  settleStep(
    runId: RunId,
    stepId: StepId,
    fact: StepCompletedFact | StepFailedFact,
  ): Promise<void>;

  recordOnce(
    runId: RunId,
    stepId: StepId,
    scope: string,
    value: Json,
  ): Promise<void>;
  getOnce(runId: RunId, stepId: StepId, scope: string): Promise<Json | null>;

  // On success, the returned fact MUST persist atomically with any tx writes
  // the body made; on step.completed the same scope MUST also persist the
  // output and release the lease. If body throws, the tx rolls back.
  runStep<T extends Json>(
    runId: RunId,
    stepId: StepId,
    attempt: AttemptNumber,
    body: (tx: Tx) => Promise<{
      readonly output: T;
      readonly fact: StepCompletedFact | StepFailedFact | StepCanceledFact;
    }>,
  ): Promise<T>;

  // Idempotent on (flowHash, dag); MUST enforce at the durability layer.
  upsertSnapshot(args: {
    readonly flowHash: string;
    readonly flowId: string;
    readonly dag: Json;
  }): Promise<void>;

  getRef(flowId: string): Promise<string | null>;

  setRef(flowId: string, flowHash: string): Promise<void>;

  loadSnapshot(
    flowHash: string,
  ): Promise<{ readonly flowId: string; readonly dag: Json } | null>;

  appendGlobalFact(fact: GlobalFact): Promise<void>;

  // MUST order by (startedAt DESC, runId DESC) for stable cursor pagination,
  // and treat opts.where.input as JSONB containment (Postgres `@>` semantics).
  queryRuns(opts: QueryRunsOpts): Promise<QueryRunsResult>;

  // MUST return null (never throw) for an unknown runId. The view reflects
  // facts visible at call time; parent/children come from the parent_run_id
  // column + a reverse lookup, not from a separate child registry.
  describe(runId: RunId): Promise<RunDescription>;

  listChildren(parentRunId: RunId): Promise<ReadonlyArray<RunId>>;

  // MUST never prune non-terminal runs; delete in batches of opts.batchSize.
  pruneFacts(opts: Required<PruneOpts>): Promise<PruneResult>;
}

// Streaming side-channel for b.streamingTask, separate from Store: chunk
// transport is ephemeral and out-of-band, never transactional. A Store may
// implement it (the in-memory reference does); otherwise nagi() throws at
// registration if a flow has a streaming step.
export interface StreamTransport {
  // The iterator MUST close when the step reaches a terminal fact.
  subscribeStream(
    runId: RunId,
    stepId: StepId,
    opts?: { readonly replayBuffered?: boolean },
  ): AsyncIterable<StreamEvent<Json>>;

  // Fire-and-forget and out-of-band — MUST NOT be routed through `tx`.
  publishChunk(runId: RunId, stepId: StepId, chunk: Json): void;
}

export interface QueryRunsWhere<FlowId extends string = string> {
  readonly flowId?: FlowId;
  readonly status?: ReadonlyArray<RunStatus>;
  readonly input?: Record<string, Json>;
}

export type QueryRunsOpts<FlowId extends string = string> =
  | {
      readonly where?: QueryRunsWhere<FlowId>;
      readonly latest: true;
      readonly limit?: never;
      readonly cursor?: never;
    }
  | {
      readonly where?: QueryRunsWhere<FlowId>;
      readonly latest?: false;
      readonly limit?: number;
      readonly cursor?: string;
    };

export interface RunSummary<FlowId extends string = string> {
  readonly runId: RunId;
  readonly flowId: FlowId;
  readonly status: RunStatus;
  readonly startedAt: Date;
  readonly completedAt: Date | null;
  readonly input: Json;
}

export interface QueryRunsResult<FlowId extends string = string> {
  readonly runs: ReadonlyArray<RunSummary<FlowId>>;
  readonly cursor: string | null;
}

export type PrunableStatus = Extract<
  RunStatus,
  "completed" | "failed" | "canceled"
>;

export interface PruneOpts {
  readonly olderThan: Date;
  readonly statuses?: ReadonlyArray<PrunableStatus>;
  readonly batchSize?: number;
  readonly keepSummary?: boolean;
}

export interface PruneResult {
  readonly runsPruned: number;
  readonly factsPruned: number;
}

export interface QueueMessage {
  readonly receipt: string;
  readonly runId: RunId;
  readonly stepId: StepId;
  readonly payload: Json;
  readonly attempt: AttemptNumber;
}

export interface QueueDequeueOpts {
  readonly count: number;
}

export interface QueueEnqueueOpts {
  readonly attempt?: AttemptNumber;
  readonly delayMs?: Millis;
  readonly payload?: Json;
}

export interface Queue {
  enqueue(runId: RunId, stepId: StepId, opts?: QueueEnqueueOpts): Promise<void>;
  dequeue(opts: QueueDequeueOpts): Promise<readonly QueueMessage[]>;
  ack(receipt: string): Promise<void>;
  nack(receipt: string, opts?: { delayMs?: Millis }): Promise<void>;
  extend(receipt: string, leaseMs: Millis): Promise<void>;
  // Optional one-shot, idempotent schema provisioning. nagi() awaits it once at
  // construction (fail-fast). Adapters needing none omit it.
  ensureSchema?(): Promise<void>;
}

export interface Clock {
  now(): Date;
  sleep(ms: Millis, signal?: AbortSignal): Promise<void>;
  schedule(at: Date, runId: RunId, stepId: StepId): Promise<void>;
}

export interface Trigger {
  subscribe(handler: (runId: RunId) => void): () => void;
}

export type FactKind =
  | "flow.started"
  | "flow.completed"
  | "flow.failed"
  | "flow.canceled"
  | "step.started"
  | "step.completed"
  | "step.failed"
  | "step.canceled"
  | "step.retried"
  | "step.skipped"
  | "step.reset"
  | "step.abort-requested"
  | "signal.sent"
  | "signal.received"
  | "signal.buffered"
  | "once.recorded"
  | "match.arm-selected"
  | "lease.reaped";

interface FactBase {
  readonly runId: RunId;
  readonly at: Date;
}

export interface FlowStartedFact extends FactBase {
  readonly kind: "flow.started";
  readonly flowId: string;
  readonly input: Json;
  readonly flowHash?: string;
  readonly codeVersion?: string;
  readonly parent?: ParentLink;
}

export interface FlowRefUpdatedFact {
  readonly kind: "flow_ref.updated";
  readonly flowId: string;
  readonly from: string | null;
  readonly to: string;
  readonly at: Date;
}

export type GlobalFact = FlowRefUpdatedFact;

export interface FlowCompletedFact extends FactBase {
  readonly kind: "flow.completed";
  readonly output: Json;
}

export interface FlowFailedFact extends FactBase {
  readonly kind: "flow.failed";
  readonly error: SerializedError;
}

export interface FlowCanceledByConcurrencyFact extends FactBase {
  readonly kind: "flow.canceled";
  readonly cause: "concurrency";
  readonly canceledByRunId: RunId;
  readonly concurrencyKey: string;
}

export interface FlowCanceledExplicitlyFact extends FactBase {
  readonly kind: "flow.canceled";
  readonly cause: "explicit";
  readonly reason: string;
  readonly note?: string;
}

export interface FlowCanceledByOperatorFact extends FactBase {
  readonly kind: "flow.canceled";
  readonly cause: "operator";
  readonly actor: string;
  readonly reason: string;
  readonly note?: string;
}

export type FlowCanceledFact =
  | FlowCanceledByConcurrencyFact
  | FlowCanceledExplicitlyFact
  | FlowCanceledByOperatorFact;

type DistributiveOmit<T, K extends PropertyKey> = T extends unknown
  ? Omit<T, K>
  : never;

// Derived from the fact types so cancel intent can't drift from the recorded
// fact. Concurrency cancellation is system-internal and intentionally excluded.
export type CancelArgs = DistributiveOmit<
  FlowCanceledExplicitlyFact | FlowCanceledByOperatorFact,
  "kind" | "runId" | "at"
>;

export interface StepStartedFact extends FactBase {
  readonly kind: "step.started";
  readonly stepId: StepId;
  readonly attempt: AttemptNumber;
  // Lets the projection fold a started step without the flow def.
  readonly stepKind: StepKind;
}

export interface StepCompletedFact extends FactBase {
  readonly kind: "step.completed";
  readonly stepId: StepId;
  readonly attempt: AttemptNumber;
  readonly output: Json;
}

export interface StepFailedFact extends FactBase {
  readonly kind: "step.failed";
  readonly stepId: StepId;
  readonly attempt: AttemptNumber;
  readonly error: SerializedError;
}

export interface StepCanceledFact extends FactBase {
  readonly kind: "step.canceled";
  readonly stepId: StepId;
  readonly attempt: AttemptNumber;
  readonly error?: SerializedError;
}

export interface StepRetriedFact extends FactBase {
  readonly kind: "step.retried";
  readonly stepId: StepId;
  readonly attempt: AttemptNumber;
  readonly nextAttemptAt: Date;
  readonly error: SerializedError;
}

export interface StepSkippedFact extends FactBase {
  readonly kind: "step.skipped";
  readonly stepId: StepId;
  readonly reason: "when-false" | "transitive" | "manual";
  readonly actor?: string;
  readonly note?: string;
}

export interface SignalSentFact extends FactBase {
  readonly kind: "signal.sent";
  readonly stepId: StepId;
  readonly payload: Json;
}

export interface SignalReceivedFact extends FactBase {
  readonly kind: "signal.received";
  readonly stepId: StepId;
  readonly payload: Json;
  readonly signalName?: string;
}

// A signal that arrived before its target step entered awaitingSignal (the
// start/await race). Parked in the fact log and applied the moment the worker
// claims the step. See Store.settleSignal.
export interface SignalBufferedFact extends FactBase {
  readonly kind: "signal.buffered";
  readonly stepId: StepId;
  readonly payload: Json;
  readonly signalName?: string;
}

export interface OnceRecordedFact extends FactBase {
  readonly kind: "once.recorded";
  readonly stepId: StepId;
  readonly scope: string;
  readonly value: Json;
}

export interface MatchArmSelectedFact extends FactBase {
  readonly kind: "match.arm-selected";
  readonly stepId: StepId;
  readonly arm: string;
}

// Written by the lease reaper when an expired lease on a non-terminal step is
// reclaimed. Audit hook: surfaces the otherwise-invisible "worker died, lease
// timed out, step re-dispatched" event that is the load-bearing crash recovery
// path. attempt is the lease's original attempt; the new dispatch carries
// attempt+1.
export interface LeaseReapedFact extends FactBase {
  readonly kind: "lease.reaped";
  readonly stepId: StepId;
  readonly attempt: AttemptNumber;
  readonly reapedAt: Date;
  readonly reason: "expired";
}

export interface StepResetFact extends FactBase {
  readonly kind: "step.reset";
  readonly stepId: StepId;
  readonly cascadedFrom?: StepId;
  readonly actor?: string;
  readonly note?: string;
}

export interface StepAbortRequestedFact extends FactBase {
  readonly kind: "step.abort-requested";
  readonly stepId: StepId;
  readonly attempt: AttemptNumber;
  readonly actor: string;
  readonly note?: string;
}

export type Fact =
  | FlowStartedFact
  | FlowCompletedFact
  | FlowFailedFact
  | FlowCanceledFact
  | StepStartedFact
  | StepCompletedFact
  | StepFailedFact
  | StepCanceledFact
  | StepRetriedFact
  | StepSkippedFact
  | StepResetFact
  | StepAbortRequestedFact
  | SignalSentFact
  | SignalReceivedFact
  | SignalBufferedFact
  | OnceRecordedFact
  | MatchArmSelectedFact
  | LeaseReapedFact;

export type RunStatus =
  | "pending"
  | "running"
  | "completed"
  | "failed"
  | "canceled";
export type StepStatus =
  | "pending"
  | "running"
  | "completed"
  | "failed"
  | "canceled"
  | "skipped";

export type { RunState, StepState };

export type ReplayMode = "inspect" | "continue";

export interface ReplayOpts {
  readonly mode: ReplayMode;
  readonly allowDrift?: boolean;
  readonly fireHooks?: boolean;
  readonly from?: StepId;
}

export interface OperatorAuditOpts {
  readonly actor: string;
  readonly note?: string;
}

export interface Operator {
  skip(runId: RunId, stepId: StepId, opts: OperatorAuditOpts): Promise<void>;

  // For a `running` step, MUST first abort the in-flight handler via
  // step.abort-requested and wait for it to settle before resetting.
  retry(runId: RunId, stepId: StepId, opts: OperatorAuditOpts): Promise<void>;

  abort(runId: RunId, opts: OperatorAuditOpts): Promise<void>;
}
