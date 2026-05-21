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

// Declaration-merging augmentation slot: users declare `interface Register { tx: ... }`.
// biome-ignore lint/suspicious/noEmptyInterface: intentional augmentation slot
export interface Register {}

export type Tx = Register extends { tx: infer T } ? T : unknown;

export type StepKind = "task" | "signal" | "match" | "subflow" | "streaming";

export interface Step<Output = unknown> {
  readonly kind: StepKind;
  readonly id: StepId;
  readonly __output?: Output;
}

export type StepOutput<S> = S extends Step<infer O> ? O : never;
export type StepMap = Readonly<Record<string, Step<unknown>>>;

export type NeedsMap = StepMap;
export type NeedsOutputs<N extends NeedsMap> = {
  readonly [K in keyof N]: StepOutput<N[K]>;
};

/**
 * Handler-facing view of `needs`: each upstream is a {@link Resolved} value, so
 * a `cascade: "continue"` skip (`{ tag: "skipped" }`) is distinct from an
 * upstream that genuinely produced `null` (`{ tag: "value", value: null }`).
 */
export type ResolvedNeeds<N extends NeedsMap> = {
  readonly [K in keyof N]: Resolved<StepOutput<N[K]>>;
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

/**
 * The element delivered by {@link Store.subscribeStream} / `wf.subscribe`: a
 * discriminated control+data envelope. A real streamed value only ever arrives
 * as `{ kind: "chunk" }`; `dropped`/`retry`/`error` are framework markers, so a
 * consumer that switches on `kind` cannot confuse a marker with a data chunk.
 * Generated as: `chunk` from `ctx.emit`, `dropped` on per-subscriber buffer
 * overflow, `retry` when a streaming step is re-attempted, `error` on terminal
 * `step.failed` (retries exhausted).
 */
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

export interface TaskConfig<Input, N extends NeedsMap, Output>
  extends StepConfigBase<Input, N> {
  readonly retry?: RetryPolicy;
  readonly run: (args: {
    readonly input: NoInfer<Input>;
    readonly needs: NoInfer<ResolvedNeeds<N>>;
    readonly ctx: StepCtx<NoInfer<Input>>;
  }) => Promise<Output>;

  readonly onStart?: (event: StepStartEvent) => void | Promise<void>;
  readonly onComplete?: (
    event: StepCompleteEvent & { readonly output: NoInfer<Output> },
  ) => void | Promise<void>;
  readonly onError?: (event: StepErrorEvent) => void | Promise<void>;
  readonly onRetry?: (event: StepRetryEvent) => void | Promise<void>;
}

export interface StreamingTaskConfig<Input, N extends NeedsMap, Output, Chunk>
  extends StepConfigBase<Input, N> {
  readonly retry?: RetryPolicy;
  readonly run: (args: {
    readonly input: NoInfer<Input>;
    readonly needs: NoInfer<ResolvedNeeds<N>>;
    readonly ctx: StreamingStepCtx<NoInfer<Input>, Chunk>;
  }) => Promise<Output>;

  readonly onStart?: (event: StepStartEvent) => void | Promise<void>;
  readonly onComplete?: (
    event: StepCompleteEvent & { readonly output: NoInfer<Output> },
  ) => void | Promise<void>;
  readonly onError?: (event: StepErrorEvent) => void | Promise<void>;
  readonly onRetry?: (event: StepRetryEvent) => void | Promise<void>;
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

/**
 * Durable parent linkage persisted on {@link FlowStartedFact}. Present when a
 * run was started as a subflow child; absent for top-level runs.
 */
export interface ParentLink {
  readonly runId: RunId;
  readonly stepId: StepId;
}

/**
 * In-process parent reference: a {@link ParentLink} plus the parent step's
 * attempt, used to thread otel spans and registry lookups. Not persisted — the
 * attempt is re-derived from parent run state when a subflow wakes its parent.
 */
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
  /**
   * Set when this run was started as a subflow child. Undefined for
   * top-level runs (start / startById). Carried in-process only — for
   * durable linkage see {@link FlowStartedFact.parent}.
   */
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
  readonly durationMs: Millis;
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

export interface Store {
  appendFact(runId: RunId, fact: Fact): Promise<void>;
  loadRunState(runId: RunId): Promise<RunState>;

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

  claimStep(
    runId: RunId,
    stepId: StepId,
    attempt: AttemptNumber,
  ): Promise<ClaimToken | null>;

  completeStep(
    runId: RunId,
    stepId: StepId,
    output: Json,
    fact: Fact,
  ): Promise<void>;
  failStep(
    runId: RunId,
    stepId: StepId,
    error: SerializedError,
    fact: Fact,
  ): Promise<void>;

  getStepOutput(runId: RunId, stepId: StepId): Promise<Json | null>;

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

  listChildren(parentRunId: RunId): Promise<ReadonlyArray<RunId>>;

  // MUST never prune non-terminal runs; delete in batches of opts.batchSize.
  pruneFacts(opts: Required<PruneOpts>): Promise<PruneResult>;

  /**
   * Optional streaming transport read-side. Present only on adapters that can
   * fan out ephemeral chunks (e.g. the in-memory store). When any registered
   * flow contains a `streaming` step and this is undefined, `nagi()` throws at
   * registration (fail-fast), mirroring {@link Queue.ensureSchema}'s gating.
   * Yields a {@link StreamEvent} envelope; the iterator closes when the step
   * reaches a terminal fact. `replayBuffered` is best-effort against a bounded,
   * terminal-dropped buffer.
   */
  subscribeStream?(
    runId: RunId,
    stepId: StepId,
    opts?: { readonly replayBuffered?: boolean },
  ): AsyncIterable<StreamEvent<Json>>;

  /**
   * Optional streaming transport write-side. `ctx.emit` pushes a raw chunk; the
   * transport wraps it as `{ kind: "chunk", chunk }` and fans out. The
   * `dropped`/`retry`/`error` events are transport/dispatch-generated, not
   * published here. Fire-and-forget and out-of-band — never routed through `tx`.
   */
  publishChunk?(runId: RunId, stepId: StepId, chunk: Json): void;
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
  /**
   * Optional one-shot, idempotent provisioning of the queue's backing schema.
   * When present, `nagi()` awaits it once at construction (fail-fast), so a
   * misconfigured queue surfaces at boot instead of on first enqueue. Adapters
   * needing no provisioning (e.g. in-memory) omit it.
   */
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
  | "once.recorded"
  | "match.arm-selected";

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
  /** Present when this run was started as a subflow child; absent for roots. */
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

export interface StepStartedFact extends FactBase {
  readonly kind: "step.started";
  readonly stepId: StepId;
  readonly attempt: AttemptNumber;
  /** The step's kind, so the projection can fold a started step into the right
   * state (running vs awaitingSignal vs awaitingChild) without the flow def. */
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
  /** The failure that triggered this retry; surfaces on the `backoff` state. */
  readonly error: SerializedError;
}

export interface StepSkippedFact extends FactBase {
  readonly kind: "step.skipped";
  readonly stepId: StepId;
  readonly reason: "when-false" | "transitive" | "manual";
  readonly actor?: string;
  readonly note?: string;
  // "continue" lets downstream run with needs.x === null; handler must tolerate.
  readonly cascade?: "skip" | "continue";
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
  | OnceRecordedFact
  | MatchArmSelectedFact;

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

/**
 * The projected run/step state machines are tagged unions defined in `state.ts`
 * ({@link RunState} carries a `phase`; {@link StepState} a `tag`). Re-exported
 * here so existing `from "./types"` imports keep resolving.
 */
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

export interface OperatorSkipOpts extends OperatorAuditOpts {
  readonly cascade?: "skip" | "continue";
}

export interface Operator {
  skip(runId: RunId, stepId: StepId, opts: OperatorSkipOpts): Promise<void>;

  // For a `running` step, MUST first abort the in-flight handler via
  // step.abort-requested and wait for it to settle before resetting.
  retry(runId: RunId, stepId: StepId, opts: OperatorAuditOpts): Promise<void>;

  abort(runId: RunId, opts: OperatorAuditOpts): Promise<void>;
}
