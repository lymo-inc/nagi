/** JSON-serializable value. Crosses persistence + queue boundaries. */
export type Json =
  | string
  | number
  | boolean
  | null
  | Json[]
  | { [key: string]: Json };

/** Integer milliseconds. The library represents every duration as integer ms — no duration strings. */
export type Millis = number;

/** ULID/UUID. Branded so a raw string can't be passed where a `RunId` is expected. */
export type RunId = string & { readonly __brand: "RunId" };

/** Stable per-flow step identifier. Survives renames in code (locked). */
export type StepId = string;

export type AttemptNumber = number;

// CHOICE: Library-agnostic schema interface. Zod 3.24+, Valibot, ArkType,
// and Effect Schema all implement Standard Schema natively. Users can pass
// any Standard Schema-compatible value to `input:` or `signal({ schema })`.
// Inlined here (rather than peer-depped on `@standard-schema/spec`) to keep
// core dep-free; the contract is small and frozen.
// Reference: https://standardschema.dev

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

// CHOICE: Drizzle / NextAuth pattern. Core does not import any DB driver.
// Users augment `Register` to set the transaction type their adapter hands to
// `ctx.tx`:
//
//   declare module "@nagi-js/core" {
//     interface Register {
//       tx: Kysely.Transaction<MyDB>;
//     }
//   }
//
// Without augmentation, `Tx` defaults to `unknown` — handlers can still write
// `ctx.tx` but lose end-to-end type safety on the transaction object.

export type Register = {};

export type Tx = Register extends { tx: infer T } ? T : unknown;

export type StepKind = "task" | "signal" | "match";

/**
 * A step in a flow. Returned by `b.task()` / `b.signal()` / `b.match()` and
 * passed by reference into downstream `needs:` maps. The `Output` type
 * parameter is phantom — never present at runtime; carries the handler's
 * return type forward for inference.
 */
export interface Step<Output = unknown> {
  readonly kind: StepKind;
  readonly id: StepId;
  /** Phantom — read via `StepOutput<S>`, not by direct access. */
  readonly __output?: Output;
}

export type StepOutput<S> = S extends Step<infer O> ? O : never;

/** A bag of named steps — the value of `needs:` and the return shape of `build:`. */
export type StepMap = Readonly<Record<string, Step<unknown>>>;

export type NeedsMap = StepMap;

/** Inside a handler, `needs.x` is typed as the upstream step's `Output`. */
export type NeedsOutputs<N extends NeedsMap> = {
  readonly [K in keyof N]: StepOutput<N[K]>;
};

export interface Logger {
  debug(message: string, attrs?: Record<string, unknown>): void;
  info(message: string, attrs?: Record<string, unknown>): void;
  warn(message: string, attrs?: Record<string, unknown>): void;
  error(message: string, attrs?: Record<string, unknown>): void;
}

/**
 * Handed to every handler. Carries the run's identity, the abort signal for
 * graceful drain, the durable transaction, and the idempotency primitives.
 */
export interface StepCtx<Input = unknown> {
  readonly input: Input;
  readonly tx: Tx;
  readonly runId: RunId;
  readonly stepId: StepId;
  readonly attempt: AttemptNumber;
  /** Aborts when the worker is draining (SIGTERM) or the lease is being relinquished. */
  readonly signal: AbortSignal;
  readonly now: () => Date;
  readonly logger: Logger;

  /**
   * Durable per-effect memoization. The first successful call for `(runId, stepId, scope)`
   * persists its return value; subsequent calls (including post-crash retries) return the
   * cached value without re-invoking `fn`. See boundary.md "Idempotency model".
   */
  once<T extends Json>(scope: string, fn: () => Promise<T>): Promise<T>;

  /**
   * Stable idempotency key for external APIs (Stripe, Mux, etc.).
   * Returns `hash(runId + stepId + scope)` — identical across retries of the same attempt,
   * different across runs.
   */
  idempotencyKey(scope: string): string;
}

export type BackoffStrategy = "exponential" | "linear" | "fixed";

export interface RetryPolicy {
  readonly maxAttempts: number;
  readonly backoff: BackoffStrategy;
  readonly initialDelayMs?: Millis;
  readonly maxDelayMs?: Millis;
  /** Errors this policy applies to. Defaults to all. */
  readonly retryOn?: (error: unknown) => boolean;
}

interface StepConfigBase<Input, N extends NeedsMap> {
  readonly needs?: N;
  /**
   * Skip this step when the predicate returns false. Locked semantic:
   * downstream steps that need a skipped step are also skipped (transitive).
   * Therefore at the type level, `needs.x` is the unconditional `Output` —
   * not `Output | undefined`.
   */
  readonly when?: (args: {
    readonly input: Input;
    readonly needs: NeedsOutputs<N>;
  }) => boolean;
  readonly timeout?: Millis;
}

export interface TaskConfig<Input, N extends NeedsMap, Output>
  extends StepConfigBase<Input, N> {
  readonly retry?: RetryPolicy;
  readonly run: (args: {
    readonly input: Input;
    readonly needs: NeedsOutputs<N>;
    readonly ctx: StepCtx<Input>;
  }) => Promise<Output>;
}

export interface SignalConfig<
  Input,
  N extends NeedsMap,
  Schema extends StandardSchemaV1,
> extends StepConfigBase<Input, N> {
  readonly schema: Schema;
}

/**
 * Discriminator mode — exhaustive at compile time. `cases` must cover every
 * value in the discriminant union; adding a new value to the discriminant
 * is a type error until the case is handled.
 */
export interface MatchDiscriminatorConfig<
  Input,
  N extends NeedsMap,
  D extends string,
  M extends Record<D, StepMap>,
> {
  readonly needs?: N;
  readonly on: (args: {
    readonly input: Input;
    readonly needs: NeedsOutputs<N>;
  }) => D;
  readonly cases: { readonly [K in D]: (b: Builder<Input>) => M[K] };
}

export interface MatchArmGuard<Input, N extends NeedsMap, M extends StepMap> {
  readonly when: (args: {
    readonly input: Input;
    readonly needs: NeedsOutputs<N>;
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

/**
 * Inference-friendly arm shape: M is not bound, so each arm in an `arms:` tuple
 * keeps its own StepMap (and per-step Output) through inference. The `arms`
 * overload of `Builder.match` uses this; user-facing arms still satisfy
 * `MatchArm<Input, N, M>` for any concrete M they construct.
 */
export type MatchArmShape<Input, N extends NeedsMap> =
  | {
      readonly when: (args: {
        readonly input: Input;
        readonly needs: NeedsOutputs<N>;
      }) => boolean;
      readonly otherwise?: never;
      readonly build: (b: Builder<Input>) => StepMap;
    }
  | {
      readonly otherwise: true;
      readonly when?: never;
      readonly build: (b: Builder<Input>) => StepMap;
    };

/**
 * Guard mode — Rust-style `match { x if guard => ... }`. Top-to-bottom,
 * first match wins. The terminal `{ otherwise: true }` arm is required so
 * guard mode can never silently fall through.
 */
export interface MatchGuardConfig<
  Input,
  N extends NeedsMap,
  M extends StepMap,
> {
  readonly needs?: N;
  // CHOICE: type-level enforcement of "last arm has otherwise" via tuple
  // shape (`readonly [...MatchArmGuard[], MatchArmOtherwise]`) is achievable
  // but adds inference cost. Starting with a runtime-validated array; tighten
  // to tuple shape later if it proves common to forget.
  readonly arms: ReadonlyArray<MatchArm<Input, N, M>>;
}

/** The output of a match step is a record of each step in the chosen arm. */
export type MatchArmOutput<M extends StepMap> = {
  readonly [K in keyof M]: StepOutput<M[K]>;
};

/** Across all cases of a discriminator match — the union of arm outputs. */
export type MatchDiscriminatorOutput<
  D extends string,
  M extends Record<D, StepMap>,
> = {
  [K in D]: MatchArmOutput<M[K]>;
}[D];

/**
 * `b` inside `flow({ build: (b) => ... })`. Constructors return `Step<Output>`
 * values whose Output has been inferred from the handler / schema / arms.
 */
export interface Builder<Input = unknown> {
  task<N extends NeedsMap, Output>(
    config: TaskConfig<Input, N, Output>,
  ): Step<Output>;

  signal<N extends NeedsMap, S extends StandardSchemaV1>(
    config: SignalConfig<Input, N, S>,
  ): Step<InferSchemaOutput<S>>;

  // Discriminator mode — TS picks this overload when `on:` and `cases:` are present.
  // `Cases` is captured as a free generic over the literal config shape so that
  // each case's StepMap (and therefore its `Output`) is preserved through inference.
  match<
    N extends NeedsMap,
    D extends string,
    Cases extends { readonly [K in D]: (b: Builder<Input>) => StepMap },
  >(config: {
    readonly needs?: N;
    readonly on: (args: {
      readonly input: Input;
      readonly needs: NeedsOutputs<N>;
    }) => D;
    readonly cases: Cases;
  }): Step<
    {
      readonly [K in keyof Cases]: MatchArmOutput<ReturnType<Cases[K]>>;
    }[keyof Cases]
  >;

  // Guard mode — TS picks this overload when `arms:` is present.
  // `Arms` is captured as a tuple-style generic so each arm's `build` return type
  // is tracked individually; the output is the union of every arm's StepMap output.
  match<
    N extends NeedsMap,
    Arms extends ReadonlyArray<MatchArmShape<Input, N>>,
  >(config: {
    readonly needs?: N;
    readonly arms: Arms;
  }): Step<MatchArmOutput<ReturnType<Arms[number]["build"]>>>;
}

export interface FlowConfig<
  InputSchema extends StandardSchemaV1,
  M extends StepMap,
  Output = unknown,
> {
  /** Stable persistence handle. TanStack-key shaped (kebab or snake). */
  readonly id: string;
  readonly input: InputSchema;
  readonly build: (b: Builder<InferSchemaOutput<InputSchema>>) => M;
  /**
   * Compute the flow's terminal output from its step outputs. Fired once at
   * `flow.completed` and persisted on the fact + `onFlowComplete` event.
   * Skipped steps land as `null` in the input record at runtime even though
   * the type claims otherwise (consistent with the "skip is transitive" lock).
   * Method-shorthand syntax keeps `M` bivariant so `Flow<S, M, O>` remains
   * assignable to `Flow<S, StepMap, O>` in fixture/test code.
   */
  output?(steps: NeedsOutputs<M>): Output;
}

export interface Flow<
  InputSchema extends StandardSchemaV1 = StandardSchemaV1,
  M extends StepMap = StepMap,
  Output = unknown,
> {
  readonly id: string;
  readonly input: InputSchema;
  readonly steps: M;
  output?(steps: NeedsOutputs<M>): Output;
}

export type FlowInput<F> =
  F extends Flow<infer S, StepMap, unknown> ? InferSchemaOutput<S> : never;

export type FlowOutput<F> =
  F extends Flow<StandardSchemaV1, StepMap, infer O> ? O : never;

export interface FlowEvent {
  readonly runId: RunId;
  readonly flowId: string;
  readonly at: Date;
}

export interface FlowStartEvent extends FlowEvent {
  readonly input: Json;
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

export interface StepStartEvent extends StepEvent {}

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
  /** AbortSignal drives graceful drain on SIGTERM/SIGINT. */
  readonly signal?: AbortSignal;
}

export interface WorkerRunOnceOpts {
  readonly maxSteps?: number;
}

export interface WorkerRunUntilEmptyOpts {
  /** Wall-clock deadline in `Date.now()` units. Edge runtimes have CPU/wall-time budgets. */
  readonly deadline?: number;
}

export interface WorkerRunResult {
  readonly processed: number;
}

export interface Worker {
  /** Long-running fleet (Cloud Run service, ECS, dedicated VM). Blocks until `signal` aborts. */
  run(): Promise<void>;
  /** HTTP server alongside requests. Returns after up to `maxSteps` are processed. */
  runOnce(opts?: WorkerRunOnceOpts): Promise<WorkerRunResult>;
  /** Serverless / edge. Drains everything available within the deadline and exits. */
  runUntilEmpty(opts?: WorkerRunUntilEmptyOpts): Promise<WorkerRunResult>;
}

// Every Store / Queue / Clock / Trigger operation is run-scoped — no method
// spans multiple runs. The sharding-safety lock is enforced at the type
// level, not by convention.

export type ClaimToken = string & { readonly __brand: "ClaimToken" };

export interface SerializedError {
  readonly name: string;
  readonly message: string;
  readonly stack?: string;
  readonly cause?: Json;
}

/**
 * Fact-ordering invariant (must hold for every Store implementation):
 * facts persisted via `appendFact` / `completeStep` / `failStep` MUST be
 * visible to the next `loadRunState` call. The scheduler decides what to run
 * by projecting `RunState.steps` from the fact log, then calls `nextRunnable`
 * — if a `step.completed` write isn't reflected in a subsequent read, the
 * scheduler will re-enqueue or skip steps incorrectly. Adapters with read
 * replicas must route `loadRunState` to the primary or wait for replication
 * within a single `advance` cycle.
 */
export interface Store {
  appendFact(runId: RunId, fact: Fact): Promise<void>;
  loadRunState(runId: RunId): Promise<RunState>;

  /**
   * Returns null if the step is already claimed under a live lease. Lease
   * duration is owned by the Store implementation; configure it on the adapter.
   */
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

  /** Memoization read — returns the persisted output of a previously completed step. */
  getStepOutput(runId: RunId, stepId: StepId): Promise<Json | null>;

  /** `ctx.once` durable record. */
  recordOnce(
    runId: RunId,
    stepId: StepId,
    scope: string,
    value: Json,
  ): Promise<void>;
  getOnce(runId: RunId, stepId: StepId, scope: string): Promise<Json | null>;
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
  /** Attempt number to publish. Defaults to 1. The dispatcher controls increments. */
  readonly attempt?: AttemptNumber;
  /** Visibility delay — the message becomes dequeueable after `delayMs`. */
  readonly delayMs?: Millis;
  /** Adapter-specific extra data. Core never reads this. */
  readonly payload?: Json;
}

export interface Queue {
  enqueue(runId: RunId, stepId: StepId, opts?: QueueEnqueueOpts): Promise<void>;
  dequeue(opts: QueueDequeueOpts): Promise<readonly QueueMessage[]>;
  ack(receipt: string): Promise<void>;
  nack(receipt: string, opts?: { delayMs?: Millis }): Promise<void>;
  extend(receipt: string, leaseMs: Millis): Promise<void>;
}

export interface Clock {
  now(): Date;
  /** Resolves when `ms` has elapsed or `signal` aborts. */
  sleep(ms: Millis, signal?: AbortSignal): Promise<void>;
  /** Persistent timer — wakes the scheduler at `at` for `(runId, stepId)`. */
  schedule(at: Date, runId: RunId, stepId: StepId): Promise<void>;
}

/**
 * Wakes the scheduler when there's work to do. Default impl polls via the
 * Queue plugin; Postgres adapter offers a NOTIFY/LISTEN variant.
 * Per the sharding-safety lock: the handler receives a `runId` — there is no
 * global tick.
 */
export interface Trigger {
  subscribe(handler: (runId: RunId) => void): () => void;
}

// CHOICE: Append-only fact stream. Adapters persist these in a single
// `fact` table; the scheduler projects them into `RunState`. Discriminated
// union by `kind` so adapters can do exhaustive switches.

export type FactKind =
  | "flow.started"
  | "flow.completed"
  | "flow.failed"
  | "step.started"
  | "step.completed"
  | "step.failed"
  | "step.retried"
  | "step.skipped"
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
}

export interface FlowCompletedFact extends FactBase {
  readonly kind: "flow.completed";
  readonly output: Json;
}

export interface FlowFailedFact extends FactBase {
  readonly kind: "flow.failed";
  readonly error: SerializedError;
}

export interface StepStartedFact extends FactBase {
  readonly kind: "step.started";
  readonly stepId: StepId;
  readonly attempt: AttemptNumber;
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

export interface StepRetriedFact extends FactBase {
  readonly kind: "step.retried";
  readonly stepId: StepId;
  readonly attempt: AttemptNumber;
  readonly nextAttemptAt: Date;
}

export interface StepSkippedFact extends FactBase {
  readonly kind: "step.skipped";
  readonly stepId: StepId;
  readonly reason: "when-false" | "transitive";
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

export type Fact =
  | FlowStartedFact
  | FlowCompletedFact
  | FlowFailedFact
  | StepStartedFact
  | StepCompletedFact
  | StepFailedFact
  | StepRetriedFact
  | StepSkippedFact
  | SignalSentFact
  | SignalReceivedFact
  | OnceRecordedFact
  | MatchArmSelectedFact;

export type RunStatus = "pending" | "running" | "completed" | "failed";
export type StepStatus =
  | "pending"
  | "running"
  | "completed"
  | "failed"
  | "skipped";

export interface StepState {
  readonly stepId: StepId;
  readonly status: StepStatus;
  readonly attempts: AttemptNumber;
  readonly output?: Json;
  readonly error?: SerializedError;
}

export interface RunState {
  readonly runId: RunId;
  readonly flowId: string;
  readonly status: RunStatus;
  readonly steps: Readonly<Record<StepId, StepState>>;
  readonly facts: ReadonlyArray<Fact>;
}

export type ReplayMode = "inspect" | "continue";

export interface ReplayOpts {
  readonly mode: ReplayMode;
}
