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

// Module-augmentation slot. Users declare `interface Register { tx: ... }`
// from their own module to make `Tx` resolve at compile time. Must be
// `interface` (not `type`) — type aliases cannot be augmented via declaration
// merging across modules.
// biome-ignore lint/suspicious/noEmptyInterface: intentional augmentation slot
export interface Register {}

export type Tx = Register extends { tx: infer T } ? T : unknown;

export type StepKind = "task" | "signal" | "match";

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

  /**
   * Durable per-effect memoization. The first successful call for `(runId, stepId, scope)`
   * persists its return value; subsequent calls (including post-crash retries) return the
   * cached value without re-invoking `fn`.
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
    readonly input: NoInfer<Input>;
    readonly needs: NoInfer<NeedsOutputs<N>>;
  }) => boolean;
  readonly timeoutMs?: Millis;
}

export interface TaskConfig<Input, N extends NeedsMap, Output>
  extends StepConfigBase<Input, N> {
  readonly retry?: RetryPolicy;
  readonly run: (args: {
    readonly input: NoInfer<Input>;
    readonly needs: NoInfer<NeedsOutputs<N>>;
    readonly ctx: StepCtx<NoInfer<Input>>;
  }) => Promise<Output>;

  /**
   * Lifecycle hooks colocated with this step. Fire *before* the cross-cutting
   * `FlowHooks` callbacks (deterministic ordering). A throwing hook is logged
   * via the runtime's `Logger` and swallowed — hooks cannot fail the run.
   *
   * `onComplete`'s event is typed against this step's `Output` — no `Json`
   * narrowing required at the call site.
   */
  readonly onStart?: (event: StepStartEvent) => void | Promise<void>;
  readonly onComplete?: (
    event: StepCompleteEvent & { readonly output: NoInfer<Output> },
  ) => void | Promise<void>;
  readonly onError?: (event: StepErrorEvent) => void | Promise<void>;
  readonly onRetry?: (event: StepRetryEvent) => void | Promise<void>;
}

/**
 * One signal step accepts one or more external signal names. Omitting
 * `names` defaults to `[stepId]` (back-compat behavior). Passing `names:
 * [...]` accepts any listed name; first arrival wins, late losers are a
 * no-op + logged. Non-empty tuple type so `names: []` is a compile error.
 */
export interface SignalConfig<
  Input,
  N extends NeedsMap,
  Schema extends StandardSchemaV1,
> extends StepConfigBase<Input, N> {
  readonly schema: Schema;
  readonly names?: readonly [string, ...string[]];
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
    readonly input: NoInfer<Input>;
    readonly needs: NoInfer<NeedsOutputs<N>>;
  }) => D;
  readonly cases: {
    readonly [K in D]: (b: Builder<Input>) => BuildResult<Input, M[K]>;
  };
}

export interface MatchArmGuard<Input, N extends NeedsMap, M extends StepMap> {
  readonly when: (args: {
    readonly input: NoInfer<Input>;
    readonly needs: NoInfer<NeedsOutputs<N>>;
  }) => boolean;
  readonly otherwise?: never;
  readonly build: (b: Builder<Input>) => BuildResult<Input, M>;
}

export interface MatchArmOtherwise<Input, M extends StepMap> {
  readonly otherwise: true;
  readonly when?: never;
  readonly build: (b: Builder<Input>) => BuildResult<Input, M>;
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
        readonly input: NoInfer<Input>;
        readonly needs: NoInfer<NeedsOutputs<N>>;
      }) => boolean;
      readonly otherwise?: never;
      readonly build: (b: Builder<Input>) => BuildResult<Input, StepMap>;
    }
  | {
      readonly otherwise: true;
      readonly when?: never;
      readonly build: (b: Builder<Input>) => BuildResult<Input, StepMap>;
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
 * Per-entry config for `b.step(key, config)`. `Needs` is a const tuple of
 * sibling keys that resolve against the chain's accumulator `A`.
 */
export interface StepEntryConfig<
  Input,
  A extends Record<string, unknown>,
  Needs extends ReadonlyArray<keyof A & string>,
  Output,
> {
  readonly needs?: Needs;
  readonly retry?: RetryPolicy;
  readonly timeoutMs?: Millis;
  readonly when?: (args: {
    readonly input: NoInfer<Input>;
    readonly needs: { readonly [P in Needs[number]]: A[P] };
  }) => boolean;
  readonly run: (args: {
    readonly input: NoInfer<Input>;
    readonly needs: { readonly [P in Needs[number]]: A[P] };
    readonly ctx: StepCtx<NoInfer<Input>>;
  }) => Promise<Output>;

  /** See {@link TaskConfig} for hook semantics. */
  readonly onStart?: (event: StepStartEvent) => void | Promise<void>;
  readonly onComplete?: (
    event: StepCompleteEvent & { readonly output: NoInfer<Output> },
  ) => void | Promise<void>;
  readonly onError?: (event: StepErrorEvent) => void | Promise<void>;
  readonly onRetry?: (event: StepRetryEvent) => void | Promise<void>;
}

/**
 * `b` inside `flow({ build: (b) => ... })`.
 *
 * Two complementary authoring styles:
 *
 *   1. **Standalone constructors** — `b.task`, `b.signal`, `b.match` each
 *      return a `Step<Output>` value the user can assign and reference. The
 *      build callback returns a `StepMap` of those values.
 *
 *   2. **Chain (key-first)** — `b.step(key, config)` is keyed and chainable.
 *      Each call extends the builder's accumulator `A` with `{ [Key]: Output }`,
 *      and subsequent `.step()` calls type `needs: ["sibling"]` against `A`.
 *      `b.include(key, prebuiltStep)` brings a `Step<O>` (from `b.task` /
 *      `b.signal` / `b.match`) into the chain under a key. The chain itself
 *      is returned from the build callback; `flow()` extracts the accumulator.
 *
 * Both styles compose in the same build callback — the chain end is the
 * canonical result; pre-built standalone steps enter the chain via
 * `.include(key, step)`.
 */
export interface Builder<
  Input = unknown,
  A extends Record<string, unknown> = Record<never, never>,
> {
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
    Cases extends {
      readonly [K in D]: (b: Builder<Input>) => BuildResult<Input, StepMap>;
    },
  >(config: {
    readonly needs?: N;
    readonly on: (args: {
      readonly input: NoInfer<Input>;
      readonly needs: NoInfer<NeedsOutputs<N>>;
    }) => D;
    readonly cases: Cases;
  }): Step<
    {
      readonly [K in keyof Cases]: MatchArmOutput<
        AsStepMap<ReturnType<Cases[K]>>
      >;
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
  }): Step<MatchArmOutput<AsStepMap<ReturnType<Arms[number]["build"]>>>>;

  /**
   * Chainable task definition. The first arg is the persisted step id; the
   * config follows the standalone `task` shape plus a `needs: ["sibling"]`
   * tuple of accumulator keys. Returns the same builder typed with the new
   * entry appended to `A`.
   *
   *   b.step('fetchRecording', { run: async () => ({ url: '…' }) })
   *    .step('transcribe', {
   *      needs: ['fetchRecording'],
   *      run: async ({ needs }) => ({ text: needs.fetchRecording.url + '…' }),
   *    })
   */
  step<
    const Key extends string,
    const Needs extends ReadonlyArray<keyof A & string>,
    Output,
  >(
    key: Exclude<Key, keyof A>,
    config: StepEntryConfig<Input, A, Needs, Output>,
  ): Builder<Input, A & { readonly [K in Key]: Output }>;

  /**
   * Include a pre-built `Step<O>` (from `b.task` / `b.signal` / `b.match`)
   * into the chain under a key. The included step's output joins `A`.
   */
  include<const Key extends string, S extends Step<unknown>>(
    key: Exclude<Key, keyof A>,
    step: S,
  ): Builder<
    Input,
    A & {
      readonly [K in Key]: S extends Step<infer O> ? O : never;
    }
  >;
}

/**
 * Acceptable shape for a build callback's return. Either a plain `StepMap`
 * (record-literal style) or a `Builder<Input, A>` whose accumulator is
 * extracted by `flow()`.
 */
export type BuildResult<Input, M extends StepMap> =
  | M
  | Builder<Input, BuilderAccumulator<M>>;

/** Convert a `StepMap` back to its accumulator shape `{ [K]: Output }`. */
export type BuilderAccumulator<M extends StepMap> = {
  readonly [K in keyof M]: M[K] extends Step<infer O> ? O : never;
};

/** Extract a `StepMap` from either a plain map or a `Builder` chain result. */
export type AsStepMap<R> =
  R extends Builder<unknown, infer A>
    ? { readonly [K in keyof A]: Step<A[K]> }
    : R extends StepMap
      ? R
      : never;

/**
 * Modes for `FlowConcurrency`. v1 implements only `cancel-in-progress`; the
 * union is a single literal so adding `queue` / `reject` later is a
 * non-breaking widening rather than a new discriminator.
 */
export type ConcurrencyMode = "cancel-in-progress";

/**
 * Per-flow concurrency control. When set, `wf.start()` derives a group key
 * from the validated input and applies the configured `mode` against any
 * prior run sharing the same `(flowId, key)` pair.
 *
 * `cancel-in-progress`: any active prior run with the same key transitions
 * to the `canceled` terminal state before the new run is inserted. In-flight
 * steps on the canceled run run to completion (their facts persist), but the
 * dispatcher stops scheduling further steps for it.
 */
export interface FlowConcurrency<Input = Json> {
  /**
   * Derive the concurrency group key from the validated flow input.
   * Synchronous + pure. Returning a non-string or an empty string throws
   * `NagiValidationError` at `wf.start()` time.
   */
  readonly keyFn: (input: Input) => string;
  readonly mode: ConcurrencyMode;
}

export interface FlowConfig<
  Id extends string,
  InputSchema extends StandardSchemaV1,
  R,
  Output = unknown,
> {
  /** Stable persistence handle. TanStack-key shaped (kebab or snake). */
  readonly id: Id;
  readonly input: InputSchema;
  /**
   * Returns either a `StepMap` (record-literal style with `b.task` / `b.signal`
   * / `b.match`) or a chained `Builder<Input, A>` (Express-style with `b.step`
   * / `b.include`). `flow()` extracts the assembled `StepMap` either way.
   */
  readonly build: (b: Builder<InferSchemaOutput<InputSchema>>) => R;
  /**
   * Compute the flow's terminal output from its step outputs. Fired once at
   * `flow.completed` and persisted on the fact + `onFlowComplete` event.
   * Skipped steps land as `null` in the input record at runtime even though
   * the type claims otherwise (consistent with the "skip is transitive" lock).
   */
  output?(steps: NeedsOutputs<AsStepMap<R>>): Output;

  /**
   * Concurrency group config. See {@link FlowConcurrency}. `keyFn` is typed
   * against the schema's parsed output — what `b.task`'s `run({ input })`
   * receives — so refactors to the input schema break the key derivation
   * at compile time.
   */
  readonly concurrency?: FlowConcurrency<InferSchemaOutput<InputSchema>>;

  /**
   * Flow-level lifecycle hooks. Like step-local hooks (see {@link TaskConfig}),
   * these fire *before* the cross-cutting `FlowHooks` callbacks, and a thrown
   * hook is swallowed + logged rather than failing the run.
   *
   * `onComplete`'s event is typed against this flow's `Output` (whatever the
   * `output()` resolver returns).
   *
   * `onError` also fires when a run is canceled by a concurrency-group
   * supersede; the event's `error.name` is `"NagiCanceledError"` and
   * `error.cause` carries `{ canceledByRunId, concurrencyKey }`.
   */
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

  /**
   * Forwarded from {@link FlowConfig}; surfaced for the dispatcher.
   *
   * Note these use the plain (non-Output-augmented) event types — the typed
   * narrowing lives on {@link FlowConfig}'s callbacks at build time, while
   * the runtime form is variance-friendly so a concrete `Flow<...{x: number}>`
   * stays assignable to `Flow<...unknown>` (the dispatcher's view).
   */
  readonly onStart?: (event: FlowStartEvent) => void | Promise<void>;
  readonly onComplete?: (event: FlowCompleteEvent) => void | Promise<void>;
  readonly onError?: (event: FlowErrorEvent) => void | Promise<void>;
  /**
   * Forwarded from {@link FlowConfig.concurrency}. The runtime form widens
   * `Input` to `Json` so a concrete `Flow<...{x: number}>` is assignable to
   * `Flow<...unknown>` (the dispatcher's view).
   */
  readonly concurrency?: FlowConcurrency<Json>;
}

export type FlowInput<F> =
  F extends Flow<string, infer S, StepMap, unknown>
    ? InferSchemaOutput<S>
    : never;

export type FlowOutput<F> =
  F extends Flow<string, StandardSchemaV1, StepMap, infer O> ? O : never;

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

export interface StepStartEvent extends StepEvent {
  /**
   * The resolved input for this step at the moment it starts.
   * - Task: the value passed to `run({ input, needs, ctx })`.
   * - Signal: `null` (signals await an external payload — no pre-execution input).
   * - Match: `null` (the discriminator value is computed inside the match
   *   handler; the start event fires before that resolves).
   */
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
   * Atomically begin a run. Inserts the `flow.started` fact and any
   * materialized run row keyed by `runId` IFF no run with this `runId` already
   * exists. Returns `{ started: true, canceled: [...] }` if this call created
   * the run, `{ started: false, canceled: [] }` if the run already exists.
   *
   * Two concurrent `tryStartRun(runId, ...)` calls with the same `runId` must
   * result in exactly one `flow.started` fact. Adapters enforce this at the
   * durability layer (e.g. Postgres `INSERT ... ON CONFLICT DO NOTHING` keyed
   * by `run_id`), NOT via app-level check-then-insert.
   *
   * When `concurrency` is supplied, the adapter ALSO atomically cancels any
   * prior runs whose `(flowId, concurrency.key)` matches and whose status is
   * still `pending` / `running`. Each canceled run gets a `flow.canceled`
   * fact appended to its log and its materialized status updated to
   * `canceled`. The returned `canceled` array carries the fact for each
   * canceled run so the runtime can fire `onFlowError` post-commit.
   *
   * Adapters MUST serialize concurrent starts for the same
   * `(flowId, concurrency.key)` (e.g. Postgres advisory-lock keyed on the
   * pair) so the partial unique index on active runs is never violated.
   *
   * The runtime calls this exactly once at `wf.start()`. Subsequent facts go
   * through `appendFact` as usual.
   */
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
      readonly fact: FlowCanceledFact;
    }>;
  }>;

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

  /**
   * Run a task step's handler inside an adapter-owned transaction.
   *
   * Implementations open a transaction (or equivalent atomic scope), invoke
   * `body(tx)`, and on a successful return persist the returned fact
   * atomically with any writes the handler made via `ctx.tx`. The output is
   * returned to the caller; for `step.failed` facts the output is ignored
   * (callers should still return the placeholder shape the type requires).
   *
   * Contract:
   * - If `body` throws, the transaction is rolled back and the error
   *   propagates. The caller (dispatcher) is responsible for recording the
   *   failure via `failStep` in a separate scope so the failure survives
   *   even when domain writes do not.
   * - On a returned `step.completed` fact, the same atomic scope must also
   *   record the step output (such that `getStepOutput` reflects it) and
   *   release any worker lease held for `(runId, stepId, attempt)`.
   *
   * For adapters with no real transaction (in-memory, sqlite without WAL),
   * `tx` is passed as `undefined as Tx` and the write is non-atomic — those
   * adapters are not used with `ctx.tx` writes.
   */
  runStep<T extends Json>(
    runId: RunId,
    stepId: StepId,
    attempt: AttemptNumber,
    body: (tx: Tx) => Promise<{
      readonly output: T;
      readonly fact: StepCompletedFact | StepFailedFact;
    }>,
  ): Promise<T>;

  /**
   * Deduped store of canonical flow DAGs, keyed by content hash. Inserts are
   * idempotent — the same `(flowHash, dag)` may be written by many concurrent
   * boots; only the first physical row survives. Subsequent writes are
   * no-ops.
   *
   * Adapters MUST enforce this at the durability layer (e.g. Postgres
   * `INSERT ... ON CONFLICT (flow_hash) DO NOTHING`).
   */
  upsertSnapshot(args: {
    readonly flowHash: string;
    readonly flowId: string;
    readonly dag: Json;
  }): Promise<void>;

  /**
   * Read the currently-published hash for a flow id. Returns `null` if no
   * `flow_ref` row exists yet — first boot of a never-published flow.
   */
  getRef(flowId: string): Promise<string | null>;

  /**
   * Atomically set the published hash for a flow id. Last writer wins; the
   * audit trail of ref changes lives in {@link appendGlobalFact} as
   * `flow_ref.updated`.
   *
   * Adapters MUST enforce atomicity at the durability layer (e.g. Postgres
   * `INSERT ... ON CONFLICT (flow_id) DO UPDATE`).
   */
  setRef(flowId: string, flowHash: string): Promise<void>;

  /**
   * Load a snapshot by its content hash. Returns `null` if no snapshot with
   * this hash has been recorded — e.g., the deployment is replaying a run
   * whose pinned topology was never seen by this store.
   */
  loadSnapshot(
    flowHash: string,
  ): Promise<{ readonly flowId: string; readonly dag: Json } | null>;

  /**
   * Append a non-run-scoped fact. See {@link GlobalFact} for the union.
   * Currently only `flow_ref.updated` events flow through this method.
   */
  appendGlobalFact(fact: GlobalFact): Promise<void>;

  /**
   * Discover runs by their input + flow + status. See {@link QueryRunsOpts}
   * and {@link Wf.queryRuns} (in `runtime.ts`) for the public contract.
   *
   * Adapters MUST order results by `(startedAt DESC, runId DESC)` so cursor
   * pagination is stable across calls. Adapters MUST treat
   * `opts.where.input` as JSONB containment (Postgres `@>` semantics —
   * the row's input is a superset of the filter object, recursively).
   */
  queryRuns(opts: QueryRunsOpts): Promise<QueryRunsResult>;
}

export interface QueryRunsWhere {
  readonly flowId?: string;
  readonly status?: RunStatus | ReadonlyArray<RunStatus>;
  /**
   * JSONB containment against `flow.started.input`. The stored input is a
   * superset of this object — recursive on nested objects, exact on
   * arrays/scalars. Example: `{ videoId: 'abc' }` matches
   * `{ videoId: 'abc', userId: 7 }`.
   *
   * Containment only — no JSONPath, no operators. Combine keys for AND.
   */
  readonly input?: Record<string, Json>;
}

/**
 * Options for {@link Wf.queryRuns}.
 *
 * Encoded as a discriminated union on `latest` so the type system rejects
 * `{ latest: true, limit, cursor }` at compile time — those fields are
 * meaningless when only one row is requested. Adapters still validate at
 * runtime as defense-in-depth.
 */
export type QueryRunsOpts =
  | {
      readonly where?: QueryRunsWhere;
      /** Return at most one run — the most recently started match. */
      readonly latest: true;
      readonly limit?: never;
      readonly cursor?: never;
    }
  | {
      readonly where?: QueryRunsWhere;
      readonly latest?: false;
      /** Page size. Default 50, max 500. */
      readonly limit?: number;
      /** Opaque cursor from a previous `queryRuns` call. */
      readonly cursor?: string;
    };

export interface RunSummary {
  readonly runId: RunId;
  readonly flowId: string;
  readonly status: RunStatus;
  readonly startedAt: Date;
  readonly completedAt: Date | null;
  readonly input: Json;
}

export interface QueryRunsResult {
  readonly runs: ReadonlyArray<RunSummary>;
  /** Pass to the next `queryRuns` to resume. `null` when no more rows. */
  readonly cursor: string | null;
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

export type FactKind =
  | "flow.started"
  | "flow.completed"
  | "flow.failed"
  | "flow.canceled"
  | "step.started"
  | "step.completed"
  | "step.failed"
  | "step.retried"
  | "step.skipped"
  | "step.reset"
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
  /**
   * Content hash of the canonical flow DAG at the moment this run started.
   * Pins the run to a specific topology; `wf.replay()` reads this to detect
   * snapshot drift. Optional in v0 — legacy runs from before the snapshot
   * store landed have `undefined`. After a soak period it becomes required.
   */
  readonly flowHash?: string;
  /**
   * Free-form handler-code identifier, typically a git SHA. Sourced from
   * `nagi({ codeVersion })`. Captures handler-source drift orthogonally to
   * `flowHash` (which only captures topology). See RFC 0001
   * "Topology vs handler code."
   */
  readonly codeVersion?: string;
}

/**
 * A non-run-scoped event, recorded as a side effect of nagi runtime activity
 * that has no specific `runId`. Lives in the `nagi.global_fact` table.
 *
 * Per RFC 0001: when nagi boots and a flow's canonical hash differs from the
 * currently-published one, the runtime updates `flow_ref` and appends a
 * `flow_ref.updated` global fact. The audit trail of ref changes is queryable
 * independently of any single run.
 */
export interface FlowRefUpdatedFact {
  readonly kind: "flow_ref.updated";
  readonly flowId: string;
  /** Previous hash, or null when this is the first publish of `flowId`. */
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

/**
 * Recorded on a prior run when a new `wf.start()` with the same concurrency
 * group supersedes it. The fact lives in the canceled run's fact log (not
 * the new run's) — observers reading that run's history see exactly when and
 * why it was canceled.
 */
export interface FlowCanceledFact extends FactBase {
  readonly kind: "flow.canceled";
  /** The new run that took over this run's concurrency slot. */
  readonly canceledByRunId: RunId;
  /** The shared concurrency key between this run and `canceledByRunId`. */
  readonly concurrencyKey: string;
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
  /**
   * The alias the caller sent, when the resolved signal step accepts
   * multiple names (or a single name != stepId). Absent when the incoming
   * name equals the step id — the back-compat single-name case.
   */
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

/**
 * Marks a step as cleared from the projected `RunState.steps` so the next
 * `nextRunnable()` pass treats it (and any cascaded descendants) as runnable
 * again. Emitted by `wf.replay(runId, { from })` — one fact per step that the
 * runtime resolved as part of the cascade. The user-named step has
 * `cascadedFrom === undefined`; transitively reset descendants record the
 * originating step so read-side UIs can group them.
 */
export interface StepResetFact extends FactBase {
  readonly kind: "step.reset";
  readonly stepId: StepId;
  /** The user-named `from` step that caused this reset, when this fact is a cascaded descendant. */
  readonly cascadedFrom?: StepId;
}

export type Fact =
  | FlowStartedFact
  | FlowCompletedFact
  | FlowFailedFact
  | FlowCanceledFact
  | StepStartedFact
  | StepCompletedFact
  | StepFailedFact
  | StepRetriedFact
  | StepSkippedFact
  | StepResetFact
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
  /** Content hash of the canonical flow DAG this run is pinned to. */
  readonly flowHash?: string;
  /** Handler-code identifier (typically a git SHA) recorded at run start. */
  readonly codeVersion?: string;
}

export type ReplayMode = "inspect" | "continue";

export interface ReplayOpts {
  readonly mode: ReplayMode;
  /**
   * When true, allow replays to proceed even if the live flow's canonical
   * hash differs from the snapshot pinned to the run. Scheduling decisions
   * come from the pinned snapshot; handler bodies are resolved from the
   * currently-registered flow by step id (best-effort).
   *
   * When false (default), drift throws `NagiSnapshotDriftError`.
   */
  readonly allowDrift?: boolean;
  /**
   * Whether step / flow lifecycle hooks (both the step-local ones on
   * `TaskConfig` / `FlowConfig` and the cross-cutting `FlowHooks`) fire during
   * replay. Defaults to `true` — replay re-executes steps via the idempotent
   * dispatcher and emits the same observable lifecycle. Set to `false` for
   * backfills or inspection-style replays where re-emitting webhooks would
   * dual-publish.
   */
  readonly fireHooks?: boolean;
  /**
   * Replay starting from a specific step instead of the default "first
   * incomplete" behavior. The runtime appends `step.reset` facts for `from`
   * and every transitive descendant, then re-dispatches — completed steps
   * downstream of `from` re-run, completed steps upstream are preserved.
   *
   * - Throws `NagiValidationError` if `from` is not a registered step in the
   *   effective flow (the snapshot topology under drift + `allowDrift`, else
   *   the live flow).
   * - Throws `NagiRuntimeError` if the run is still `running` — resetting
   *   mid-flight races in-flight workers.
   * - Ignored under `mode: "inspect"` (inspect remains side-effect-free).
   */
  readonly from?: StepId;
}
