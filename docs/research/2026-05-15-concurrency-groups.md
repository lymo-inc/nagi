# Concurrency groups with cancel-in-progress on flow config

**Date**: 2026-05-15 (JST)
**Issue**: [lymo-inc/nagi#2](https://github.com/lymo-inc/nagi/issues/2)
**Status**: research → implementation

## Goal

Add a `concurrency` field to `FlowConfig` that lets a flow declare "only one active run per logical key — supersede the previous one":

```ts
flow({
  id: "videoAnalysis",
  input: z.object({ videoId: z.string(), scope: z.string() }),
  concurrency: {
    keyFn: (input) => `${input.videoId}:${input.scope}`,
    mode: "cancel-in-progress",
  },
  build: (b) => ...,
});
```

When `wf.start()` is called with a concurrency-keyed input and a prior run for that `(flowId, key)` is still active, the prior run is canceled (new terminal state) and the new run proceeds. This eliminates the dedup/coalesce boilerplate every caller writes today.

Out-of-scope modes (`queue`, `reject`) are kept in the type union shape for forward compatibility, but only `cancel-in-progress` is implemented in v1.

## Codebase map (verified line numbers)

### Type sites — `packages/core/src/types.ts`

| Type | Lines | Notes |
|---|---|---|
| `FlowConfig<Id, InputSchema, R, Output>` | 419-455 | Add `concurrency?:` here |
| `Flow<Id, InputSchema, M, Output>` | 457-479 | Pipe forward for the dispatcher |
| `Store` | 612-747 | Extend `tryStartRun` signature |
| `FlowStartedFact` | 815-833 | No change |
| `FactKind` | 796-808 | Add `"flow.canceled"` |
| `Fact` union | 923-935 | Add `FlowCanceledFact` |
| `RunStatus` | 937 | Add `"canceled"` |
| `FlowEvent` / `FlowErrorEvent` | 489-505 | Reused unchanged (`onFlowError` carries the canceled error) |

### Runtime — `packages/core/src/runtime.ts`

| Site | Line | What |
|---|---|---|
| `nagi()` start | 195-271 | Where the new cancellation pre-step goes |
| `wf.replay()` | 357-414 | Add canceled-run rejection at the top |
| `NagiValidationError` / `NagiRuntimeError` / `NagiSnapshotDriftError` | 90-131 | New `NagiCanceledError` slots in here, same style |
| `tryStartRun` call site | 240 | Pass new concurrency context; consume returned `canceledRuns` |

### Dispatcher — `packages/core/src/dispatch.ts`

| Site | Line | What |
|---|---|---|
| `isFlowTerminal` | 592-598 | Extend to recognize `flow.canceled` |
| `advance()` | 366-435 | No new logic needed once `isFlowTerminal` is updated — the existing line 382 guard already short-circuits |
| `dispatchMessage()` | 89-203 | Add an early ack-and-skip if the run is `canceled` (before `claimStep`) |

### Storage — `packages/core/src/memory.ts` + `packages/postgres/src/store.ts`

| Site | Where | What |
|---|---|---|
| `InMemoryStore.tryStartRun` | memory.ts:61-74 | Extend signature; new internal `activeByKey: Map<string, RunId>` index |
| `projectRunState` | memory.ts:192-265 | Handle `flow.canceled` case |
| `PostgresStore.tryStartRun` | store.ts:78-108 | Extend signature; advisory-lock + cancel-then-insert |
| `applyFactToMaterialized` | store.ts:280-345 | Handle `flow.canceled` case (update `workflow_run.status = 'canceled'`) |

### Migrations — `packages/postgres/src/migrations.ts`

| Site | Line | What |
|---|---|---|
| `migrations[]` | 16-125 | New entry `0003_concurrency_groups` |

Adds: `concurrency_key TEXT NULL`, `canceled_by_run_id TEXT NULL`, partial unique index on `(flow_id, concurrency_key) WHERE status IN ('pending', 'running')`, and `'canceled'` to the `workflow_run.status` CHECK constraint.

## Design decisions

### 1. Cancellation is atomic at the Store layer, hooks fire at the runtime layer

`wf.start()` itself does not do "lookup → cancel → insert" as three round-trips. It delegates the atomic part to `Store.tryStartRun`, which is the only method that's allowed to mutate the canonical "is this run active" state.

The signature becomes:

```ts
tryStartRun(
  runId: RunId,
  fact: FlowStartedFact,
  concurrency?: {
    readonly key: string;
    readonly mode: "cancel-in-progress";
  },
): Promise<{
  readonly started: boolean;
  readonly canceled: ReadonlyArray<{
    readonly runId: RunId;
    readonly fact: FlowCanceledFact;
  }>;
}>;
```

- `concurrency` is optional — flows without `concurrency` config call as before.
- `canceled` is the list of prior runs that were marked canceled. For each one, the runtime fires `flow.onError` + `onFlowError` with a `NagiCanceledError`.
- The `FlowCanceledFact` is already persisted to each prior run's fact log inside the same transaction as the new run's `flow.started` — observers reading the fact log see the cancellation atomically. The hook fire is post-commit; if the process dies between commit and fire, observers can derive the same signal from the fact log (the fire is best-effort, the fact log is durable).

Why a single method rather than separate `cancelActiveByKey()` + `tryStartRun()`: atomicity. With two methods, two concurrent starters can both pass the "no active run" check, then both insert; we'd need application-level retry. One method lets each adapter (Postgres, in-memory) own the concurrency primitive that fits its substrate.

### 2. Postgres race handling: advisory-lock keyed on `(flowId, key)`

The proposal sketches `SELECT ... FOR UPDATE SKIP LOCKED`, but SKIP LOCKED has a known race: two starters that arrive before the first has committed will *both* skip the locked-but-uncommitted row, see "no active", and both insert as new runs — yielding two active runs for the same key.

Cleaner: `pg_advisory_xact_lock(hashtext('nagi:conc:' || flow_id || ':' || key))` at the top of the start transaction. Serializes all starts for the same `(flowId, key)` to a single line; inside the lock we do `SELECT ... FOR UPDATE` (without SKIP) on the active rows, update them to `canceled`, then insert. The lock auto-releases on commit/rollback.

This is the same pattern used by `flow_ref` updates (single-writer serialized by `flow_id`); the difference is the lock key is composite.

As a defense-in-depth, the partial unique index `(flow_id, concurrency_key) WHERE status IN ('pending', 'running')` catches any double-active situation that escapes — the second `INSERT` raises a unique-violation. Without this index, a bug in the advisory lock would silently corrupt invariants.

### 3. In-flight steps run to completion in v1 — AbortSignal threading deferred

The proposal explicitly says: *"AbortSignal threading to step handlers is a nice-to-have but not required for v1 — the dispatcher just stops scheduling new steps."*

For v1:

- `StepCtx.signal` (types.ts:97) keeps its current behavior — created in `makeStepCtx` (dispatch.ts:544), never aborted.
- When a run is canceled, in-flight steps run to completion. Their `step.completed` / `step.failed` facts are persisted. Their step-local `onComplete` / `onError` hooks fire normally.
- The next call to `advance()` for the canceled run sees `flow.canceled` in the fact log via the updated `isFlowTerminal`, and returns immediately — no further steps enqueued.
- `dispatchMessage()` (dispatch.ts:89) gets a new early-ack-and-skip: if the run is canceled before the step is claimed, the message is acked without execution. This handles the case where a step was enqueued before cancellation and is sitting in the queue.

This means: a step that *was already executing* may produce a `step.completed` fact AFTER the `flow.canceled` fact in the run's fact log. The fact log is append-only and totally ordered; consumers reading it must be tolerant of post-cancellation step facts (which is fine — they were always going to land late by definition of being in-flight). The materialized `workflow_run.status` is set to `'canceled'` and stays there — `applyFactToMaterialized` for `flow.canceled` does an unconditional UPDATE, but subsequent `step.completed` facts in the same run don't touch `workflow_run.status`.

Cross-process AbortSignal propagation (cancel a run on worker A while a step is mid-execution on worker B) is a separate feature, deferred. It requires the worker to subscribe to cancellation notifications or poll run status — neither is plumbed today.

### 4. `wf.replay(canceledRunId)` throws

Replay against a canceled run has no useful semantic — there are no further steps to dispatch (run is terminal), and re-running side effects on a deliberately-superseded run would defeat the purpose of canceling it. Throwing surfaces the mistake to the caller.

```ts
async replay(runId, opts) {
  const runState = await store.loadRunState(runId);
  if (runState.status === "canceled") {
    throw new NagiRuntimeError(
      `Run ${runId} was canceled (superseded by a newer run with the same concurrency key). Replay is not supported for canceled runs.`,
    );
  }
  // ... existing replay logic ...
}
```

### 5. `NagiCanceledError` follows the existing error style

New class in `runtime.ts` alongside `NagiValidationError` / `NagiRuntimeError` / `NagiSnapshotDriftError`:

```ts
export class NagiCanceledError extends Error {
  readonly runId: RunId;
  readonly canceledByRunId: RunId;
  readonly concurrencyKey: string;
  constructor(args: { runId; canceledByRunId; concurrencyKey }) {
    super(
      `Run ${args.runId} was canceled (superseded by run ${args.canceledByRunId} for concurrency key "${args.concurrencyKey}").`,
    );
    this.name = "NagiCanceledError";
    /* ... */
  }
}
```

It's the value carried by `event.error` when `onFlowError` fires for a canceled run. Consumers can discriminate via `event.error.name === "NagiCanceledError"` (works through `SerializedError` too — the `name` field is preserved when serialized).

### 6. `concurrency.keyFn` runs on parsed input (after schema validation)

Order in `wf.start()`:

1. Validate runId format (existing).
2. Validate input against `flow.input` schema → `validated: Json` (existing, line 225).
3. **NEW**: If `flow.concurrency` is set, call `flow.concurrency.keyFn(validated)`. Throw `NagiValidationError` if `keyFn` returns non-string or empty string. (Catches typos like `(input) => input.videoId` where `videoId` doesn't exist on the parsed shape.)
4. Build `flow.started` fact.
5. Call `store.tryStartRun(runId, fact, concurrency)` (extended signature).
6. Fire onError hooks for `result.canceled` runs.
7. Fire onStart hooks for the new run.
8. Call `advance()`.

Why parsed input: matches `b.task`'s `run({ input })` — handlers and `keyFn` see the same `Input` type. Pure synchronous function — async `keyFn` would force the validation path to wait on it, and there's no compelling use case for derived-from-async-call keys (those should be a step, not a key derivation).

### 7. `canceled_by_run_id` is a column, not a separate fact field

Each canceled run gets:

- A `flow.canceled` fact in its fact log with `canceledByRunId` in the payload (for fact-log consumers).
- A materialized `workflow_run.canceled_by_run_id` column update (for SQL queries: "which run took over for the canceled one").

The fact carries everything the column does; the column exists so analysts don't have to JSON-extract from `payload`. Same pattern as `workflow_run.error` mirroring `flow.failed.error`.

## Implementation order

1. **Types** — `types.ts` additions (FlowConfig.concurrency, RunStatus, FlowCanceledFact, FactKind). All additive; no breakage.
2. **NagiCanceledError** — `runtime.ts` new error class.
3. **Store interface** — extend `tryStartRun` signature in `types.ts`. Builds break here for `InMemoryStore` and `PostgresStore` adapters until step 4/5 land.
4. **InMemoryStore** — implement extended `tryStartRun`. Update `projectRunState` for `flow.canceled`.
5. **Postgres adapter** — migration 0003 (column, index, CHECK update). Implement extended `tryStartRun` (advisory lock + cancel-then-insert). Update `applyFactToMaterialized` for `flow.canceled`.
6. **Builder** — `builder.ts:flow()` propagates `concurrency` from `FlowConfig` to `Flow`.
7. **Runtime** — `wf.start()` derives key + calls extended `tryStartRun` + fires onError for canceled runs. `wf.replay()` rejects canceled runs.
8. **Dispatcher** — `isFlowTerminal` extended; `dispatchMessage` early-acks canceled runs.
9. **Tests** — see test plan below.

## Test plan

### Core (`packages/core/src/runtime.test.ts` or new `concurrency.test.ts`)

- **Happy path**: flow with `keyFn`, start with key K → first run is `running`. Start again with key K → first run is `canceled`, second is `running`. Verify the canceled run has a `flow.canceled` fact and `flow.onError` fired with `NagiCanceledError`.
- **Different keys don't interfere**: start with K1, start with K2 → both `running`, no cancellation.
- **No concurrency config**: flow without `concurrency`, two starts with the same input → both `running` (no key, no cancel).
- **Idempotent runId still works**: flow with `concurrency`, call `start({ runId: X, ... })` twice with same runId → second is a no-op (no cancel, no double-emit). Concurrency key doesn't override the runId-idempotency contract.
- **Replay rejects canceled**: cancel a run, call `wf.replay(canceledRunId)` → throws `NagiRuntimeError`.
- **In-flight step completes**: start a long-running task, cancel the run via a second start, verify the in-flight step's `step.completed` fact lands AFTER `flow.canceled` and that `advance()` does not schedule downstream steps.
- **keyFn validation**: `keyFn` returns non-string → throws `NagiValidationError` at start time.

### Dispatcher

- **`isFlowTerminal` recognizes `flow.canceled`** — direct unit test.
- **`dispatchMessage` early-acks canceled run** — enqueue a step, append `flow.canceled` fact, call `dispatchMessage` → step is not claimed, message is acked.

### Postgres (`packages/postgres/src/integration.test.ts`)

- **Cross-process cancel**: two `nagi()` instances on the same DB, start on instance A with key K, start on instance B with same key K → instance A's run is `canceled` in the shared DB.
- **Partial unique index**: insert a second active run for the same key directly (bypassing `tryStartRun`) → unique violation.
- **Status enum migration**: verify `'canceled'` is accepted by the CHECK constraint after 0003.
- **Advisory lock under load**: spawn N concurrent starts with the same key → exactly one ends up `running`, all others either no-op (idempotent runId match) or `canceled`. No two runs end up `running` simultaneously.

## Out of scope (deliberately)

- **`queue` mode** — keep the type union shape (`mode: "cancel-in-progress"`) restrictive in v1 so the discriminator is enforced; add `"queue"` later as a non-breaking addition.
- **`reject` mode** — same.
- **Cross-flow concurrency groups** — per-flow only. The proposal explicitly defers cross-flow.
- **Concurrency limits > 1** (max-parallelism N) — out of scope.
- **AbortSignal propagation to in-flight step handlers** — deferred per the proposal's explicit note. Step handlers don't receive a cancel notification in v1; they run to completion.
- **Cross-process abort** — Postgres adapter doesn't notify in-flight workers on other processes. Their in-flight steps complete on their own; only the dispatcher's scheduling decisions honor the cancellation.
- **`opts.concurrency: 'bypass'` at the start site** — explicitly listed in the proposal as future work.

## Open questions for review

1. **Advisory lock vs. SKIP LOCKED**: I'm proposing advisory-lock for correctness. SKIP LOCKED is what the proposal text shows. Calling this out — fine to defer to SKIP LOCKED if you're comfortable with the race window in the cancel-in-progress use case.
2. **`canceled_by_run_id` on the canceled-row vs. `canceling_run_id` on the new-row**: I'm picking the canceled-row direction (each canceled run points to its successor). Easier to query "what canceled this run?" Reverse direction would be easier to query "is this new run a supersede or a fresh start?" but the SQL is simpler the way I've specified.
3. **`mode` discriminator with one value**: do you want me to make `mode` optional with `cancel-in-progress` as default, or required-but-discriminated? I'm leaning toward required so adding `queue` later is a non-breaking type change (no implicit narrowing).
