# @nagi-js/core

## 0.1.1-rc.9

### Patch Changes

- RFCs #10, #11 implemented!

## 0.1.1-rc.8

### Patch Changes

- f926424: Internal engine cleanup — no public API change, behavior preserved.

  - **Extract `nextTransition(flow, runState)` from the `advance` loop** (RFC 0011). The engine's per-tick decision is now a pure, exhaustively-typed `Transition` union (`promote-match | complete | fail | dispatch | skip | settled | waiting`) in `scheduler.ts`, and `advance` is a thin executor switch in `dispatch.ts`. Double-finalize is now structurally prevented (`complete`/`fail` are only emitted when the flow is done and not already terminal). Adds 9 direct `nextTransition` unit tests.
  - **Consolidate match/subflow step finalization** into shared `markStepComplete` / `markStepFail` helpers (was duplicated across match promotion and subflow wake).
  - **Reuse `serializeError()` in runtime** instead of hand-building `SerializedError`; dedupe a repeated cancel-error literal.
  - **`instanceof NagiAbortError`** for our own abort (keeps the foreign-`AbortError` name check for handler-thrown DOMExceptions).
  - **Drop dead defensive guards** on `def.needs` (the type already guarantees `Step` refs).
  - **Split pre-walk vs finalized match arms**: the `_nested` ghost field is gone from `MatchArmDef`; pre-walk concerns live on new builder-only `PendingMatchArm` / `PendingMatchDef`.
  - **`DispatchDeps.lookupFlow` / `.startChildRun` are now required** (always wired by the runtime), removing a runtime "missing dep" guard from subflow dispatch.

- b79ede2: `wf.operator()` — programmatic skip/retry/abort for oncall. The three
  primitives an operator needs when a run is stuck no longer require
  direct-editing `nagi.fact` / `nagi.step_run`:

  - `operator.skip(runId, stepId, { actor, note?, cascade? })` — appends
    `step.skipped` with `reason: "manual"`. `cascade: "skip"` (default)
    keeps the locked transitive semantic. `cascade: "continue"` lets
    downstream steps run with `needs.x === null` for the skipped need —
    the handler is responsible for tolerating null; the type contract on
    `needs.x` is unchanged.

  - `operator.retry(runId, stepId, { actor, note? })` — re-runs `stepId`
    and its descendants. For terminal steps, mirrors
    `wf.replay({ from })` with `actor` / `note` stamped onto the named
    `step.reset`. For a `running` step, appends
    `step.abort-requested`; the dispatcher's cancel watcher fires
    `ctx.signal.abort()` cross-process; the in-flight attempt
    reclassifies as `step.canceled`; then the reset cascade lands and
    re-dispatches.

  - `operator.abort(runId, { actor, note? })` — cancels the run with
    `cause: "operator"`, structured `actor` / `note`. Cascades to subflow
    children. In-flight handlers see `ctx.signal.abort()` via the watcher.

  Active cancel watcher landed alongside: `executeTask` now polls
  `store.loadRunState` at `DispatchDeps.cancelPollIntervalMs` (default
  250 ms) and aborts `ctx.signal` when the run reaches terminal status OR
  a matching `step.abort-requested` fact appears. Handlers that pass
  `ctx.signal` to `fetch` / `anthropic.messages.create({ signal })` now
  interrupt mid-call, not just at the boundary.

  Fact-log changes (additive, no migration):

  - `StepSkippedFact.reason` widens to `"when-false" | "transitive" | "manual"`,
    with optional `actor` / `note` / `cascade` populated on manual skips.
  - `FlowCanceledFact` gains optional `cause: "concurrency" | "explicit" | "operator"`
    and optional `actor` / `note`. `wf.cancel()` records `cause: "explicit"`,
    keeping back-compat via the existing `concurrencyKey`-as-reason carrier.
  - `StepResetFact` gains optional `actor` / `note` (populated by
    `operator.retry`; `wf.replay({ from })` leaves them undefined).
  - New `step.abort-requested` fact kind for the operator-issued
    per-step abort signal. Audit-only; the cancel watcher reads it and
    reclassifies the in-flight attempt.

  `@nagi-js/postgres`: zero migration. All new fields ride through the
  existing `payload jsonb` column; `fact.kind` has no CHECK constraint.

- f926424: `@nagi-js/otel` now nests a subflow child flow's root span under its
  parent's subflow-step span, producing a single contiguous trace tree from
  the top-level run down through arbitrary subflow depth. Previously each
  child run rooted its own trace, leaving operators to correlate parent and
  child via `nagi.run.id` / `nagi.parent.run.id` attributes by hand.

  `FlowStartEvent` gains an optional `parent: { runId, stepId, attempt }`
  struct (single optional, not three — by construction either all three
  parent fields are set or none are). `nagi()` populates it on every
  subflow-spawned run; top-level runs leave it `undefined`. The struct is
  in-process only; durable parent linkage continues to live on
  `FlowStartedFact.parentRunId / parentStepId` unchanged.

  `otelHooks.onFlowStart` resolves the child flow span's parent context via
  a three-step fallback: parent step span in registry → local parent flow
  context (`flowCtxs`) → OTel `context.active()`. Never throws. Records
  `nagi.parent.run.id`, `nagi.parent.step.id`, `nagi.parent.step.attempt`
  as attributes on the child flow span when `event.parent` is set so
  cross-process linkage remains queryable even when the in-process anchor
  is missing (process restart, replay).

  Top-level run traces are byte-equivalent to prior behavior. See
  `docs/rfcs/0010-otel-subflow-span-linkage.md`.

- f926424: Simplify the core public surface — three breaking changes that collapse parallel APIs into a single canonical shape. Net: −250 production LOC, −3.5 KB bundle, −2.7 KB d.ts.

  **`FlowCanceledFact` is now a discriminated union by `cause`.** The previous shape had an optional `cause` plus an always-required `concurrencyKey` that was abused to carry the `reason` string on explicit cancels. The new shape is three concrete arms:

  - `{ cause: "concurrency", canceledByRunId, concurrencyKey }`
  - `{ cause: "explicit", reason, note? }`
  - `{ cause: "operator", actor, reason, note? }`

  `Store.tryStartRun`'s returned `canceled[].fact` is now typed `FlowCanceledByConcurrencyFact` — adapters writing concurrency cancel facts must set `cause: "concurrency"` explicitly. Adapter persistence that previously stored `canceledByRunId` unconditionally should null it out on non-concurrency arms (see the postgres adapter for an example).

  **`b.step` chain API and `b.include` are removed.** The single canonical way to declare a step is `b.task({ needs: { key: stepRef }, ... })`. Migration:

  ```ts
  // Before
  build: (b) =>
    b
      .step("a", { run: async () => ({ v: 1 }) })
      .step("b", { needs: ["a"], run: async ({ needs }) => needs.a.v + 1 });

  // After
  build: (b) => {
    const a = b.task({ run: async () => ({ v: 1 }) });
    const c = b.task({ needs: { a }, run: ({ needs }) => needs.a.v + 1 });
    return { a, c };
  };
  ```

  `StepEntryConfig`, `BuildResult`, `BuilderAccumulator`, `AsStepMap`, and the `Builder<Input, A>` second generic parameter are removed. `FlowConfig.build` is now typed `(b: Builder<Input>) => R extends StepMap`.

  **`b.match({ on, cases })` discriminator form is removed.** Only `b.match({ arms: [...] })` remains. Migration:

  ```ts
  // Before
  b.match({
    on: ({ input }) => input.kind,
    cases: {
      a: (b1) => ({ x: b1.task({ run: ... }) }),
      b: (b1) => ({ y: b1.task({ run: ... }) }),
    },
  })

  // After
  b.match({
    arms: [
      { when: ({ input }) => input.kind === "a", build: (b1) => ({ x: b1.task({ run: ... }) }) },
      { when: ({ input }) => input.kind === "b", build: (b1) => ({ y: b1.task({ run: ... }) }) },
      // ...or use { otherwise: true } for the fallback arm
    ],
  })
  ```

  `MatchDiscriminatorConfig` and `MatchDiscriminatorOutput` types are removed. The internal `MatchDef` no longer carries a `mode` field; `matchArms()` helper is dropped (just read `def.arms` directly). Match arms identified by case-key (e.g. `m.a.x`) are now positionally identified (`m.arm0.x`, `m.otherwise.y`); flow snapshots will rehash. The `CanonicalStep.matchMode` and `matchOnHash` fields are removed (no longer meaningful with single-arm semantics).

- f926424: `wf.startById(flowId, input, opts?)` — registry-aware dispatch for callers
  that hold a runtime-typed input rather than the original `Flow` object.
  Intended for transactional-outbox reconcilers, queue consumers replaying a
  DLQ, and admin CLIs that replay a `runId` from disk. Validates `input`
  against the registered flow's schema before the run is created, mirroring
  `start`'s runtime contract without requiring a compile-time-typed input.

  Throws `NagiRuntimeError` when `flowId` is not registered with `nagi()`,
  and `NagiValidationError` when the input fails the flow's schema or when
  `opts.runId` is invalid.

  Motivation: callers with a serialized payload (`pending_workflow_run`-style
  outbox rows, pgmq DLQ entries) were forced to launder both arguments
  through `as any` because `start<F extends Flow>(flow: F, input: FlowInput<F>)`
  demands a statically-known input type — even though the runtime already
  re-validates against `flow.input` unconditionally. `startById` exposes the
  runtime contract directly.

  `start(flow, input, opts)` now delegates to `startById(flow.id, input, opts)`
  internally; behavior is observably unchanged.

## 0.1.1-rc.7

### Patch Changes

- c4e1459: `ctx.signal` and a new `step.canceled` fact kind for cancel-aware step
  classification. `StepCtx.signal: AbortSignal` is now constructed per step run
  and threaded into handlers — pass it to `fetch` or
  `anthropic.messages.create({ signal })` so handlers can be composed with
  user-supplied timeout signals (`AbortSignal.any([ctx.signal, AbortSignal.timeout(60_000)])`).
  The wakeup that fires `ctx.signal.abort()` on `cancel-in-progress` is a
  follow-up; today the signal aborts only when the user composes it.

  When a run transitions to `canceled` while a step is in flight (a newer
  `wf.start()` superseded it via a concurrency group), the dispatcher
  reclassifies the step at the boundary:

  - **Handler returns normally on a canceled run** → records `step.canceled`
    instead of `step.completed`. Domain writes from the handler still commit
    atomically with the canceled fact; read-side projection no longer leaks
    a "completed" status onto a canceled run.
  - **Handler throws on a canceled run** → records `step.canceled` instead of
    `step.failed`. Retry is suppressed (the run is terminal) and `onStepError`
    does not fire — it's a relabel, not an error. `AbortError`-shaped throws
    preserve the error on the canceled fact for downstream observability.

  `FactKind` gains `"step.canceled"`; `StepStatus` gains `"canceled"`;
  `Fact` widens to include `StepCanceledFact` (optional `error` field).
  `Store.runStep`'s body return type widens to accept
  `StepCanceledFact` so adapters can record the boundary classification
  atomically with the handler's transaction.

  `@nagi-js/postgres`: new migration `0006_step_canceled_status` widens the
  `step_run.status` CHECK constraint to accept `'canceled'`. Existing rows are
  unaffected; the constraint is reapplied with the added value.

- Implement RFCs #7 #9 #10
- c4e1459: Implement issue #9 — `wf.pruneFacts({ olderThan, statuses })` for fact-log
  retention. Deletes facts (and per-step rows, leases, timers, dedupes) for
  terminal runs whose `completedAt < olderThan`. `pending` / `running` runs are
  excluded at the type level via `PrunableStatus` and re-validated at runtime.

  Defaults: `statuses: ["completed"]`, `batchSize: 1000`, `keepSummary: true`
  (retains a summary row so `queryRuns` still lists the pruned run; both
  adapters honor this — postgres keeps the `workflow_run` row, in-memory keeps
  a shadow `RunSummary`). After a prune, `loadRunState` and `replay` for that
  run return an empty state — documented trade-off: fact-fidelity traded for
  storage.

  Postgres uses `FOR UPDATE SKIP LOCKED` on the victim CTE so concurrent
  pruners share work without contention. New partial index
  `workflow_run_completed_at_idx` (migration `0007`) backs the per-batch
  victim selection on `(completed_at)` filtered to terminal-status rows. The
  SELECT requires `EXISTS (SELECT 1 FROM fact WHERE run_id = ...)` so the
  batch loop terminates when `keepSummary: true` (otherwise it would
  re-select the same kept summary rows forever).

  See `docs/rfcs/0009-prune-facts.research.md`.

- c4e1459: `b.subflow(child, { input })` now embeds another flow as a step. The child
  runs as an independent run on the same store/queue; the parent's subflow
  step parks in `running` until the child reaches terminal state, then resumes
  with `step.output = { childRunId, output }`. The wake-up mechanism mirrors
  `wf.signal()` — child's `flow.completed` / `flow.failed` writes the parent's
  `step.completed` / `step.failed` directly via the `finalizeFlowCompletion` /
  `finalizeFlowFailure` hooks; no parent-side dispatch re-trip.

  `wf.cancel(runId, opts?)` is now a public API. It writes `flow.canceled` to
  the run, transitively cancels every child run spawned via `b.subflow()`, and
  surfaces the cancellation to a higher parent (if any) as a `step.failed`
  with a structured `NagiCanceledError`. Idempotent on already-terminal runs.

  Child flows must be passed explicitly to `nagi({ flows: [parent, child] })` —
  referencing an unregistered child throws at dispatch with an actionable error.
  Parent linkage is recorded on the child's `flow.started` fact via two new
  optional fields `parentRunId` + `parentStepId`, and on the in-memory Store
  via a `parent → children` index that backs `Store.listChildren(parentRunId)`.

  Postgres adapter migration `0005_subflow_parent_link` adds
  `workflow_run.parent_run_id` + `workflow_run.parent_step_id` columns with a
  partial btree index on `parent_run_id WHERE parent_run_id IS NOT NULL`. The
  index backs the cancel cascade query and stays empty for non-subflow runs.

  Sibling cancellations triggered by a child's own `concurrency` config now
  propagate to that sibling's parent's subflow step (no more silent hangs when
  two children share a concurrency key). Canonical flow hash gains
  `childFlowId` + `subflowInputHash` fields for subflow steps; pre-existing
  flows hash byte-identically.

  Replay-memo for subflow children is intentionally out of scope for this
  release — a parent replay will re-execute its child rather than memoizing
  the prior child's output. Handler idempotency at the child level is the
  correctness story for now; explicit memoization will land in a follow-up
  RFC. See `docs/research/issue-10-subflow-runtime.md`.

## 0.2.0-rc.6

### Minor Changes

- 735cea4: `wf.replay(runId, { mode, from })` now supports step-scoped replay. Pass
  `from: stepId` to reset that step and every transitive descendant — completed
  steps downstream of `from` re-run, completed steps upstream are preserved.
  This is the primitive for "re-run just this tab" affordances on already-
  completed runs; previously the only retry path was the default replay from
  the first incomplete step, which was a no-op on a completed run.

  A new `step.reset` fact is appended for `from` and one per cascaded
  descendant. Cascade follows two edges: forward `needs` (anything reading the
  reset step's output) and match-arm membership (resetting a match step
  invalidates the prior arm selection and re-runs every step in every arm).
  Sibling arm steps do not cascade across each other; resetting an arm step
  does not reset the parent match.

  The `step.reset` fact carries an optional `cascadedFrom: StepId` field on
  descendants — the user-named step has `cascadedFrom === undefined`,
  runtime-emitted cascades record the originating `from` step so read-side UIs
  can group them.

  Validation: `from` must reference a step in the effective flow (snapshot
  topology under `allowDrift`, else live) — unknown ids throw
  `NagiValidationError`. Calling `replay({ from })` on a still-running run
  throws `NagiRuntimeError`; reset mid-flight races in-flight workers. `from`
  is ignored under `mode: "inspect"`.

  `@nagi-js/postgres`: `appendFact` now clears the materialized `step_run` row
  and releases the lease for the reset step so the next dispatch can re-claim
  and re-execute at the same attempt.

### Patch Changes

- Implement issue #5

## 0.1.1-rc.5

### Patch Changes

- c728826: `b.signal({ ... })` can now accept one or more external names. Pass
  `names: ['audioReady', 'recordingReady']` to let one signal step resolve on
  the first arrival from any of N upstream sources, or `names: ['approval']` to
  decouple a single signal name from the step id. Omitting `names` keeps today's
  behaviour — the step id is the signal name. Late-arriving losers (a recognized
  alias for an already-resolved step) are a no-op + logged, not a throw.

  `SignalReceivedFact` gains an optional `signalName` field that records which
  alias triggered resolution when it differs from the step id. Construction-time
  flow validation rejects overlapping signal names (alias-vs-stepId or
  alias-vs-alias) so an ambiguous flow can't boot. See RFC 0004.

  Behavior note: pre-existing flows that don't pass `name` or `names` are
  unaffected — neither at the source level, nor in their canonical flow hash,
  nor in `code_version`.

## 0.1.1-rc.4

### Patch Changes

- Realign release cohort: republish all four packages on the 0.1.x line.
  @nagi-js/core@0.2.0-rc.3 (and the otel/pgmq/postgres rc.3 cohort that
  pinned it as a workspace dep) was an unintended minor bump and will be
  unpublished from npm. No code changes — this changeset exists to produce
  a clean rc.4 cohort with core back on 0.1.x.

## 0.1.1-rc.3

### Patch Changes

- d67d361: `nagi({ codeVersion })` now auto-defaults to a structural fingerprint of the
  registered flows when omitted. The audit field on `workflow_run.code_version`
  and `flow.started` facts is meaningful by default and shifts only when flow
  topology changes — not on every deploy. Explicit `codeVersion: string` is
  unchanged and still taken as-is. Exposes `fingerprintFlows(flows)` for
  callers who want to compute or compare the value directly. See RFC 0003.

  Behavior note: callers who previously omitted `codeVersion` will see
  `code_version` flip from `NULL` to a SHA-256 hex string for runs started
  after upgrading. Any dashboard filtering on `code_version IS NULL` to detect
  un-tagged deploys should be updated.

- fix rc tagging

## 0.1.1-rc.1

### Patch Changes

- step hooks

## 1.0.0

### Major Changes

- 2f4b9f0: Add content-addressed snapshot store. Every run is pinned to the exact DAG
  topology that existed when it started. Replays read the pinned snapshot, not
  the current in-memory flow definition. See RFC 0001.

  New surface:

  - `canonicalize(flow)` and `sha256Canonical(dag)` — turn a flow into a
    byte-stable canonical form keyed by content hash.
  - `diffSnapshots(a, b)` — structural delta between two canonical DAGs
    (added/removed steps, edge changes, predicate changes).
  - `nagi({ codeVersion })` — handler-code identifier (typically a git SHA),
    persisted on every run alongside the topology hash.
  - `ReplayOpts.allowDrift` — opt-in escape hatch for replays whose live
    topology differs from the pinned snapshot.
  - `NagiSnapshotDriftError` — thrown by `wf.replay()` on detected drift.
  - New `Store` methods: `upsertSnapshot`, `getRef`, `setRef`, `loadSnapshot`,
    `appendGlobalFact`.
  - `@nagi-js/postgres`: new `0002_snapshot_tables` migration adds
    `flow_snapshot`, `flow_ref`, `global_fact` tables; adds `flow_hash` +
    `code_version` columns to `workflow_run`.

  Breaking changes (`@nagi-js/core`):

  - `nagi()` now returns `Promise<Wf>` (was `Wf`). The snapshot upsert and ref
    resolution at boot are async.
  - `wf.replay()` throws `NagiSnapshotDriftError` when the live flow's hash
    differs from the pinned snapshot. Pass `replayOpts.allowDrift: true` to
    proceed against the live code anyway (best-effort hybrid: scheduling from
    the snapshot, handlers from live).
  - `Store` interface gains 5 new methods. Custom implementations must add
    them.

### Minor Changes

- 2f4b9f0: Add `b.step()` chainable task builder (RFC 0002). Replaces the previously
  proposed `b.steps({...})` record literal. The chain delivers full type
  inference for sibling references without manual annotation:

  ```ts
  flow({
    id: "demo",
    input: passthroughSchema<{ start: number }>(),
    build: (b) =>
      b
        .step("a", { run: async ({ input }) => ({ doubled: input.start * 2 }) })
        .step("b", {
          needs: ["a"],
          run: async ({ needs }) => ({
            // needs.a is typed as { doubled: number }
            next: needs.a.doubled + 1,
          }),
        }),
  });
  ```

  The first argument is the persisted step id; the config follows the
  standalone `b.task` shape plus a `needs: ["sibling"]` tuple of accumulator
  keys. Each `.step(key, config)` extends the builder's accumulator type, so:

  - Typo in `needs: [...]` → compile error
  - Duplicate chain key → compile error
  - `needs.<sibling>` access inside `run` / `when` is fully typed

  The chain coexists with `b.task` / `b.signal` / `b.match` — pre-built steps
  enter the chain via `b.include(key, step)`:

  ```ts
  build: (b) => {
    const route = b.match({ ... });
    return b
      .step("a", { ... })
      .step("b", { needs: ["a"], ... })
      .include("route", route);
  }
  ```

  `flow()` accepts either a chain return or a plain `StepMap` (back-compat for
  existing `b.task` / `b.signal` / `b.match` patterns).

  **Also breaking:** `timeout` field on task/signal configs renamed to
  `timeoutMs` for unit clarity. Affects `TaskConfig`, `SignalConfig`, and
  the internal `TaskDef` / `SignalDef`. Replace `timeout: 30_000` with
  `timeoutMs: 30_000` in any step config. No runtime behavior change.

## 0.1.0

### Minor Changes

- 3bceb7a: Implement the `@nagi-js/postgres` Store adapter and the `Store.runStep` widening that makes it possible.

  Core (`@nagi-js/core`):

  - Add `Store.runStep(runId, stepId, attempt, body)` — adapter-owned atomic scope for a step. `body` receives the adapter's transaction handle (`Tx` from the `Register` augmentation pattern); on a returned `step.completed` / `step.failed` fact, the adapter persists the output / error, the fact, and releases the worker lease atomically.
  - `dispatch.executeTask` now calls `runStep` and threads the handed-back `tx` into `ctx.tx`, so user-handler writes commit atomically with the step's completion. In-memory runs pass `tx: undefined` — handlers that touch `ctx.tx` only run under a real Store adapter (e.g. `@nagi-js/postgres`).
  - Export `projectRunState` so adapters share one canonical fact-stream → `RunState` projection.

  Postgres (`@nagi-js/postgres`):

  - `postgresStore({ db, schema?, leaseMs?, notifyChannel? })` — Kysely-shaped, driver-agnostic. Implements every `Store` method including `runStep`, which opens a Kysely transaction, passes it to the handler as `ctx.tx`, and atomically commits the user's domain writes with `step_run` + `fact` + lease release.
  - Inline SQL migrations (`migrate(db, { schema? })`) — no `fs.readFileSync`, edge-safe. v0 schema: `workflow_run`, `step_run`, `fact`, `lease`, `timer`, `dedupe`, plus `schema_migrations` bookkeeping.
  - `postgresTrigger({ listen, channel? })` — wraps a long-lived LISTEN client (e.g. `pg.Client`) and turns `pg_notify(channel, runId)` events emitted by the Store into scheduler wake-ups. Pair with `postgresStore({ notifyChannel })`.
  - Hand-rolled RFC 9562 `uuidv7()` for `fact_id` — time-ordered, no external dep, edge-safe (`crypto.getRandomValues` only).
  - Sharding-safe by construction: every operation is `runId`-scoped, no `bigserial` PKs, IDs are text/UUID throughout.
  - Env-gated integration tests (`NAGI_POSTGRES_TEST_URL`) — run conformance against a real Postgres without bundling testcontainers.
