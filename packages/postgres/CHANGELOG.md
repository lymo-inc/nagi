# @nagi-js/postgres

## 0.1.1-rc.21

### Patch Changes

- 8a7891f: Require `kysely` ^0.29.0 as the peer (was ^0.28.0), and build and test against 0.29.

## 0.1.1-rc.20

### Patch Changes

- Updated dependencies [5a4a1d7]
  - @nagi-js/core@0.1.1-rc.20

## 0.1.1-rc.19

### Patch Changes

- d468140: Collapse hypothetical seams (#42): one adapter is not a seam. Interfaces now declare only what the engine exercises.

  **Removed (`@nagi-js/core`)**

  - `Clock.schedule(at, runId, stepId)` — zero engine callers; only its own test exercised it. `Clock` is now `{ now, sleep }`.
  - `InMemoryClock.dispose()` and the `InMemoryClock` constructor options (`{ trigger }`) — existed only to back `schedule`. `new InMemoryClock()` is unchanged.
  - `Trigger` interface and `InMemoryTrigger` — `Trigger.subscribe` had no subscriber anywhere in the engine.
  - `NagiConfig.trigger` — accepted and never read.
  - `NagiConfig.streamTransport` — streaming is now a Store capability (below); there is no second injection path.

  **Removed (`@nagi-js/postgres`)**

  - `postgresTrigger`, `PostgresTriggerOpts`, `ListenClient`, `NotificationMessage` — the only implementation of the removed `Trigger` interface. Nothing in the runtime consumed it; a LISTEN/NOTIFY wake-up can return as a real seam once the engine has a caller for it.

  **`Queue.withTx?(tx): Queue` is now declared once, in core.** It has two consumers (`wf.startStaged` and the Postgres store's lease sweep), so it is a real seam. Both call sites use the typed optional method (`queue.withTx?.(tx) ?? queue`); the two private `typeof q.withTx === "function"` duck-typing helpers in core and postgres are gone. The returned Queue MUST route every write through `tx`. `PgmqQueue` still narrows it to required, the same way it narrows `ensureSchema`.

  **`StreamTransport` folded into `Store.stream?: StreamTransport`.** The runtime no longer sniffs the Store for `subscribeStream`/`publishChunk`; it reads `config.store.stream`. It lives on the Store, not beside it, because the close contract (the iterator MUST end when the step reaches a terminal fact) can only be honored by whoever observes the fact log — which is the Store. `InMemoryStore.stream` is the reference implementation (`store.subscribeStream` / `store.publishChunk` moved to `store.stream.subscribeStream` / `store.stream.publishChunk`). A Postgres deployment satisfies it the same way: a Store that exposes `stream`, driving close/retry off its own `appendFact` (e.g. LISTEN/NOTIFY fan-out of chunks with the in-memory hub's semantics). `postgresStore` does not implement it yet, so a flow with a `b.streamingTask` step still throws at `nagi()` registration on Postgres — unchanged behavior, now stated by the type instead of discovered at runtime.

  `Operator` is left as is: it is referenced by name in docs and the operator changeset.

- bbeeef7: Own each fact kind in one place (#38). `packages/core/src/facts/` now holds,
  per lifecycle (`flow` / `step` / `signal` / `lease`), a kind's shape, its
  constructor, its fold arm, and its read-model row delta. `foldRun` moved from
  `state.ts` to `facts/`; `state.ts` is the read model only. The public `Fact`
  union and every `*Fact` type still come from `@nagi-js/core` (single door).

  New: `rowDeltaOf(fact): RowDelta | null` and the `RowDelta` type. The Postgres
  store no longer hand-transcribes an 18-case fact→table switch; it interprets
  the small `RowDelta` vocabulary core declares per kind, and every persistence
  path (`appendFact`, `settleStep`, `runStep`, `settleSignal`,
  `sweepSignalTimeouts`, concurrency cancel) goes through the same
  `persistFact`. A new fact kind that lacks a fold arm or a row-delta declaration
  fails to compile; audit-only kinds declare `rows: null` explicitly.

  Removed (dead, never produced or read — public-surface removals):

  - `signal.sent`: `FactKind` member, `SignalSentFact`, `SignalSentEvent`,
    `FlowHooks.onSignalSent`, and the `@nagi-js/otel` `composeHooks` fan-out.
    No runtime path ever constructed or fired it.
  - `RunState.anomalies` and the `Anomaly` type. Nothing read them; the fold is
    still total (a contradictory fact keeps the prior step state).

  Persisted logs fold unchanged; unknown kinds in a log (e.g. a removed one)
  fold and materialize as no-ops instead of throwing.

- b10921c: Move the remaining `Store` policy into core and make the prose `MUST`
  contracts executable.

  New pure functions in `@nagi-js/core` (the seam `decideSignal` /
  `decideExpiredLeaseAction` already use — core decides, the adapter owns only
  its tx boundary): `factEffects` (which leases / timers / concurrency slots a
  fact releases), the `queryRuns` cursor codec and limit clamp
  (`encodeRunCursor` / `decodeRunCursor` / `clampQueryLimit` / `compareRunOrder`
  / `isPastCursor`), `jsonContains` (reference `@>` semantics for
  `where.input`), `selectExpired` (expiry filter before limit for lease and
  timer sweeps) and `selectPruneBatch` (prune eligibility, oldest-first
  batching). Both adapters call them; the 48 byte-identical cursor lines in
  `@nagi-js/postgres` are gone.

  `InMemoryStore` now agrees with `postgresStore` where the two had drifted:

  - Leases are released on `settleStep`, `runStep`, `settleSignal` delivery,
    signal timeout, `step.canceled` and `step.reset`, driven by `factEffects`
    at the single `appendFact` choke point. `describe()` no longer reports a
    `lease` on a settled step, and `sweepLeases` no longer sees settled steps as
    candidates.
  - `sweepLeases` / `sweepSignalTimeouts` filter on expiry before applying
    `limit`, so an expired lease can no longer starve behind `limit` live ones.
  - `pruneFacts` drains in batches of `batchSize` (loop until empty), matching
    the Postgres loop.
  - `recordOnce` is first-write-wins (was last-write-wins).
  - `describe()` returns the retained summary (no steps) for a run pruned with
    `keepSummary: true` instead of `null`.

  `postgresStore` applies `factEffects` in `appendFact` and every settle path,
  replacing six hand-placed lease deletes. One behavioural change: `settleStep`
  / `runStep` with `step.completed` now also drop a stale signal timer for that
  step (previously only the signal-delivery path did). The conformance suite
  also caught a real bug: `queryRuns` without a `where.input` filter failed on
  Postgres with `could not determine data type of parameter` (an untyped
  `$n IS NULL`); the parameter is now cast to `jsonb`.

  New subpath `@nagi-js/core/testing` exports `storeContract`, a
  framework-agnostic conformance suite (one case per `Store` `MUST`, plus one
  per divergence above) and `passthroughSchema`. Run it against any adapter:

  ```ts
  for (const c of storeContract) it(c.name, () => c.run({ makeStore }));
  ```

  `@nagi-js/core` runs it against `InMemoryStore`; `@nagi-js/postgres` runs it
  against `postgresStore` in `store-contract.test.ts` (gated on
  `NAGI_POSTGRES_TEST_URL`, like the integration suite).

- Updated dependencies [d468140]
- Updated dependencies [bbeeef7]
- Updated dependencies [9624f15]
- Updated dependencies [bbeeef7]
- Updated dependencies [ef71525]
- Updated dependencies [b10921c]
- Updated dependencies [179d3e7]
  - @nagi-js/core@0.1.1-rc.19

## 0.1.1-rc.18

### Patch Changes

- 7867f5c: `pgmqQueue` default `visibilityTimeoutMs` is now 120s (was 30s), above core's
  40s heartbeat interval — with stock settings a step longer than 30s was
  redelivered before its first lease extension. `postgresStore.claimStep` now
  computes lease expiry on the database clock, matching `extendLease`, so
  application clock skew can no longer grant a second claim on a live lease.
- f3268eb: `postgresStore.pruneFacts` over-reported `runsPruned` when two pruners ran
  concurrently: under READ COMMITTED the second pruner's victim SELECT could
  return runs whose facts the first had already deleted, and each was counted as
  pruned. `runsPruned` now counts only runs whose facts the call actually
  removed, and the batch loop stops on no candidates rather than on no deletions.
- 9478345: `postgresStore.queryRuns` without a `where.input` filter failed on a real
  database with `could not determine data type of parameter` — the absent
  filter was bound as an untyped NULL. It is now cast to `jsonb`. Surfaced by the
  first CI run of the Postgres integration suite.
- 6d9c0c5: `step.reset` (from `wf.replay({ from })` and `operator.retry`) now reopens a
  `completed`/`failed` run to `running`. Previously the run stayed terminal:
  the re-run steps finished but no `flow.completed` was ever written
  (`describe()`/`queryRuns` kept reporting `failed`, `onFlowComplete` never
  fired, a waiting parent subflow was never woken), and the cancel watcher
  aborted any re-run handler honoring `ctx.signal` after 250 ms because the run
  looked terminal. Reopening a run whose concurrency key another active run
  holds throws `NagiConcurrencyConflictError`. `canceled` runs are not reopened.
- Updated dependencies [0d1203c]
- Updated dependencies [4d4b178]
- Updated dependencies [f27b5b9]
- Updated dependencies [eff6275]
- Updated dependencies [4b20b2e]
- Updated dependencies [0d22d1f]
- Updated dependencies [4b20b2e]
- Updated dependencies [6d9c0c5]
- Updated dependencies [4b20b2e]
- Updated dependencies [d751145]
  - @nagi-js/core@0.1.1-rc.18

## 0.1.1-rc.17

### Patch Changes

- 771a6a2: Enforce `b.signal({ timeoutMs })` so a signal step no longer parks forever when
  its signal never arrives.

  `timeoutMs` on a signal step was previously accepted, canonicalized, and diffed
  but never enforced — a run waiting on a signal that was never delivered (a lost
  webhook, an upstream that silently produced nothing) stayed `awaitingSignal`
  indefinitely. It now fails on deadline: the awaiting step is settled `failed`
  with a `NagiSignalTimeoutError`, which the scheduler propagates to `flow.failed`
  (downstream steps cascade to `skipped`). A signal step without `timeoutMs` keeps
  the previous park-forever behavior.

  The timeout is the deadline counterpart to signal delivery — it resolves the
  same step the same way `wf.signal()` does, but to a failure: under the same
  per-run lock as `settleSignal` (so a delivery and a timeout can never both win),
  the deadline derives from the persisted `step.started.at` fact plus `timeoutMs`
  (replay-safe). The deadline anchors to when the step FIRST started awaiting —
  `upsertTimer` keeps the earliest `fire_at`, so a lease-reap re-dispatch of a
  still-parked signal can't push it out (a reaper running every lease interval
  would otherwise reset it forever). It fires `onStepError` before finalizing the
  flow, the same as every other step failure. The worker sweeps elapsed timeouts
  itself on a fixed cadence (`WorkerConfig.timerSweepIntervalMs`, default 30s;
  checked by wall-clock each loop so a fully-busy worker still sweeps on schedule),
  so no extra loop is needed alongside the worker.

  Adds `Store.upsertTimer(...)` and `Store.sweepSignalTimeouts(...)` — custom
  `Store` implementations must add both (the in-memory and Postgres stores already
  do, using the `nagi.timer` table that already shipped). Also adds the exported
  `NagiSignalTimeoutError`, the pure `decideTimeout(...)` decision, the
  `TimedOutSignal` type, `Dispatcher.sweepTimers(...)`, and
  `WorkerConfig.timerSweepIntervalMs`. No migration: the `timer` table and its
  `(run_id, step_id)` primary key already existed.

- Updated dependencies [771a6a2]
  - @nagi-js/core@0.1.1-rc.17

## 0.1.1-rc.16

### Patch Changes

- subflow lease on park
- Updated dependencies
  - @nagi-js/core@0.1.1-rc.16

## 0.1.1-rc.15

### Patch Changes

- Fix subflow idempotent spawning
- Updated dependencies
  - @nagi-js/core@0.1.1-rc.15

## 0.1.1-rc.14

### Patch Changes

- Apply fixes
- 3b8e7a9: Consumer-driven stability fixes (RFC 0014):

  - **N1 (pgmq)** — internal queue SQL now strips Kysely plugins so consumer-installed transformers (e.g. `CamelCasePlugin`) can't break receipt reads. Fixes silent-ack failures when the caller's `db` has CamelCasePlugin installed; the workaround (`db.withoutPlugins()` at the consumer) is no longer required.

  - **N3 + N4 (core)** — new `wf.describe(runId)` returns a typed projection `{run: RunView, steps: StepView[]}` covering parent/child links + step lease state, replacing direct `nagi.workflow_run` / `nagi.step_run` table reads. New `RunId.parse(s)` / `RunId.fromTrusted(s)` constructors retire the `as RunId` cast.

  - **N5 (core + postgres)** — new `wf.startStaged(flow, input, { tx, runId? })` enqueues a run inside the caller's Kysely tx. Returns `{ runId, started, canceled, applyOnCommit }`; the caller commits their tx, then awaits `applyOnCommit()` to fire cancellation hooks. Skips the concurrency advisory lock under shared tx (relies on the partial unique index); retries once on unique-violation, then throws `NagiConcurrencyConflictError`. Retires the consumer-side `pending_workflow_run` + reconciler outbox pattern. `wf.start` and `Store.tryStartRun` are unchanged.

  - **N7 audit pins (core)** — 8 tests proving `decideSignal` correctly buffers signals across `pending` / `running` / `backoff` / `aborting` states. No code change; pins the invariant ahead of N8.

  - **N8 (core + postgres) — load-bearing reliability fix.** New lease autoreaper detects expired leases on non-terminal steps, deletes the lease, writes a `lease.reaped` audit fact, and re-enqueues the step at `attempt + 1` — all atomically. Heartbeat now extends both queue VT AND store lease per tick. Periodic reaper loop is built into `nagi.run` (configurable `NagiConfig.reaperIntervalMs`, default 30s; `0` disables). Resolves the steady-state stuck-run failure mode where a dead worker stranded steps indefinitely. Resolves RFC 0013 Open Question 3.

  - **N9 (core)** — new `NagiFlowSnapshotGoneError` thrown at dispatch when the run's pinned `flowHash` ≠ current registry's hash for that `flowId`. Replaces silent code-version drift with a typed, catchable error. `wf.cancel` and `wf.signal` bypass the hash check (fact-only paths).

  - **N10 (core)** — handler-thrown `NagiCanceledError` now reclassifies the run as `flow.canceled` (cause: `concurrency`), not `flow.failed`. `workflow_run.canceled_by_run_id` populates from the canonical fact, not from the error JSONB. Closes the LYMO-12P false-positive watchdog alert class. Walks the error cause chain via `instanceof` (defends against forged JSONB).

  - **N12 (core)** — `NagiAbortError.name = "AbortError"` (WHATWG conformance). Run-vs-step discriminator moved from `scope` to `kind`. Class exported from `@nagi-js/core` for typed consumer-side translation. Fetch-based SDKs (`@ai-sdk/provider-utils`, OpenRouter) now correctly recognize nagi-originated aborts via the standard `isAbortError` allowlist — no more Sentry spam / wasted LLM retries when a watchdog fires mid-call.

  N11 (phantom superseder, 1-in-53 frequency in consumer data) is deferred to a follow-up RFC pending root-cause investigation. N2 (CHANGELOG/README) and N6 (multi-name buffering — already correct in rc.12) intentionally not addressed.

  See `docs/rfcs/0014-consumer-driven-stability-fixes.md` for the full decision log + per-phase implementation notes.

- Updated dependencies
- Updated dependencies [3b8e7a9]
  - @nagi-js/core@0.1.1-rc.14

## 0.1.1-rc.13

### Patch Changes

- Implement RFC#13
- Updated dependencies [92f9d9f]
- Updated dependencies
- Updated dependencies [92f9d9f]
  - @nagi-js/core@0.1.1-rc.13

## 0.1.1-rc.12

### Patch Changes

- Internal refactors for increased readability and improved type safety
- 803ebb9: Extract signal reconciliation into a core policy so `Store` adapters stop
  reimplementing it. New exports `decideSignal(args)` and `SignalDecision`: given
  the projected run state, the target step, and an optional incoming signal,
  `decideSignal` returns the deliver/buffer/noop decision together with the
  fact(s) to persist and the `SettleSignalResult` to return. An adapter's
  `settleSignal` now loads run state under its per-run lock, calls `decideSignal`,
  persists the returned fact(s), and returns `result` — owning only the
  transaction boundary, never the policy. `@nagi-js/postgres` adopts this (and no
  longer hand-builds signal facts inline); the in-memory store does too.

  Also trims redundant run-state projections on the dispatch hot path: the state
  loaded when a step is admitted is threaded through start-recording and execution
  instead of being re-folded for the flow input and each step's needs (a task
  dispatch drops from four full fact folds to two). Internal only — no behavior
  change.

- Updated dependencies
- Updated dependencies [803ebb9]
  - @nagi-js/core@0.1.1-rc.12

## 0.1.1-rc.11

### Patch Changes

- c8041c5: Buffer signals that arrive before their target step is claimed, closing the
  start/await race.

  `wf.signal()` no longer throws `Step "<id>" is not waiting for signal (status:
pending)` when a signal lands before the worker has claimed the signal step. The
  payload is parked as a new `signal.buffered` fact and applied atomically the
  moment the worker claims the step (it enters `awaitingSignal`), so an early
  `audioReady` / `recordingReady`-style signal can no longer be lost to dispatch
  timing.

  Adds `Store.settleSignal(...)`, which reconciles a signal with its step under a
  per-run lock (the Postgres store uses `pg_advisory_xact_lock`); a new
  `SignalBufferedFact`; and `RunState.bufferedSignals`. Custom `Store`
  implementations must add `settleSignal` — the in-memory and Postgres stores
  already do.

  Delivery stays exactly-once: a buffered signal and a late direct signal cannot
  both apply, and a signal that arrives after the step has already resolved is
  still a no-op.

- Stronger state and type representation
- Updated dependencies [c8041c5]
- Updated dependencies [5cbca32]
- Updated dependencies
- Updated dependencies [e451bfd]
- Updated dependencies [5cbca32]
  - @nagi-js/core@0.1.1-rc.11

## 0.1.1-rc.10

### Patch Changes

- Fold the subflow parent linkage on `FlowStartedFact` from two independently
  optional fields (`parentRunId?` / `parentStepId?`) into a single optional
  `parent?: ParentLink` (`{ runId, stepId }`). A run is either a root (no
  `parent`) or a child (a complete `parent`); the previous shape allowed the
  unrepresentable half-state of a `parentRunId` with no `parentStepId`, which the
  runtime then had to guard against at every read. Two new types are exported:
  `ParentLink` (the durable link persisted on the fact) and
  `ParentRef extends ParentLink` (adds `attempt`, the in-process reference used
  for otel span/registry lookups). `FlowStartEvent.parent` now references
  `ParentRef` — its `{ runId, stepId, attempt }` shape is unchanged.

  Optionality now lives only at the boundary: `startRunInternal` keeps a single
  `parent?: ParentRef` (the one genuine root-vs-child fork), while the
  subflow-only internals (`startChildRun`, `DispatchDeps.startChildRun`,
  `executeSubflow`) take a required `parent: ParentRef`. `propagateToParent`
  collapses its two `=== undefined` guards into one `parent === undefined` check.
  The fact still drops `attempt` (it is re-derived from parent run state on wake),
  so the durable record is unchanged in information, only in shape.

  **Persisted-shape change (breaking for in-flight subflows across upgrade).**
  The Postgres event log serializes/revives fact payloads structurally
  (`{...rest}` → JSONB → `{...body}`), so new `flow.started` payloads round-trip
  the nested `parent` object with no adapter change; only the two denormalized
  column writes were repointed to `fact.parent?.runId` / `fact.parent?.stepId`
  (the `parent_run_id` / `parent_step_id` columns and `listChildren` are
  unchanged). However, child runs persisted **before** this release carry the old
  top-level `parentRunId` / `parentStepId` keys in their payload and will revive
  with `parent === undefined`, so their parent will not be woken on completion. No
  migration shim is provided (pre-1.0); drain in-flight subflows before upgrading,
  or backfill the payloads. The in-memory store has no cross-restart persistence
  and is unaffected.

- Updated dependencies
- Updated dependencies
- Updated dependencies
- Updated dependencies
  - @nagi-js/core@0.1.1-rc.10

## 0.1.1-rc.9

### Patch Changes

- RFCs #10, #11 implemented!
- Updated dependencies
  - @nagi-js/core@0.1.1-rc.9

## 0.1.1-rc.8

### Patch Changes

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

- Updated dependencies [f926424]
- Updated dependencies [b79ede2]
- Updated dependencies [f926424]
- Updated dependencies [f926424]
- Updated dependencies [f926424]
  - @nagi-js/core@0.1.1-rc.8

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

- Updated dependencies [c4e1459]
- Updated dependencies
- Updated dependencies [c4e1459]
- Updated dependencies [c4e1459]
  - @nagi-js/core@0.1.1-rc.7

## 0.1.1-rc.6

### Patch Changes

- Implement issue #5
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

- Updated dependencies
- Updated dependencies [735cea4]
  - @nagi-js/core@0.2.0-rc.6

## 0.1.1-rc.5

### Patch Changes

- Updated dependencies [c728826]
  - @nagi-js/core@0.1.1-rc.5

## 0.1.1-rc.4

### Patch Changes

- Realign release cohort: republish all four packages on the 0.1.x line.
  @nagi-js/core@0.2.0-rc.3 (and the otel/pgmq/postgres rc.3 cohort that
  pinned it as a workspace dep) was an unintended minor bump and will be
  unpublished from npm. No code changes — this changeset exists to produce
  a clean rc.4 cohort with core back on 0.1.x.
- Updated dependencies
  - @nagi-js/core@0.1.1-rc.4

## 0.1.1-rc.3

### Patch Changes

- fix rc tagging
- Updated dependencies
  - @nagi-js/core@0.2.0-rc.3

## 0.1.1-rc.2

### Patch Changes

- Updated dependencies [d67d361]
  - @nagi-js/core@0.2.0-rc.2

## 0.1.1-rc.1

### Patch Changes

- step hooks
- Updated dependencies
  - @nagi-js/core@0.1.1-rc.1

## 0.2.0

### Minor Changes

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

### Patch Changes

- Updated dependencies [2f4b9f0]
- Updated dependencies [2f4b9f0]
  - @nagi-js/core@1.0.0

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

### Patch Changes

- Updated dependencies [3bceb7a]
  - @nagi-js/core@0.1.0
