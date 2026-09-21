# @nagi-js/core

## 0.1.1-rc.21

### Patch Changes

- c50101f: `canceled_by_run_id` can no longer name a run that does not exist (nagi#29).

  The orphan was blamed on custom stores, legacy rows or admin writes, because `tryStartRun` is single-tx atomic and so cannot produce it. Two canonical paths produce it anyway, and both are now closed. Each was reproduced against real Postgres and against the in-memory store before being fixed, and both are pinned by the shared conformance suite.

  **Retention.** `pruneFacts` deletes run rows without regard for who points at them, so a policy that keeps `canceled` for audit while dropping `completed` deletes the superseder and strands its victim's reference. Migration `0008_canceled_by_run_id_fk` nulls existing orphans, indexes the column (every `workflow_run` delete re-checks the constraint, and retention deletes in batches) and adds a self-referencing FK with `ON DELETE SET NULL`.

  **A handler's own claim.** `NagiCanceledError` is public and takes any `canceledByRunId`; `classifyFailure` turns it into a concurrency-cause `flow.canceled` fact, which projects straight into the column. Nothing checked that the named run existed — so a consumer reporting its own supersession wrote an orphan directly. The column now takes the id only when it resolves, in both stores. The fact log is untouched and keeps the claim verbatim: facts are immutable, and the column is a projection.

  Because of that, the constraint can be immediate rather than deferred. `tryStartRun` has to cancel the prior run _before_ inserting the superseder — the partial unique index on `(flow_id, concurrency_key)` only frees the slot once the prior leaves `pending`/`running` — so the cancel write cannot name the superseder. It resolves the reference after the insert instead.

  See `docs/OPERATIONS.md` for applying `0008` to a large live table without the validation lock.

- b35291a: `timeoutMs` returns to task / activity / streaming steps, this time ENFORCED. At the deadline the step's `ctx.signal` aborts with a `NagiStepTimeoutError` and the step settles as failed, retryable under its own `retry` policy — a slow upstream gets another attempt, a genuinely stuck step exhausts `maxAttempts` and fails the run.

  A deadline is deliberately not a cancellation: `classifyFailure` reads cancellation from the persisted `step.abort-requested` fact and the run phase, neither of which a deadline writes, so a timed-out step reaches the normal failure path instead of settling as `canceled`. The abort _reason_ carries the discriminator, so a handler that throws its own error on abort (fetch, an SDK) is still recorded as `NagiStepTimeoutError` rather than a generic `AbortError`.

  Enforcement is cooperative — nagi aborts the signal and lets the body unwind, because a task's handler runs inside `store.runStep`'s transaction and abandoning it mid-flight would strand that tx. A handler that never checks `ctx.signal` still holds its slot; `leaseHoldWarnMs` remains the detector for that.

  On a streaming step the deadline also closes its subscribers: the `wf.subscribe` iterator ends with `{ kind: "error" }` carrying the timeout, rather than hanging on a generator that stopped producing.

  `timeoutMs` participates in the flow hash, like the signal-step timeout: changing a deadline changes run semantics, so it changes the flow.

- c1e0331: `b.streamingTask` now works on Postgres. `postgresStore()` takes a `listener` and implements `StreamTransport` over `LISTEN`/`NOTIFY`; until now the transport existed only on the in-memory store, so a flow declaring a streaming step was refused against any production store.

  The listening connection is injected rather than opened by the adapter: Kysely hides the driver (`PostgresConnection` holds the `pg` client privately and exposes no notification event) and this package keeps `pg` a devDependency, so neon / postgres.js / pglite users are not forced onto it. Consumers pass a `StreamListener`, the same way they already pass `db`.

  Two ordering properties the transport has to guarantee, neither of which is free over a connection pool:

  - **Chunks of one step stay in order.** `db` is a pool, so un-awaited `pg_notify` calls can take different connections and arrive reversed. Publishes are chained per step; different steps stay independent.
  - **A close never overtakes its chunks.** Close/retry frames ride the fact's transaction and land at commit, while chunks commit immediately on their own connections. Before emitting a close the adapter drains that step's publish chain, otherwise the channel shuts and in-flight chunks are discarded.

  `postgresStore()` returns a `PostgresStoreHandle` with `ready()`, which resolves once both channels are actually receiving NOTIFY. A constructor cannot await `listen()`, and NOTIFY does not queue for a connection that is not yet listening, so chunks and events published in that window are lost rather than delayed — `await store.ready()` before starting work you intend to watch. It is total: with no `listener` there is nothing to wait for and it resolves immediately, so a consumer never branches on whether it wired one.

  `core` exports `InMemoryStreamHub` and the buffer caps so adapters reuse one fan-out implementation instead of reimplementing subscriber buffering and close semantics. Chunks are capped at `MAX_CHUNK_BYTES` (7000, inside PostgreSQL's 8000-byte NOTIFY payload limit) and an oversized chunk throws from `ctx.emit` rather than being dropped silently.

- 5887534: Non-cascading single-step rerun. `operator.retry(runId, stepId, { actor, scope: "step" })` resets ONLY the named step, leaving completed descendants untouched — the regenerate-one-output shape. `wf.replay(runId, { mode, from, scope })` takes the same control. `scope` defaults to `"cascade"`, so existing callers are unaffected.

  Exposed as a control marker on the existing methods rather than a second `rerunStep` method: the two behaviors differ only in which steps the reset covers, and both call sites now resolve that through one `resetSetOf(flow, stepId, scope)` so they cannot drift apart.

  Under `"step"` the descendants keep outputs derived from the step's PREVIOUS output. That is the contract, not a bug — callers who need a consistent run want `"cascade"`.

  The origin `step.reset` fact now carries `scope` for an isolated rerun (omitted for `"cascade"`, so existing facts keep their shape). Recording the operator's intent beats inferring it: a leaf step has no descendants, so a cascading retry on a leaf is otherwise indistinguishable from an isolated one.

- 37086c5: Live run-event subscription (nagi#16 Layer 1). `wf.watchRun(runId, handler)` and `wf.watchRuns(handler)` push `RunEvent`s — flow started/completed/failed/canceled, step started/completed/failed/retried/skipped — as the facts that produce them commit. Both return a disposer; `watchRun` also disposes itself once the run reaches a terminal event, so watching many runs does not leak a handler each.

  The transport lives on the **Store** (`Store.events`), not in core, and that placement is forced rather than stylistic: `flow.canceled(cause: "concurrency")` and `flow.started` are minted inside the adapter, in the same transaction as the row writes. A core-side decorator would silently miss supersession — a terminal event, and one of the most important things an observer wants. Both adapters now publish from where those facts are actually written, and the shared fan-out (`InMemoryRunEventHub`) is exported from core so neither reimplements subscriber bookkeeping.

  On Postgres, events ride the fact's transaction: PostgreSQL holds a `NOTIFY` until commit, so an observer hears about a fact only once it is durable, and a rolled-back attempt announces nothing. `postgresStore()`'s `listener` option now powers both this and streaming chunks — one LISTEN connection, two channels.

  Not durable by design: a handler sees events from the moment it subscribes, and a restart starts over. Pair with `describe()` / `queryRuns()` to catch up. `watchRuns` does not filter — a live stream cannot honestly filter on `status` (the event _is_ the status change) and filtering on `input` would need a state load per event.

  Stores without the transport throw from `watchRun` / `watchRuns` rather than returning a subscription that silently never fires. On Postgres the events channel shares the store's one LISTEN connection, so `await store.ready()` before starting a run you mean to watch — events published before the socket is live are lost, not delayed.

## 0.1.1-rc.20

### Patch Changes

- 5a4a1d7: `nagi({ driftPolicy })` — what a worker does with a live run pinned to a flow
  hash this code no longer produces (a deploy changed the flow mid-run).

  - `"freeze"` (default, today's behavior): the run is snapshot-gone — nacked
    for a frozen-version worker per `snapshotGonePolicy`, then terminally failed.
  - `"synthesize"`: the run continues on the new code — the pinned DAG shape
    with the live handlers attached, exactly what `replay({ allowDrift: true })`
    already did, now at dispatch too. Falls back to snapshot-gone handling (with
    a `warn` log, `drift synthesis failed`) only when the snapshot is missing or
    a pinned step no longer exists live. Under `"synthesize"`,
    `replay({ mode: "continue" })` no longer needs `allowDrift`.

  Motivation: `allowDrift` replay only synthesized the flow inside the replay
  dispatcher. The step it enqueued was then dequeued by the ordinary worker,
  which resolved the run by its pinned hash and went straight back to
  snapshot-gone — so on a one-task rolling deploy (no frozen-version worker
  ever exists) every deploy stranded every in-flight run, and the orphan
  sweeper's replay looped every 5 minutes until the redelivery budget failed
  the run. Observed 2026-09-16.

  Also: synthesized flows are now cached per pinned hash for the life of the
  process.

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

- 9624f15: Internal: every retry/backoff decision now lives in one module (`retry.ts`).
  `DEFAULT_RETRY` was defined twice with the same values — once for execution
  (`step-exec.ts`) and once for flow hashing (`canonicalize.ts`) — so a change
  to one either silently re-hashed every flow (stranding in-flight runs as
  snapshot-gone) or silently never took effect at runtime. It is now defined
  once and imported by both; the canonical values are pinned by test and are
  byte-identical to before, so **no flow hash changes**.

  The four backoff curves (step retry, dequeue outage backoff, snapshot-gone
  redelivery budget, lease-reap re-dispatch) are named pure functions with one
  signature shape, covered by a single table-driven test. Policy resolution
  (`handler.retry` → `nagi({ defaultRetry })` → built-in) is `resolveRetry`.

  No public API change: `defaultSnapshotGonePolicy` is still exported from the
  package root with identical behavior.

- bbeeef7: Run lifecycle (start / supersede / cancel / spawn-child) now lives in one
  internal module with a single "start a run" path. `wf.startById`,
  `wf.startStagedById`, and subflow child spawning are thin callers over it; the
  own-tx vs caller-tx choice is resolved at exactly one fork, and the post-commit
  effects (superseded-run hooks + parent propagation, the start event, the
  initial dispatch) are a value applied by one function — immediately, or from
  `applyOnCommit`.

  Two staged-start (`wf.startStaged`) gaps close along the way:

  - Roots that are all gated (`when: () => false`) used to enqueue nothing and
    leave the run parked; the run now advances from `applyOnCommit`, as
    `wf.start` always did.
  - A mix of runnable and gated roots still enqueues the runnable roots on the
    caller's tx, exactly as before; the gated siblings' `step.skipped` facts are
    now recorded from `applyOnCommit` instead of waiting for the next advance.

  `applyOnCommit` memoizes its first invocation: later calls are no-ops on
  success, and re-throw the same rejection if that first application failed
  (previously a second call after a failure resolved silently).

  No public API or fact-shape changes.

- ef71525: Internal: the snapshot-gone condition (a run pinned to a `flowHash` this
  process did not register) now has one owner. Flow registration, the hash
  table, and the disposition live in one module that answers "which Flow runs
  this run" with a discriminated result — `current`, `gone-live` (the worker's
  `snapshotGonePolicy` decides), or `gone-terminal` (ack and drop). The message
  handler, worker, and replay consume that result instead of each re-deriving
  the condition from the error. No change to `SnapshotGonePolicy`,
  `NagiFlowSnapshotGoneError`, the default retry budget, or replay's
  drift-allowed synthesis as observed by consumers.
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

- 179d3e7: Remove the undocumented `__dispatchDeps` property `nagi()` planted on the `wf`
  object. It was never part of the public API (non-enumerable, untyped) and
  existed only so core's own test harness could bypass the `Worker` with a
  private dispatcher; the harness now drives `wf.worker(...)` directly, so the
  real dequeue/admission/snapshot-gone loop is what every harness test covers.

  `Worker.runUntilEmpty({ deadline })` now reads the injected `Clock` for its
  deadline instead of `Date.now()` — the bounded drain's only wall-clock read.
  No behaviour change for any shipped clock.

## 0.1.1-rc.18

### Patch Changes

- 0d1203c: A signal step's buffered early signal is now cleared when it is delivered and
  when the step is reset (`operator.retry`, `wf.replay({ from })`). Previously
  the first buffered payload lived on the projection forever: a reset signal
  step completed instantly with the stale payload, and a genuinely new
  `wf.signal` for that step was reported as buffered without writing a fact.
- 4d4b178: A run whose step settled but whose follow-up `advance` was lost (worker crash
  or store error between the terminal fact and the next enqueue) now self-heals:
  redelivery of the step's message re-drives the run instead of being dropped,
  and the message is acked only after the advance succeeds.
- f27b5b9: Operator plane: `wf.inspectQueue(runId)` — read-only triage view of a run's in-queue messages (stepId, attempt, readCount, visibleAt) via optional `Queue.inspect()`; implemented for pgmq and the in-memory queue. Together with `wf.describe()` this replaces the hand-run SQL triage recipes; docs/OPERATIONS.md is the runbook.
- eff6275: The signal-timeout sweep now isolates each run: a run whose advance throws
  (flow snapshot gone, transient store error) is logged at `error` level and
  skipped, and the remaining runs in the batch are still advanced to
  `flow.failed`. Previously the first throw aborted the whole batch, and because
  the store had already dropped the fired timers, the stranded runs were never
  swept again.
- 4b20b2e: `NagiNonRetryableError`: throw from a step to fail immediately, skipping the remaining retry budget. Honored anywhere on the cause chain.
- 0d22d1f: Per-flow blast-radius bound: `WorkerConfig.maxConcurrencyPerFlow` caps the worker slots any single flow may hold; over-cap messages defer via delayed nack. `flowId` now rides the message envelope (stamped at every enqueue path incl. lease-reap; absent pre-upgrade messages are exempt). Multi-flow deployments should set the cap ≤ concurrency − 1 so one wedged flow can never occupy the whole pool.
- 4b20b2e: Bound snapshot-gone redelivery. `QueueMessage.readCount` (pgmq `read_ct`) now travels with every delivery; the worker consults a `SnapshotGonePolicy(readCount)` — retry = delayed nack for the rolling-deploy window, then terminally fail the run with the real error and ack. Terminal runs' messages are acked at dispatch (a canceled run can no longer nack-loop). Default policy: quadratic backoff capped at 5 min, fail past 60 deliveries (~4.2h window).
- 6d9c0c5: `step.reset` (from `wf.replay({ from })` and `operator.retry`) now reopens a
  `completed`/`failed` run to `running`. Previously the run stayed terminal:
  the re-run steps finished but no `flow.completed` was ever written
  (`describe()`/`queryRuns` kept reporting `failed`, `onFlowComplete` never
  fired, a waiting parent subflow was never woken), and the cancel watcher
  aborted any re-run handler honoring `ctx.signal` after 250 ms because the run
  looked terminal. Reopening a run whose concurrency key another active run
  holds throws `NagiConcurrencyConflictError`. `canceled` runs are not reopened.
- 4b20b2e: BREAKING: `b.signal` now requires `timeoutMs: Millis | "unbounded"` — unbounded parking must be an explicit opt-in, never an omission. `"unbounded"` canonicalizes as omission, so existing flows keep their hashes (and in-flight runs) when migrating a timeout-less signal to `"unbounded"`. Also BREAKING: the unenforced `timeoutMs` knob is removed from task/activity/streaming/subflow configs (it armed nothing; a flow that set it changes hash on upgrade). New lease-hold watchdog: `leaseHoldWarnMs` (default 5 min, 0 disables) warns whenever a step body holds a worker slot past each threshold multiple.
- d751145: Worker loop survives transient queue failures. A rejected `queue.dequeue` used
  to propagate out of `worker.run()` and terminate the poll loop: a single
  `pg-pool` connection timeout inside `pgmq.read` silently stopped ALL flow
  processing in a process that stayed otherwise healthy — no steps scheduled, no
  messages consumed, and no signal until stuck-run alerts fired hours later.

  `run()` now catches dequeue failures, logs `worker.dequeue failed; backing off`
  (with `consecutiveFailures` and `backoffMs`), sleeps, and keeps polling —
  exponential from `pollIntervalMs`, capped at 30s, reset on the first success.
  Its contract is now explicit: `run()` settles only via the abort signal.

  `runOnce()` / `runUntilEmpty()` are unchanged and still reject on queue
  failure — they are bounded drains whose caller is awaiting a result, so a
  rejection there is observed rather than silent.

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

## 0.1.1-rc.16

### Patch Changes

- subflow lease on park

## 0.1.1-rc.15

### Patch Changes

- Fix subflow idempotent spawning

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

## 0.1.1-rc.13

### Patch Changes

- 92f9d9f: Add `b.activity({...})`, a step kind for external-effect work (LLM/HTTP calls)
  that runs its body **outside** the durable transaction. `b.task` is unchanged
  and still runs its body inside a short tx so DB writes commit atomically with
  `step.completed` — correct for DB-only work. An activity instead runs the
  handler with no ambient tx, then commits only its terminal fact in a short
  transaction, so a multi-minute handler never holds a connection open.

  `ActivityConfig.run` receives an `ActivityCtx` (`Omit<StepCtx, "tx">`): there is
  no `ctx.tx`, enforced at the type level and backed by a throwing runtime getter.
  Activities are at-least-once and must be idempotent (use `ctx.idempotencyKey` /
  upsert on a deterministic key; optionally an `idempotency-key` header to the
  external provider).

  Cancellation, retry, replay, scheduling, and output typing are identical to
  `task`. See `docs/rfcs/0013-activity-steps.md`. This removes the need for the
  PENDING-row + reaper pattern consumers used to track in-flight external calls,
  and demotes the visibility heartbeat from a correctness mechanism to a cost
  optimization.

- Implement RFC#13
- 92f9d9f: The worker now heartbeats a step's queue message while its handler runs, so a
  long step (e.g. a multi-minute LLM call) no longer outlives the queue's
  visibility timeout and gets redelivered + re-executed concurrently.

  `dispatchMessage` starts a `startHeartbeat` loop before executing a step and
  stops it (in a `finally`) just before ack. Each tick calls the existing
  `queue.extend(receipt, leaseMs)`; a failed extension is logged and the loop
  continues (an early redelivery is still deduped by `admit()`'s `claimStep`). A
  crashed worker simply stops extending, so the message redelivers after at most
  one lease — crash recovery is preserved.

  Tunable via two new optional `NagiConfig` fields, `heartbeatIntervalMs` and
  `heartbeatLeaseMs` (defaults `DEFAULT_HEARTBEAT_INTERVAL_MS` 40s /
  `DEFAULT_HEARTBEAT_LEASE_MS` 120s). `heartbeatIntervalMs` must be shorter than
  the queue's initial visibility timeout, or the first redelivery happens before
  the first extension lands.

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

- 5cbca32: Replace the four-method `Logger` interface in `NagiConfig` with a single
  structured-record callback `onLog?: (entry: LogEntry) => void` (RFC 0020).

  **Breaking:** `NagiConfig.logger` is removed. nagi now produces a structured
  record per diagnostic and the host decides how to render it, so the
  object-first/message-first adapter every pino/bunyan consumer wrote disappears:

  ```ts
  // before
  nagi({ ..., logger: adaptLogger(pino) })
  // after — one line, format-agnostic
  nagi({ ..., onLog: ({ level, msg, attrs }) => pino[level](attrs ?? {}, msg) })
  ```

  `LogEntry` is `{ readonly level: "debug" | "info" | "warn" | "error"; readonly msg: string; readonly attrs?: Record<string, unknown> }`.

  Other behavior changes:

  - **Silent by default.** Omitting `onLog` makes nagi completely silent — the
    in-step `consoleLogger` fallback is removed, so nagi never writes to
    `console.*` behind the host's back, and no `LogEntry` is allocated when there
    is no sink.
  - **In-step `ctx.logger` stays method-shaped** (`ctx.logger.info(msg, attrs)`)
    and now auto-enriches every entry with `runId` / `stepId` / `attempt`
    (runtime-authoritative: a handler cannot clobber the real ids), funneling into
    the same `onLog` sink.
  - **A throwing `onLog` is swallowed** — a logging bug can never fail or retry a
    workflow step.
  - `attrs` is `undefined` (never `{}`) when a diagnostic carries none.

- Stronger state and type representation
- e451bfd: `StepState.output` is now always present (`Json`) instead of optional
  (`Json | undefined`). Non-completed step states (running, failed, canceled,
  skipped) carry `output: null`; completed steps carry their value. This removes
  the ambiguity between "step produced no output" and "step produced `null`" —
  both were already collapsed to `null` at every read via `?? null`, so observable
  behavior is unchanged. `projectRunState` populates the field for every step
  state it emits.

  Reading `state.output` is now total (no `undefined` check needed). The only
  affected callers are ones that hand-constructed `StepState` literals, which must
  now supply `output`.

- 5cbca32: Add `b.streamingTask` — a step primitive for LLM token streaming. Inside `run`,
  the handler calls `ctx.emit(chunk)` to push ephemeral chunks to live subscribers
  while still `return`ing a value captured as the durable `step.output` (identical
  to `b.task` for downstream `needs`). Consumers read via
  `wf.subscribe<C>(runId, stepId): AsyncIterable<StreamEvent<C>>`.

  Chunks are deliberately ephemeral: they never enter the fact log, the canonical
  flow hash, or replay — the final output is the only durable artifact. Delivery
  is fan-out (every subscriber sees every chunk), future-only by default
  (opt-in `{ replayBuffered: true }`), and non-blocking with bounded
  per-subscriber buffers (drop-oldest, surfaced as a `{ kind: "dropped" }`
  marker). The subscription element is a discriminated envelope
  `StreamEvent<C>` (`chunk` / `dropped` / `retry` / `error`) so control markers
  can never be confused with data. Stream termination is derived from the durable
  terminal fact (`step.completed`/`step.failed`/run-terminal), so a consumer's
  `for await` never hangs.

  Streaming is an optional `Store` capability (`subscribeStream?`/`publishChunk?`):
  the in-memory store implements it; a flow using `b.streamingTask` against a store
  without the capability throws at registration. (`@nagi-js/postgres` streaming is
  deferred to a follow-up RFC.)

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

- `Wf` is now generic over the registered flow tuple, so `flowId` carries its
  literal type through queries instead of widening to `string`.
  `nagi({ flows: [videoAnalysisFlow, dealAnalysisFlow] })` (no `as const` needed —
  the factory uses a `const` type parameter) yields a `Wf` whose
  `queryRuns()` returns `RunSummary<"videoAnalysis" | "dealAnalysis">`. Consumers
  with a closed flow set can delete hand-maintained id sets / `narrowFlowId`
  helpers at the `queryRuns` projection boundary — the union is now derived from
  the registration and cannot drift.

  `RunSummary`, `QueryRunsResult`, `QueryRunsWhere`, and `QueryRunsOpts` each gain
  a `FlowId extends string = string` type parameter, and a `FlowIdOf<TFlows>`
  helper is exported. `Wf.start` is narrowed to `start<F extends TFlows[number]>`,
  so starting a flow that was not registered with `nagi({ flows })` is now a
  compile error rather than a runtime throw. `where.flowId` accepts only the
  registered union (a typo is a compile error).

  Pure typing change — **no runtime delta**, no fact-shape change, no migration.
  Fully backwards-compatible: every new type parameter defaults to today's
  behavior, so bare `Wf` / `RunSummary` / `queryRuns` still resolve `flowId` to
  `string`. `startById(flowId: string)` is intentionally left untyped for
  outbox/DLQ-replay callers holding a serialized id. The `Store` contract is
  unchanged (adapters read `flow_id` as `string`); the literal narrowing is a
  single documented assertion at the `wf.queryRuns` read boundary. Tracks #18.

- Runtime bootstrap ergonomics (RFC 0013, #17) — three additive, backwards-compatible changes.

  - **`nagi.run({ ... }) → { wf, stop }`** — a turnkey worker lifecycle. Collapses
    the four-step `nagi()` + `new AbortController()` + `wf.worker({ signal })` +
    `worker.run().catch(graceful-vs-crash)` bootstrap (and its three module-level
    refs) into one call with a single idempotent `stop()` that aborts an internal
    controller and awaits the loop. Graceful shutdown resolves cleanly; only a true
    loop crash is logged once via the configured `logger`. Accepts an optional
    external `signal`, merged with the internal controller via `AbortSignal.any`.
    The existing `nagi()` + `wf.worker()` + `worker.run()` path is unchanged.

  - **Auto queue-schema bootstrap** — the `Queue` contract gains an optional
    `ensureSchema?(): Promise<void>`. `nagi()` awaits it once at construction
    (eager, fail-fast), closing the "runtime error on first enqueue" trap when the
    pgmq queue schema was never provisioned. Adapters without the hook (e.g. the
    in-memory queue) are unaffected.

  - **`pgmqQueue<DB>`** — `pgmqQueue` and `PgmqQueueOpts` are now generic over the
    Kysely schema (`db: Kysely<DB>`, defaulting to `unknown`), erasing the
    `db as unknown as Kysely<unknown>` cast at every callsite. Pure typing change;
    runtime is byte-identical.

- `flow()`'s `concurrency` config gains a bare-string shorthand and an optional
  `mode`. `concurrency: "videoId"` is now equivalent to
  `{ keyFn: (i) => i.videoId, mode: "cancel-in-progress" }`, and `mode` may be
  omitted on the object form (defaults to `"cancel-in-progress"`). The existing
  `{ keyFn, mode }` form is unchanged.

  The string shorthand is typed against the string-valued keys of the flow's
  input (`StringKeyOf<Input>`), so a misspelled or non-string key is a compile
  error; composite / computed keys continue to use `keyFn`. Internally the config
  normalizes to a canonical `{ keyFn, mode }` at the builder boundary, so the
  runtime cancellation path (`store.tryStartRun`) is untouched and dedupe / crash
  semantics are identical. See `docs/rfcs/0012-shorthand-concurrency-config.md`.

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
