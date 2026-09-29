# @nagi-js/core

## 0.1.1-rc.24

### Patch Changes

- f4fda8d: A step whose `optional()` upstream is skipped now runs as soon as the skip is
  recorded. Previously, when the same scheduling pass also dispatched other
  steps, it waited for an unrelated step to settle, and on `wf.startStaged` it
  could wait until then as well.
- a564da3: A canceled step now closes its stream. Before, a step aborted by `replay({ from })` whose abort wait timed out stayed `canceled` on a live run, and a live `wf.subscribe` iterator on it stayed open until the step was replayed or the run ended. It now ends as soon as the step is canceled, as it does on completion. A later `replay({ from })` streams the rerun to new subscribers.
- a564da3: A canceled step (a `replay({ from })` abort, run cancel, or concurrency
  supersession) now fires the global `onStepError` hook with an
  `AbortError`/cancellation error, so every consumer sees each step end exactly
  once. A body that returns normally after its run was canceled no longer fires
  `onStepComplete` — it is recorded as `step.canceled` and fires `onStepError`
  instead. The step-level `onError` handler is unaffected; it still only runs
  for real failures, not cancellations.
- a564da3: A canceled step on a live run no longer counts as success: `flowTermination`
  treats it as not-yet-done, so the run stays `running` and waits for
  `replay({ from })` instead of completing with the step unresolved. A
  redelivered message for a canceled step is no longer re-run by `admit` — it
  is acked and skipped. When nothing else is in flight and a canceled step
  blocks the run, `advance` now logs a `warn` (`run stalled on canceled
step(s)`) naming the stalled steps.
- 3740cc6: Hardens handler-step (`b.task`, `b.activity`, `b.streamingTask`) deadline
  enforcement:

  - A body that honors the abort by returning a value (instead of rethrowing)
    now still fails the step as `NagiStepTimeoutError`, retryable under
    `retry`. Previously the step was recorded as completed, and for a task its
    `ctx.tx` writes committed too.
  - A commit error (`store.runStep`) that happens after the handler already
    returned is no longer relabelled as `NagiStepTimeoutError`; only what the
    handler itself throws is unwrapped, so `retryOn` predicates and operators
    see the real error.
  - `timeoutMs` outside the range Node's `setTimeout` can represent (below 1,
    non-integer, `NaN`, or above 2,147,483,647ms) now throws
    `NagiValidationError` at `flow()` build time instead of firing the
    deadline on every attempt at runtime. `b.signal`'s `timeoutMs` similarly
    rejects non-finite/negative values (no upper cap — signal deadlines are
    timer rows, not `setTimeout`).

- a564da3: A message whose dispatch throws an unexpected error is now redelivered with backoff instead of at once. The delay starts at one `pollIntervalMs` and doubles per delivery up to 30 s, the same curve as a failing dequeue. Before, a dispatch that threw on every delivery redelivered in a tight loop: 100% CPU in memory, and a `set_vt(0)` loop on the database with pgmq. The `worker.dispatch threw uncaught` log entry now carries `runId`, `stepId`, `readCount` and `delayMs`.
- 158e8ac: Fix drift synthesis (`driftPolicy: "synthesize"` / `replay({ allowDrift })`) rebuilding a step's `needs` in the wrong shape — bare step objects keyed by upstream id, instead of `{ step, optional }` records keyed by the handler's local alias. Every consumer read `ref.step.id`, which was `undefined` on the bare shape, so the first `advance` of any drifted run whose flow had at least one edge threw a raw `TypeError` outside `NagiRuntimeError`, skipping the snapshot-gone fallback and nack-looping forever. `needs` is now rebuilt from the live handler's `needs` map, validated against the pinned snapshot's edges and step kind; a live flow whose edges or step kinds changed for a pinned step now falls back to snapshot-gone handling instead of crash-looping.
- eded7cd: A `b.signal` step now releases its store lease when it parks. Previously the
  lease expired while the step waited, and the lease reaper re-dispatched the
  step every lease period (60 s by default) for the whole wait. Each re-dispatch
  wrote a `lease.reaped` and a `step.started` fact and fired `onStepStart`.
  `decideSignal` returns a new `park` decision; custom stores must apply its
  `release`.
- a564da3: `postgresStore()` now returns a `close()` that stops both LISTEN channels
  (idempotent, tolerant of a LISTEN that never succeeded).

  `StreamListener.listen` gains an optional third parameter, `onReconnect`: a
  listener that re-`LISTEN`s after losing its connection should call it once
  re-`LISTEN` succeeds. The store then re-checks every stream still open
  against durable state and ends the ones whose step has settled — healing an
  iterator that would otherwise hang forever because its close frame fell in
  the reconnect gap.

  `@nagi-js/core`'s `InMemoryStreamHub` gains `openStreams()`, listing open
  channels by `(runId, stepId)`. `closeRun` now matches channels by their
  stored run id instead of a `` `${runId}::` `` key prefix, fixing a latent bug
  where `closeRun("a")` also closed channels belonging to a run literally named
  `"a::b"`.

- 8207d4c: Fix: run events and the stream `error` event no longer carry payloads.

  With `postgresStore({ listener })` configured (required for `b.streamingTask`
  and `wf.watchRun` / `wf.watchRuns`), every fact write called `pg_notify`
  **inside the fact's transaction** with a JSON payload embedding the step's or
  flow's full `output`, or the full `error` (stack and cause). PostgreSQL
  rejects a NOTIFY payload of 8000 bytes or more ("payload string too long"),
  which aborted the transaction. A step returning more than ~8 KB — ordinary
  for LLM text — could therefore never be recorded as completed; it retried
  until it failed. A flow output over ~8 KB could never commit
  `flow.completed`, and a terminal `step.failed` with a large serialized error
  had the same problem via the stream `err` frame. This happened even when
  nobody was watching: the NOTIFY fires whenever a listener is configured.

  **Breaking change**: `RunEvent`'s `flow.completed`, `flow.failed`,
  `step.completed` and `step.failed` members no longer carry `output` /
  `error`; the stream `StreamEvent` `error` member is now `{ kind: "error" }`
  with no `error` field. Run events and the stream error event are now
  reference-only on every store — identity and status (run, step, attempt,
  type), never payloads.

  **Migration**: a consumer that needs an output or error calls
  `wf.describe(runId)`, which still returns the full payload via
  `RunView.output` / `RunView.error` / `StepView.output` / `StepView.error`.

- a564da3: After a `listener` reconnect, the Postgres store ends each stream whose close frame was lost the way the frame would have: with a final `{ kind: "error" }` event when the step failed, not a plain end. Late subscribers are unchanged and still get an empty stream.

  For adapter authors: `isStreamOver(state, stepId)` is replaced by `streamEndOf(state, stepId)`, which returns `"open" | "ok" | "error"`. `isStreamOver(...)` is `streamEndOf(...) !== "open"`.

- a564da3: Fix `replay({ from })` on a live run: it previously only aborted `from`
  itself if it was running, leaving other running descendants in the reset set
  free to keep executing and settle with stale inputs once they returned, and
  leaving a parked subflow step's old child free to keep running and — via its
  terminal fact — settle the _new_ generation's parent step with its stale
  output.

  `replay({ from })` now aborts every member of the reset set that is `running`
  (under one shared 30s deadline for the whole batch, not one per step) and
  cancels the child of every reset step that was `awaitingChild`, after writing
  the resets and before re-dispatching. `propagateToParent` also now checks
  that the waking child is the parent step's _current_ generation before
  settling it, guarding against any stale child wake, not only ones `replay`
  leaves behind.

- a564da3: A retried attempt now releases its lease, so a retry backoff longer than the
  lease period is honoured and no spurious `lease.reaped` is written.

  A message for an attempt the step has moved past is now dropped at admission
  instead of re-running the handler next to the current attempt.

- a564da3: `wf.watchRun` / `wf.watchRuns` now emit `step.canceled` (an aborted or
  run-canceled step) and `step.reset` (a step reset by `replay({ from })`; on
  a finished run, the reopen). Exhaustive `switch`
  statements over `RunEvent` need the two new cases.
- a564da3: **Breaking:** `worker.runUntilEmpty({ deadline })` is now `worker.runUntilEmpty({ timeoutMs })`. `timeoutMs` is a duration, measured on the injected clock from the call. `deadline` was an absolute epoch-ms timestamp, but nothing said so: `{ deadline: 10_000 }` meant 1970 and returned `{ processed: 0 }` at once. Replace `{ deadline: Date.now() + n }` with `{ timeoutMs: n }`. Passing `deadline` is now a type error; untyped callers that still pass it get an unbounded drain.
- 623c89e: `wf.replay(runId, { mode: "continue", from, scope: "step" })` now reruns any descendant that holds no value — `failed`, `canceled`, or `skipped` — alongside the named step, instead of leaving every descendant untouched. A `completed` descendant still keeps its output from the previous value. On a live run, the descendants to reset are chosen after a `running` `from` step has settled its abort.

  Previously, replaying a failed step with `scope: "step"` could let the run settle "completed" while its transitively-skipped descendants never ran, silently omitting them from the flow output.

- a564da3: Fix a pool deadlock: a task/streaming step body runs inside `store.runStep`'s
  transaction, which holds one pool connection. After the body returns, core's
  `settle` used to call `store.loadRunState(runId)` on the **pool**, so each
  in-flight step needed a **second** connection while holding the first. With
  worker concurrency ≥ pool max (or several workers sharing one pool), every
  connection could end up held by a step's transaction whose `settle` was
  waiting on another connection that would never free — a permanent deadlock,
  since `pg.Pool`'s default `connectionTimeoutMillis` is 0 (wait forever).

  `settle` now reads run state on the step's own transaction instead, which
  sees the same committed facts (READ COMMITTED) without needing a second
  connection.

  `Store.loadRunState` gains an optional second parameter:
  `loadRunState(runId: RunId, tx?: Tx): Promise<RunState>`. Custom stores
  should honour `tx` when supplied — read on that transaction rather than the
  pool — to get the same fix.

- a564da3: A start's first steps are now enqueued inside the start's own transaction,
  alongside the run row and `flow.started` fact, instead of after it commits.
  Previously a crash, deploy or queue error between that commit and the
  post-commit enqueue left a `running` run with nothing queued, no lease and no
  timer — never picked up by any worker.

  `Store.tryStartRun` takes an optional `seed` (`{ queue, flowId, steps }`) as
  its last parameter; a custom `Store` implementation should enqueue `seed`'s
  steps in the same transaction as the start, only when `started` is true.

  Because the seed is visible to workers as soon as the start commits, a worker
  can dequeue and start a root step before the starting process fires
  `onFlowStart`. A custom `onStepStart` hook can therefore fire before the
  run's `onFlowStart`; hooks that correlate the two must not assume the flow
  event comes first.

  A subflow re-attach (a redelivered `sub` step meeting an already-started
  child) now re-seeds the child via `dispatcher.advance`, so a child whose first
  message was lost before this fix is recovered instead of staying stranded.

- a564da3: A streaming step's channel now reopens for its rerun. Previously, once a
  streaming step's channel closed (`step.completed`/`step.failed`), the hub
  remembered the key as closed forever: a later reset by `replay({ from })`
  (either scope) dropped the rerun's chunks and
  handed a new `subscribeStream` an already-closed empty stream, even though
  the step ran and completed again.

  `step.reset` now declares a `reopen` stream effect, carried over Postgres
  NOTIFY like the other stream effects, that clears the closed-key marker (and
  any stale replay buffer) so the rerun streams normally to whoever is
  currently subscribed.

  Also: a lease reap (`lease.reaped`, written when the reaper re-enqueues a
  step whose worker died mid-stream) now declares the same `retry` stream
  effect a normal `step.retried` does, so subscribers see a `retry` marker
  between the superseded attempt's partial chunks and the next attempt's
  output, instead of the two attempts running together with no marker.

## 0.1.1-rc.23

### Patch Changes

- 30103e5: `FlowStartEvent.parent` is now a `ParentLink` (`{ runId, stepId }`), and the `ParentRef` type is removed. Its extra `attempt` field existed only for the removed `@nagi-js/otel` span keys and was never stored with the child run.
- 9133e4e: `replay({ from })` now aborts every attempt that starts while it waits, so a lease-reaped step restarted mid-replay no longer outlives the reset. A step's completion, failure or cancel now applies only to an attempt that has started since its last reset, in the fold and both read models.

## 0.1.1-rc.22

### Patch Changes

- 674bea6: Fix: a long-lived process no longer accumulates stream-hub channels, and a settled step no longer leaves its cancel-watcher timer behind.

  `InMemoryStreamHub` marked a channel closed but never deleted it, and `step.retried` created a channel for every retried step, streaming or not. Since `postgresStore` feeds a hub in every listening process and broadcasts retry and close frames cluster-wide, a long-running Postgres worker grew that map by one entry per retried step and per streaming step, forever. A retry now touches only a channel that already exists, and closing a channel deletes it. The keys of recently closed channels are remembered (at most 1024) so a chunk arriving after its step's close is still dropped rather than reopening a channel nothing would close. Subscribe-after-close and replay behave as before.

  The cancel watcher's `stop()` only set a flag, so every finished step left a pending timer for up to one poll interval (250 ms by default) that kept the event loop alive. `stop()` now clears it and ends the poll loop immediately.

- d424f0a: `wf.queryRuns` and `Store.queryRuns` are removed, along with `RunSummary`, `QueryRunsOpts` / `QueryRunsResult` / `QueryRunsWhere` and the cursor helpers (`encodeRunCursor`, `decodeRunCursor`, `clampQueryLimit`, `compareRunOrder`, `isPastCursor`, `jsonContains`, `QUERY_RUNS_DEFAULT_LIMIT`, `QUERY_RUNS_MAX_LIMIT`, `RunCursor`). Use `describe(runId)` for one run, and list or join runs with SQL against `workflow_run` and `step_run`, whose columns are now a documented read contract (docs/OPERATIONS.md, "Reading runs with SQL").
- 2fc52df: Fix: `diffSnapshots` now reports every hashed field. Changes to a signal's `names`, a subflow's child flow, or a subflow's input builder flipped the snapshot hash but produced an empty diff; they now surface as `signalNames`, `childFlowId`, and `subflowInput` in `changedPredicates`. Flow-level changes to the input schema or flow id now surface in the new `SnapshotDiff.changedFlowFields` (`"inputSchema"`, `"flowId"`).
- 2fc52df: What a fact implies (read-model rows, lease/timer release, stream close, run event) is now one core table, `factConsequences`, and each store applies all of it through a single write path; `supersede` decides concurrency cancellation and the lease reaper's decision carries its `lease.reaped` fact. Fixes `watchRuns` on Postgres never seeing `flow.started` for runs started with `startStaged`. `Store.settleStep` (use `appendFact`), the unused `once.recorded` fact kind, and the `factEffects` / `rowDeltaOf` / `runEventOf` exports are removed.
- ecb5ac6: Fix: `ctx.once()` now memoizes a callback that returns `null`, so its side effect no longer re-runs on every redelivery or replay of the step.

  `Store.getOnce` returned `Json | null` and used `null` to mean "nothing recorded". `Json` includes `null`, so a recorded `null` was indistinguishable from a miss and `once()` called the callback again — exactly the duplicate side effect it exists to prevent.

  `Store.getOnce` now returns `GetOnceResult` (`{ tag: "hit", value }` or `{ tag: "miss" }`), and a recorded `null` reads back as a hit. The Postgres `dedupe` table is unchanged; no migration is needed.

  Custom `Store` implementations must update `getOnce` to return the new shape. The shared conformance suite in `@nagi-js/core/testing` checks it.

- 2fc52df: Every path that needs a run's flow (dispatch, `wf.signal`, the operator, cancel, concurrency supersede and `replay`) now resolves it through one drift-aware resolver, checking step names against the run's pinned flow rather than the live one. User-visible changes:

  - A signal to a step that exists only in the live flow, not the run's pinned one, is rejected.
  - Operator `skip`/`retry` under the `"freeze"` drift policy throws before writing anything.
  - Cancelling an unpinned run whose flow is not registered throws.
  - `replay({ allowDrift })` no longer resolves a parent it wakes to the child's synthesized flow.
  - `replay({ fireHooks: false })` dispatches only the messages its own replay enqueued, never another run's, and the children it spawns stay quiet too. It runs on a private in-memory queue, so a crash mid-replay needs `replay({ mode: "continue" })` to recover.

- 2fc52df: Task, activity and streaming steps now share one internal def and one execute path, and step-kind branches are exhaustive switches; authoring APIs, `Step.kind` values, persisted `stepKind` and flow hashes are unchanged. `ActivityConfig`, `ActivityCtx` and `StepLifecycleHooks` are now exported, `MatchArmShape` is removed in favour of `MatchArm` (identical shape), and `RunState` gains `resetCounts`, the per-step count of `step.reset` facts.
- 2fc52df: **`@nagi-js/postgres` ships migration `0009_step_run_per_step`: run migrations before deploying.** It deletes duplicate per-attempt `step_run` rows, keeping one row per step, and changes the table's primary key to `(run_id, step_id)`.

  `describe`, `queryRuns` and `pruneFacts` now answer from one read model in both stores: the in-memory store materializes the same fact row deltas Postgres does. `describe` returns one view per step (its latest attempt, updated in place on retry), and step timestamps come from the facts' `at` rather than the database clock.

- 2fc52df: Every way a run ends now fires the flow error hooks and wakes a parked parent subflow step the same way:

  - Cancel paths now fire `flow.onError` and `onFlowError`, and a handler that throws `NagiCanceledError` wakes its parent right away instead of waiting for the lease reaper.
  - A run failed by snapshot-gone handling now fires `onFlowError`.
  - A canceled child's parent sees the same canonical error whichever path canceled it; operator aborts now read `was canceled by <actor>: <reason>`.

- a87b71c: `wf.operator()` is removed. Rerun a step with `wf.replay(runId, { mode: "continue", from, scope })`, which now also works on a live run and aborts an in-flight `from` step before resetting it; stop a run with `wf.cancel`. `skip` is gone, along with the `"manual"` skip reason, the `"operator"` cancel cause and the `actor`/`note` audit fields on step facts.
- 9b8b6fa: `b.match` is removed, along with its types (`MatchArm`, `MatchArmGuard`, `MatchArmOtherwise`, `MatchArmOutput`, `MatchGuardConfig`, `MatchArmSelectedFact`, `CanonicalMatchArm`), the `"match"` step kind and `RunState.selectedArms`. Branch with a step-level `when:` guard instead; a step whose guard is false is skipped as before. Flow hashes of flows that never used `match` are unchanged, so in-flight runs keep their pinned snapshots.
- 2fc52df: A run now ends exactly once. Run-end facts (`flow.completed`, `flow.failed`, `flow.canceled`) go through the new `Store.endRun(runId, fact): Promise<boolean>`, which refuses the fact when the run's row is already terminal and writes nothing. The check runs under the store's lock: Postgres locks the `workflow_run` row, and the core policy lives in `admitsRunEnd`. Only the winning writer fires the flow hooks and wakes the parent, so racing cancels, a cancel against completion, or two snapshot-gone messages for one run no longer fire `onFlowError` twice. `appendFact` no longer accepts run-end facts.

  Postgres: `tryStartRunOnTx` now runs each concurrency start attempt under a savepoint. Its retry after a unique violation used to fail with "current transaction is aborted", and it now completes, leaving the caller's transaction usable. `tryStartRun` uses the same retry, so it no longer surfaces a raw unique violation when it races a start on a caller's transaction.

  In-memory store: `sweepLeases` now reads step status and flowId from the read-model rows, matching Postgres, instead of from the fact fold.

- b9a0c46: Fix: a step's projected state (`loadRunState`) and its `describe()` view now apply the same attempt rules, so they can no longer disagree.

  After a lease reap, the re-dispatched attempt's `step.started` was ignored by the projection: the step kept reading as the dead attempt, so `operator.retry` wrote its abort request for that attempt and the live handler never saw it: the retry waited for the handler to finish on its own, or timed out. A start now supersedes the step whenever its attempt is newer than the one in flight, in both the projection and the read model.

  A duplicate `step.started` no longer moves the view's `startedAt`, and a `step.retried` or `step.abort-requested` for an attempt that is not in flight no longer changes the step in either. A test now explores every single-step fact history against both and fails on any disagreement.

- 4cc20fd: A step's heartbeat and cancel watcher now finish any in-flight tick before the step is acked, so no lease extension or run-state read outlives the step. Worker shutdown now waits on its in-flight dispatches instead of polling `clock.sleep`.
- 58458fa: `diffSnapshots` (with `SnapshotDiff`, `SnapshotChangedEdge`, `SnapshotChangedField`, `SnapshotChangedFlowField`) and the canonicalization exports (`canonicalize`, `fingerprintFlows`, `sha256Canonical`, `CanonicalDag`, `CanonicalMatchArm`, `CanonicalRetryPolicy`, `CanonicalSchema`, `CanonicalStep`) are removed from the public API. Flow hashing is unchanged, so existing flow hashes and in-flight runs are unaffected.
- 2fc52df: The worker now owns lease reaping alongside the signal-timeout sweep, on its own clock, store and queue, so workers built with `wf.worker()` outside `nagi.run` reap a crashed peer's expired leases too. Configure it with the new `WorkerConfig.reaperIntervalMs` (default 30s, `0` disables); `NagiConfig.reaperIntervalMs` is deprecated and still honored as its fallback.

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
