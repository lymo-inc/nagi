# RFC 0012 — Runtime bootstrap ergonomics (research notes)

- **Tracking issue:** lymo-inc/nagi#17
- **Date:** 2026-05-21 (JST)
- **Scope:** `@nagi-js/core` (turnkey lifecycle + `ensureSchema` hook on the
  `Queue` contract) and `@nagi-js/pgmq` (generic `pgmqQueue<DB>`). No
  `@nagi-js/postgres` / `@nagi-js/otel` change.

These are the working notes that back the RFC. They cover (1) what the current
codebase already does for each of the three proposed changes — several premises
in the issue are partly stale — and (2) a prior-art survey of how comparable
durable-job / workflow systems handle the same three concerns. Throughout, the
survey separates **"what they do"** from **"what nagi should consider"** so the
RFC can lift conclusions without re-litigating the evidence.

The three changes (all backwards-compatible, per issue #17):

1. **`nagi.run({...})` turnkey worker lifecycle** — collapse the 4-step
   `nagi()` → `wf.worker()` → `worker.run()` → manual-abort dance into one call
   returning `{ wf, stop }`.
2. **Auto queue-schema bootstrap** — add optional `ensureSchema?()` to the queue
   contract; `nagi()` calls it once at construction.
3. **Generic `pgmqQueue<DB>`** — drop the `db as unknown as Kysely<unknown>`
   cast consumers must write today.

---

## Part A — Current state of the codebase (what we have to work with)

### Change 1 — the lifecycle dance is real, and `worker.run()` already drains

The 4-step pattern in the issue is accurate. `nagi()` returns a `Wf`
(`packages/core/src/runtime.ts:180`, `:481`); `wf.worker(config?)` builds a
`WorkerImpl` (`packages/core/src/runtime.ts:632–639`); and the worker loop lives
in `WorkerImpl.run()` (`packages/core/src/worker.ts:39–56`).

The important detail the issue *understates*: graceful drain **already exists**
inside `worker.run()`. The loop checks `this.aborted()` each tick
(`worker.ts:40`, `:122–124` — `this.signal?.aborted === true`), and on exit
calls `await this.drain()` (`worker.ts:55`, `:116–120`) which spins until
`inFlight === 0`. So "stop pulling new work, then await in-flight" is a solved
problem at the worker layer. `WorkerConfig.signal` is the only stop mechanism
(`packages/core/src/types.ts:357–361`).

What `nagi.run` adds is therefore **not** drain logic — it is the *plumbing*
around it that every consumer currently hand-writes:

- owning an `AbortController` and threading `signal` into `wf.worker({ signal })`;
- holding the `worker.run()` promise so it can be awaited at shutdown;
- the crash-vs-graceful discrimination in the `.catch()` (the issue's
  `if (abort.signal.aborted) return;` line);
- exposing a single idempotent `stop()`.

`worker.run()` resolves `void` on clean exit and only rejects if the loop
itself throws (per-message failures are already swallowed and `nack`ed in
`dispatchSafely`, `worker.ts:103–114`). So the crash signal `nagi.run` must
surface is "the `run()` promise rejected **and** we did not ask it to stop."

**Naming-collision facts (issue open question):** `wf.start(flow, input)`
exists (`runtime.ts:482`) and `worker.run()` exists (`worker.ts:39`). So both
`start` and `run` are already verbs in the API — but at *different* nouns
(`wf.start` starts a *flow run*; `worker.run` runs the *worker loop*).
`nagi.run` would be a verb on the *namespace/factory* (`nagi`), a third noun.
`nagi.spawn` / `runRuntime` have no existing collision.

### Change 2 — `ensureSchema` exists on pgmq, but not on the contract, and `nagi()` never calls it

This premise is **half-done already**:

- `PgmqQueue` (the concrete return type) **already declares and implements**
  `ensureSchema(): Promise<void>` (`packages/pgmq/src/pgmq-queue.ts:25–28`,
  `:58–65`). It runs `CREATE EXTENSION IF NOT EXISTS pgmq` then
  `SELECT pgmq.create(...)` (or `pgmq.create_partitioned(...)`).
- But the **core `Queue` contract has no such method**
  (`packages/core/src/types.ts:550–556` — only `enqueue` / `dequeue` / `ack` /
  `nack` / `extend`). So `nagi()` cannot call it: it holds a `Queue`, not a
  `PgmqQueue`.
- `nagi()` does **not** call `ensureSchema` today (grep: no reference in
  `packages/core/src/*.ts`). Consumers must call `queue.ensureSchema()`
  themselves before first enqueue, exactly as the issue states.

**Correction to the issue text.** The issue says "`nagi()` calls it once at
construction *after migrate*." But `migrate()` lives in `@nagi-js/postgres`
(`packages/postgres/src/migrations.ts:182`) and is **not** invoked by `nagi()` —
core has no dependency on postgres, and `migrate()` is a separate function the
operator runs as a deploy step. There is no "after migrate" hook point inside
`nagi()`. The real design is: `nagi()` optionally calls `queue.ensureSchema?.()`
at construction, *independent of* `migrate()`. (Whether that races a
concurrently-running `migrate()` is a non-issue — `pgmq.create()` and `migrate()`
touch different schemas and both are `IF NOT EXISTS`.)

`pgmq.create()` is idempotent (see Part B §3), so a one-shot call at boot is
safe to repeat across process restarts and across N worker replicas.

### Change 3 — the cast is real, and `migrate<DB>` is in-repo precedent

`PgmqQueueOpts.db` is typed `Kysely<unknown>`
(`packages/pgmq/src/pgmq-queue.ts:17–18`), and the type test pins exactly that
(`packages/pgmq/src/types.test-d.ts:6`, `:26`). A consumer holding a
`Kysely<MyDB>` must write `db as unknown as Kysely<unknown>` because
`Kysely<MyDB>` is **not** assignable to `Kysely<unknown>` (the generic is
invariant in practice — see Part B §4).

Crucially, the fix already has a working precedent **in this monorepo**:
`migrate<DB>(db: Kysely<DB>, opts?)` (`packages/postgres/src/migrations.ts:182`)
is generic and accepts any caller schema with zero casts. `pgmqQueue` should
mirror it. Internally pgmq erases the type immediately
(`buildQueue(db, config)` takes `Kysely<unknown>`; `withTx` already does
`tx as unknown as Kysely<unknown>` at `pgmq-queue.ts:68`), so making the
*public* signature generic is a pure typing change with no runtime effect — the
internal erasure cast stays exactly where it is.

---

## Part B — Prior-art survey

Ordered by the four research areas. TS/Node ecosystems first; Go/Ruby where the
design is instructive.

### 1. Turnkey worker lifecycle & graceful shutdown

#### Temporal TypeScript SDK

*What they do.* `Worker.create(options): Promise<Worker>` then either
`worker.run(): Promise<void>` (runs until shutdown) or — the recommended shape —
`worker.runUntil(fnOrPromise, options?): Promise<R>`, which "runs the Worker
until `fnOrPromise` completes, then shuts down and waits for `run` to complete."
`run()` **resolves** when the worker reaches the `STOPPED` state and **throws on
fatal errors or failure to shut down gracefully** — i.e. the promise itself is
the crash-vs-graceful signal. Shutdown is a state machine
(`STOPPING → DRAINING → DRAINED → STOPPED`); `Worker.shutdown()` stops polling
and cancels in-flight Activities with a `CancelledFailure`. Signal handlers
(`shutdownSignals`: SIGINT/SIGTERM/SIGQUIT/SIGUSR2) are **installed by default**.
Grace is bounded by `shutdownGraceTime`, with an optional hard `shutdownForceTime`
that throws `GracefulShutdownPeriodExpiredError`. `shutdown()` is not documented
as explicitly idempotent but is "safe to call multiple times given the state
transitions."
(typescript.temporal.io/api/classes/worker.Worker; SDK ≥1.5 / ≥1.11.3 notes;
docs.temporal.io/encyclopedia/workers/worker-shutdown)

*What nagi should consider.* `runUntil(promise)` is the single closest analog to
the proposed `nagi.run` + `stop()` pair: it ties the worker's life to an
external completion signal and folds shutdown+await into one awaitable. nagi's
`stop()` is the inverse ergonomics (imperative call vs. a completion promise) but
the *contract* is identical: resolve on graceful, reject/throw only on a true
crash. Temporal installing signal handlers **by default** is the opposite of
nagi's proposal (opt-in `signal`); see §1-synthesis.

#### Graphile Worker (closest structural match)

*What they do.* `run(options): Promise<Runner>`. The resolved **`Runner`** object
exposes `stop(): Promise<void>` ("stops the runner from accepting new jobs, and
returns a promise that resolves when all the in-progress tasks are complete") and
a **`promise`** property that "resolves/rejects when the worker exits." Signal
handlers are installed by default; `noHandleSignals: true` opts out and hands
graceful shutdown back to the caller. `gracefulShutdownAbortTimeout` bounds how
long after shutdown the internal `AbortController` waits before cancelling
supported async work. `runOnce(options): Promise<void>` drains once and resolves
(≈ nagi's `worker.runOnce` / `runUntilEmpty`). The docs do **not** state whether
`stop()` is idempotent. Crash-vs-graceful is read off `runner.promise`
(resolve = clean, reject = fatal). The docs explicitly warn to await the promise
"to avoid unhandled-rejection errors causing a process crash."
(worker.graphile.org/docs/library/run)

*What nagi should consider.* This is essentially the exact shape the issue
proposes: `run(config) → handle` where the handle carries `stop()` **and** a
lifecycle promise. nagi's `{ wf, stop }` is missing the third member Graphile
exposes — a **`promise`/`done`** the caller can await for "the worker exited on
its own (crash or otherwise)." Worth weighing: `{ wf, stop, done }` lets advanced
callers observe an unexpected exit without subscribing to the logger. Also note
Graphile's `noHandleSignals` flag is the design knob for "own your own signal
handling," which maps to nagi's open question about owning vs. accepting a signal.

#### BullMQ

*What they do.* `new Worker(name, processor, opts)` starts processing
immediately on construction; `await worker.close()` "marks the worker as closing
so it will not pick up new jobs… and waits for all current jobs to be processed
(or failed)." `close()` "will not timeout by itself" — the caller is expected to
bound it externally; jobs left running on a hard kill are recovered by the
**stalled-job** mechanism (no separate `QueueScheduler` needed since BullMQ 2.0).
In-flight cancellation (distinct from drain) is via the standard `AbortSignal`
passed as the processor's 3rd arg, triggered by `worker.cancelAllJobs(reason)`.
Multiple workers are closed with `Promise.all([...close()])`.
(docs.bullmq.io/guide/workers/graceful-shutdown; .../workers/cancelling-jobs)

*What nagi should consider.* Confirms the canonical drain contract ("stop
acquiring + await in-flight") that nagi's `worker.drain()` already implements.
BullMQ deliberately does **not** put a timeout inside `close()`; nagi's `stop()`
likewise need not own a timeout in v1 (the worker drains on its own; the operator
can race `stop()` against their platform's grace period). BullMQ separates
**drain** (`close`) from **cancel** (`AbortSignal` to handlers) — nagi already
has the same split (worker `signal` drains the loop; `step.abort-requested` /
`ctx.signal` cancels a running handler, `runtime.ts:962–974`). No new surface
needed there.

#### pg-boss (closest backing-store match — Postgres)

*What they do.* `boss.start(): Promise<PgBoss>` and
`boss.stop(options?): Promise<void>`. As of v10, `stop()` **waits for the
graceful timeout (default 30s) before resolving** unless `wait: false`.
`StopOptions`: `graceful` (wait for in-flight handlers), `timeout` (max wait,
commonly set just under the platform grace period, e.g. 25s), `wait` (whether
the promise waits for the drain), `destroy` (tear down the pg pool). Real-world
issues (#421, #303) report jobs that exceed `timeout` getting stuck in `active`
until they expire — a caution about unbounded handlers.
(github.com/timgit/pg-boss — releases/10.0.0, issues #421/#303/#164; pg-boss v12.x current)

*What nagi should consider.* pg-boss is the nearest neighbour
(Postgres-backed, Node, long-running). Two takeaways: (a) its v10 default of
`stop()` **awaiting the drain** validates nagi's `stop()` returning a promise
that resolves only after in-flight work settles. (b) pg-boss's `timeout`
escalation is something nagi can defer — nagi's `worker.drain()` is unbounded
today, matching BullMQ's "no internal timeout"; an optional `stop({ timeout })`
is a clean future addition but is **out of scope** for issue #17's three changes.

#### River (Go) — escalation model

*What they do.* `Client.Start(ctx)`; two stops: `Stop(ctx)` (soft — stop
fetching, wait for in-flight to finish) and `StopAndCancel(ctx)` (hard — also
cancels the work context of running jobs). Both block until done **or until the
passed `ctx` is cancelled/timed out** (so the *caller's context* is the timeout
knob). The documented production pattern is a 3-stage escalation: 1st
SIGINT/SIGTERM → soft stop; 2nd or 10s → hard stop; 3rd or 10s → exit uncleanly.
Workers must respect context cancellation or stop "may hang forever"; stuck jobs
are rescued after `RescueStuckJobsAfter` (~1h).
(riverqueue.com/docs/graceful-shutdown; pkg.go.dev/github.com/riverqueue/river)

*What nagi should consider.* River formalizes the **soft vs. hard** distinction
nagi already has structurally (drain via worker `signal`; per-handler cancel via
`ctx.signal`). River threading the timeout through the **caller-supplied context**
rather than an options bag is a strong argument for nagi's open question:
accepting an external `AbortSignal` lets the *caller* own the deadline (abort the
signal on their own timer) instead of nagi growing a `timeout` option. But note
River's hang warning applies to nagi too: `stop()` can only resolve as fast as
the slowest in-flight handler that honours `ctx.signal`.

#### Sidekiq (Ruby) — quiet → stop

*What they do.* Two-phase: **quiet** (TSTP) = stop accepting new jobs, finish
current; **stop** (TERM) = quiet + raise `Sidekiq::Shutdown` on worker threads
after a deadline (`-t`, default 25s, chosen to sit under the typical 30s
orchestrator grace period). K8s pattern uses a `preStop` hook to quiet first.
(mikeperham.com/how-sidekiq-works; bigbinary/cloud66 K8s guides)

*What nagi should consider.* The **quiet-then-stop** two-phase idea is more than
nagi needs for v1 (nagi's single `stop()` already does quiet+drain atomically).
The reusable lesson is the **25s-under-30s** convention: any timeout nagi
eventually adds should default below the common 30s platform grace. Not a v1
requirement.

#### Inngest / Trigger.dev / Hatchet / Quirrel (scanned, lower relevance)

*What they do.* These are mostly **service-oriented** (HTTP webhooks or a
persistent gateway), so "lifecycle" means connection/health rather than an
in-process worker handle. Inngest "Connect" exposes a worker lifecycle
(`CONNECTING → ACTIVE`) and leans on K8s readiness probes for graceful rollout.
Hatchet's TS SDK uses `worker.start()` with a fixed-slot concurrency model.
Quirrel is in maintenance mode (last publish ~2y). Trigger.dev abstracts workers
away behind a checkpoint/resume supervisor.
(inngest.com/docs/setup/connect; docs.hatchet.run/v1/workers; quirrel.dev)

*What nagi should consider.* Limited direct applicability — none expose a
"`run() → { stop }` in-process handle" cleaner than Temporal/Graphile/pg-boss
already do. The one transferable note: Inngest/Sidekiq both lean on
**readiness/preStop** at the orchestrator layer, reinforcing that nagi's job is
only to drain correctly and resolve `stop()` honestly; it should **not** try to
own SIGTERM by default (the platform already coordinates that).

#### §1 synthesis — signal handlers: own, accept, or neither?

Temporal and Graphile **install SIGTERM/SIGINT handlers by default** (with
`noHandleSignals` to opt out). River/BullMQ/pg-boss/Sidekiq leave signal
trapping to the **caller**. nagi today is in the second camp (the consumer wires
`abort.abort()` on SIGTERM). The issue's `nagi.run({ signal? })` keeps nagi in
the **caller-owns-the-signal** camp — which is the safer default for a *library*
(installing global `process.on('SIGTERM')` from a library is surprising and
fights other handlers). Recommendation surfaced for the RFC: do **not**
auto-install signal handlers; accept an optional external `signal` and document
the SIGTERM wiring as a one-liner.

### 2. `AbortSignal.any([...])` composition

*What they do / the facts.*

- **Availability.** `AbortSignal.any()` landed in Node **20.3.0** (Current) and
  **18.17.0** (LTS) via PR #47821. nagi requires Node **>=22** across every
  package (`package.json:8–9`, all `packages/*/package.json:17–18`,
  `mise.toml:2`), so the API is unconditionally available. (nodejs.org release
  notes v20.3.0 / v18.17.0)
- **The leak.** `AbortSignal.any()` **eagerly adds a listener to each source
  signal** and does **not** remove it when the composite is GC'd or when
  `removeEventListener` is called on the composite. The leak bites specifically
  when a **source signal outlives the composite** — e.g. a long-lived/global
  signal that is only aborted on SIGTERM, with many short-lived composites built
  from it. Adding an `abort` listener (with a closure capturing the source) to
  the composite pins everything. (nodejs/node issues #54614, #55328, #57584;
  denoland/deno #24842)
- **The timeout GC bug.** `AbortSignal.any([AbortSignal.timeout(ms)])` could
  *flakily fail to fire* because the timeout signal inside the composite was
  garbage-collected. Fixed in PR #57867, released in Node **24.0.0** and
  backported to **22.16.0** (both 2025-05-21). Below those patch versions the
  bug is live. (nodejs/node #57736, PR #57867; nodejs.org v24.0.0 / v22.16.0)
- **Mitigations.** `{ once: true }` on manual listeners; pass a teardown
  `AbortSignal` into `addEventListener`; clean up in `finally`; avoid retaining
  source-signal references inside abort callbacks.

*What nagi should consider.* This is the crux of the issue's first open question
("merge external signal via `AbortSignal.any` vs. always own its own"). The leak
profile is *exactly* nagi's scenario if implemented naively: the external signal
a consumer passes is typically the **long-lived** one (their app-wide SIGTERM
controller), and nagi's internal controller is the short-lived composite source.
But note the leak is about *many composites over one long-lived signal*; `nagi.run`
creates **one** composite per call, and a runtime usually lives for the whole
process. So a single `AbortSignal.any([external, internal.signal])` per
`nagi.run` is **one** listener on the external signal for the runtime's lifetime —
not the unbounded-loop pathology in the bug reports. The real risk is only if
`stop()` is meant to fully detach (so a caller could `nagi.run` repeatedly
against the same external signal in a loop). Two viable designs:

1. **Compose with `AbortSignal.any`** (one composite, lifetime = runtime). Low
   risk given Node >=22 (and especially >=22.16). Cleanest semantics: aborting
   *either* the external signal *or* `stop()`'s internal controller drains the
   worker. Must register the worker's own abort-listener with `{ once: true }`
   and drop references in `stop()` to be tidy.
2. **Own the controller; subscribe to the external signal with `{ once: true }`**
   and call `internal.abort()` from that one listener (and remove it in `stop()`).
   Avoids `AbortSignal.any` entirely, sidesteps every bug above, and is trivial
   to reason about. Slightly more code; identical observable behavior.

Either is fine on nagi's Node floor. Design 2 is the more conservative default
and fully dodges the (now-fixed-but-historically-flaky) `any()` edge cases.

### 3. Auto-bootstrapping queue / schema / tables

*What they do.*

- **pg-boss.** `start()` **auto-creates** the `pgboss` schema, the partitioned
  `job` table, and supporting tables on first run, and **auto-migrates** to the
  latest schema version on subsequent boots. `migrate: true` is the **default**;
  v10 added `migrate: false` for operators who want migrations as a deliberate
  deploy step (no `CREATE` privilege at runtime, or to avoid a slow migration
  delaying `start()` resolution). `PgBoss.getConstructionPlans()` exports the SQL
  for manual provisioning. (pg-boss releases/10.0.0; issue #164)
- **Graphile Worker.** `run()` **automatically runs migrations** (creates/updates
  the `graphile_worker` schema) on start; `runMigrations()` is the
  install-only/exit variant for a separate deploy step. Each migration runs in a
  transaction so concurrent worker boots are safe. Caveat: the runtime role
  should equal the migration role; breaking migrations want a "scale to zero."
  (worker.graphile.org/docs/library/run; deepwiki graphile/worker migrations)
- **pgmq itself.** `pgmq.create(name)` "sets up the queue's tables, indexes, and
  metadata. It is idempotent, but does not check if the queue already exists" —
  it is effectively `CREATE … IF NOT EXISTS` per object, so re-calling on an
  intact queue is a safe no-op (the Ruby client returns `true` on first create,
  `false` thereafter). **Caveat:** because it only relies on per-object
  `IF NOT EXISTS` and does not validate overall state, a *partially* created
  queue can still error (e.g. the Supabase branch-reset case where
  `pgmq.q_*_msg_id_seq` was missing → `42P01`). (pgmq.github.io docs; pgmq Ruby
  client; supabase/cli #4492)

*What nagi should consider.* Strong precedent (pg-boss, Graphile) for
**eager-at-boot** provisioning being the *expected* DX — both auto-provision on
`start()`/`run()` by default. That supports Change 2's "`nagi()` calls
`ensureSchema?()` at construction." Eager-at-boot is **fail-fast**: a missing
extension / permission error surfaces at process startup (and at deploy time in a
healthcheck), not on the first enqueue under load — which is the precise failure
mode the issue wants to kill ("forgetting it = runtime error on first enqueue").

The counter-considerations both these libraries surface, and how they map to nagi:

- **Permissions.** `CREATE EXTENSION` / `pgmq.create` need elevated privileges
  the *runtime* role may not have. pg-boss's answer is the `migrate: false`
  escape hatch. nagi's equivalent: `ensureSchema` is **optional** on the contract
  and the call should be **best-effort/guarded** — but more importantly, because
  it lives on the *queue adapter*, an operator who provisions out-of-band simply
  uses an adapter build that no-ops `ensureSchema`, or nagi exposes a
  `skipEnsureSchema`-style opt-out (surfaced below as an open question). Do not
  make boot *hard-fail* solely because the role can't `CREATE EXTENSION` if the
  objects already exist.
- **Idempotency under concurrency.** N worker replicas all calling
  `ensureSchema` at boot is fine — `pgmq.create` and `CREATE EXTENSION IF NOT
  EXISTS` are idempotent. Matches Graphile's transactional-migration safety.
- **Eager vs. lazy.** Lazy-on-first-enqueue would re-introduce the very
  surprise the issue removes (latency + first-call failure), and would need a
  "have I ensured yet?" guard on the hot path. Eager-at-boot pays the cost once,
  visibly. The survey consensus (pg-boss/Graphile) is eager. Recommend eager,
  one-shot, at `nagi()` construction.

### 4. Generic DB-handle typing in adapter libraries (Kysely)

*What they do.* The idiomatic Kysely pattern for "accept a caller's typed handle
without forcing a cast" is to make the consuming function/class **generic over
`DB`**: `function f<DB>(db: Kysely<DB>)`, constraining `DB` only if specific
tables are needed (`<DB extends { users: UsersTable }>`). `Kysely<any>` is used
**deliberately only in migrations** (`up(db: Kysely<any>)`) because past
migrations must survive schema drift — it discards type-checking and is the wrong
tool for an adapter that wants to *preserve* the caller's types. `Kysely<unknown>`
forces callers to cast because a concrete `Kysely<MyDB>` is not assignable to it
(the schema parameter does not behave covariantly for assignment here). Some
libraries instead subclass `Kysely` (Auth.js `KyselyAuth`) to assert required
tables, but that is heavier than nagi needs. (kysely.dev; kysely-org/kysely #122;
authjs.dev Kysely adapter)

*What nagi should consider.* Change 3 is exactly the textbook fix:
`pgmqQueue<DB>(opts: { db: Kysely<DB> })`. nagi does not constrain `DB` (pgmq
issues raw `sql\`…\`` against `pgmq.*`, not the caller's tables), so an
**unconstrained** `<DB>` is correct — it threads the caller's type through and
then erases it internally. This is precisely what in-repo `migrate<DB>` already
does (`packages/postgres/src/migrations.ts:182`). Avoid `Kysely<any>` (would let
malformed handles through silently) and keep the existing internal
`as unknown as Kysely<unknown>` erasure at `buildQueue` / `withTx` — the public
generic is a compile-time-only change. The type test
(`packages/pgmq/src/types.test-d.ts:26`) currently asserts
`PgmqQueueOpts["db"]` equals `Kysely<unknown>` and will need updating to assert
the generic accepts a concrete `Kysely<SomeDB>` without a cast.

---

## Part C — Open questions, mapped to the evidence

### Q1 — External `AbortSignal`: merge via `AbortSignal.any`, or always own?

**Evidence:** Node >=22 floor makes `AbortSignal.any` available and (>=22.16)
de-bugged; the listener-leak pathology needs *many composites over one long-lived
signal*, which `nagi.run` does not do (one composite, runtime-lifetime). River
threads the deadline through the caller's context; Graphile uses `noHandleSignals`
to hand control back.

**Leaning:** **Accept** an optional external `signal` and **own an internal
controller**, wiring them with a single `{ once: true }` listener
(survey Design 2) rather than `AbortSignal.any`. Behavior: aborting the external
signal *or* calling `stop()` both drain the worker; `stop()` removes the listener
and is idempotent. This gives callers the River-style "own your deadline" knob
(abort your signal on your own timer) without nagi taking on `any()`'s historical
edge cases. `AbortSignal.any` (Design 1) is acceptable but offers no behavioral
upside here.

### Q2 — `ensureSchema`: eager one-shot at construction, or lazy on first enqueue?

**Evidence:** pg-boss `start()` and Graphile `run()` both provision **eagerly**
by default; `pgmq.create()` is idempotent; lazy re-introduces first-enqueue
failure + a hot-path guard.

**Leaning:** **Eager, one-shot, at `nagi()` construction**, via optional
`queue.ensureSchema?.()`. Adapters without the hook keep current behavior
(structural-optional method on the `Queue` contract). Add an **opt-out**
(`nagi({ ensureSchema: false })` or similar) for the pg-boss-style case where the
runtime role lacks `CREATE` and provisioning is a deploy step — surfaced for the
maintainer below. Best-effort: do not hard-fail boot if objects already exist and
the only error is a privilege error on a redundant `CREATE`.

### Q3 — Naming: `nagi.run` vs `nagi.start()` vs `nagi.spawn()` vs `runRuntime(...)`

**Evidence:** `wf.start(flow, input)` and `worker.run()` already exist, at
different nouns. Graphile's top-level entry is literally `run()`; Temporal's is
`runUntil`; pg-boss `start()`; River `Start`; Hatchet/BullMQ/Sidekiq use
`start`/construction. So `run` and `start` are both well-trodden names for "boot
the whole thing."

**Leaning:** `nagi.run({...})` reads consistently with Graphile's top-level
`run()` and with nagi's own `worker.run()` ("run the runtime" ⊃ "run the
worker"), and the noun is unambiguous (`nagi` the factory, not `wf` or
`worker`). The collision is *nominal only* — `wf.start` and `worker.run` are
methods on different objects, so there is no shadowing. `nagi.start()` risks
read-confusion with `wf.start(flow)` ("start what?"); `spawn`/`runRuntime` are
collision-free but less idiomatic. **Recommend `nagi.run`**, with the handle
shaped `{ wf, stop }` — and consider adding a `done` promise (Graphile parity,
see below).

### Surfacing for the maintainer (Jay)

1. **Add a `done`/`promise` member to the handle?** Graphile's `Runner` exposes
   both `stop()` and a lifecycle `promise`; nagi's proposed `{ wf, stop }` omits
   the latter. `{ wf, stop, done }` lets advanced callers `await handle.done` to
   observe an *unexpected* worker exit without subscribing to the logger. The
   issue scopes the handle to `{ wf, stop }`; flagging `done` as a cheap,
   in-scope-adjacent addition (it is the same `worker.run()` promise nagi already
   holds internally). Defer if you want the minimal surface.
2. **`ensureSchema` opt-out.** Confirm whether v1 ships a
   `nagi({ ensureSchema: false })` escape hatch for the no-`CREATE`-privilege /
   provision-as-deploy-step case (pg-boss `migrate: false` precedent), or whether
   "use an adapter that no-ops `ensureSchema`" is the sanctioned workaround for
   v1.
3. **Does `stop()` own a timeout?** pg-boss/Sidekiq/River all bound the drain
   (timeout or caller context). nagi's `worker.drain()` is currently unbounded
   (matches BullMQ `close()`). Recommend **no `timeout` in v1** (caller races
   `stop()` against their own deadline / aborts the external signal), with
   `stop({ timeout })` noted as a clean future addition. Out of scope for #17 as
   written.
4. **Logger semantics.** The issue says "only true crashes log via the passed
   logger." Concretely: `nagi.run` wraps `worker.run().catch(err => { if
   (stopped) return; logger.error(...) })` — identical to the snippet consumers
   write today, moved inside. No new logging surface; confirming the wording.

---

## Risk surface

Small, and concentrated in `@nagi-js/core` typing + one new optional contract
method.

- **Change 1** is pure composition over existing primitives (`wf.worker`,
  `worker.run`, `worker.drain`) plus an `AbortController` and a `stop()` closure.
  No new failure modes in the worker loop itself; the only new behavior is the
  crash-vs-graceful branch, which mirrors code consumers already write. The
  existing `nagi()` / `wf.worker()` / `worker.run()` surface is untouched
  (`worker.ts`, `runtime.ts:632`).
- **Change 2** adds **one optional method** to the `Queue` contract
  (`types.ts:550`) and **one call site** in `nagi()` (after the flow-registration
  loop, `runtime.ts:212`). Adapters without `ensureSchema` are unaffected
  (optional method). pgmq already implements it
  (`pgmq-queue.ts:58`). The call is idempotent and safe across replicas.
  Main watch-item: don't hard-fail boot on a redundant-`CREATE` privilege error.
- **Change 3** is compile-time only. The internal `as unknown as Kysely<unknown>`
  erasure stays (`pgmq-queue.ts:68`, `:73`); only the public signature gains
  `<DB>`. Risk is limited to updating the type test
  (`types.test-d.ts:26`) and any consumer that *relied on* passing
  `Kysely<unknown>` explicitly (still valid: `unknown` satisfies `<DB>`).

## File map

Files that would change for the implementation:

- `packages/core/src/types.ts` — add optional `ensureSchema?(): Promise<void>`
  to `Queue` (`:550–556`); the `WorkerConfig.signal` surface (`:357–361`) is
  reused as-is.
- `packages/core/src/runtime.ts` — add the `nagi.run(...)` entry (factory-level,
  alongside / wrapping `nagi()`); call `config.queue.ensureSchema?.()` once at
  construction after the flow loop (`:212`). `wf` / `worker()` unchanged.
- `packages/core/src/index.ts` — export `nagi.run` (and the handle type).
- `packages/pgmq/src/pgmq-queue.ts` — make `pgmqQueue<DB>(opts: { db:
  Kysely<DB> })` generic (`:17–18`, `:42`); keep internal erasure (`:68`, `:73`).
- `packages/pgmq/src/types.test-d.ts` — update the `db` assertion (`:26`) to
  prove a concrete `Kysely<SomeDB>` is accepted without a cast.
- Tests — `nagi.run` lifecycle (graceful resolve, crash logs, idempotent
  `stop()`, external-signal abort) in `packages/core`; `ensureSchema`-at-boot
  invocation in `packages/core` (the pgmq `ensureSchema` SQL is already covered
  at `packages/pgmq/src/pgmq-queue.test.ts:209–231`).

Files that do **not** need to change:

- `packages/postgres/src/*` — `migrate()` is unrelated to `ensureSchema`; already
  generic over `DB`.
- `packages/otel/src/*` — no lifecycle / queue / Kysely surface.
- `README.md` — owned by Jay; these changes are for him to document.
- Any consumer code — all three changes are strictly additive / type-widening.

---

## Sources consulted

Lifecycle & graceful shutdown:
- Temporal TS SDK — typescript.temporal.io/api/classes/worker.Worker;
  docs.temporal.io/encyclopedia/workers/worker-shutdown;
  docs.temporal.io/develop/typescript/workers/run-worker-process
- Graphile Worker — worker.graphile.org/docs/library/run (run/runOnce/Runner.stop/promise/noHandleSignals)
- BullMQ — docs.bullmq.io/guide/workers/graceful-shutdown; .../workers/cancelling-jobs
- pg-boss — github.com/timgit/pg-boss (releases/10.0.0; issues #421, #303, #164); v12.x current
- River (Go) — riverqueue.com/docs/graceful-shutdown; pkg.go.dev/github.com/riverqueue/river
- Sidekiq (Ruby) — mikeperham.com/how-sidekiq-works; bigbinary / cloud66 K8s shutdown guides
- Inngest / Hatchet / Quirrel (scanned) — inngest.com/docs/setup/connect; docs.hatchet.run/v1/workers; quirrel.dev

`AbortSignal.any`:
- Availability — nodejs.org release notes v20.3.0, v18.17.0 (PR #47821)
- Leak — nodejs/node issues #54614, #55328, #57584; denoland/deno #24842
- Timeout-GC bug + fix — nodejs/node #57736, PR #57867; released v24.0.0 & v22.16.0 (2025-05-21)
- Mitigations — Nearform "Using AbortSignal in Node.js"; MDN AbortSignal

Auto-bootstrap / pgmq idempotency:
- pgmq — pgmq.github.io/pgmq/latest (create() idempotency); pgmq Ruby client (mensfeld/pgmq-ruby); supabase/cli #4492 (partial-state caveat)
- pg-boss schema/migrate — github.com/timgit/pg-boss releases/10.0.0; issue #164
- Graphile migrations — worker.graphile.org/docs/library/run; deepwiki graphile/worker migrations

Kysely generics:
- kysely.dev; kysely-org/kysely issue #122; authjs.dev Kysely adapter (KyselyAuth wrapper)

*Search reliability note:* the canonical pg-boss `docs/readme.md` path 404'd on
direct fetch; pg-boss `start()`/`stop()`/`StopOptions` and schema-migration
semantics above are drawn from the v10 release notes, the issue tracker
(#421/#303/#164), and corroborating tutorials rather than the single API page.
All other libraries were confirmed against their primary docs.
