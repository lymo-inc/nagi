# @nagi-js/pgmq

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
- Updated dependencies
- Updated dependencies [803ebb9]
  - @nagi-js/core@0.1.1-rc.12

## 0.1.1-rc.11

### Patch Changes

- Stronger state and type representation
- Updated dependencies [c8041c5]
- Updated dependencies [5cbca32]
- Updated dependencies
- Updated dependencies [e451bfd]
- Updated dependencies [5cbca32]
  - @nagi-js/core@0.1.1-rc.11

## 0.1.1-rc.10

### Patch Changes

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

- Updated dependencies [f926424]
- Updated dependencies [b79ede2]
- Updated dependencies [f926424]
- Updated dependencies [f926424]
- Updated dependencies [f926424]
  - @nagi-js/core@0.1.1-rc.8

## 0.1.1-rc.7

### Patch Changes

- Implement RFCs #7 #9 #10
- Updated dependencies [c4e1459]
- Updated dependencies
- Updated dependencies [c4e1459]
- Updated dependencies [c4e1459]
  - @nagi-js/core@0.1.1-rc.7

## 0.1.1-rc.6

### Patch Changes

- Implement issue #5
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

## 0.1.1

### Patch Changes

- Updated dependencies [2f4b9f0]
- Updated dependencies [2f4b9f0]
  - @nagi-js/core@1.0.0

## 0.1.0

### Minor Changes

- 3bceb7a: Implement the `pgmqQueue` adapter — a `Queue` implementation backed by the [PGMQ](https://github.com/tembo-io/pgmq) Postgres extension.

  - Maps the locked `Queue` contract to `pgmq.send` / `pgmq.read` / `pgmq.delete` / `pgmq.set_vt` / `pgmq.archive`.
  - Receipts are stringified `msg_id` values; the dispatcher remains the sole owner of attempt counters (per the in-memory queue's invariant, `nack` never mutates `attempt`).
  - Configurable: `queueName` (default `"nagi"`), `visibilityTimeoutMs` (default 30 s), `partitioned`, `archiveOnAck`.
  - Exposes `ensureSchema()` for dev/test bootstrapping: runs `CREATE EXTENSION IF NOT EXISTS pgmq` plus `pgmq.create` (or `pgmq.create_partitioned`). Production setups should run these out-of-band.
  - Exposes `withTx(ctx.tx)` returning a `Queue` bound to the handler's Kysely transaction. Lets handlers atomically commit domain writes + outbound pgmq messages alongside `step.completed`. Requires `@nagi-js/postgres` wired and `Register.tx` augmented (the standard transactional setup).
  - Peer-depends on `kysely`; the user owns the connection.

### Patch Changes

- Updated dependencies [3bceb7a]
  - @nagi-js/core@0.1.0
