# @nagi-js/pgmq

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
