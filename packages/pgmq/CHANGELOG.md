# @nagi-js/pgmq

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
