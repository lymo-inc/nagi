# @nagi-js/pgmq

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
