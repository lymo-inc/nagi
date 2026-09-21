---
"@nagi-js/postgres": patch
"@nagi-js/core": patch
---

`b.streamingTask` now works on Postgres. `postgresStore()` takes a `listener` and implements `StreamTransport` over `LISTEN`/`NOTIFY`; until now the transport existed only on the in-memory store, so a flow declaring a streaming step was refused against any production store.

The listening connection is injected rather than opened by the adapter: Kysely hides the driver (`PostgresConnection` holds the `pg` client privately and exposes no notification event) and this package keeps `pg` a devDependency, so neon / postgres.js / pglite users are not forced onto it. Consumers pass a `StreamListener`, the same way they already pass `db`.

Two ordering properties the transport has to guarantee, neither of which is free over a connection pool:

- **Chunks of one step stay in order.** `db` is a pool, so un-awaited `pg_notify` calls can take different connections and arrive reversed. Publishes are chained per step; different steps stay independent.
- **A close never overtakes its chunks.** Close/retry frames ride the fact's transaction and land at commit, while chunks commit immediately on their own connections. Before emitting a close the adapter drains that step's publish chain, otherwise the channel shuts and in-flight chunks are discarded.

`postgresStore()` returns a `PostgresStoreHandle` with `ready()`, which resolves once both channels are actually receiving NOTIFY. A constructor cannot await `listen()`, and NOTIFY does not queue for a connection that is not yet listening, so chunks and events published in that window are lost rather than delayed — `await store.ready()` before starting work you intend to watch. It is total: with no `listener` there is nothing to wait for and it resolves immediately, so a consumer never branches on whether it wired one.

`core` exports `InMemoryStreamHub` and the buffer caps so adapters reuse one fan-out implementation instead of reimplementing subscriber buffering and close semantics. Chunks are capped at `MAX_CHUNK_BYTES` (7000, inside PostgreSQL's 8000-byte NOTIFY payload limit) and an oversized chunk throws from `ctx.emit` rather than being dropped silently.
