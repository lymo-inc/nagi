---
"@nagi-js/core": minor
"@nagi-js/postgres": minor
---

Implement the `@nagi-js/postgres` Store adapter and the `Store.runStep` widening that makes it possible.

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
