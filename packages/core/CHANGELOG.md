# @nagi-js/core

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
