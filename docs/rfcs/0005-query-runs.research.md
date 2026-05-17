# RFC 0005 — `wf.queryRuns()` (research + plan)

- **Tracking issue:** lymo-inc/nagi#5
- **Date:** 2026-05-17 (JST)
- **Scope:** `@nagi-js/core` (type + runtime method), `@nagi-js/postgres` (adapter + migration). No `pgmq` / `otel` change.

Working notes that back the implementation. Maps the current state, locks the
API shape against existing conventions, and lays out the build sequence.

## Why

Read-side surfaces ("show me the current run for video X", "list running
deal-analysis runs", "history for product Y") need to discover runs by their
input fields. Today consumers either hand-roll a JSONB containment query
against `nagi.workflow_run` or maintain a per-entity FK column on their
domain tables. Both leak nagi's schema into consumers. See issue #5 for the
full motivation.

## Current state — what we have to work with

### Store contract (`packages/core/src/types.ts:664`)

Sixteen methods, all keyed by `runId`. No method takes a filter and returns
a list of runs. The closest existing read is `loadRunState(runId)`
(`types.ts:666`) which materializes one run from its fact log via
`projectRunState` (`memory.ts:246`).

### `Wf` interface (`packages/core/src/runtime.ts:78`)

```ts
interface Wf {
  start<F extends Flow>(flow: F, input, opts?): Promise<RunId>;
  signal(runId: RunId, stepName: string, payload: unknown): Promise<void>;
  worker(config?: WorkerConfig): Worker;
  replay(runId: RunId, opts?: ReplayOpts): Promise<void>;
}
```

Four methods. All are run-scoped (`runId` in, or hand back a fresh one).
`queryRuns` is the first read that spans runs.

### Storage — Postgres adapter

Migration `0001_init` (`packages/postgres/src/migrations.ts:17`) defines
`nagi.workflow_run`:

```sql
CREATE TABLE nagi.workflow_run (
  run_id       text        PRIMARY KEY,
  flow_id      text        NOT NULL,
  status       text        NOT NULL CHECK (status IN ('pending','running','completed','failed')),
  input        jsonb       NOT NULL,
  output       jsonb,
  error        jsonb,
  started_at   timestamptz NOT NULL DEFAULT now(),
  completed_at timestamptz
);
CREATE INDEX workflow_run_flow_status_idx ON nagi.workflow_run (flow_id, status);
```

Migration `0003_concurrency_groups` adds `concurrency_key`,
`canceled_by_run_id`, and extends the status check with `'canceled'`.

Critical observation: **`workflow_run.input` is already a `jsonb NOT NULL`
column populated at `tryStartRun` from `flow.started.input`** (`store.ts:103`
and `store.ts:188`). No schema change is required for the filter to work —
only a GIN index for it to scale.

### Storage — in-memory adapter

`InMemoryStore` (`packages/core/src/memory.ts:40`) keeps facts in
`Map<runId, Fact[]>`. No separate materialized run record. Getting a run's
input means reading the first fact in its list and casting to
`FlowStartedFact`.

### Pagination / cursors

`grep -rn "cursor"` on the codebase: zero hits in source. No cursor
convention exists yet — we define one.

### Test setup

- Core: vitest, `packages/core/src/*.test.ts`, no external deps.
- Postgres: vitest gated on `NAGI_POSTGRES_TEST_URL`
  (`packages/postgres/src/integration.test.ts:28`). Each test mints a fresh
  schema name `nagi_test_<uuid7>` and drops it after. Docker compose at
  `docker-compose.test.yml` boots postgres:16-alpine on port 5433.

## API — locked shape

This matches the issue's proposal exactly, with one fix: `RunSummary.input`
is `Json` per the codebase's `Json` type (`types.ts:1`), and we'll add the
`completedAt: Date | null` field even when there's no `flow.completed` fact
yet (it's null while running).

```ts
// packages/core/src/types.ts (new exports)

export interface QueryRunsWhere {
  readonly flowId?: string;
  readonly status?: RunStatus | ReadonlyArray<RunStatus>;
  /**
   * JSONB containment against `flow.started.input`. Matches runs whose input
   * is a superset of this object. Example: `{ videoId: 'abc-123' }` matches
   * `{ videoId: 'abc-123', userId: 7 }` but not `{ videoId: 'xyz' }`.
   *
   * Containment only — no JSONPath, no operators. Use multiple keys for AND.
   * See "Open questions" in RFC 0005.
   */
  readonly input?: Record<string, Json>;
}

export interface QueryRunsOpts {
  readonly where?: QueryRunsWhere;
  /**
   * When true, return at most one run — the most recently started match.
   * Equivalent to `limit: 1` ordered by `(startedAt DESC, runId DESC)`,
   * with `cursor` returned as `null`.
   */
  readonly latest?: boolean;
  /** Page size. Default 50, max 500. */
  readonly limit?: number;
  /** Opaque cursor from a previous `queryRuns` call. */
  readonly cursor?: string;
}

export interface RunSummary {
  readonly runId: RunId;
  readonly flowId: string;
  readonly status: RunStatus;
  readonly startedAt: Date;
  readonly completedAt: Date | null;
  readonly input: Json;
}

export interface QueryRunsResult {
  readonly runs: ReadonlyArray<RunSummary>;
  /** Pass to the next `queryRuns` to resume. `null` when no more rows. */
  readonly cursor: string | null;
}
```

`RunSummary` is intentionally minimal — `RunState` (the full projection) stays
behind `loadRunState` / `replay({ mode: 'inspect' })`. Discovery returns just
what's needed to identify and route the run.

### Cursor encoding

A cursor is opaque to callers but defined as: base64url of
`JSON.stringify({ t: startedAt.toISOString(), r: runId })`. Ordering is
`(started_at DESC, run_id DESC)` for stable keyset pagination — runs sharing
a `started_at` tiebreak by `run_id` (which is a UUID v4 from `mintRunId`, so
order is arbitrary-but-consistent).

The next page's WHERE clause is:
```sql
WHERE (started_at, run_id) < (:cursor.t, :cursor.r)
```

Strict `<` (not `<=`) because the previous page's last row is exclusive.

### `Wf` interface change

```ts
interface Wf {
  // existing
  start, signal, worker, replay;
  queryRuns(opts?: QueryRunsOpts): Promise<QueryRunsResult>;
}
```

## Store contract change

Add one method to the `Store` interface:

```ts
queryRuns(opts: QueryRunsOpts): Promise<QueryRunsResult>;
```

Why on `Store` and not just a thin helper over `loadRunState`: the Postgres
adapter has to push the filter to the DB (the in-memory full scan is fine
for the tens-of-runs case tests cover, but it's the wrong shape over a
million-run table). Putting the method on the interface makes the
adapter-owned filter the only path.

The runtime's `wf.queryRuns` is then a one-liner delegating to
`store.queryRuns` (with defaulting and a `latest` shortcut), mirroring how
`wf.start` defers atomic insertion to `store.tryStartRun`.

## Adapter implementations

### Postgres

Build the query with Kysely's `sql` tag for the containment operator (no
Kysely operator binding for `@>` exists; raw it is). Key SQL:

```sql
SELECT run_id, flow_id, status, input, started_at, completed_at
  FROM nagi.workflow_run
 WHERE (:flowId::text IS NULL OR flow_id = :flowId)
   AND (:statuses::text[] IS NULL OR status = ANY(:statuses))
   AND (:input::jsonb IS NULL OR input @> :input)
   AND (:cursorT::timestamptz IS NULL OR (started_at, run_id) < (:cursorT, :cursorR))
 ORDER BY started_at DESC, run_id DESC
 LIMIT :limit + 1
```

The `LIMIT :limit + 1` trick: fetch one extra row to detect whether a next
page exists. If `rows.length > limit`, drop the last row and emit a cursor;
otherwise return `cursor: null`.

`canceled` is part of `RunStatus` — make sure the test for `status` filter
covers it (the migration `0003` extended the CHECK constraint).

### Postgres — index

New migration `0004_query_runs_input_idx`:

```sql
CREATE INDEX IF NOT EXISTS workflow_run_input_gin_idx
  ON nagi.workflow_run USING gin (input jsonb_path_ops);
```

`jsonb_path_ops` is the smaller, faster opclass when queries are
containment-only (which they are by the API design). It does NOT support
`?` / `?|` / `?&` existence operators, but we don't expose those.

The existing `workflow_run_flow_status_idx` on `(flow_id, status)` already
covers the no-input case.

### In-memory

Iterate `this.facts`. For each entry, the first fact is `flow.started`
(invariant from `tryStartRun`). Read `flowId`, `input`, `flowHash`,
`startedAt = fact.at` directly. Project the full fact list to derive
`status` and `completedAt` (terminal facts' `at`). Filter, sort, slice.

Approximately 30 lines. No optimization — in-memory is for tests; the
Postgres adapter is where production performance lives.

## Build sequence

1. **Types** (`packages/core/src/types.ts`): add
   `QueryRunsWhere`, `QueryRunsOpts`, `RunSummary`, `QueryRunsResult`. Append
   `queryRuns` to `Store`.
2. **In-memory adapter** (`packages/core/src/memory.ts`): implement
   `InMemoryStore.queryRuns`. Helper to extract `RunSummary` from a fact list.
3. **Runtime** (`packages/core/src/runtime.ts`): add `queryRuns` to `Wf`,
   delegate to `store.queryRuns`; `latest: true` rewrites to `limit: 1` and
   drops the cursor.
4. **In-memory tests** (`packages/core/src/memory.test.ts` or a new
   `queryRuns.test.ts`): the table of cases below.
5. **Postgres migration** (`packages/postgres/src/migrations.ts`): append
   `0004_query_runs_input_idx`.
6. **Postgres adapter** (`packages/postgres/src/store.ts`): implement
   `PostgresStore.queryRuns` with the SQL above.
7. **Postgres integration tests** (`integration.test.ts`): same test table,
   gated on `NAGI_POSTGRES_TEST_URL`.
8. **Changeset** (`.changeset/query-runs.md`): patch bump on
   `@nagi-js/core` and `@nagi-js/postgres`. User-facing summary.
9. **Index exports** (`packages/core/src/index.ts`): the new types are
   re-exported by `export type * from "./types"` already, but verify.

## Test cases (shared across both adapters)

| # | Setup | Query | Expect |
|---|---|---|---|
| 1 | One running run with `input: { videoId: "abc" }` | `where: { input: { videoId: "abc" } }` | 1 row, that run |
| 2 | Same | `where: { input: { videoId: "xyz" } }` | 0 rows |
| 3 | One run with `input: { videoId: "abc", userId: 7 }` | `where: { input: { videoId: "abc" } }` | 1 row (containment) |
| 4 | One run with `input: { videoId: "abc" }` | `where: { input: { videoId: "abc", userId: 7 } }` | 0 rows (filter is the superset; row is the subset) |
| 5 | Three runs of same flow, distinct inputs | `where: { flowId }` | 3 rows, DESC by startedAt |
| 6 | Mix of running + completed | `where: { status: "running" }` | only running |
| 7 | Mix of statuses | `where: { status: ["running", "completed"] }` | both |
| 8 | Three runs | `latest: true` | 1 row, newest |
| 9 | Three runs | `limit: 2`, then follow cursor | first page 2 rows, second page 1 row + `cursor: null` |
| 10 | Empty store | (no filters) | `runs: []`, `cursor: null` |
| 11 | Canceled run (via concurrency) | `where: { status: "canceled" }` | the canceled run |

Postgres-only:
| 12 | 1000 runs, GIN index | `EXPLAIN ANALYZE` shows `Bitmap Index Scan on workflow_run_input_gin_idx` | not a sequential scan |

Test #12 is a smoke test that the index is actually picked, not a perf assertion.

## Open questions — answered for v0

| Q | A |
|---|---|
| `input` containment only, or JSONPath? | Containment only. Covers ≥95% of real-world needs per the issue. Add `inputPath` later if asked. |
| Default-filter cancelled runs? | No. Explicit `status` filter — same shape as Temporal's `WorkflowQuery`. |
| `total` count? | No. Cursor-based pagination only. A `total` would force a second `COUNT(*)` query and undermine the cheap-discovery design point. |
| Sort field? | `started_at DESC, run_id DESC`. Fixed for v0. If callers ask for `completed_at`-sorted history, add a sort option later. |
| `cursor` validation? | Decode + sanity check (`t` parses as ISO date, `r` is a non-empty string). Malformed → throw `NagiValidationError`. |

## Non-goals

- Subscriptions / live updates. Use `nagi.step_run` realtime / SSE.
- Full per-step state. That's `loadRunState` / `replay({ mode: 'inspect' })`.
- Filtering by step state (e.g. "runs where step X failed"). Use the fact
  log directly for that until there's a clear pattern of need.

## Risks

- **GIN on a busy table slows inserts.** `tryStartRun` is the only writer
  to `workflow_run.input`, and it's a once-per-run write. The index is
  appropriate.
- **Cursor encoding lock-in.** Once shipped, the base64-JSON shape is
  publicly observable. Mitigation: it's documented as opaque, but a future
  migration to a different encoding would need a version byte. Defer until
  needed.
- **Status filter on the partial unique index path.** The
  `workflow_run_concurrency_active_uidx` index is partial (`WHERE status IN
  ('pending','running')`); the filter `status = ANY(...)` should plan
  correctly off either `workflow_run_flow_status_idx` or the partial
  index. Verify with `EXPLAIN`.
