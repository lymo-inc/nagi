# RFC 0009 — `wf.pruneFacts()` (research + plan)

- **Tracking issue:** lymo-inc/nagi#9
- **Date:** 2026-05-18 (JST)
- **Scope:** `@nagi-js/core` (type + runtime method + in-memory adapter), `@nagi-js/postgres` (adapter + migration). No `pgmq` / `otel` change.

Working notes that back the implementation. Maps current state, calls out where the issue's sketch diverges from the codebase, and lays out a build sequence. **Several open questions surface below that I want to confirm before coding — see "Stop and check" at the bottom.**

## Why

`nagi.fact` and the run tables grow unbounded. Operators currently have to hand-roll DELETEs that know nagi's schema (don't touch active runs, no FK cascade exists, the in-memory adapter has no "run" table at all). The issue motivates a built-in retention API.

## Current state — what we have to work with

### Store contract (`packages/core/src/types.ts:664-831`)

Sixteen methods. None mutate or delete existing facts/runs. All writes are append-only (`appendFact`, `tryStartRun`, `completeStep`, `failStep`, `runStep`, `recordOnce`, `upsertSnapshot`, `setRef`, `appendGlobalFact`). `pruneFacts` would be the first deletion path on the interface.

### `Wf` interface (`packages/core/src/runtime.ts:81-114`)

Currently 5 methods: `start`, `signal`, `worker`, `replay`, `queryRuns`. All run-scoped or worker-scoped reads/writes. `pruneFacts` is the first cross-run mutation.

### Status enum (`packages/core/src/types.ts:1114-1119`, verified via `memory.ts:434`)

`RunStatus = "pending" | "running" | "completed" | "failed" | "canceled"`.

**The issue's sketch uses `"succeeded" | "failed" | "cancelled" | "aborted"` — none of those exist.** Real terminal statuses are `completed`, `failed`, `canceled` (single-l). No `aborted`. Plan uses the real enum.

### Postgres schema (`packages/postgres/src/migrations.ts`)

Run-scoped tables, all keyed on `run_id text`:

| Table | PK | Cascade FK? | Used by |
|---|---|---|---|
| `workflow_run` | `run_id` | — | summary (status, started_at, completed_at, input, output, error, flow_hash, concurrency_key) — migrations.ts:22-31, 117-122, 128-147 |
| `step_run` | `(run_id, step_id, attempt)` | **No FK** — migrations.ts:35-44 | per-step status snapshot |
| `fact` | `(run_id, fact_id)` | **No FK** — migrations.ts:49-55 | append-only event log |
| `lease` | `(run_id, step_id, attempt)` | **No FK** — migrations.ts:60-67 | worker claim tokens |
| `timer` | `(run_id, step_id)` | **No FK** — migrations.ts:71-76 | scheduled wake-ups |
| `dedupe` | `(run_id, step_id, scope)` | **No FK** — migrations.ts:80-87 | `ctx.once` records |

**Critical observations:**
- No explicit FK constraints anywhere — DELETE order must be enforced in app code. The issue's "Cascade-delete `step_run` rows" phrasing assumes a CASCADE that does not exist.
- The issue only names `fact`, `step_run`, `workflow_run`. It does not mention `lease`, `timer`, `dedupe` — but those are also run-scoped and would orphan if a `workflow_run` row is deleted.
- **No index on `completed_at`.** Existing indexes are `(flow_id, status)` migrations.ts:32-33, `(flow_hash)` migrations.ts:121-122, partial `(flow_id, concurrency_key)` migrations.ts:144-147, GIN `(input)` migrations.ts:157-158, and per-table lookups. Prune query filtered on `completed_at < olderThan` would seq-scan today.

### In-memory adapter (`packages/core/src/memory.ts:43-311`)

No materialized "run" row. The Maps:

- `facts: Map<runId, Fact[]>` — memory.ts:44 (source of truth)
- `outputs: Map<runId::stepId, Json>` — memory.ts:45
- `onces: Map<runId::stepId::scope, Json>` — memory.ts:46
- `leases: Map<runId::stepId::attempt, MemoryLease>` — memory.ts:47
- `activeByKey: Map<flowId::concurrencyKey, runId>` — memory.ts:59
- `keyByActiveRun: Map<runId, slot>` — memory.ts:61

`summarize()` (memory.ts:313-340) derives `RunSummary` (including `completedAt` from the terminal fact) by scanning `facts`. `queryRuns` (memory.ts:253-310) iterates `facts` and summarizes.

**This means in-memory has nowhere to put a "summary row after pruning facts" — deleting from `facts` makes the run disappear from `queryRuns` entirely.** See open question #1.

### `replay()` (`packages/core/src/runtime.ts:512-614`)

Two modes: `inspect | continue` (runtime.ts:1147). Both load facts via `store.loadRunState` (runtime.ts:1149-1183 doc, memory.ts:143-145 impl). **Pruning a run's facts breaks both modes for that run**, not just `inspect`. The issue acknowledges `inspect` is broken but doesn't mention `continue`.

`continue` on a terminal-status pruned run is moot in practice (terminal runs don't continue), so this is a documentation-only concern.

### Test infrastructure

- Core: vitest, `packages/core/src/*.test.ts` (e.g. `queryRuns.test.ts:1-186`).
- Postgres: vitest gated on `NAGI_POSTGRES_TEST_URL`, fresh `nagi_test_<uuid7>` schema per test (`packages/postgres/src/integration.test.ts:1-59`). Docker compose at `docker-compose.test.yml`.
- Shared-test pattern: in-memory tests in `queryRuns.test.ts` are *mirrored* in `integration.test.ts` (header comment at `queryRuns.test.ts:1-7`) so both adapters must produce the same answer.

### Adapter cardinality

`pgmq` is a Queue, not a Store (`packages/pgmq/src/index.ts:1-52`, `pgmq-queue.ts:1-52`). It holds work envelopes only, not facts. No change needed for `pruneFacts`.

`otel` is observability, also no change.

So: 2 Store implementations to touch (`InMemoryStore`, `PostgresStore`).

## API — proposed shape (with deviations from issue)

```ts
// packages/core/src/types.ts (new exports)

/** Terminal statuses prunable by `pruneFacts`. Excludes `pending` / `running`. */
export type PrunableStatus = Extract<RunStatus, "completed" | "failed" | "canceled">;

export interface PruneOpts {
  /** Prune runs whose `completed_at < olderThan`. */
  readonly olderThan: Date;
  /**
   * Restrict prune to these terminal statuses. Default: `["completed"]`.
   * `pending` / `running` are never prunable (compile-time + runtime).
   */
  readonly statuses?: ReadonlyArray<PrunableStatus>;
  /** Per-batch deletion size. Default: 1000. */
  readonly batchSize?: number;
  /**
   * Keep the `workflow_run` summary row (and `queryRuns` visibility) after
   * deleting facts/step_runs/leases/timers/dedupes. Default: `true`.
   *
   * In-memory adapter: see Open Question #1 — semantics are non-obvious.
   */
  readonly keepSummary?: boolean;
}

export interface PruneResult {
  readonly runsPruned: number;
  readonly factsPruned: number;
}
```

Deltas from the issue:

| Issue | Plan | Reason |
|---|---|---|
| `'succeeded' \| 'failed' \| 'cancelled' \| 'aborted'` | `'completed' \| 'failed' \| 'canceled'` | Real `RunStatus` enum (types.ts:1114-1119). |
| Default `statuses: ['succeeded']` | Default `statuses: ['completed']` | Map onto real enum. |
| "Cascade-delete `step_run` rows" | Explicit ordered DELETEs on `fact` → `step_run` → `lease` → `timer` → `dedupe` → optionally `workflow_run` | No FK constraints exist (migrations.ts). |
| Only mentions `fact`, `step_run`, `workflow_run` | Also `lease`, `timer`, `dedupe` | All are run-scoped; silently orphaning is worse than handling them. |

### `Wf` interface change (`runtime.ts:81-114`)

```ts
interface Wf {
  // existing: start, signal, worker, replay, queryRuns
  pruneFacts(opts: PruneOpts): Promise<PruneResult>;
}
```

### `Store` contract change (`types.ts:664-831`)

```ts
pruneFacts(opts: Required<PruneOpts>): Promise<PruneResult>;
```

Runtime applies defaults (`statuses`, `batchSize`, `keepSummary`) before delegating, so adapters receive a fully-specified opts object — same pattern as `queryRuns` (runtime delegating, adapter implementing).

## Adapter implementations

### Postgres (`packages/postgres/src/store.ts`)

Per-batch transaction loop:

```sql
-- inside one transaction, repeat until selected_run_ids is empty:

WITH victims AS (
  SELECT run_id
    FROM nagi.workflow_run
   WHERE status = ANY(:statuses)
     AND completed_at IS NOT NULL
     AND completed_at < :olderThan
   ORDER BY completed_at ASC, run_id ASC
   LIMIT :batchSize
   FOR UPDATE SKIP LOCKED  -- concurrent pruners safe; running flows not blocked
)
DELETE FROM nagi.fact         WHERE run_id IN (SELECT run_id FROM victims) RETURNING 1; -- count = factsPruned (this batch)
DELETE FROM nagi.step_run     WHERE run_id IN (SELECT run_id FROM victims);
DELETE FROM nagi.lease        WHERE run_id IN (SELECT run_id FROM victims);
DELETE FROM nagi.timer        WHERE run_id IN (SELECT run_id FROM victims);
DELETE FROM nagi.dedupe       WHERE run_id IN (SELECT run_id FROM victims);
-- only if !keepSummary:
DELETE FROM nagi.workflow_run WHERE run_id IN (SELECT run_id FROM victims);
```

Loop until a batch returns 0 victims. `SKIP LOCKED` makes concurrent pruners (or a `pruneFacts` racing with `tryStartRun` updating a different terminal run) safe.

Tally `runsPruned` from the victims count per batch, `factsPruned` from the `fact` DELETE row-count per batch.

**New migration `0005_workflow_run_completed_at_idx`:**

```sql
-- Backs `pruneFacts` scanning by completion time on terminal rows. Partial
-- index because pending/running rows have completed_at IS NULL and are not
-- prune candidates.
CREATE INDEX IF NOT EXISTS workflow_run_completed_at_idx
  ON nagi.workflow_run (completed_at)
  WHERE completed_at IS NOT NULL
    AND status IN ('completed','failed','canceled');
```

`VACUUM` / `ANALYZE` after prune is **not** built in — flagged in the issue as adapter-opt-in. Defer to a follow-up; manual `VACUUM` is cheap to invoke externally.

### In-memory (`packages/core/src/memory.ts`)

If `keepSummary: false` (or whatever we decide — see open question #1):

```ts
for (const [runId, factList] of this.facts) {
  const s = summarize(runId, factList);
  if (s === null || s.completedAt === null) continue;
  if (!statuses.includes(s.status as PrunableStatus)) continue;
  if (s.completedAt >= opts.olderThan) continue;

  factsPruned += factList.length;
  this.facts.delete(runId);
  // cascade: keys prefixed by `${runId}::`
  for (const k of this.outputs.keys()) if (k.startsWith(`${runId}::`)) this.outputs.delete(k);
  for (const k of this.onces.keys())   if (k.startsWith(`${runId}::`)) this.onces.delete(k);
  for (const k of this.leases.keys())  if (k.startsWith(`${runId}::`)) this.leases.delete(k);
  // activeByKey / keyByActiveRun: terminal facts already cleared these (memory.ts:72-84)
  runsPruned += 1;
}
```

`batchSize` semantics in-memory: honor it as a slicing limit per call iteration so it doesn't behave differently to postgres, but it has no transactional meaning. Realistically a no-op for test-scale data.

## Build sequence

1. **Types** (`packages/core/src/types.ts`): add `PrunableStatus`, `PruneOpts`, `PruneResult`. Append `pruneFacts` to `Store`.
2. **In-memory adapter** (`packages/core/src/memory.ts`): implement `InMemoryStore.pruneFacts`.
3. **Runtime** (`packages/core/src/runtime.ts`): add `pruneFacts` to `Wf`, apply defaults, delegate to `store.pruneFacts`. Runtime-validate that `statuses` excludes `pending`/`running` (defense-in-depth alongside the `PrunableStatus` compile-time guard).
4. **Core tests** (`packages/core/src/pruneFacts.test.ts`): the table below.
5. **Postgres migration** (`packages/postgres/src/migrations.ts`): append `0005_workflow_run_completed_at_idx`.
6. **Postgres adapter** (`packages/postgres/src/store.ts`): implement `PostgresStore.pruneFacts` with the batched CTE loop.
7. **Postgres integration tests** (`packages/postgres/src/integration.test.ts`): mirror the table.
8. **Changeset** (`.changeset/prune-facts.md`): **patch bump** on `@nagi-js/core` and `@nagi-js/postgres` (per `feedback-changeset-bump-type.md` — minor would burn a release name on 0.1.x).
9. **Docs**: add `pruneFacts` to relevant README sections only if Jay wants it (per `feedback_readme_ownership.md` — I will not touch README without explicit ask).

## Test cases (shared across both adapters)

| # | Setup | Call | Expect |
|---|---|---|---|
| 1 | 3 completed runs, 1 running, all old | `pruneFacts({ olderThan: now })` | `runsPruned: 3, factsPruned: ≥3`; running run untouched |
| 2 | 1 completed yesterday, 1 completed today | `pruneFacts({ olderThan: midnight })` | only yesterday's run pruned |
| 3 | 1 completed, 1 failed, 1 canceled, all old | default `statuses` | only `completed` pruned (1 run) |
| 4 | Same | `statuses: ['completed', 'failed', 'canceled']` | all 3 pruned |
| 5 | Empty store | any opts | `runsPruned: 0, factsPruned: 0` |
| 6 | 5 completed runs, `batchSize: 2` | default | all 5 pruned across batches; result aggregates totals |
| 7 | Pruned run: `loadRunState(runId)` (postgres only) | — | (TBD — see Open Question #2) |
| 8 | Pruned run: `queryRuns({ where: { flowId } })` | with `keepSummary: true` (default, postgres) | run still listed |
| 9 | Pruned run: `queryRuns(...)` | with `keepSummary: false` | run not listed |
| 10 | `pruneFacts({ statuses: ['running'] as any })` (cast past compile check) | — | throws `NagiValidationError` |
| 11 | Run with leases/timers/dedupe entries, completed + old | prune | all per-run rows gone in postgres |
| 12 | Two concurrent `pruneFacts` calls (postgres only) | — | both return; sum of `runsPruned` = total old runs; no errors |

## Open questions — STOP AND CHECK BEFORE CODING

These are the design forks where I want your call, Jay:

### Q1. `keepSummary` semantics in the in-memory adapter

In postgres, `keepSummary: true` keeps the `workflow_run` row (so `queryRuns` still finds it) and deletes everything else. **In-memory has no separate summary row — the facts ARE the summary** (memory.ts:313-340). Options:

- **A. In-memory ignores `keepSummary`** — pruned runs always vanish from `queryRuns`. Document the asymmetry.
- **B. Add `summaries: Map<runId, RunSummary>` to InMemoryStore** — populate on terminal facts, consult in `queryRuns` as a fallback after pruning. Matches postgres semantics.
- **C. Change default to `false`** — sidestep the issue; if you want summaries kept, use postgres.

My lean: **B**. It's ~20 LOC and preserves the unrepresentable-invalid-states principle (memory feedback `feedback_unrepresentable_invalid_states.md`) — `keepSummary` would not silently mean different things across adapters.

### Q2. Should `loadRunState` / `replay` on a pruned run throw a specific error?

Today both would return an empty/synthetic `RunState` (memory.ts:143-145 returns `projectRunState(runId, [])` → `status: "pending"`, `flowId: ""`, `steps: {}`). That's misleading after a prune — a pruned run looks indistinguishable from a never-existed run.

Options:

- **A. Do nothing.** Document "facts pruned → run looks gone". Cheap.
- **B. New `NagiPrunedRunError`** raised by `loadRunState`/`replay` when `keepSummary: true` and a summary row exists but no facts. Requires a "facts existed, were pruned" marker per run.

My lean: **A** for v0. It's the documented trade-off ("you traded fact-fidelity for storage"). `replay` failing loud is a v2 feature.

### Q3. Add the `completed_at` partial index?

The prune query without it is a seq-scan over the whole `workflow_run` table. At Lymo's projected ~860K/day this becomes painful within weeks.

Options:

- **A. Add migration `0005_workflow_run_completed_at_idx` as part of this RFC.** Partial index on terminal status.
- **B. Defer the index, add a `pruneFacts` Postgres-only flag like `useFullScan?: true` to acknowledge.**
- **C. Skip — let operators add their own index.**

My lean: **A**. The whole point of `pruneFacts` is operational hygiene; making it self-DoS without an index defeats the feature. The partial index is small (only terminal rows) and write-cheap (rows transition to terminal exactly once).

The `feedback_complexity_must_pay_for_itself.md` principle says "defer trivially-additive optimizations (indexes/caches) until profiling demands them" — but here the *primary* query of the *new feature* has no supporting index. I think this counts as "demanded by the feature", not a speculative optimization. Want your call.

### Q4. Should we rename `pruneFacts` → `pruneRuns`?

The method removes runs (plus their facts/steps/leases/timers/dedupes), not just facts. `pruneFacts` is the issue's name but the framing is "fact log retention". `pruneRuns` is more accurate post-implementation.

My lean: **keep `pruneFacts`** — it's what the issue says, the user-facing motivation is "fact log grows unbounded", and the operation is run-scoped only as an implementation detail.

### Q5. Should the `statuses` option also accept `'pending' | 'running'` and reject at runtime, or block at compile?

I have it as `ReadonlyArray<PrunableStatus>` (compile-time block via `Extract<RunStatus, 'completed' | 'failed' | 'canceled'>`). The issue's prose says "Never prunes 'running'" implying a runtime guard.

My lean: **both**. Compile-time `PrunableStatus` type for the 99% case + runtime validation in the runtime layer so callers casting through `any` still get a `NagiValidationError`. Defense in depth, matches existing patterns (e.g. cursor validation in `queryRuns`).

## Non-goals

- Archival to cold storage. Separate `archiveFacts` RFC if requested.
- Per-flow retention policies. `queryRuns + loop` covers it.
- `VACUUM` / `ANALYZE` automation. Adapter-opt-in flag (deferred).
- `pruneFacts` on `global_fact` or `flow_snapshot` / `flow_ref`. Different lifecycle; future RFC.

## Risks

- **In-memory `keepSummary`** could ship with adapter-asymmetric behavior if we pick Option A or C for Q1. Worth a clear test.
- **Partial-index write amplification** is negligible — `workflow_run` rows transition to terminal once.
- **Concurrent prune + write**: `SKIP LOCKED` on the victim CTE handles it for postgres. In-memory is single-process JS so it's atomic between awaits.
- **`step.reset` on a pruned-then-running-again run**: impossible — pruned runs are terminal-status only; `step.reset` only fires for active runs.
