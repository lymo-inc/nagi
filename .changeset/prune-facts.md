---
"@nagi-js/core": patch
"@nagi-js/postgres": patch
---

Implement issue #9 — `wf.pruneFacts({ olderThan, statuses })` for fact-log
retention. Deletes facts (and per-step rows, leases, timers, dedupes) for
terminal runs whose `completedAt < olderThan`. `pending` / `running` runs are
excluded at the type level via `PrunableStatus` and re-validated at runtime.

Defaults: `statuses: ["completed"]`, `batchSize: 1000`, `keepSummary: true`
(retains a summary row so `queryRuns` still lists the pruned run; both
adapters honor this — postgres keeps the `workflow_run` row, in-memory keeps
a shadow `RunSummary`). After a prune, `loadRunState` and `replay` for that
run return an empty state — documented trade-off: fact-fidelity traded for
storage.

Postgres uses `FOR UPDATE SKIP LOCKED` on the victim CTE so concurrent
pruners share work without contention. New partial index
`workflow_run_completed_at_idx` (migration `0007`) backs the per-batch
victim selection on `(completed_at)` filtered to terminal-status rows. The
SELECT requires `EXISTS (SELECT 1 FROM fact WHERE run_id = ...)` so the
batch loop terminates when `keepSummary: true` (otherwise it would
re-select the same kept summary rows forever).

See `docs/rfcs/0009-prune-facts.research.md`.
