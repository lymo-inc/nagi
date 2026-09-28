---
"@nagi-js/core": patch
"@nagi-js/postgres": patch
---

**`@nagi-js/postgres` ships migration `0009_step_run_per_step`: run migrations before deploying.** It deletes duplicate per-attempt `step_run` rows, keeping one row per step, and changes the table's primary key to `(run_id, step_id)`.

`describe`, `queryRuns` and `pruneFacts` now answer from one read model in both stores: the in-memory store materializes the same fact row deltas Postgres does. `describe` returns one view per step (its latest attempt, updated in place on retry), and step timestamps come from the facts' `at` rather than the database clock.
