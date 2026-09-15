---
"@nagi-js/postgres": patch
---

`postgresStore.queryRuns` without a `where.input` filter failed on a real
database with `could not determine data type of parameter` — the absent
filter was bound as an untyped NULL. It is now cast to `jsonb`. Surfaced by the
first CI run of the Postgres integration suite.
