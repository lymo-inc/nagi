---
"@nagi-js/core": patch
"@nagi-js/postgres": patch
---

`wf.queryRuns` and `Store.queryRuns` are removed, along with `RunSummary`, `QueryRunsOpts` / `QueryRunsResult` / `QueryRunsWhere` and the cursor helpers (`encodeRunCursor`, `decodeRunCursor`, `clampQueryLimit`, `compareRunOrder`, `isPastCursor`, `jsonContains`, `QUERY_RUNS_DEFAULT_LIMIT`, `QUERY_RUNS_MAX_LIMIT`, `RunCursor`). Use `describe(runId)` for one run, and list or join runs with SQL against `workflow_run` and `step_run`, whose columns are now a documented read contract (docs/OPERATIONS.md, "Reading runs with SQL").
