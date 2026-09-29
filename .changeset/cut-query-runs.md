---
"@nagi-js/core": patch
"@nagi-js/postgres": patch
---

`wf.queryRuns` and `Store.queryRuns` are removed, along with `RunSummary`, `QueryRunsOpts` / `QueryRunsResult` / `QueryRunsWhere` and the cursor helpers (`encodeRunCursor`, `decodeRunCursor`, `clampQueryLimit`, `compareRunOrder`, `isPastCursor`, `jsonContains`, `QUERY_RUNS_DEFAULT_LIMIT`, `QUERY_RUNS_MAX_LIMIT`, `RunCursor`). Use `describe(runId)` for one run, and list runs with SQL against the nagi schema; the read view is documented in docs/OPERATIONS.md.
