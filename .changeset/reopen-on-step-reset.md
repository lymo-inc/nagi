---
"@nagi-js/core": patch
"@nagi-js/postgres": patch
---

`step.reset` (from `wf.replay({ from })` and `operator.retry`) now reopens a
`completed`/`failed` run to `running`. Previously the run stayed terminal:
the re-run steps finished but no `flow.completed` was ever written
(`describe()`/`queryRuns` kept reporting `failed`, `onFlowComplete` never
fired, a waiting parent subflow was never woken), and the cancel watcher
aborted any re-run handler honoring `ctx.signal` after 250 ms because the run
looked terminal. Reopening a run whose concurrency key another active run
holds throws `NagiConcurrencyConflictError`. `canceled` runs are not reopened.
