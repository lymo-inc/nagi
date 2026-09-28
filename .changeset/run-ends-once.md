---
"@nagi-js/core": patch
"@nagi-js/postgres": patch
---

A run now ends exactly once. Run-end facts (`flow.completed`, `flow.failed`, `flow.canceled`) go through the new `Store.endRun(runId, fact): Promise<boolean>`, which refuses the fact when the run's row is already terminal and writes nothing. The check runs under the store's lock: Postgres locks the `workflow_run` row, and the core policy lives in `admitsRunEnd`. Only the winning writer fires the flow hooks and wakes the parent, so racing cancels, a cancel against completion, or two snapshot-gone messages for one run no longer fire `onFlowError` twice. `appendFact` no longer accepts run-end facts.

Postgres: `tryStartRunOnTx` now runs each concurrency start attempt under a savepoint. Its retry after a unique violation used to fail with "current transaction is aborted", and it now completes, leaving the caller's transaction usable. `tryStartRun` uses the same retry, so it no longer surfaces a raw unique violation when it races a start on a caller's transaction.

In-memory store: `sweepLeases` now reads step status and flowId from the read-model rows, matching Postgres, instead of from the fact fold.
