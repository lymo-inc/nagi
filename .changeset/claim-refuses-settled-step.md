---
"@nagi-js/core": patch
"@nagi-js/postgres": patch
---

Fix a step running twice when two copies of its message are dispatched at once.
`advance()` enqueues every pending runnable step, so a step can sit in the
queue twice; admission reads the run before it claims, and settling a step
deletes its lease. A copy that read the step as pending, then claimed just
after the other copy settled it, got a fresh lease and ran the step again.

`Store.claimStep` MUST now return `null` once the step has settled (completed,
failed, canceled or skipped), judged after any settle racing the claim has
committed, and leave no lease behind. `InMemoryStore` checks the step row;
`postgresStore` takes the lease, then gives it back if `step_run` shows the
step settled — a guard inside the INSERT would read `step_run` from a snapshot
taken before a settle the INSERT waited on. `storeContract` checks the new
rule, and its lease-release cases for settled steps now read `describe()`
instead of re-claiming.

Third-party Store adapters must apply the same refusal.
