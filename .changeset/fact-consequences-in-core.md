---
"@nagi-js/core": patch
"@nagi-js/postgres": patch
---

What a fact implies (read-model rows, lease/timer release, stream close, run event) is now one core table, `factConsequences`, and each store applies all of it through a single write path; `supersede` decides concurrency cancellation and the lease reaper's decision carries its `lease.reaped` fact. Fixes `watchRuns` on Postgres never seeing `flow.started` for runs started with `startStaged`. `Store.settleStep` (use `appendFact`), the unused `once.recorded` fact kind, and the `factEffects` / `rowDeltaOf` / `runEventOf` exports are removed.
