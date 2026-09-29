---
"@nagi-js/core": patch
"@nagi-js/postgres": patch
---

`wf.watchRun` / `wf.watchRuns` now emit `step.canceled` (an aborted or
run-canceled step) and `step.reset` (a step reset by `replay({ from })`; on
a finished run, the reopen). Exhaustive `switch`
statements over `RunEvent` need the two new cases.
