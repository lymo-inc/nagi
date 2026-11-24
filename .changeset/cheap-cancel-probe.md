---
"@nagi-js/core": patch
"@nagi-js/postgres": patch
---

The cancel watcher no longer re-reads the run's whole fact log every 250 ms while a step runs. A store may implement the new optional `Store.probeAbort(runId, stepId, attempt)`. `postgresStore` answers the common case, a live run with no abort request, with one query that returns two scalars, and uses the fold only when an abort request exists. Stores without `probeAbort` keep the old fold. `storeContract` checks that a probe agrees with the fold.
