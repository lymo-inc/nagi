---
"@nagi-js/core": patch
---

The signal-timeout sweep now isolates each run: a run whose advance throws
(flow snapshot gone, transient store error) is logged at `error` level and
skipped, and the remaining runs in the batch are still advanced to
`flow.failed`. Previously the first throw aborted the whole batch, and because
the store had already dropped the fired timers, the stranded runs were never
swept again.
