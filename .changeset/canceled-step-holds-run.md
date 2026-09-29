---
"@nagi-js/core": patch
---

A canceled step on a live run no longer counts as success: `flowTermination`
treats it as not-yet-done, so the run stays `running` and waits for
`replay({ from })` instead of completing with the step unresolved. A
redelivered message for a canceled step is no longer re-run by `admit` — it
is acked and skipped. When nothing else is in flight and a canceled step
blocks the run, `advance` now logs a `warn` (`run stalled on canceled
step(s)`) naming the stalled steps.
