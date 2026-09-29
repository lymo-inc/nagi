---
"@nagi-js/core": patch
"@nagi-js/postgres": patch
---

`wf.operator()` is removed. Rerun a step with `wf.replay(runId, { mode: "continue", from, scope })`, which now also works on a live run and aborts an in-flight `from` step before resetting it; stop a run with `wf.cancel`. `skip` is gone, along with the `"manual"` skip reason, the `"operator"` cancel cause and the `actor`/`note` audit fields on step facts.
