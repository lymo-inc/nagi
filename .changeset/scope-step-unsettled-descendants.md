---
"@nagi-js/core": patch
---

`wf.replay(runId, { mode: "continue", from, scope: "step" })` now reruns any descendant that holds no value — `failed`, `canceled`, or `skipped` — alongside the named step, instead of leaving every descendant untouched. A `completed` descendant still keeps its output from the previous value. On a live run, the descendants to reset are chosen after a `running` `from` step has settled its abort.

Previously, replaying a failed step with `scope: "step"` could let the run settle "completed" while its transitively-skipped descendants never ran, silently omitting them from the flow output.
