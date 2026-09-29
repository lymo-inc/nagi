---
"@nagi-js/core": patch
---

A step whose `optional()` upstream is skipped now runs as soon as the skip is
recorded. Previously, when the same scheduling pass also dispatched other
steps, it waited for an unrelated step to settle, and on `wf.startStaged` it
could wait until then as well.
