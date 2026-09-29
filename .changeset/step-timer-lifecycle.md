---
"@nagi-js/core": patch
---

A step's heartbeat and cancel watcher now finish any in-flight tick before the step is acked, so no lease extension or run-state read outlives the step. Worker shutdown now waits on its in-flight dispatches instead of polling `clock.sleep`.
