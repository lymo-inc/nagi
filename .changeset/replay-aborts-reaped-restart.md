---
"@nagi-js/core": patch
"@nagi-js/postgres": patch
---

`replay({ from })` now aborts every attempt that starts while it waits, so a lease-reaped step restarted mid-replay no longer outlives the reset. A step's completion, failure or cancel now applies only to an attempt that has started since its last reset, in the fold and both read models.
