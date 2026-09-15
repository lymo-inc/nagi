---
"@nagi-js/core": patch
---

Remove the undocumented `__dispatchDeps` property `nagi()` planted on the `wf`
object. It was never part of the public API (non-enumerable, untyped) and
existed only so core's own test harness could bypass the `Worker` with a
private dispatcher; the harness now drives `wf.worker(...)` directly, so the
real dequeue/admission/snapshot-gone loop is what every harness test covers.

`Worker.runUntilEmpty({ deadline })` now reads the injected `Clock` for its
deadline instead of `Date.now()` — the bounded drain's only wall-clock read.
No behaviour change for any shipped clock.
