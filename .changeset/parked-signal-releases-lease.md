---
"@nagi-js/core": patch
"@nagi-js/postgres": patch
---

A `b.signal` step now releases its store lease when it parks. Previously the
lease expired while the step waited, and the lease reaper re-dispatched the
step every lease period (60 s by default) for the whole wait. Each re-dispatch
wrote a `lease.reaped` and a `step.started` fact and fired `onStepStart`.
`decideSignal` returns a new `park` decision; custom stores must apply its
`release`.
