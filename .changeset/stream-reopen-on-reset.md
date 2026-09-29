---
"@nagi-js/core": patch
"@nagi-js/postgres": patch
---

A streaming step's channel now reopens for its rerun. Previously, once a
streaming step's channel closed (`step.completed`/`step.failed`), the hub
remembered the key as closed forever: a later reset by `replay({ from })`
(either scope) dropped the rerun's chunks and
handed a new `subscribeStream` an already-closed empty stream, even though
the step ran and completed again.

`step.reset` now declares a `reopen` stream effect, carried over Postgres
NOTIFY like the other stream effects, that clears the closed-key marker (and
any stale replay buffer) so the rerun streams normally to whoever is
currently subscribed.

Also: a lease reap (`lease.reaped`, written when the reaper re-enqueues a
step whose worker died mid-stream) now declares the same `retry` stream
effect a normal `step.retried` does, so subscribers see a `retry` marker
between the superseded attempt's partial chunks and the next attempt's
output, instead of the two attempts running together with no marker.
