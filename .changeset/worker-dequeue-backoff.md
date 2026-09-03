---
"@nagi-js/core": patch
---

Worker loop survives transient queue failures. A rejected `queue.dequeue` used
to propagate out of `worker.run()` and terminate the poll loop: a single
`pg-pool` connection timeout inside `pgmq.read` silently stopped ALL flow
processing in a process that stayed otherwise healthy — no steps scheduled, no
messages consumed, and no signal until stuck-run alerts fired hours later.

`run()` now catches dequeue failures, logs `worker.dequeue failed; backing off`
(with `consecutiveFailures` and `backoffMs`), sleeps, and keeps polling —
exponential from `pollIntervalMs`, capped at 30s, reset on the first success.
Its contract is now explicit: `run()` settles only via the abort signal.

`runOnce()` / `runUntilEmpty()` are unchanged and still reject on queue
failure — they are bounded drains whose caller is awaiting a result, so a
rejection there is observed rather than silent.
