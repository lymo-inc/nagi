---
"@nagi-js/core": minor
"@nagi-js/postgres": minor
---

Buffer signals that arrive before their target step is claimed, closing the
start/await race.

`wf.signal()` no longer throws `Step "<id>" is not waiting for signal (status:
pending)` when a signal lands before the worker has claimed the signal step. The
payload is parked as a new `signal.buffered` fact and applied atomically the
moment the worker claims the step (it enters `awaitingSignal`), so an early
`audioReady` / `recordingReady`-style signal can no longer be lost to dispatch
timing.

Adds `Store.settleSignal(...)`, which reconciles a signal with its step under a
per-run lock (the Postgres store uses `pg_advisory_xact_lock`); a new
`SignalBufferedFact`; and `RunState.bufferedSignals`. Custom `Store`
implementations must add `settleSignal` — the in-memory and Postgres stores
already do.

Delivery stays exactly-once: a buffered signal and a late direct signal cannot
both apply, and a signal that arrives after the step has already resolved is
still a no-op.
