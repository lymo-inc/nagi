---
"@nagi-js/core": minor
"@nagi-js/postgres": minor
---

Enforce `b.signal({ timeoutMs })` so a signal step no longer parks forever when
its signal never arrives.

`timeoutMs` on a signal step was previously accepted, canonicalized, and diffed
but never enforced — a run waiting on a signal that was never delivered (a lost
webhook, an upstream that silently produced nothing) stayed `awaitingSignal`
indefinitely. It now fails on deadline: the awaiting step is settled `failed`
with a `NagiSignalTimeoutError`, which the scheduler propagates to `flow.failed`
(downstream steps cascade to `skipped`). A signal step without `timeoutMs` keeps
the previous park-forever behavior.

The timeout is the deadline counterpart to signal delivery — it resolves the
same step the same way `wf.signal()` does, but to a failure: under the same
per-run lock as `settleSignal` (so a delivery and a timeout can never both win),
the deadline derives from the persisted `step.started.at` fact plus `timeoutMs`
(replay-safe). The deadline anchors to when the step FIRST started awaiting —
`upsertTimer` keeps the earliest `fire_at`, so a lease-reap re-dispatch of a
still-parked signal can't push it out (a reaper running every lease interval
would otherwise reset it forever). It fires `onStepError` before finalizing the
flow, the same as every other step failure. The worker sweeps elapsed timeouts
itself on a fixed cadence (`WorkerConfig.timerSweepIntervalMs`, default 30s;
checked by wall-clock each loop so a fully-busy worker still sweeps on schedule),
so no extra loop is needed alongside the worker.

Adds `Store.upsertTimer(...)` and `Store.sweepSignalTimeouts(...)` — custom
`Store` implementations must add both (the in-memory and Postgres stores already
do, using the `nagi.timer` table that already shipped). Also adds the exported
`NagiSignalTimeoutError`, the pure `decideTimeout(...)` decision, the
`TimedOutSignal` type, `Dispatcher.sweepTimers(...)`, and
`WorkerConfig.timerSweepIntervalMs`. No migration: the `timer` table and its
`(run_id, step_id)` primary key already existed.
