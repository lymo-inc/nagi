---
"@nagi-js/core": patch
"@nagi-js/postgres": patch
---

Extract signal reconciliation into a core policy so `Store` adapters stop
reimplementing it. New exports `decideSignal(args)` and `SignalDecision`: given
the projected run state, the target step, and an optional incoming signal,
`decideSignal` returns the deliver/buffer/noop decision together with the
fact(s) to persist and the `SettleSignalResult` to return. An adapter's
`settleSignal` now loads run state under its per-run lock, calls `decideSignal`,
persists the returned fact(s), and returns `result` — owning only the
transaction boundary, never the policy. `@nagi-js/postgres` adopts this (and no
longer hand-builds signal facts inline); the in-memory store does too.

Also trims redundant run-state projections on the dispatch hot path: the state
loaded when a step is admitted is threaded through start-recording and execution
instead of being re-folded for the flow input and each step's needs (a task
dispatch drops from four full fact folds to two). Internal only — no behavior
change.
