---
"@nagi-js/core": patch
"@nagi-js/postgres": patch
"@nagi-js/otel": patch
---

Own each fact kind in one place (#38). `packages/core/src/facts/` now holds,
per lifecycle (`flow` / `step` / `signal` / `lease`), a kind's shape, its
constructor, its fold arm, and its read-model row delta. `foldRun` moved from
`state.ts` to `facts/`; `state.ts` is the read model only. The public `Fact`
union and every `*Fact` type still come from `@nagi-js/core` (single door).

New: `rowDeltaOf(fact): RowDelta | null` and the `RowDelta` type. The Postgres
store no longer hand-transcribes an 18-case fact→table switch; it interprets
the small `RowDelta` vocabulary core declares per kind, and every persistence
path (`appendFact`, `settleStep`, `runStep`, `settleSignal`,
`sweepSignalTimeouts`, concurrency cancel) goes through the same
`persistFact`. A new fact kind that lacks a fold arm or a row-delta declaration
fails to compile; audit-only kinds declare `rows: null` explicitly.

Removed (dead, never produced or read — public-surface removals):

- `signal.sent`: `FactKind` member, `SignalSentFact`, `SignalSentEvent`,
  `FlowHooks.onSignalSent`, and the `@nagi-js/otel` `composeHooks` fan-out.
  No runtime path ever constructed or fired it.
- `RunState.anomalies` and the `Anomaly` type. Nothing read them; the fold is
  still total (a contradictory fact keeps the prior step state).

Persisted logs fold unchanged; unknown kinds in a log (e.g. a removed one)
fold and materialize as no-ops instead of throwing.
