---
"@nagi-js/core": patch
---

`b.signal({ ... })` can now accept one or more external names. Pass
`names: ['audioReady', 'recordingReady']` to let one signal step resolve on
the first arrival from any of N upstream sources, or `names: ['approval']` to
decouple a single signal name from the step id. Omitting `names` keeps today's
behaviour — the step id is the signal name. Late-arriving losers (a recognized
alias for an already-resolved step) are a no-op + logged, not a throw.

`SignalReceivedFact` gains an optional `signalName` field that records which
alias triggered resolution when it differs from the step id. Construction-time
flow validation rejects overlapping signal names (alias-vs-stepId or
alias-vs-alias) so an ambiguous flow can't boot. See RFC 0004.

Behavior note: pre-existing flows that don't pass `name` or `names` are
unaffected — neither at the source level, nor in their canonical flow hash,
nor in `code_version`.
