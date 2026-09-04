---
"@nagi-js/core": patch
---

Internal: the snapshot-gone condition (a run pinned to a `flowHash` this
process did not register) now has one owner. Flow registration, the hash
table, and the disposition live in one module that answers "which Flow runs
this run" with a discriminated result — `current`, `gone-live` (the worker's
`snapshotGonePolicy` decides), or `gone-terminal` (ack and drop). The message
handler, worker, and replay consume that result instead of each re-deriving
the condition from the error. No change to `SnapshotGonePolicy`,
`NagiFlowSnapshotGoneError`, the default retry budget, or replay's
drift-allowed synthesis as observed by consumers.
