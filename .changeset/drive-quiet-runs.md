---
"@nagi-js/core": patch
"@nagi-js/postgres": patch
---

The worker drives stranded runs to an end by itself. A live run with no lease, no timer, no queued message, no live child and no fact for `quietRunMs` (default 2 min) is re-driven: each in-flight step gets its message back and the run is advanced. If that enqueues nothing, the run fails with the new typed `NagiRunStrandedError`. A run pinned to a snapshot that is gone fails with `NagiFlowSnapshotGoneError` instead of sitting. A signal step with `timeoutMs: "unbounded"` is left waiting.

New `Store.listQuietRuns({ quietSince, limit })`, covered by `storeContract`; custom stores must implement it. New `WorkerConfig.quietRunSweepIntervalMs` (default 60 s, `0` disables) and `quietRunMs`. The sweep needs `Queue.inspect`; without it the sweep does nothing.
