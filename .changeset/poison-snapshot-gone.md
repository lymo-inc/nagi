---
"@nagi-js/core": patch
"@nagi-js/pgmq": patch
---

Bound snapshot-gone redelivery. `QueueMessage.readCount` (pgmq `read_ct`) now travels with every delivery; the worker consults a `SnapshotGonePolicy(readCount)` — retry = delayed nack for the rolling-deploy window, then terminally fail the run with the real error and ack. Terminal runs' messages are acked at dispatch (a canceled run can no longer nack-loop). Default policy: quadratic backoff capped at 5 min, fail past 60 deliveries (~4.2h window).
