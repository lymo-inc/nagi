---
"@nagi-js/core": patch
---

The worker now heartbeats a step's queue message while its handler runs, so a
long step (e.g. a multi-minute LLM call) no longer outlives the queue's
visibility timeout and gets redelivered + re-executed concurrently.

`dispatchMessage` starts a `startHeartbeat` loop before executing a step and
stops it (in a `finally`) just before ack. Each tick calls the existing
`queue.extend(receipt, leaseMs)`; a failed extension is logged and the loop
continues (an early redelivery is still deduped by `admit()`'s `claimStep`). A
crashed worker simply stops extending, so the message redelivers after at most
one lease — crash recovery is preserved.

Tunable via two new optional `NagiConfig` fields, `heartbeatIntervalMs` and
`heartbeatLeaseMs` (defaults `DEFAULT_HEARTBEAT_INTERVAL_MS` 40s /
`DEFAULT_HEARTBEAT_LEASE_MS` 120s). `heartbeatIntervalMs` must be shorter than
the queue's initial visibility timeout, or the first redelivery happens before
the first extension lands.
