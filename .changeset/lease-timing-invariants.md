---
"@nagi-js/pgmq": patch
"@nagi-js/postgres": patch
---

`pgmqQueue` default `visibilityTimeoutMs` is now 120s (was 30s), above core's
40s heartbeat interval — with stock settings a step longer than 30s was
redelivered before its first lease extension. `postgresStore.claimStep` now
computes lease expiry on the database clock, matching `extendLease`, so
application clock skew can no longer grant a second claim on a live lease.
