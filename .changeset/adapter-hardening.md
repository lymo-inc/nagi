---
"@nagi-js/postgres": patch
"@nagi-js/pgmq": patch
---

`sweepLeases` now judges lease expiry on the database clock instead of the
reaping worker's app clock, matching `claimStep`/`extendLease`. A worker
clock running ahead of the database could previously reap a still-live
lease before its first heartbeat and double-run the step.

`pgmq` `dequeue` now archives a malformed message envelope instead of
throwing. Previously one bad envelope in a batch left the good messages in
that batch hidden for the visibility timeout, and kept failing every
subsequent batch that read it back — forever.
