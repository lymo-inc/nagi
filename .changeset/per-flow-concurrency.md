---
"@nagi-js/core": minor
"@nagi-js/pgmq": minor
---

Per-flow blast-radius bound: `WorkerConfig.maxConcurrencyPerFlow` caps the worker slots any single flow may hold; over-cap messages defer via delayed nack. `flowId` now rides the message envelope (stamped at every enqueue path incl. lease-reap; absent pre-upgrade messages are exempt). Multi-flow deployments should set the cap ≤ concurrency − 1 so one wedged flow can never occupy the whole pool.
