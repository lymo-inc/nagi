---
"@nagi-js/core": patch
---

A message whose dispatch throws an unexpected error is now redelivered with backoff instead of at once. The delay starts at one `pollIntervalMs` and doubles per delivery up to 30 s, the same curve as a failing dequeue. Before, a dispatch that threw on every delivery redelivered in a tight loop: 100% CPU in memory, and a `set_vt(0)` loop on the database with pgmq. The `worker.dispatch threw uncaught` log entry now carries `runId`, `stepId`, `readCount` and `delayMs`.
