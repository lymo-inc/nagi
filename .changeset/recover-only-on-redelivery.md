---
"@nagi-js/core": patch
---

A duplicate message for a step that has already settled is now acked without
re-driving the run. `advance()` enqueues every pending runnable step, including
ones already queued, so a flow that fans out produces such duplicates. Each
duplicate used to take the recovery path meant for a redelivered message and
re-drive the run, which enqueued the still-queued steps again, and each of those
copies did the same. On a Postgres queue drained by one worker slot, a
30-step flow processed about 160 messages and took more than 90 seconds.
Recovery now runs only for a redelivery (`readCount > 1`), which is the case it
exists for: a settle that committed before its `advance()` was lost.
