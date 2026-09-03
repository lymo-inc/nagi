---
"@nagi-js/core": minor
"@nagi-js/pgmq": minor
---

Operator plane: `wf.inspectQueue(runId)` — read-only triage view of a run's in-queue messages (stepId, attempt, readCount, visibleAt) via optional `Queue.inspect()`; implemented for pgmq and the in-memory queue. Together with `wf.describe()` this replaces the hand-run SQL triage recipes; docs/OPERATIONS.md is the runbook.
