---
"@nagi-js/core": patch
---

`nagi({ driftPolicy })` — what a worker does with a live run pinned to a flow
hash this code no longer produces (a deploy changed the flow mid-run).

- `"freeze"` (default, today's behavior): the run is snapshot-gone — nacked
  for a frozen-version worker per `snapshotGonePolicy`, then terminally failed.
- `"synthesize"`: the run continues on the new code — the pinned DAG shape
  with the live handlers attached, exactly what `replay({ allowDrift: true })`
  already did, now at dispatch too. Falls back to snapshot-gone handling (with
  a `warn` log, `drift synthesis failed`) only when the snapshot is missing or
  a pinned step no longer exists live. Under `"synthesize"`,
  `replay({ mode: "continue" })` no longer needs `allowDrift`.

Motivation: `allowDrift` replay only synthesized the flow inside the replay
dispatcher. The step it enqueued was then dequeued by the ordinary worker,
which resolved the run by its pinned hash and went straight back to
snapshot-gone — so on a one-task rolling deploy (no frozen-version worker
ever exists) every deploy stranded every in-flight run, and the orphan
sweeper's replay looped every 5 minutes until the redelivery budget failed
the run. Observed 2026-09-16.

Also: synthesized flows are now cached per pinned hash for the life of the
process.
