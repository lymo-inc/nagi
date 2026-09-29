---
"@nagi-js/core": patch
"@nagi-js/postgres": patch
---

A start's first steps are now enqueued inside the start's own transaction,
alongside the run row and `flow.started` fact, instead of after it commits.
Previously a crash, deploy or queue error between that commit and the
post-commit enqueue left a `running` run with nothing queued, no lease and no
timer — never picked up by any worker.

`Store.tryStartRun` takes an optional `seed` (`{ queue, flowId, steps }`) as
its last parameter; a custom `Store` implementation should enqueue `seed`'s
steps in the same transaction as the start, only when `started` is true.

Because the seed is visible to workers as soon as the start commits, a worker
can dequeue and start a root step before the starting process fires
`onFlowStart`. A custom `onStepStart` hook can therefore fire before the
run's `onFlowStart`; hooks that correlate the two must not assume the flow
event comes first.

A subflow re-attach (a redelivered `sub` step meeting an already-started
child) now re-seeds the child via `dispatcher.advance`, so a child whose first
message was lost before this fix is recovered instead of staying stranded.
