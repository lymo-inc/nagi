---
"@nagi-js/core": patch
---

`wf.startById(flowId, input, opts?)` — registry-aware dispatch for callers
that hold a runtime-typed input rather than the original `Flow` object.
Intended for transactional-outbox reconcilers, queue consumers replaying a
DLQ, and admin CLIs that replay a `runId` from disk. Validates `input`
against the registered flow's schema before the run is created, mirroring
`start`'s runtime contract without requiring a compile-time-typed input.

Throws `NagiRuntimeError` when `flowId` is not registered with `nagi()`,
and `NagiValidationError` when the input fails the flow's schema or when
`opts.runId` is invalid.

Motivation: callers with a serialized payload (`pending_workflow_run`-style
outbox rows, pgmq DLQ entries) were forced to launder both arguments
through `as any` because `start<F extends Flow>(flow: F, input: FlowInput<F>)`
demands a statically-known input type — even though the runtime already
re-validates against `flow.input` unconditionally. `startById` exposes the
runtime contract directly.

`start(flow, input, opts)` now delegates to `startById(flow.id, input, opts)`
internally; behavior is observably unchanged.
