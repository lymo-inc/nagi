---
"@nagi-js/core": patch
"@nagi-js/postgres": patch
---

`b.subflow(child, { input })` now embeds another flow as a step. The child
runs as an independent run on the same store/queue; the parent's subflow
step parks in `running` until the child reaches terminal state, then resumes
with `step.output = { childRunId, output }`. The wake-up mechanism mirrors
`wf.signal()` — child's `flow.completed` / `flow.failed` writes the parent's
`step.completed` / `step.failed` directly via the `finalizeFlowCompletion` /
`finalizeFlowFailure` hooks; no parent-side dispatch re-trip.

`wf.cancel(runId, opts?)` is now a public API. It writes `flow.canceled` to
the run, transitively cancels every child run spawned via `b.subflow()`, and
surfaces the cancellation to a higher parent (if any) as a `step.failed`
with a structured `NagiCanceledError`. Idempotent on already-terminal runs.

Child flows must be passed explicitly to `nagi({ flows: [parent, child] })` —
referencing an unregistered child throws at dispatch with an actionable error.
Parent linkage is recorded on the child's `flow.started` fact via two new
optional fields `parentRunId` + `parentStepId`, and on the in-memory Store
via a `parent → children` index that backs `Store.listChildren(parentRunId)`.

Postgres adapter migration `0005_subflow_parent_link` adds
`workflow_run.parent_run_id` + `workflow_run.parent_step_id` columns with a
partial btree index on `parent_run_id WHERE parent_run_id IS NOT NULL`. The
index backs the cancel cascade query and stays empty for non-subflow runs.

Sibling cancellations triggered by a child's own `concurrency` config now
propagate to that sibling's parent's subflow step (no more silent hangs when
two children share a concurrency key). Canonical flow hash gains
`childFlowId` + `subflowInputHash` fields for subflow steps; pre-existing
flows hash byte-identically.

Replay-memo for subflow children is intentionally out of scope for this
release — a parent replay will re-execute its child rather than memoizing
the prior child's output. Handler idempotency at the child level is the
correctness story for now; explicit memoization will land in a follow-up
RFC. See `docs/research/issue-10-subflow-runtime.md`.
