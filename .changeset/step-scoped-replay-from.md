---
"@nagi-js/core": minor
"@nagi-js/postgres": patch
---

`wf.replay(runId, { mode, from })` now supports step-scoped replay. Pass
`from: stepId` to reset that step and every transitive descendant — completed
steps downstream of `from` re-run, completed steps upstream are preserved.
This is the primitive for "re-run just this tab" affordances on already-
completed runs; previously the only retry path was the default replay from
the first incomplete step, which was a no-op on a completed run.

A new `step.reset` fact is appended for `from` and one per cascaded
descendant. Cascade follows two edges: forward `needs` (anything reading the
reset step's output) and match-arm membership (resetting a match step
invalidates the prior arm selection and re-runs every step in every arm).
Sibling arm steps do not cascade across each other; resetting an arm step
does not reset the parent match.

The `step.reset` fact carries an optional `cascadedFrom: StepId` field on
descendants — the user-named step has `cascadedFrom === undefined`,
runtime-emitted cascades record the originating `from` step so read-side UIs
can group them.

Validation: `from` must reference a step in the effective flow (snapshot
topology under `allowDrift`, else live) — unknown ids throw
`NagiValidationError`. Calling `replay({ from })` on a still-running run
throws `NagiRuntimeError`; reset mid-flight races in-flight workers. `from`
is ignored under `mode: "inspect"`.

`@nagi-js/postgres`: `appendFact` now clears the materialized `step_run` row
and releases the lease for the reset step so the next dispatch can re-claim
and re-execute at the same attempt.
