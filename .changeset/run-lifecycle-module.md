---
"@nagi-js/core": patch
---

Run lifecycle (start / supersede / cancel / spawn-child) now lives in one
internal module with a single "start a run" path. `wf.startById`,
`wf.startStagedById`, and subflow child spawning are thin callers over it; the
own-tx vs caller-tx choice is resolved at exactly one fork, and the post-commit
effects (superseded-run hooks + parent propagation, the start event, the
initial dispatch) are a value applied by one function — immediately, or from
`applyOnCommit`.

Two staged-start (`wf.startStaged`) gaps close along the way:

- Roots that are all gated (`when: () => false`) used to enqueue nothing and
  leave the run parked; the run now advances from `applyOnCommit`, as
  `wf.start` always did.
- A mix of runnable and gated roots still enqueues the runnable roots on the
  caller's tx, exactly as before; the gated siblings' `step.skipped` facts are
  now recorded from `applyOnCommit` instead of waiting for the next advance.

`applyOnCommit` memoizes its first invocation: later calls are no-ops on
success, and re-throw the same rejection if that first application failed
(previously a second call after a failure resolved silently).

No public API or fact-shape changes.
