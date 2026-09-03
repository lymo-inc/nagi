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

Behavioral fix riding along: a staged start (`wf.startStaged`) whose first
transition is not a plain dispatch (a `when: () => false` root step) used to
enqueue nothing and leave the run parked; it now advances the run from
`applyOnCommit`, exactly as `wf.start` always did.

No public API or fact-shape changes.
