---
"@nagi-js/core": patch
---

Every path that needs a run's flow (dispatch, `wf.signal`, the operator, cancel, concurrency supersede and `replay`) now resolves it through one drift-aware resolver, checking step names against the run's pinned flow rather than the live one. User-visible changes:

- A signal to a step that exists only in the live flow, not the run's pinned one, is rejected.
- Operator `skip`/`retry` under the `"freeze"` drift policy throws before writing anything.
- Cancelling an unpinned run whose flow is not registered throws.
- `replay({ allowDrift })` no longer resolves a parent it wakes to the child's synthesized flow.
- `replay({ fireHooks: false })` dispatches only the messages its own replay enqueued, never another run's, and the children it spawns stay quiet too. It runs on a private in-memory queue, so a crash mid-replay needs `replay({ mode: "continue" })` to recover.
