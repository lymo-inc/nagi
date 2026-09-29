---
"@nagi-js/core": patch
---

A canceled step (a `replay({ from })` abort, run cancel, or concurrency
supersession) now fires the global `onStepError` hook with an
`AbortError`/cancellation error, so every consumer sees each step end exactly
once. A body that returns normally after its run was canceled no longer fires
`onStepComplete` — it is recorded as `step.canceled` and fires `onStepError`
instead. The step-level `onError` handler is unaffected; it still only runs
for real failures, not cancellations.
