---
"@nagi-js/core": patch
---

Hardens handler-step (`b.task`, `b.activity`, `b.streamingTask`) deadline
enforcement:

- A body that honors the abort by returning a value (instead of rethrowing)
  now still fails the step as `NagiStepTimeoutError`, retryable under
  `retry`. Previously the step was recorded as completed, and for a task its
  `ctx.tx` writes committed too.
- A commit error (`store.runStep`) that happens after the handler already
  returned is no longer relabelled as `NagiStepTimeoutError`; only what the
  handler itself throws is unwrapped, so `retryOn` predicates and operators
  see the real error.
- `timeoutMs` outside the range Node's `setTimeout` can represent (below 1,
  non-integer, `NaN`, or above 2,147,483,647ms) now throws
  `NagiValidationError` at `flow()` build time instead of firing the
  deadline on every attempt at runtime. `b.signal`'s `timeoutMs` similarly
  rejects non-finite/negative values (no upper cap — signal deadlines are
  timer rows, not `setTimeout`).
