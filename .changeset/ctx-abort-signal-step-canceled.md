---
"@nagi-js/core": patch
"@nagi-js/postgres": patch
---

`ctx.signal` and a new `step.canceled` fact kind for cancel-aware step
classification. `StepCtx.signal: AbortSignal` is now constructed per step run
and threaded into handlers — pass it to `fetch` or
`anthropic.messages.create({ signal })` so handlers can be composed with
user-supplied timeout signals (`AbortSignal.any([ctx.signal, AbortSignal.timeout(60_000)])`).
The wakeup that fires `ctx.signal.abort()` on `cancel-in-progress` is a
follow-up; today the signal aborts only when the user composes it.

When a run transitions to `canceled` while a step is in flight (a newer
`wf.start()` superseded it via a concurrency group), the dispatcher
reclassifies the step at the boundary:

- **Handler returns normally on a canceled run** → records `step.canceled`
  instead of `step.completed`. Domain writes from the handler still commit
  atomically with the canceled fact; read-side projection no longer leaks
  a "completed" status onto a canceled run.
- **Handler throws on a canceled run** → records `step.canceled` instead of
  `step.failed`. Retry is suppressed (the run is terminal) and `onStepError`
  does not fire — it's a relabel, not an error. `AbortError`-shaped throws
  preserve the error on the canceled fact for downstream observability.

`FactKind` gains `"step.canceled"`; `StepStatus` gains `"canceled"`;
`Fact` widens to include `StepCanceledFact` (optional `error` field).
`Store.runStep`'s body return type widens to accept
`StepCanceledFact` so adapters can record the boundary classification
atomically with the handler's transaction.

`@nagi-js/postgres`: new migration `0006_step_canceled_status` widens the
`step_run.status` CHECK constraint to accept `'canceled'`. Existing rows are
unaffected; the constraint is reapplied with the added value.
