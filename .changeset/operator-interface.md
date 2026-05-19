---
"@nagi-js/core": patch
"@nagi-js/postgres": patch
---

`wf.operator()` — programmatic skip/retry/abort for oncall. The three
primitives an operator needs when a run is stuck no longer require
direct-editing `nagi.fact` / `nagi.step_run`:

- `operator.skip(runId, stepId, { actor, note?, cascade? })` — appends
  `step.skipped` with `reason: "manual"`. `cascade: "skip"` (default)
  keeps the locked transitive semantic. `cascade: "continue"` lets
  downstream steps run with `needs.x === null` for the skipped need —
  the handler is responsible for tolerating null; the type contract on
  `needs.x` is unchanged.

- `operator.retry(runId, stepId, { actor, note? })` — re-runs `stepId`
  and its descendants. For terminal steps, mirrors
  `wf.replay({ from })` with `actor` / `note` stamped onto the named
  `step.reset`. For a `running` step, appends
  `step.abort-requested`; the dispatcher's cancel watcher fires
  `ctx.signal.abort()` cross-process; the in-flight attempt
  reclassifies as `step.canceled`; then the reset cascade lands and
  re-dispatches.

- `operator.abort(runId, { actor, note? })` — cancels the run with
  `cause: "operator"`, structured `actor` / `note`. Cascades to subflow
  children. In-flight handlers see `ctx.signal.abort()` via the watcher.

Active cancel watcher landed alongside: `executeTask` now polls
`store.loadRunState` at `DispatchDeps.cancelPollIntervalMs` (default
250 ms) and aborts `ctx.signal` when the run reaches terminal status OR
a matching `step.abort-requested` fact appears. Handlers that pass
`ctx.signal` to `fetch` / `anthropic.messages.create({ signal })` now
interrupt mid-call, not just at the boundary.

Fact-log changes (additive, no migration):

- `StepSkippedFact.reason` widens to `"when-false" | "transitive" | "manual"`,
  with optional `actor` / `note` / `cascade` populated on manual skips.
- `FlowCanceledFact` gains optional `cause: "concurrency" | "explicit" | "operator"`
  and optional `actor` / `note`. `wf.cancel()` records `cause: "explicit"`,
  keeping back-compat via the existing `concurrencyKey`-as-reason carrier.
- `StepResetFact` gains optional `actor` / `note` (populated by
  `operator.retry`; `wf.replay({ from })` leaves them undefined).
- New `step.abort-requested` fact kind for the operator-issued
  per-step abort signal. Audit-only; the cancel watcher reads it and
  reclassifies the in-flight attempt.

`@nagi-js/postgres`: zero migration. All new fields ride through the
existing `payload jsonb` column; `fact.kind` has no CHECK constraint.
