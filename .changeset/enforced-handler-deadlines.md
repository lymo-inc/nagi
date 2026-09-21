---
"@nagi-js/core": patch
---

`timeoutMs` returns to task / activity / streaming steps, this time ENFORCED. At the deadline the step's `ctx.signal` aborts with a `NagiStepTimeoutError` and the step settles as failed, retryable under its own `retry` policy — a slow upstream gets another attempt, a genuinely stuck step exhausts `maxAttempts` and fails the run.

A deadline is deliberately not a cancellation: `classifyFailure` reads cancellation from the persisted `step.abort-requested` fact and the run phase, neither of which a deadline writes, so a timed-out step reaches the normal failure path instead of settling as `canceled`. The abort *reason* carries the discriminator, so a handler that throws its own error on abort (fetch, an SDK) is still recorded as `NagiStepTimeoutError` rather than a generic `AbortError`.

Enforcement is cooperative — nagi aborts the signal and lets the body unwind, because a task's handler runs inside `store.runStep`'s transaction and abandoning it mid-flight would strand that tx. A handler that never checks `ctx.signal` still holds its slot; `leaseHoldWarnMs` remains the detector for that.

On a streaming step the deadline also closes its subscribers: the `wf.subscribe` iterator ends with `{ kind: "error" }` carrying the timeout, rather than hanging on a generator that stopped producing.

`timeoutMs` participates in the flow hash, like the signal-step timeout: changing a deadline changes run semantics, so it changes the flow.
