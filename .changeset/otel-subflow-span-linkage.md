---
"@nagi-js/core": patch
"@nagi-js/otel": patch
---

`@nagi-js/otel` now nests a subflow child flow's root span under its
parent's subflow-step span, producing a single contiguous trace tree from
the top-level run down through arbitrary subflow depth. Previously each
child run rooted its own trace, leaving operators to correlate parent and
child via `nagi.run.id` / `nagi.parent.run.id` attributes by hand.

`FlowStartEvent` gains an optional `parent: { runId, stepId, attempt }`
struct (single optional, not three — by construction either all three
parent fields are set or none are). `nagi()` populates it on every
subflow-spawned run; top-level runs leave it `undefined`. The struct is
in-process only; durable parent linkage continues to live on
`FlowStartedFact.parentRunId / parentStepId` unchanged.

`otelHooks.onFlowStart` resolves the child flow span's parent context via
a three-step fallback: parent step span in registry → local parent flow
context (`flowCtxs`) → OTel `context.active()`. Never throws. Records
`nagi.parent.run.id`, `nagi.parent.step.id`, `nagi.parent.step.attempt`
as attributes on the child flow span when `event.parent` is set so
cross-process linkage remains queryable even when the in-process anchor
is missing (process restart, replay).

Top-level run traces are byte-equivalent to prior behavior. See
`docs/rfcs/0010-otel-subflow-span-linkage.md`.
