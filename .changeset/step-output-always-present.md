---
"@nagi-js/core": patch
---

`StepState.output` is now always present (`Json`) instead of optional
(`Json | undefined`). Non-completed step states (running, failed, canceled,
skipped) carry `output: null`; completed steps carry their value. This removes
the ambiguity between "step produced no output" and "step produced `null`" —
both were already collapsed to `null` at every read via `?? null`, so observable
behavior is unchanged. `projectRunState` populates the field for every step
state it emits.

Reading `state.output` is now total (no `undefined` check needed). The only
affected callers are ones that hand-constructed `StepState` literals, which must
now supply `output`.
