---
"@nagi-js/core": patch
---

A signal step's buffered early signal is now cleared when it is delivered and
when the step is reset (`operator.retry`, `wf.replay({ from })`). Previously
the first buffered payload lived on the projection forever: a reset signal
step completed instantly with the stale payload, and a genuinely new
`wf.signal` for that step was reported as buffered without writing a fact.
