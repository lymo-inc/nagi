---
"@nagi-js/core": patch
---

Task, activity and streaming steps now share one internal def and one execute path, and step-kind branches are exhaustive switches; authoring APIs, `Step.kind` values, persisted `stepKind` and flow hashes are unchanged. `ActivityConfig`, `ActivityCtx` and `StepLifecycleHooks` are now exported, `MatchArmShape` is removed in favour of `MatchArm` (identical shape), and `RunState` gains `resetCounts`, the per-step count of `step.reset` facts.
