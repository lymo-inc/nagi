---
"@nagi-js/core": patch
---

`b.match` is removed, along with its types (`MatchArm`, `MatchArmGuard`, `MatchArmOtherwise`, `MatchArmOutput`, `MatchGuardConfig`, `MatchArmSelectedFact`, `CanonicalMatchArm`), the `"match"` step kind and `RunState.selectedArms`. Branch with a step-level `when:` guard instead; a step whose guard is false is skipped as before. Flow hashes of flows that never used `match` are unchanged, so in-flight runs keep their pinned snapshots.
