---
"@nagi-js/core": patch
---

`diffSnapshots` (with `SnapshotDiff`, `SnapshotChangedEdge`, `SnapshotChangedField`, `SnapshotChangedFlowField`) and the canonicalization exports (`canonicalize`, `fingerprintFlows`, `sha256Canonical`, `CanonicalDag`, `CanonicalMatchArm`, `CanonicalRetryPolicy`, `CanonicalSchema`, `CanonicalStep`) are removed from the public API. Flow hashing is unchanged, so existing flow hashes and in-flight runs are unaffected.
