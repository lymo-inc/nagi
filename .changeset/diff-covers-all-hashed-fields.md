---
"@nagi-js/core": patch
---

Fix: `diffSnapshots` now reports every hashed field. Changes to a signal's `names`, a subflow's child flow, or a subflow's input builder flipped the snapshot hash but produced an empty diff; they now surface as `signalNames`, `childFlowId`, and `subflowInput` in `changedPredicates`. Flow-level changes to the input schema or flow id now surface in the new `SnapshotDiff.changedFlowFields` (`"inputSchema"`, `"flowId"`).
