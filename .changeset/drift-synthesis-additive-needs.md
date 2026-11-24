---
"@nagi-js/core": patch
---

Drift synthesis (`driftPolicy: "synthesize"` / `replay({ allowDrift })`) no longer refuses a pinned run whose live step dropped a need or added an **optional** need on a step the snapshot declares. Previously any edge difference fell back to snapshot-gone and failed the run, so adding `optional(x)` to a step stranded every in-flight run on deploy. Adding a **required** need still falls back to snapshot-gone.
