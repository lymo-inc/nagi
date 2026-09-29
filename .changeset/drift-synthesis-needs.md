---
"@nagi-js/core": patch
---

Fix drift synthesis (`driftPolicy: "synthesize"` / `replay({ allowDrift })`) rebuilding a step's `needs` in the wrong shape — bare step objects keyed by upstream id, instead of `{ step, optional }` records keyed by the handler's local alias. Every consumer read `ref.step.id`, which was `undefined` on the bare shape, so the first `advance` of any drifted run whose flow had at least one edge threw a raw `TypeError` outside `NagiRuntimeError`, skipping the snapshot-gone fallback and nack-looping forever. `needs` is now rebuilt from the live handler's `needs` map, validated against the pinned snapshot's edges and step kind; a live flow whose edges or step kinds changed for a pinned step now falls back to snapshot-gone handling instead of crash-looping.
