---
"@nagi-js/core": patch
---

A canceled step now closes its stream. Before, a step aborted by `replay({ from })` whose abort wait timed out stayed `canceled` on a live run, and a live `wf.subscribe` iterator on it stayed open until the step was replayed or the run ended. It now ends as soon as the step is canceled, as it does on completion. A later `replay({ from })` streams the rerun to new subscribers.
