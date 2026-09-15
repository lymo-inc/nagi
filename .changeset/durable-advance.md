---
"@nagi-js/core": patch
---

A run whose step settled but whose follow-up `advance` was lost (worker crash
or store error between the terminal fact and the next enqueue) now self-heals:
redelivery of the step's message re-drives the run instead of being dropped,
and the message is acked only after the advance succeeds.
