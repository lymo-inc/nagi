---
"@nagi-js/core": patch
---

`NagiNonRetryableError`: throw from a step to fail immediately, skipping the remaining retry budget. Honored anywhere on the cause chain.
