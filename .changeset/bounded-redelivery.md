---
"@nagi-js/core": patch
---

No message is read more than 10 times (`MAX_DELIVERIES`). A dispatch that throws on its 10th delivery now fails the run with the typed `NagiDeliveryExhaustedError`, which names the flow, step, run, attempt and delivery count and keeps the last error as `cause`. Before, it redelivered forever on a capped backoff. `defaultSnapshotGonePolicy` retries once a minute and fails on the 10th delivery: about 9 minutes, where it used to be 60 deliveries over about 4.2 hours. `snapshotGoneBackoff` is removed. `serializeError` now keeps the error's `cause` chain, 3 levels deep, without stacks.
