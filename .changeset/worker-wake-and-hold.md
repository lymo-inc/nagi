---
"@nagi-js/core": patch
---

An idle worker now starts work as soon as this process enqueues it, instead of sleeping out `pollIntervalMs`. That covers step hand-offs, `wf.start`, `applyOnCommit`, signals, replays and lease reaps. A finished step also wakes the worker at once. A message over `maxConcurrencyPerFlow` is held in memory and runs as soon as a slot of its flow frees. It used to be nacked and read again about once a second. The worker holds at most `concurrency` such messages, keeps them invisible on the heartbeat cadence, and hands them back on shutdown. Past that limit it falls back to the delayed nack.
