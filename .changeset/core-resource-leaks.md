---
"@nagi-js/core": patch
---

Fix: a long-lived process no longer accumulates stream-hub channels, and a settled step no longer leaves its cancel-watcher timer behind.

`InMemoryStreamHub` marked a channel closed but never deleted it, and `step.retried` created a channel for every retried step, streaming or not. Since `postgresStore` feeds a hub in every listening process and broadcasts retry and close frames cluster-wide, a long-running Postgres worker grew that map by one entry per retried step and per streaming step, forever. A retry now touches only a channel that already exists, and closing a channel deletes it. The keys of recently closed channels are remembered (at most 1024) so a chunk arriving after its step's close is still dropped rather than reopening a channel nothing would close. Subscribe-after-close and replay behave as before.

The cancel watcher's `stop()` only set a flag, so every finished step left a pending timer for up to one poll interval (250 ms by default) that kept the event loop alive. `stop()` now clears it and ends the poll loop immediately.
