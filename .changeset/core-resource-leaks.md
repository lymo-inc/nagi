---
"@nagi-js/core": patch
---

Fix: a long-lived process no longer accumulates stream-hub channels.

`InMemoryStreamHub` marked a channel closed but never deleted it, and `step.retried` created a channel for every retried step, streaming or not. Since `postgresStore` feeds a hub in every listening process and broadcasts retry and close frames cluster-wide, a long-running Postgres worker grew that map by one entry per retried step and per streaming step, forever. A retry now touches only a channel that already exists, and closing a channel deletes it. The keys of recently closed channels are remembered (at most 1024) so a chunk arriving after its step's close is still dropped rather than reopening a channel nothing would close. Subscribe-after-close and replay behave as before.
