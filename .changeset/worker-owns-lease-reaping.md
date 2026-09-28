---
"@nagi-js/core": patch
---

The worker now owns lease reaping alongside the signal-timeout sweep, on its own clock, store and queue, so workers built with `wf.worker()` outside `nagi.run` reap a crashed peer's expired leases too. Configure it with the new `WorkerConfig.reaperIntervalMs` (default 30s, `0` disables); `NagiConfig.reaperIntervalMs` is deprecated and still honored as its fallback.
