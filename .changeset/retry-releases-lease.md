---
"@nagi-js/core": patch
---

A retried attempt now releases its lease, so a retry backoff longer than the
lease period is honoured and no spurious `lease.reaped` is written.

A message for an attempt the step has moved past is now dropped at admission
instead of re-running the handler next to the current attempt.
