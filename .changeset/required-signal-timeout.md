---
"@nagi-js/core": minor
---

BREAKING: `b.signal` now requires `timeoutMs: Millis | "unbounded"` — unbounded parking must be an explicit opt-in, never an omission. `"unbounded"` canonicalizes as omission, so existing flows keep their hashes (and in-flight runs) when migrating a timeout-less signal to `"unbounded"`. Also BREAKING: the unenforced `timeoutMs` knob is removed from task/activity/streaming/subflow configs (it armed nothing; a flow that set it changes hash on upgrade). New lease-hold watchdog: `leaseHoldWarnMs` (default 5 min, 0 disables) warns whenever a step body holds a worker slot past each threshold multiple.
