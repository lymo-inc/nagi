---
"@nagi-js/postgres": patch
---

`migrate()` no longer waits forever. It waits up to `lockWaitMs` (default 60 s) for another process's migrate lock, then rejects and names the holding session: pid, application_name, state and connect time. Each migration runs with `lock_timeout` set to `lockTimeoutMs` (default 5 s). A DDL that would queue behind a long transaction, and stall every later query on that table, fails the boot instead. The failed migration is rolled back and is applied on the next try.
