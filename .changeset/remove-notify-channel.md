---
"@nagi-js/postgres": patch
---

Remove `postgresStore({ notifyChannel })`. It was the write side of `postgresTrigger`, which was removed in #55, and nothing in nagi listened to it. To react to run changes from another process, pass `listener` and use `wf.watchRun` / `wf.watchRuns`. Setting `notifyChannel` is now a type error; at runtime it is ignored.
