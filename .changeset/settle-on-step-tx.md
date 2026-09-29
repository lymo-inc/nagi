---
"@nagi-js/core": patch
"@nagi-js/postgres": patch
---

Fix a pool deadlock: a task/streaming step body runs inside `store.runStep`'s
transaction, which holds one pool connection. After the body returns, core's
`settle` used to call `store.loadRunState(runId)` on the **pool**, so each
in-flight step needed a **second** connection while holding the first. With
worker concurrency ≥ pool max (or several workers sharing one pool), every
connection could end up held by a step's transaction whose `settle` was
waiting on another connection that would never free — a permanent deadlock,
since `pg.Pool`'s default `connectionTimeoutMillis` is 0 (wait forever).

`settle` now reads run state on the step's own transaction instead, which
sees the same committed facts (READ COMMITTED) without needing a second
connection.

`Store.loadRunState` gains an optional second parameter:
`loadRunState(runId: RunId, tx?: Tx): Promise<RunState>`. Custom stores
should honour `tx` when supplied — read on that transaction rather than the
pool — to get the same fix.
