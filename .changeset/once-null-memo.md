---
"@nagi-js/core": patch
"@nagi-js/postgres": patch
---

Fix: `ctx.once()` now memoizes a callback that returns `null`, so its side effect no longer re-runs on every redelivery or replay of the step.

`Store.getOnce` returned `Json | null` and used `null` to mean "nothing recorded". `Json` includes `null`, so a recorded `null` was indistinguishable from a miss and `once()` called the callback again — exactly the duplicate side effect it exists to prevent.

`Store.getOnce` now returns `GetOnceResult` (`{ tag: "hit", value }` or `{ tag: "miss" }`), and a recorded `null` reads back as a hit. The Postgres `dedupe` table is unchanged; no migration is needed.

Custom `Store` implementations must update `getOnce` to return the new shape. The shared conformance suite in `@nagi-js/core/testing` checks it.
