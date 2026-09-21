---
"@nagi-js/postgres": patch
---

Fix: `uuidv7()` is now monotonic within a millisecond, so the fact log cannot come back out of order.

`fact_id` is a uuidv7 and `loadRunState` reads facts with `ORDER BY fact_id ASC`, which makes fact_id order *the* append order that `foldRun` replays a run from. The previous implementation filled all 74 non-timestamp bits with fresh randomness, so two facts written in the same millisecond sorted at random — a run could fold with `step.started` ahead of its own `flow.started`. This surfaced as an intermittent store-conformance failure against real Postgres.

RFC 9562's 12-bit `rand_a` now carries a per-millisecond counter, seeded in its low half so at least 2048 ids per millisecond are guaranteed ordered, and saturating rather than wrapping past that (wrapping would sort an id before the one it follows). `rand_b` stays fully random, so uniqueness never depends on the counter having headroom.
