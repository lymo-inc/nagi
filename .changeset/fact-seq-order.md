---
"@nagi-js/postgres": patch
---

Fix: facts now fold in insert order across processes, and id generation no longer goes backwards when the clock does.

`loadRunState` read facts by `fact_id`, a uuidv7 minted by the writing process's clock, so a worker with a lagging clock could write a causally later fact that sorted earlier. Facts now carry a `seq` from a database sequence and are read `ORDER BY seq ASC NULLS FIRST, fact_id ASC`; pre-existing rows (NULL `seq`) keep `fact_id` order and precede every new fact. Run `migrate()` to apply the new migration `0011_fact_seq`. `uuidv7()` also holds its last timestamp when the clock steps back instead of re-seeding at a smaller one.
