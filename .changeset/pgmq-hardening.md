---
"@nagi-js/pgmq": patch
---

Fix: `pgmqQueue()` now validates `queueName` at construction, and `dequeue()` reads `msg_id` as text.

The queue name must match `/^[A-Za-z0-9_]{1,47}$/` — pgmq's own client rule, with 47 as pgmq's maximum queue-name length. `inspect()` splices the name into an unquoted `pgmq.q_<name>` identifier, so a name outside that set (a hyphen, a quote, a `;`) either broke `inspect()` or reached the SQL unescaped. Such names now throw from `pgmqQueue()` before any SQL runs.

`msg_id` is a `bigint`; reading it as `::text` keeps receipts exact even when the application installs a pg type parser that turns int8 into a JS `number`, which loses precision past 2^53.
