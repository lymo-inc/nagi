---
"@nagi-js/core": patch
---

`advance()` no longer queues a step that already has a message in the queue. Before, every settle re-queued each sibling still waiting for a worker slot, so a wide fan-out under `maxConcurrencyPerFlow` sent several messages per step and every extra copy cost a read, a fold and an ack. Lymo's prod saw 2.06 messages per started step; a 14-step fan-out on Postgres + pgmq with `concurrency: 7, maxConcurrencyPerFlow: 3` sent 7.6–8.9 and now sends 1.00. The check uses `Queue.inspect`; a queue without it keeps the old behaviour. `replay({ from })` still queues the reset step, since the old attempt's message will not run it. `Dispatcher.advance` takes an optional `{ requeue }`.
