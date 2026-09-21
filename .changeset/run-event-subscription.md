---
"@nagi-js/core": patch
"@nagi-js/postgres": patch
---

Live run-event subscription (nagi#16 Layer 1). `wf.watchRun(runId, handler)` and `wf.watchRuns(handler)` push `RunEvent`s — flow started/completed/failed/canceled, step started/completed/failed/retried/skipped — as the facts that produce them commit. Both return a disposer; `watchRun` also disposes itself once the run reaches a terminal event, so watching many runs does not leak a handler each.

The transport lives on the **Store** (`Store.events`), not in core, and that placement is forced rather than stylistic: `flow.canceled(cause: "concurrency")` and `flow.started` are minted inside the adapter, in the same transaction as the row writes. A core-side decorator would silently miss supersession — a terminal event, and one of the most important things an observer wants. Both adapters now publish from where those facts are actually written, and the shared fan-out (`InMemoryRunEventHub`) is exported from core so neither reimplements subscriber bookkeeping.

On Postgres, events ride the fact's transaction: PostgreSQL holds a `NOTIFY` until commit, so an observer hears about a fact only once it is durable, and a rolled-back attempt announces nothing. `postgresStore()`'s `listener` option now powers both this and streaming chunks — one LISTEN connection, two channels.

Not durable by design: a handler sees events from the moment it subscribes, and a restart starts over. Pair with `describe()` / `queryRuns()` to catch up. `watchRuns` does not filter — a live stream cannot honestly filter on `status` (the event *is* the status change) and filtering on `input` would need a state load per event.

Stores without the transport throw from `watchRun` / `watchRuns` rather than returning a subscription that silently never fires. On Postgres the events channel shares the store's one LISTEN connection, so `await store.ready()` before starting a run you mean to watch — events published before the socket is live are lost, not delayed.
