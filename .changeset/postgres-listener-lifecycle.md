---
"@nagi-js/postgres": patch
"@nagi-js/core": patch
---

`postgresStore()` now returns a `close()` that stops both LISTEN channels
(idempotent, tolerant of a LISTEN that never succeeded).

`StreamListener.listen` gains an optional third parameter, `onReconnect`: a
listener that re-`LISTEN`s after losing its connection should call it once
re-`LISTEN` succeeds. The store then re-checks every stream still open
against durable state and ends the ones whose step has settled — healing an
iterator that would otherwise hang forever because its close frame fell in
the reconnect gap.

`@nagi-js/core`'s `InMemoryStreamHub` gains `openStreams()`, listing open
channels by `(runId, stepId)`. `closeRun` now matches channels by their
stored run id instead of a `` `${runId}::` `` key prefix, fixing a latent bug
where `closeRun("a")` also closed channels belonging to a run literally named
`"a::b"`.
