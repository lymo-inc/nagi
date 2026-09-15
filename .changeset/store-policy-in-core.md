---
"@nagi-js/core": patch
"@nagi-js/postgres": patch
---

Move the remaining `Store` policy into core and make the prose `MUST`
contracts executable.

New pure functions in `@nagi-js/core` (the seam `decideSignal` /
`decideExpiredLeaseAction` already use — core decides, the adapter owns only
its tx boundary): `factEffects` (which leases / timers / concurrency slots a
fact releases), the `queryRuns` cursor codec and limit clamp
(`encodeRunCursor` / `decodeRunCursor` / `clampQueryLimit` / `compareRunOrder`
/ `isPastCursor`), `jsonContains` (reference `@>` semantics for
`where.input`), `selectExpired` (expiry filter before limit for lease and
timer sweeps) and `selectPruneBatch` (prune eligibility, oldest-first
batching). Both adapters call them; the 48 byte-identical cursor lines in
`@nagi-js/postgres` are gone.

`InMemoryStore` now agrees with `postgresStore` where the two had drifted:

- Leases are released on `settleStep`, `runStep`, `settleSignal` delivery,
  signal timeout, `step.canceled` and `step.reset`, driven by `factEffects`
  at the single `appendFact` choke point. `describe()` no longer reports a
  `lease` on a settled step, and `sweepLeases` no longer sees settled steps as
  candidates.
- `sweepLeases` / `sweepSignalTimeouts` filter on expiry before applying
  `limit`, so an expired lease can no longer starve behind `limit` live ones.
- `pruneFacts` drains in batches of `batchSize` (loop until empty), matching
  the Postgres loop.
- `recordOnce` is first-write-wins (was last-write-wins).
- `describe()` returns the retained summary (no steps) for a run pruned with
  `keepSummary: true` instead of `null`.

`postgresStore` applies `factEffects` in `appendFact` and every settle path,
replacing six hand-placed lease deletes. One behavioural change: `settleStep`
/ `runStep` with `step.completed` now also drop a stale signal timer for that
step (previously only the signal-delivery path did). The conformance suite
also caught a real bug: `queryRuns` without a `where.input` filter failed on
Postgres with `could not determine data type of parameter` (an untyped
`$n IS NULL`); the parameter is now cast to `jsonb`.

New subpath `@nagi-js/core/testing` exports `storeContract`, a
framework-agnostic conformance suite (one case per `Store` `MUST`, plus one
per divergence above) and `passthroughSchema`. Run it against any adapter:

```ts
for (const c of storeContract) it(c.name, () => c.run({ makeStore }));
```

`@nagi-js/core` runs it against `InMemoryStore`; `@nagi-js/postgres` runs it
against `postgresStore` in `store-contract.test.ts` (gated on
`NAGI_POSTGRES_TEST_URL`, like the integration suite).
