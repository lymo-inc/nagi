---
"@nagi-js/core": patch
"@nagi-js/pgmq": patch
---

Runtime bootstrap ergonomics (RFC 0013, #17) — three additive, backwards-compatible changes.

- **`nagi.run({ ... }) → { wf, stop }`** — a turnkey worker lifecycle. Collapses
  the four-step `nagi()` + `new AbortController()` + `wf.worker({ signal })` +
  `worker.run().catch(graceful-vs-crash)` bootstrap (and its three module-level
  refs) into one call with a single idempotent `stop()` that aborts an internal
  controller and awaits the loop. Graceful shutdown resolves cleanly; only a true
  loop crash is logged once via the configured `logger`. Accepts an optional
  external `signal`, merged with the internal controller via `AbortSignal.any`.
  The existing `nagi()` + `wf.worker()` + `worker.run()` path is unchanged.

- **Auto queue-schema bootstrap** — the `Queue` contract gains an optional
  `ensureSchema?(): Promise<void>`. `nagi()` awaits it once at construction
  (eager, fail-fast), closing the "runtime error on first enqueue" trap when the
  pgmq queue schema was never provisioned. Adapters without the hook (e.g. the
  in-memory queue) are unaffected.

- **`pgmqQueue<DB>`** — `pgmqQueue` and `PgmqQueueOpts` are now generic over the
  Kysely schema (`db: Kysely<DB>`, defaulting to `unknown`), erasing the
  `db as unknown as Kysely<unknown>` cast at every callsite. Pure typing change;
  runtime is byte-identical.
