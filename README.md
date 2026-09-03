# nagi

> 凪 — the calm when the wind dies down and the sea becomes glassy.

Type-safe, declarative workflow library for TypeScript. Define DAGs as code; the runtime owns retry, idempotency, and durability so handlers stay focused on domain logic.

## Waiting on external facts

A step that waits for something outside the run — a webhook, a row another
service writes, a transcript settling — must be a `b.signal` step, never an
in-step poll loop. A parked signal holds **no worker slot**; a polling task
body holds its slot for the whole wait, and a handful of them can starve the
entire worker pool (this has caused a multi-day production outage).

Two guardrails enforce this:

- `b.signal` requires `timeoutMs` — a deadline that fails the step as
  `NagiSignalTimeoutError`, or the explicit `"unbounded"` opt-in. There is no
  default: parking forever must be a written decision.
- The lease-hold watchdog (`leaseHoldWarnMs`, default 5 min) warns every time
  a step body holds a worker slot for another threshold multiple, so an
  accidental in-step wait announces itself long before the pool wedges.

For inputs that can never succeed (an orphaned webhook with no owning row),
throw `NagiNonRetryableError` — the step fails immediately instead of burning
its retry budget reaching the same terminal state.

## License

[MIT](./LICENSE)
