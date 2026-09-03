# Operations runbook

Triage and recovery for nagi runs, using the built-in API — no hand-run SQL.
Every recipe here previously existed only as incident folklore; the API calls
are the supported path.

## Triage a stuck run

Start with two calls:

```ts
const desc = await wf.describe(runId); // run + steps + lease state
const q = await wf.inspectQueue(runId); // in-queue messages for the run
```

| describe() says                   | inspectQueue() says            | Diagnosis                                                        |
| --------------------------------- | ------------------------------ | ---------------------------------------------------------------- |
| running, 0 started steps          | entry with `readCount: 0`      | Never scheduled — workers starved or not consuming               |
| running, step has live `lease`    | entry with future `visibleAt`  | Actively executing (slow handler — watch for watchdog warns)     |
| running, signal step `running`    | no entries                     | Parked on a signal/child — check the signal source, not the pool |
| running, no lease, no entries     | —                              | Advance was lost — `operator().retry(runId, stepId)` re-drives   |
| any status                        | entry with high `readCount`    | Redelivery loop — see poison messages below                      |

## Nothing is being consumed (fleet-wide stall)

Every flow stuck at once, `inspectQueue` entries all at `readCount: 0`, no live
leases: the worker loop itself is not polling. `worker.run()` settles **only**
via its abort signal — queue failures are logged as
`worker.dequeue failed; backing off` and retried (exponential from
`pollIntervalMs`, capped at 30s, reset on success), so a database blip can no
longer end it.

If the loop does exit, something outside that guard threw. `nagi.run()` logs
`nagi.run: worker exited unexpectedly`; it does **not** restart the loop, so
supervise it:

- Page on `nagi.run: worker exited unexpectedly` (or restart `worker.run()` in
  a loop if you drive the worker yourself).
- Alert on oldest-unread-message age in the queue. That catches any
  consumption stall, whatever the cause — including ones no log line covers.

Sustained `worker.dequeue failed; backing off` at `error` level means the queue
is unreachable, not that runs are broken; the loop resumes on its own once the
queue comes back.

## Poison messages / snapshot-gone (deploy replaced an in-flight flow)

Self-healing since the snapshot-gone policy: the worker retries with backoff
for the rolling-deploy window, then **terminally fails the run** with
`NagiFlowSnapshotGoneError` in `workflow_run.error` and acks the message.
Nothing loops forever.

Intervene earlier if needed:

- `wf.cancel(runId)` — fact-only, bypasses the flow registry, and the
  dispatcher acks a terminal run's messages on next delivery. One call; no
  queue surgery.
- To re-run the work on current code: start a fresh run (concurrency
  cancel-in-progress supersedes the dead one).

Tune via `WorkerConfig.snapshotGonePolicy`; wrap `defaultSnapshotGonePolicy`
to widen/narrow the retry window.

## Permanently-unprocessable input

Throw `NagiNonRetryableError` (anywhere on the cause chain) from the step —
the step fails on that attempt without consuming the remaining retry budget.
Use for orphaned webhooks, schema-invalid payloads, deleted parents.

## Wedged worker pool

Prevention layers, in order:

1. `b.signal` requires a timeout — external waits park (no slot held) and
   fail honestly as `NagiSignalTimeoutError` when the input never arrives.
2. The lease-hold watchdog (`leaseHoldWarnMs`, default 5 min) warns every
   threshold multiple a step body holds a slot — grep for
   `holding a worker slot` before the pool saturates.
3. `WorkerConfig.maxConcurrencyPerFlow` (multi-flow deployments: set it to at
   most `concurrency - 1`) keeps one flow from occupying every slot.

## Operator actions

`wf.operator()` (all take `{ actor, note? }` for the audit trail):

- `skip(runId, stepId)` — settle a step as skipped and advance past it.
- `retry(runId, stepId)` — abort if running, reset the step **and its
  descendants**, re-dispatch. (Non-cascading single-step rerun is tracked in
  issue #34.)
- `abort(runId)` — cancel the run and its children, recursively.

Plus `wf.replay(runId, { mode, from })` for whole-run replay on the current
flow version, and `wf.cancel(runId)` for a plain stop.
