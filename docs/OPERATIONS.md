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
| running, no lease, no entries     | —                              | Advance was lost — self-heals on next redelivery; `operator().retry(runId, stepId, { actor })` re-drives immediately |
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

## Deploy replaced an in-flight flow (drift / snapshot-gone)

Every run is pinned to the hash of the flow it started on. What a worker on
new code does with a live run pinned to an old hash is `nagi({ driftPolicy })`:

- `"synthesize"` — the run **continues on the new code**: nagi rebuilds the
  pinned DAG shape from the snapshot and attaches the live handlers (the same
  thing `replay({ allowDrift: true })` does), so completed steps keep their
  outputs and the interrupted step re-runs. Choose this when nothing keeps old
  code running after a deploy (one task, rolling replace) — under `"freeze"`
  such a deploy strands every in-flight run. Falls through to the
  snapshot-gone handling below only when the snapshot is missing or a pinned
  step no longer exists live (grep `drift synthesis failed`).
- `"freeze"` (default) — the run is **snapshot-gone**: the worker nacks its
  messages with backoff so a still-running old-code worker can claim them,
  then **terminally fails the run** with `NagiFlowSnapshotGoneError` in
  `workflow_run.error` and acks the message. Nothing loops forever.

Intervene earlier if needed:

- `wf.cancel(runId)` — fact-only, bypasses the flow registry, and the
  dispatcher acks a terminal run's messages on next delivery. One call; no
  queue surgery.
- To re-run the work on current code: start a fresh run (concurrency
  cancel-in-progress supersedes the dead one).

Tune the `"freeze"` window via `WorkerConfig.snapshotGonePolicy`; wrap
`defaultSnapshotGonePolicy` to widen/narrow it.

## Permanently-unprocessable input

Throw `NagiNonRetryableError` (anywhere on the cause chain) from the step —
the step fails on that attempt without consuming the remaining retry budget.
Use for orphaned webhooks, schema-invalid payloads, deleted parents.

## Wedged worker pool

Prevention layers, in order:

1. `b.signal` requires a timeout — external waits park (no slot held) and
   fail honestly as `NagiSignalTimeoutError` when the input never arrives.
2. `timeoutMs` on a task / activity / streaming step arms an enforced
   deadline: at the deadline `ctx.signal` aborts with `NagiStepTimeoutError`
   and the step fails, retryable under its own `retry` policy. Enforcement is
   cooperative — a body that never awaits or checks `ctx.signal` keeps its
   slot, so the deadline is a contract with handlers that honor the signal,
   not a kill switch. Pass `ctx.signal` to your HTTP/LLM client.
3. The lease-hold watchdog (`leaseHoldWarnMs`, default 5 min) warns every
   threshold multiple a step body holds a slot — grep for
   `holding a worker slot` before the pool saturates. The watchdog detects;
   the deadline in layer 2 acts. Set `leaseHoldWarnMs` below your longest
   `timeoutMs` and a firing watchdog means "a step ignored its deadline".
4. `WorkerConfig.maxConcurrencyPerFlow` (multi-flow deployments: set it to at
   most `concurrency - 1`) keeps one flow from occupying every slot.

If you override `pgmqQueue({ visibilityTimeoutMs })` or
`nagi({ heartbeatIntervalMs })`, keep `heartbeatIntervalMs < visibilityTimeoutMs`;
otherwise every step longer than the visibility timeout is redelivered before
its first lease extension (defaults: 40s interval, 120s visibility).

## Streaming steps on Postgres

`b.streamingTask` needs a `stream` transport on the store. The in-memory store
always has one; `postgresStore()` has one only when given a `listener`,
and `nagi()` refuses to register a streaming flow without it.

nagi does not open the listening connection itself — Kysely hides the driver, and
this package does not depend on `pg`. Wire one:

```ts
const client = new pg.Client({ connectionString });
await client.connect();

const store = postgresStore({
  db,
  listener: {
    async listen(channel, onNotify) {
      client.on("notification", (m) => {
        if (m.channel === channel && m.payload) onNotify(m.payload);
      });
      await client.query(`LISTEN "${channel}"`);
      return () => client.end();
    },
  },
});

await store.ready(); // both channels live; safe to start runs
```

Operational limits:

- Chunks are capped at 7000 bytes serialized (PostgreSQL's NOTIFY payload limit
  is 8000). An oversized chunk throws from `ctx.emit`, failing the step, rather
  than vanishing. Stream references, not payloads.
- Chunks published before `LISTEN` is established are lost. NOTIFY does not queue
  for a connection that is not yet listening. Chunks are ephemeral by design —
  a subscriber that reconnects sees the stream from that point on.
- `postgresStore()` cannot await `listen()` from a constructor, so the socket
  goes live shortly after the call returns. **`await store.ready()` before
  starting work you intend to watch** — it resolves once both channels are
  receiving. A process that boots its store long before its first run never
  notices; one that builds a store and immediately starts a run loses the head
  of the stream, silently and in full order, which reads like a truncated
  response rather than a race.
- Chunks never enter the fact log, so they are not replayed. A replayed step
  re-runs and re-emits.

## Retention and superseded runs

A canceled run's `canceled_by_run_id` names the run that superseded it.
`pruneFacts` deletes run rows, so a retention policy that keeps `canceled` for
audit while dropping `completed` deletes the superseder and leaves the
reference behind.

Postgres nulls the column when that happens (`ON DELETE SET NULL`, migration
`0008`), so an audit for references naming a missing run returns nothing. The
victim's own `flow.canceled` fact still carries the id, because facts are
immutable: the column is a projection, the fact is the record. `wf.describe()`
matches the column on both stores and omits `canceledByRunId` once the
superseder is gone.

**Applying `0008` to a large live table.** It nulls any pre-existing orphans,
then adds the constraint — which takes an `ACCESS EXCLUSIVE` lock on
`workflow_run` and scans it to validate, blocking reads and writes for the
duration. On a table big enough for that to matter, run the cleanup `UPDATE`
and `ADD CONSTRAINT ... NOT VALID` yourself, `VALIDATE CONSTRAINT` separately
(it takes only `SHARE UPDATE EXCLUSIVE`), then insert the id
`0008_canceled_by_run_id_fk` into `<schema>.schema_migrations` so `migrate()`
skips it.

## Operator actions

`wf.operator()` (all take `{ actor, note? }` for the audit trail):

- `skip(runId, stepId)` — settle a step as skipped and advance past it.
- `retry(runId, stepId)` — abort if running, reset the step **and its
  descendants**, re-dispatch.
- `retry(runId, stepId, { actor, scope: "step" })` — rerun **only** that step.
  Completed descendants are left alone, so they keep outputs derived from the
  step's PREVIOUS output; the run is deliberately inconsistent until you rerun
  them too. Use it to regenerate one artifact when downstream consumers read
  from their own storage. On a settled run the reset reopens the run, and the
  flow output recomputes when it re-completes.
- `abort(runId)` — cancel the run and its children, recursively.

Plus `wf.replay(runId, { mode, from, scope? })` for whole-run replay on the
current flow version — `scope` behaves exactly as on `retry` — and
`wf.cancel(runId)` for a plain stop.

The origin `step.reset` fact records `scope: "step"` for an isolated rerun.
Intent is recorded rather than inferred: a leaf step has no descendants, so a
cascading retry on a leaf writes the same single fact an isolated one does.

A subflow step reset under either scope bumps its generation, so it spawns a
FRESH child run rather than re-attaching to the finished one.
