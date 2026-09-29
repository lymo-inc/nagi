# Operations runbook

Setup prerequisites, then triage and recovery for nagi runs. Recovery uses the
built-in API — no hand-run SQL. Every recipe here previously existed only as
incident folklore; the API calls are the supported path.

## Setup and prerequisites

**Migrations.** Run `migrate(db, { schema })` from `@nagi-js/postgres` before
the first `nagi()`, and again on every nagi upgrade before the new code serves
traffic, with the same `schema` you pass to `postgresStore()` (default
`nagi`). Unmigrated, `nagi()` rejects at boot when it registers flows
(`relation "nagi.flow_snapshot" does not exist`).

- Applied ids live in `<schema>.schema_migrations`; a rerun skips them and
  returns `{ applied, skipped }`.
- Each migration runs in its own transaction with its `schema_migrations`
  insert: a failure rolls that migration back whole, and the next `migrate()`
  retries it. The transaction also rules out `CREATE INDEX CONCURRENTLY`, so
  index and constraint migrations lock `workflow_run` while they build. On a
  large table, apply the equivalent yourself and record the id — the `0008`
  recipe under [Retention and superseded runs](#retention-and-superseded-runs)
  is the pattern.
- Concurrent `migrate()` calls against the same schema serialize on a session
  advisory lock (`nagi:migrate:<schema>`): the first applies, and the rest
  wait, then skip. Running it from every replica's startup is safe; running it
  once per deploy is still cheaper.
- It opens with `CREATE SCHEMA IF NOT EXISTS <schema>`, which PostgreSQL checks
  against `CREATE` on the database even when the schema exists
  (`permission denied for database <db>`). Run it as a role that has it.

**pgmq privileges.** `nagi()` awaits `queue.ensureSchema()` on every boot. For
`pgmqQueue` that is `CREATE EXTENSION IF NOT EXISTS pgmq` then
`SELECT pgmq.create(<queueName>)` (`pgmq.create_partitioned` with
`partitioned: true`). The `@nagi-js/pgmq` 0.1.0 changelog says to run these
out-of-band in production; doing so does not skip the boot call, so the app
role still needs the privileges below. Checked on PostgreSQL 16 with pgmq
1.13.0, default non-partitioned queue:

- Extension already installed: `CREATE EXTENSION IF NOT EXISTS` is a
  `NOTICE: extension "pgmq" already exists, skipping` — no privilege check, so
  a role that could not have created it boots past it. Extension absent: the
  role needs `CREATE` on the database (pgmq 1.13.0 ships `superuser = false`),
  else boot fails with `permission denied for database <db>`.
- `pgmq.create` is not `SECURITY DEFINER` and re-runs its `CREATE TABLE IF NOT
  EXISTS` / `CREATE INDEX IF NOT EXISTS` / `INSERT INTO pgmq.meta ... ON
  CONFLICT DO NOTHING` on every boot. PostgreSQL checks privileges before
  "already exists", so every boot needs `USAGE` and `CREATE` on schema `pgmq`,
  `INSERT` on `pgmq.meta`, and ownership of `pgmq.q_<queueName>` and
  `pgmq.a_<queueName>`.

Minimal setup: install the extension as a privileged role and let the app role
create the queue on first boot. It then owns the queue tables, which also
covers every queue operation and `inspectQueue`.

```sql
CREATE EXTENSION IF NOT EXISTS pgmq;  -- superuser, once per database
GRANT USAGE, CREATE ON SCHEMA pgmq TO app;
GRANT INSERT ON pgmq.meta TO app;
```

A queue pre-created by another role works at runtime on `USAGE` plus DML
grants, but `nagi()` will not boot: `permission denied for schema pgmq`, then
with `CREATE`, `must be owner of table q_nagi`. Add the grants above and
`ALTER TABLE pgmq.q_nagi OWNER TO app` (same for `a_nagi`).

**Custom `Queue` adapters.** Stores have an executable contract —
`storeContract` from `@nagi-js/core/testing`; run it against any new store.
Queues have no conformance suite. What core relies on (`Queue` in
`packages/core/src/types.ts` and its callers):

- A dequeued message stays invisible until `ack`, `nack` or visibility expiry,
  then is redelivered (at-least-once; core dedupes). `extend(receipt, leaseMs)`
  moves visibility to now + `leaseMs` (the heartbeat); `nack(receipt,
  { delayMs })` makes it visible again after `delayMs`.
- `readCount` counts every delivery, including nacks and lease expiries.
  `attempt` is only what was stamped at enqueue; `nack` must not change it. The
  snapshot-gone policy and poison-message triage key on `readCount`.
- `flowId` passed to `enqueue` comes back on the message; drop it and the
  message escapes `maxConcurrencyPerFlow`.
- `withTx(tx)`, if present, MUST route every write through `tx` and nothing
  else — `startStaged` and the Postgres lease sweep commit their enqueue with
  store writes. Omit it rather than approximate it.
- `ensureSchema()` is optional and must be idempotent: every `nagi()` awaits it
  and fails fast on rejection. `inspect()` is optional; without it
  `wf.inspectQueue` throws.

**Postgres listener.** `postgresStore({ listener })` calls `listen()` once per
channel (`<schema>_stream`, `<schema>_events`) at construction and never again,
and `StreamListener` requires delivery of every NOTIFY until disposed.
PostgreSQL does not queue NOTIFY for a connection that is not listening, so
while the LISTEN connection is down notifications are lost and the store is
not told. The listener must reconnect and re-LISTEN itself; see
[Streaming steps on Postgres](#streaming-steps-on-postgres) for what a gap
costs.

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
| running, no lease, no entries     | —                              | Advance was lost — self-heals on next redelivery; `replay(runId, { mode: "continue", from: stepId })` re-drives immediately |
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

## Reading runs with SQL

For one run, use `wf.describe(runId)`. To list or filter runs, or to join them
to your own tables, query the store's tables directly. The columns below are a
read contract: a migration may add columns, but it will not rename, retype or
drop these without a changeset that says so. Everything else in the schema
(`fact`, `dedupe`, snapshot tables, other columns) is internal. Never write to
any nagi table.

`workflow_run`, one row per run:

| column               | type        | meaning                                                            |
| -------------------- | ----------- | ------------------------------------------------------------------ |
| `run_id`             | text        | primary key                                                        |
| `flow_id`            | text        |                                                                    |
| `status`             | text        | `pending` `running` `completed` `failed` `canceled`                |
| `input`              | jsonb       | the validated flow input                                           |
| `output`             | jsonb       | set when `completed`                                               |
| `error`              | jsonb       | a `SerializedError` when `failed`                                  |
| `started_at`         | timestamptz |                                                                    |
| `completed_at`       | timestamptz | set on every terminal status; cleared if a replay reopens the run  |
| `canceled_by_run_id` | text        | the superseding run, for a concurrency cancel                      |
| `parent_run_id`      | text        | set on a subflow child, with `parent_step_id`                      |
| `parent_step_id`     | text        |                                                                    |

`step_run`, one row per started step (primary key `(run_id, step_id)`), updated
in place to its latest attempt. A replay's reset deletes the row until the step
starts again:

| column         | type        | meaning                                                                  |
| -------------- | ----------- | ------------------------------------------------------------------------ |
| `run_id`       | text        |                                                                          |
| `step_id`      | text        |                                                                          |
| `attempt`      | integer     | starts at 1; 0 for a step skipped before it ever ran                     |
| `status`       | text        | `running` `completed` `failed` `canceled` `skipped`; a step parked on a signal or child reads `running` |
| `output`       | jsonb       |                                                                          |
| `error`        | jsonb       | a `SerializedError`; set on a `running` row while it backs off to retry  |
| `started_at`   | timestamptz |                                                                          |
| `completed_at` | timestamptz |                                                                          |

For monitoring, `lease (run_id, step_id, attempt, expires_at)` has a row while
an attempt holds its lease, and `timer (run_id, step_id, fire_at)` has a row
while a signal wait's timeout is pending.

`input @> '{"key": "value"}'` is served by a GIN index (`jsonb_path_ops`), and
`(flow_id, status)`, `completed_at` and `parent_run_id` are indexed. Rows are
written in the same transaction as the fact that changes them, so a reader never
sees a status the fact log does not back. `pruneFacts` deletes `step_run`,
`lease` and `timer` rows of pruned runs, and the `workflow_run` row too unless
`keepSummary` is set.

## Watching runs live

`wf.watchRun(runId, handler)` and `wf.watchRuns(handler)` push lifecycle events
as their facts commit. Both return a disposer; `watchRun` also stops on its own
once the run is terminal.

```ts
const off = wf.watchRun(runId, (e) => {
  if (e.type === "step.completed") console.log(e.stepId, e.output);
});
```

This needs `Store.events`. The in-memory store always has it; `postgresStore()`
has it when given a `listener` (the same one streaming uses — one LISTEN
connection, two channels). Without it both methods throw rather than returning
a subscription that never fires.

What it is not:

- **Not durable.** A handler sees events from the moment it subscribes; a
  restart starts over, and a dropped LISTEN connection loses the gap (see
  [Streaming steps on Postgres](#streaming-steps-on-postgres)). Catch up with
  `describe()`, then watch.
  Events ride the same LISTEN connection as streaming chunks, so the same
  `await store.ready()` applies before starting a run you mean to watch.
- **Not filtered.** `watchRuns` delivers every run this process observes.
  A live stream cannot honestly filter on `status` — the event IS the status
  change — and filtering on `input` would cost a state load per event.
- **Not a delivery guarantee.** Events are observation, not execution. A
  handler that throws is swallowed, so watching a run can never break it.

Concurrency supersession IS observable (`flow.canceled`, `cause:
"concurrency"`), which matters when a run vanishes from under a client: the
event carries `canceledByRunId`.

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

This client never reconnects. When its connection drops, a bare `pg.Client`
with no `error` listener crashes the process on the unhandled `error` event;
with one, it silently stops delivering. In production, on `error`/`end` open a
new client, re-`LISTEN` every channel `listen()` was called with, and route its
notifications to the same `onNotify` callbacks. What was published during the
gap stays lost:

- Streaming: chunks in the gap are gone, and if the step's close frame falls
  in it, an open `wf.subscribe()` iterator never ends. Subscribe again after
  reconnecting — a new subscription checks durable state and closes at once
  if the step has settled — and take the output from `describe()`.
- Watching: events in the gap are gone, and a `watchRun` whose terminal event
  was missed never stops on its own; call its disposer. Catch up with
  `describe()`.

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

## Recovery actions

- `wf.replay(runId, { mode: "continue", from: stepId })` — reset the step
  **and its descendants** and re-dispatch. A `running` step is aborted first
  (its handler sees `ctx.signal` abort, and replay waits up to 30s for it to
  settle). Works on a live run — including a `canceled` step holding it open —
  and on a completed/failed run, which the reset reopens.
- `wf.replay(runId, { mode: "continue", from: stepId, scope: "step" })` — rerun
  **only** that step. Completed descendants are left alone, so they keep
  outputs derived from the step's PREVIOUS output; the run is deliberately
  inconsistent until you rerun them too. Use it to regenerate one artifact when
  downstream consumers read from their own storage. On a settled run the reset
  reopens the run, and the flow output recomputes when it re-completes.
- `wf.replay(runId, { mode: "continue" })` — re-drive the run on the current
  flow version without resetting anything.
- `wf.cancel(runId, { reason })` — cancel the run and its children,
  recursively.

A canceled run cannot be replayed; start a new run instead.

The origin `step.reset` fact records `scope: "step"` for an isolated rerun.
Intent is recorded rather than inferred: a leaf step has no descendants, so a
cascading reset on a leaf writes the same single fact an isolated one does.

A subflow step reset under either scope bumps its generation, so it spawns a
FRESH child run rather than re-attaching to the finished one.
