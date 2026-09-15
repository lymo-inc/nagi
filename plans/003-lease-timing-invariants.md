# Plan 003: Make the default queue visibility timeout and lease clock honor the heartbeat invariants

> **Executor instructions**: Follow this plan step by step. Run every
> verification command and confirm the expected result before moving to the
> next step. If anything in the "STOP conditions" section occurs, stop and
> report — do not improvise. When done, update the status row for this plan
> in `plans/README.md` — unless a reviewer dispatched you and told you they
> maintain the index.
>
> **Drift check (run first)**: `git diff --stat ced05c2..HEAD -- packages/pgmq/src packages/postgres/src/store.ts packages/core/src/step-exec.ts packages/core/src/runtime.ts docs/OPERATIONS.md`
> If any in-scope file changed since this plan was written, compare the
> "Current state" excerpts against the live code before proceeding; on a
> mismatch, treat it as a STOP condition.

## Status

- **Priority**: P1
- **Effort**: S
- **Risk**: LOW
- **Depends on**: none (Plan 001 recommended first so the Postgres integration test runs somewhere)
- **Category**: bug
- **Planned at**: commit `ced05c2`, 2026-09-04
- **Issue**: https://github.com/lymo-inc/nagi/issues/45

## Why this matters

Two cross-package defaults contradict invariants the code itself states.

1. Core's heartbeat extends a running step's queue message every
   `DEFAULT_HEARTBEAT_INTERVAL_MS = 40_000`, and its own comment says the
   interval "must be shorter than the queue's initial visibility timeout, or
   the first redelivery happens before the first extension lands". The PGMQ
   adapter's `DEFAULT_VISIBILITY_TIMEOUT_MS` is `30_000`. With stock defaults,
   every step whose handler runs longer than 30 seconds (a normal LLM call) is
   redelivered at t=30s. The duplicate is absorbed by `claimStep` returning
   `null`, so the step does not double-execute, but the redelivery burns a
   worker slot, increments PGMQ `read_ct` (the exact signal the snapshot-gone
   poison policy reads), and — because the duplicate is acked as a skip — the
   original in-flight message is **deleted out from under the running
   worker**, so a crash after that point relies solely on the lease reaper.
2. Postgres `claimStep` writes `expires_at` from the **application** clock
   (`Date.now() + leaseMs`) but grants a new claim when the **database** clock
   says the old lease expired (`WHERE expires_at < now()`). `extendLease`
   already computes server-side and its comment says it matches `claimStep` —
   it does not. A host clock a few seconds fast lets a second worker claim a
   step the first is still executing.

Both fixes are one-liners with tests; they close real double-dispatch windows.

## Current state

- `packages/pgmq/src/pgmq-queue.ts:15-16`:

  ```ts
  const DEFAULT_QUEUE_NAME = "nagi";
  const DEFAULT_VISIBILITY_TIMEOUT_MS: Millis = 30_000;
  ```

  used at lines 52-57 to derive `vtSeconds` (ceil to seconds, min 1), which
  `dequeue` passes to `pgmq.read(${queueName}, ${vtSeconds}::int, ${count}::int)`
  (line 122).
- `packages/core/src/step-exec.ts:138-139`:

  ```ts
  export const DEFAULT_HEARTBEAT_LEASE_MS: Millis = 120_000;
  export const DEFAULT_HEARTBEAT_INTERVAL_MS: Millis = 40_000;
  ```

  and the invariant, in the comment above `startHeartbeat` (lines 153-154):
  "intervalMs must be shorter than the queue's initial visibility timeout, or
  the first redelivery happens before the first extension lands." Each tick
  calls `queue.extend(receipt, leaseMs)` (i.e. pushes visibility out by
  `DEFAULT_HEARTBEAT_LEASE_MS`), so after the first tick the message is
  invisible for 120s at a time.
- `packages/core/src/runtime.ts:78-81` — `NagiConfig` repeats the invariant:

  ```ts
  // heartbeatIntervalMs must stay below the queue's initial visibility timeout,
  // or a slow step's message is redelivered before the first lease extension.
  readonly heartbeatIntervalMs?: Millis;
  readonly heartbeatLeaseMs?: Millis;
  ```
- `packages/core/src/memory.ts:880` — the in-memory reference queue uses
  `DEFAULT_QUEUE_LEASE_MS: Millis = 60_000`, which satisfies the invariant.
- `packages/postgres/src/store.ts:392-411` — `claimStep`:

  ```ts
  const token = `lease-${crypto.randomUUID()}`;
  const expiresAt = new Date(Date.now() + this.leaseMs);

  const result = await sql<{ token: string }>`
    INSERT INTO ${sql.raw(this.t("lease"))} (run_id, step_id, attempt, token, expires_at)
    VALUES (${runId}, ${stepId}, ${attempt}, ${token}, ${expiresAt})
    ON CONFLICT (run_id, step_id, attempt) DO UPDATE
      SET token = EXCLUDED.token, expires_at = EXCLUDED.expires_at
      WHERE ${sql.raw(this.t("lease"))}.expires_at < now()
    RETURNING token
  `.execute(this.db);
  ```

  and `extendLease` at lines 420-426, which is the pattern to copy:

  ```ts
  UPDATE ${sql.raw(this.t("lease"))}
     SET expires_at = now() + (${leaseMs}::int * interval '1 ms')
  ```
- `packages/postgres/src/integration.test.ts:123-135` — existing lease tests
  (`claimStep returns null on a live lease`, `claimStep re-acquires after lease
  expiry` with `leaseMs: 50` and an 80ms sleep). They run only when
  `NAGI_POSTGRES_TEST_URL` is set (`pnpm db:up && pnpm test:integration`).
- `packages/pgmq/src/pgmq-queue.test.ts` — unit tests against a capturing fake
  Kysely (`createCapturingDb` from `./test-helpers`). Existing dequeue test at
  lines 45-57 shows the shape:

  ```ts
  const fake = createCapturingDb();
  fake.enqueueRows([]);
  const q = pgmqQueue({ db: fake.db, visibilityTimeoutMs: 45_000 });
  await q.dequeue({ count: 5 });
  const query = fake.queries[0];
  expect(query?.sql).toContain("pgmq.read");
  expect(query?.parameters).toEqual(["nagi", 45, 5]);
  ```

  No test currently pins the **default** visibility timeout.
- Changesets: `.changeset/<slug>.md` with `patch` bumps; one changeset may list
  several packages.

## Commands you will need

| Purpose   | Command                  | Expected on success |
|-----------|--------------------------|---------------------|
| Typecheck | `pnpm typecheck`         | exit 0 |
| pgmq tests| `pnpm -F @nagi-js/pgmq test` | exit 0, 53 + new passed |
| Postgres integration | `pnpm db:up && pnpm test:integration; pnpm db:down` | exit 0, 117 passed |
| Lint      | `pnpm lint`              | exit 0 |

## Scope

**In scope** (the only files you should modify):
- `packages/pgmq/src/pgmq-queue.ts` (the default constant + its comment)
- `packages/pgmq/src/pgmq-queue.test.ts` (one new test)
- `packages/postgres/src/store.ts` (`claimStep` only)
- `packages/postgres/src/integration.test.ts` (one new test)
- `docs/OPERATIONS.md` (one short note)
- `.changeset/lease-timing-invariants.md` (create)

**Out of scope** (do NOT touch, even though they look related):
- `packages/core/src/step-exec.ts` heartbeat constants — core cannot know the
  queue's timeout; the fix belongs in the adapter default.
- Adding a `visibilityMs` option to `Queue.dequeue` or any other port change.
- `packages/core/src/memory.ts` — the in-memory queue already satisfies the invariant.
- `describe()` / `sweepLeases` SQL — they already use server `now()`.

## Git workflow

- Branch: `advisor/003-lease-timing-invariants`
- Commit subject: `fix(pgmq,postgres): default visibility timeout above heartbeat interval; lease expiry on the DB clock`
- Do NOT push or open a PR unless the operator instructed it.

## Steps

### Step 1: Raise the PGMQ default visibility timeout

In `packages/pgmq/src/pgmq-queue.ts` replace line 16 with:

```ts
// Must exceed core's DEFAULT_HEARTBEAT_INTERVAL_MS (40s), or a slow step is
// redelivered before its first lease extension. Matches core's
// DEFAULT_HEARTBEAT_LEASE_MS so a crashed worker's message and its store
// lease become reclaimable at the same time.
const DEFAULT_VISIBILITY_TIMEOUT_MS: Millis = 120_000;
```

**Verify**: `pnpm -F @nagi-js/pgmq test` → all existing tests pass (none pins the old default; if one does, see STOP conditions).

### Step 2: Pin the default with a unit test

In `packages/pgmq/src/pgmq-queue.test.ts`, inside `describe("pgmqQueue.dequeue", ...)`, add:

```ts
it("defaults the visibility timeout above core's 40s heartbeat interval", async () => {
  const fake = createCapturingDb();
  fake.enqueueRows([]);
  const q = pgmqQueue({ db: fake.db });

  await q.dequeue({ count: 1 });

  const query = fake.queries[0];
  expect(query?.sql).toContain("pgmq.read");
  // 120s: > DEFAULT_HEARTBEAT_INTERVAL_MS (40s), = DEFAULT_HEARTBEAT_LEASE_MS.
  expect(query?.parameters).toEqual(["nagi", 120, 1]);
});
```

**Verify**: `pnpm -F @nagi-js/pgmq test` → 54 passed.

### Step 3: Compute lease expiry on the database clock

In `packages/postgres/src/store.ts` `claimStep`, delete the
`const expiresAt = new Date(Date.now() + this.leaseMs);` line and change the
`VALUES` clause so the expiry is computed by Postgres, mirroring `extendLease`:

```ts
VALUES (${runId}, ${stepId}, ${attempt}, ${token}, now() + (${this.leaseMs}::int * interval '1 ms'))
```

Leave `ON CONFLICT ... WHERE ...expires_at < now()` and `RETURNING token` as they are.

**Verify**: `pnpm typecheck` → exit 0. `pnpm db:up && pnpm test:integration` →
`claimStep returns null on a live lease` and `claimStep re-acquires after lease
expiry` both pass (the 50ms lease + 80ms sleep still expires on the DB clock).

### Step 4: Add an integration test that the app clock no longer matters

In `packages/postgres/src/integration.test.ts`, next to the existing
`claimStep` tests, add:

```ts
it("claimStep expiry is computed on the database clock, not the app clock", async () => {
  const store = postgresStore({ db, schema, leaseMs: 30_000 });
  const runId = `run-${uuidv7()}` as RunId;
  const realNow = Date.now;
  // Skew the app clock 10 minutes into the past: a JS-computed expires_at
  // would already be "expired" by DB time and let a second claim through.
  Date.now = () => realNow() - 600_000;
  try {
    expect(await store.claimStep(runId, "step", 1)).not.toBeNull();
    expect(await store.claimStep(runId, "step", 1)).toBeNull();
  } finally {
    Date.now = realNow;
  }
});
```

**Verify**: `pnpm test:integration` → the new test passes. Then `git stash`
the `store.ts` change and re-run: the new test must **fail** (second claim
succeeds); `git stash pop`; passes again. `pnpm db:down` when finished.

### Step 5: Runbook note and changeset

In `docs/OPERATIONS.md`, under "## Wedged worker pool" (after the numbered
prevention layers), add one sentence:

```
If you override `pgmqQueue({ visibilityTimeoutMs })` or
`nagi({ heartbeatIntervalMs })`, keep `heartbeatIntervalMs < visibilityTimeoutMs`;
otherwise every step longer than the visibility timeout is redelivered before
its first lease extension (defaults: 40s interval, 120s visibility).
```

Create `.changeset/lease-timing-invariants.md`:

```md
---
"@nagi-js/pgmq": patch
"@nagi-js/postgres": patch
---

`pgmqQueue` default `visibilityTimeoutMs` is now 120s (was 30s), above core's
40s heartbeat interval — with stock settings a step longer than 30s was
redelivered before its first lease extension. `postgresStore.claimStep` now
computes lease expiry on the database clock, matching `extendLease`, so
application clock skew can no longer grant a second claim on a live lease.
```

**Verify**: `pnpm lint` → exit 0. `pnpm test` → green.

## Test plan

- New unit test in `packages/pgmq/src/pgmq-queue.test.ts` (Step 2).
- New integration test in `packages/postgres/src/integration.test.ts` (Step 4).
- Existing `claimStep`, `sweepLeases`, and heartbeat-related tests must stay green:
  `pnpm test:integration`, `pnpm -F @nagi-js/core test` (heartbeat.test.ts).
- Verification: `pnpm test` exit 0; `pnpm test:integration` 117 passed (with Docker).

## Done criteria

- [ ] `grep -n "DEFAULT_VISIBILITY_TIMEOUT_MS: Millis = 120_000" packages/pgmq/src/pgmq-queue.ts` matches
- [ ] `grep -n "Date.now() + this.leaseMs" packages/postgres/src/store.ts` returns **no** match inside `claimStep`
- [ ] `pnpm -F @nagi-js/pgmq test` → 54 passed
- [ ] `pnpm test:integration` → 117 passed, including the new clock-skew test (state if Docker was unavailable — then the plan is not DONE, mark BLOCKED with the reason)
- [ ] `pnpm typecheck`, `pnpm lint`, `pnpm test` exit 0
- [ ] `.changeset/lease-timing-invariants.md` exists; runbook sentence added
- [ ] `git status` shows only in-scope files modified
- [ ] `plans/README.md` status row updated

## STOP conditions

Stop and report back (do not improvise) if:
- An existing pgmq test asserts the old 30s default — report which one; do
  not change its expectation without confirming it isn't testing something else.
- `claimStep` no longer matches the excerpt (someone already moved expiry to SQL).
- The clock-skew integration test does not fail with the old code (Step 4's
  stash check) — the test is not exercising the change; report instead of
  weakening it.
- Docker/Postgres is unavailable: complete Steps 1–3 and 5, write the Step 4
  test, and mark the plan BLOCKED in the index with "integration not executed".

## Maintenance notes

- If core's `DEFAULT_HEARTBEAT_INTERVAL_MS` or `DEFAULT_HEARTBEAT_LEASE_MS`
  ever change, the pgmq default and the runbook sentence must move with them.
  A future improvement (not this plan) is a `Queue.dequeue({ visibilityMs })`
  option so core can pass its own lease and delete the cross-package constant.
- Reviewer: confirm no other `Date.now()` crept into a lease/timer SQL path
  (`grep -n "Date.now" packages/postgres/src/store.ts` should show no hits in
  `claimStep`, `extendLease`, `sweepLeases`, `sweepSignalTimeouts`).
