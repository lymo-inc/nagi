# RFC 0013 — Activity steps (external-effect steps that run outside the durable transaction)

- **Status:** Draft
- **Author:** @jay (lymo-inc)
- **Created:** 2026-05-25 (JST)
- **Tracking issue:** lymo-inc/nagi#13
- **Related:** RFC 0001 (content-addressed snapshot store), `@nagi-js/core`, `@nagi-js/postgres`
- **Research notes:** `0013-activity-steps.research.md`

## Summary

Introduce a new step kind, `b.activity({...})`, for steps whose body performs an
**external effect** (an LLM/HTTP call, a third-party API) rather than local
database work. An activity runs its handler **outside any durable transaction**,
then commits only its terminal fact (`step.completed` / `step.failed` /
`step.canceled`) in a short transaction. `b.task({...})` keeps today's semantics:
the body runs *inside* a transaction so its writes commit atomically with the
step fact — correct for DB-only work, where the body is milliseconds.

This splits one execution model that is currently applied to two fundamentally
different kinds of step.

## Motivation

`store.runStep` wraps the **entire** step body in a Postgres transaction
(`packages/postgres/src/store.ts`):

```ts
await this.db.transaction().execute(async (trx) => {
  const result = await body(trx);          // the whole handler runs here
  await upsertStepCompleted(trx, ...);
  await insertFact(trx, result.fact);
  await deleteLease(trx, ...);
});
```

For a DB-only step this is correct and cheap. For a step whose body is a
multi-minute LLM call, it is a defect with two faces:

1. **A transaction is held open for minutes** around work that touches no rows —
   idle-in-transaction, connection-pool pressure, vacuum bloat. nagi's own
   `concurrency: 4` worker comment already references DB-pool headroom as the
   limiting factor; long-held txns are why.
2. **In-progress state leaks into durable storage.** Because the long body can't
   atomically own an external side effect, callers (e.g. usage metering) write a
   separate `PENDING` row on a *different* connection, then update it at the end —
   a two-phase, non-atomic write. If the execution is abandoned (redelivery,
   crash) the `PENDING` row is orphaned and needs a reaper to sweep it.

Both faces are the **same modeling error**: nagi treats every step as a database
transaction. The fix is to name the other kind of step — the *activity* — and
give it the execution model external effects actually need.

This is the split every mature durable-execution engine makes: Temporal
*workflow vs activity*, DBOS `@Transaction` vs `@Step`/communicator, Restate
*handler vs side-effect*. Transactional steps get atomicity; activity steps get
at-least-once + idempotency.

## Proposed API

```ts
// DB-only work — unchanged. Body runs inside a short tx; ctx.tx is the tx.
persistScores: b.task({
  needs: { score },
  run: async ({ needs, ctx }) => {
    await ctx.tx.insertInto("deal_score").values(needs.score).execute();
    return { ok: true };
  },
});

// External effect — new. Body runs outside any tx; there is no ctx.tx.
analyze: b.activity({
  needs: { transcript },
  run: async ({ needs, ctx }) => {
    // ctx.signal, ctx.logger, ctx.once, ctx.idempotencyKey are available.
    // ctx.tx is intentionally absent: write idempotently via your own client.
    const out = await llm.generate({ input: needs.transcript });
    return out;
  },
});
```

`ActivityConfig` is `TaskConfig` with one change: `run` receives an
**`ActivityCtx`** = `Omit<StepCtx, "tx">`. The absence of `ctx.tx` is enforced at
the type level — an activity handler cannot reference a transaction that isn't
there. (Internally, the ctx object also has a `tx` getter that throws, as a
runtime backstop.)

Everything else is identical to `task`: `needs`, `when`, `retry`, `timeoutMs`,
`onStart`/`onComplete`/`onError`/`onRetry`, output typing, scheduling.

## Execution semantics

`dispatchMessage` for an activity:

1. **admit** (claim + dedupe) — unchanged.
2. **record `step.started`** — unchanged.
3. **run the handler with no ambient transaction.** Its own DB writes (if any)
   use the caller's connection and **must be idempotent** (deterministic keys).
4. **commit the terminal fact in a short transaction** — reuses `store.runStep`
   with a no-op body that only resolves and returns the fact, so the existing
   completed/failed/canceled + lease-release logic is shared verbatim. The tx now
   spans a single `loadRunState` + fact write, not the LLM call.
5. **ack + advance** — unchanged.

Failure, retry, and cancellation flow through the existing `handleStepError`
path. Replay is unchanged: a completed activity has a memoized `step.completed`
fact, so replay returns its output without re-running.

The only behavioral difference from `task`: an activity's own writes are **not**
atomic with its `step.completed` fact (they can't be — one side is an external
system). Correctness comes from at-least-once delivery + idempotent writes + the
fact log as the source of truth, exactly as Temporal/DBOS do for activities.

## The idempotency contract (non-negotiable)

Activities are at-least-once. A redelivery or crash-retry **will** occasionally
run the body twice. The contract:

- **DB side:** writes must be idempotent (upsert on a deterministic key, e.g.
  `(runId, stepId, attempt, scope)` via `ctx.idempotencyKey`).
- **Provider side (optional but recommended):** pass an `idempotency-key` header
  to the external API so a duplicate call is de-duplicated at the provider and
  not billed twice.

With this contract, a duplicate activity execution is observably a no-op. The
contract is *enforced by the kind*: choosing `b.activity` is the author stating
"this body is safe to run more than once."

## What this removes downstream (in consumers like Lymo)

- The `PENDING` two-phase usage row → replaced by a single terminal cost fact
  written when the activity completes. No in-progress row to strand.
- The orphan reaper → unnecessary; the orphan state is no longer representable.
- The visibility heartbeat (RFC-less interim, shipped in `worker-visibility-
  heartbeat`) → demoted from correctness mechanism to a **cost optimization**
  (avoid paying for duplicate LLM calls on long steps). Keep or drop on cost
  grounds, not correctness.

## Relationship to the visibility heartbeat

The heartbeat (worker re-extends the queue message's visibility while a step
runs) and activity steps are orthogonal and complementary:

- **Heartbeat** reduces *redelivery* of long steps → fewer duplicate executions →
  less wasted LLM spend. It does not change the transaction model.
- **Activity steps** fix the *transaction model* and make any duplicate that does
  slip through *harmless* (idempotent), and remove the orphan/reaper entirely.

Heartbeat is the interim band-aid; activity steps are the structural fix. After
this RFC, the heartbeat is optional.

## Alternatives considered

- **A flag on `task` (`transactional: false`)** instead of a new kind. Rejected:
  the presence/absence of `ctx.tx` should be a *type* difference, not a runtime
  flag — a flagged task would still type `ctx.tx` and let a handler reach for a
  transaction that isn't held.
- **Lazy / deferred `ctx.tx`** (open the tx on first use, commit at the end).
  Rejected: only helps handlers that write at the very end; a handler that writes
  early then makes a long call still holds the tx. Doesn't address the model.
- **Long static lease / large visibility timeout, no heartbeat.** Rejected
  separately: needs a known max duration, and worsens crash-recovery latency.
- **Keep `PENDING` + reaper.** Rejected: that is the band-aid this RFC removes.

## Migration

Additive and back-compatible. No existing flow changes; `task` semantics are
untouched. Consumers migrate external-effect steps from `task` to `activity` and
drop their `PENDING`/reaper scaffolding (Lymo's `withUsageTracking` is the first
target). Persistence schema is unchanged.

## Open questions

1. **`ctx.beginTx()` for activities?** Some activities may want a *short* tx for a
   multi-statement idempotent write after the effect. This RFC ships activities
   with **no** tx access (forcing idempotent single writes); an on-demand
   `ctx.beginTx()` that opens its own short tx can be added later if a real use
   case appears (YAGNI for now).
2. **Should `runStep` ever hold the body for `task`?** Out of scope; `task`
   stays as-is. A future RFC could bound `task` body duration.
