---
"@nagi-js/core": patch
---

Add `b.activity({...})`, a step kind for external-effect work (LLM/HTTP calls)
that runs its body **outside** the durable transaction. `b.task` is unchanged
and still runs its body inside a short tx so DB writes commit atomically with
`step.completed` — correct for DB-only work. An activity instead runs the
handler with no ambient tx, then commits only its terminal fact in a short
transaction, so a multi-minute handler never holds a connection open.

`ActivityConfig.run` receives an `ActivityCtx` (`Omit<StepCtx, "tx">`): there is
no `ctx.tx`, enforced at the type level and backed by a throwing runtime getter.
Activities are at-least-once and must be idempotent (use `ctx.idempotencyKey` /
upsert on a deterministic key; optionally an `idempotency-key` header to the
external provider).

Cancellation, retry, replay, scheduling, and output typing are identical to
`task`. See `docs/rfcs/0013-activity-steps.md`. This removes the need for the
PENDING-row + reaper pattern consumers used to track in-flight external calls,
and demotes the visibility heartbeat from a correctness mechanism to a cost
optimization.
