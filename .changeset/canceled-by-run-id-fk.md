---
"@nagi-js/postgres": patch
"@nagi-js/core": patch
---

`canceled_by_run_id` can no longer name a run that does not exist (nagi#29).

The orphan was blamed on custom stores, legacy rows or admin writes, because `tryStartRun` is single-tx atomic and so cannot produce it. Two canonical paths produce it anyway, and both are now closed. Each was reproduced against real Postgres and against the in-memory store before being fixed, and both are pinned by the shared conformance suite.

**Retention.** `pruneFacts` deletes run rows without regard for who points at them, so a policy that keeps `canceled` for audit while dropping `completed` deletes the superseder and strands its victim's reference. Migration `0008_canceled_by_run_id_fk` nulls existing orphans, indexes the column (every `workflow_run` delete re-checks the constraint, and retention deletes in batches) and adds a self-referencing FK with `ON DELETE SET NULL`.

**A handler's own claim.** `NagiCanceledError` is public and takes any `canceledByRunId`; `classifyFailure` turns it into a concurrency-cause `flow.canceled` fact, which projects straight into the column. Nothing checked that the named run existed — so a consumer reporting its own supersession wrote an orphan directly. The column now takes the id only when it resolves, in both stores. The fact log is untouched and keeps the claim verbatim: facts are immutable, and the column is a projection.

Because of that, the constraint can be immediate rather than deferred. `tryStartRun` has to cancel the prior run *before* inserting the superseder — the partial unique index on `(flow_id, concurrency_key)` only frees the slot once the prior leaves `pending`/`running` — so the cancel write cannot name the superseder. It resolves the reference after the insert instead.

See `docs/OPERATIONS.md` for applying `0008` to a large live table without the validation lock.
