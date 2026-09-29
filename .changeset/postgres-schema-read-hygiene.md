---
"@nagi-js/postgres": patch
---

Postgres schema and read-hygiene fixes. Run `migrate()`.

- Migration `0010_canceled_by_fk_deferrable` makes `workflow_run_canceled_by_fk` `DEFERRABLE INITIALLY DEFERRED`. Code older than `c50101f` writes a victim's `canceled_by_run_id` before it inserts the superseder, in the same transaction; the immediate FK added by `0008` rejected that write during a rolling upgrade. Deferred, the check runs at commit, when the superseder row exists. `ON DELETE SET NULL` stays immediate — PostgreSQL never defers referential actions — so a dangling reference is still refused and still nulled on delete.
- `describe()` now runs its three SELECTs in a `REPEATABLE READ`, `READ ONLY` transaction instead of a default-isolation one, so the run, step and child rows come from one snapshot instead of three.
- Schema names are capped at 56 characters (63-byte PostgreSQL identifier limit minus the 7-byte `_stream`/`_events` channel suffixes). Past the cap, `pg_notify` rejected every NOTIFY-issuing write with "channel name too long" while `LISTEN` silently truncated.
