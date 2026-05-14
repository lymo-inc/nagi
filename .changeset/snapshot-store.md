---
"@nagi-js/core": major
"@nagi-js/postgres": minor
---

Add content-addressed snapshot store. Every run is pinned to the exact DAG
topology that existed when it started. Replays read the pinned snapshot, not
the current in-memory flow definition. See RFC 0001.

New surface:

- `canonicalize(flow)` and `sha256Canonical(dag)` — turn a flow into a
  byte-stable canonical form keyed by content hash.
- `diffSnapshots(a, b)` — structural delta between two canonical DAGs
  (added/removed steps, edge changes, predicate changes).
- `nagi({ codeVersion })` — handler-code identifier (typically a git SHA),
  persisted on every run alongside the topology hash.
- `ReplayOpts.allowDrift` — opt-in escape hatch for replays whose live
  topology differs from the pinned snapshot.
- `NagiSnapshotDriftError` — thrown by `wf.replay()` on detected drift.
- New `Store` methods: `upsertSnapshot`, `getRef`, `setRef`, `loadSnapshot`,
  `appendGlobalFact`.
- `@nagi-js/postgres`: new `0002_snapshot_tables` migration adds
  `flow_snapshot`, `flow_ref`, `global_fact` tables; adds `flow_hash` +
  `code_version` columns to `workflow_run`.

Breaking changes (`@nagi-js/core`):

- `nagi()` now returns `Promise<Wf>` (was `Wf`). The snapshot upsert and ref
  resolution at boot are async.
- `wf.replay()` throws `NagiSnapshotDriftError` when the live flow's hash
  differs from the pinned snapshot. Pass `replayOpts.allowDrift: true` to
  proceed against the live code anyway (best-effort hybrid: scheduling from
  the snapshot, handlers from live).
- `Store` interface gains 5 new methods. Custom implementations must add
  them.
