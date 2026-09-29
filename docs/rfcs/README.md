# RFCs

Each RFC's own `Status:` line is the source of truth for where it stands.
Research notes and implementation handoffs sit next to the RFC they belong to.

| RFC | Title | Also |
|-----|-------|------|
| 0001 | [Content-addressed snapshot store](0001-content-addressed-snapshot-store.md) | [research](0001-content-addressed-snapshot-store.research.md) |
| 0002 | [Record-literal builder API](0002-record-literal-builder-api.md) | [research](0002-research.md) |
| 0003 | [Auto-compute `codeVersion` from registered flows](0003-auto-code-version.md) | [research](0003-auto-code-version.research.md) |
| 0004 | [Multi-name signal waits](0004-multi-name-signal-waits.md) | [research](0004-multi-name-signal-waits.research.md) |
| 0005 | `wf.queryRuns()` | [research + plan](0005-query-runs.research.md) (no separate RFC) |
| 0009 | `wf.pruneFacts()` | [research + plan](0009-prune-facts.research.md) (no separate RFC) |
| 0010 | [OTel parent-span linkage for subflow runs](0010-otel-subflow-span-linkage.md) — removed along with `@nagi-js/otel` | [handoff](0010-otel-subflow-span-linkage.handoff.md) |
| 0011 | [Extract `nextTransition` from the `advance` loop](0011-next-transition-state-machine.md) | [handoff](0011-next-transition-state-machine.handoff.md) |
| 0012 | [Shorthand concurrency config](0012-shorthand-concurrency-config.md) | [handoff](0012-shorthand-concurrency-config.handoff.md) |
| 0013 | [Activity steps](0013-activity-steps.md) | [research](0013-activity-steps.research.md) |
| 0013 | [Runtime bootstrap ergonomics](0013-runtime-bootstrap-ergonomics.md) | [research](0013-runtime-bootstrap-ergonomics.research.md), [handoff](0013-runtime-bootstrap-ergonomics.handoff.md) |
| 0014 | [Consumer-driven stability fixes (N1–N12)](0014-consumer-driven-stability-fixes.md) | [handoff](0014-consumer-driven-stability-fixes.handoff.md) |
| 0018 | [Parameterize `Wf` over registered flows](0018-parameterize-wf-over-flows.md) | [handoff](0018-parameterize-wf-over-flows.handoff.md) |
| 0019 | [`b.streamingTask`: streaming step outputs](0019-streaming-task.md) | [handoff](0019-streaming-task.handoff.md) |
| 0020 | [`onLog` callback replaces the `Logger` interface](0020-onlog-callback.md) | [handoff](0020-onlog-callback.handoff.md) |
| 0021 | [Core internal reorganization](0021-core-module-reorganization.md) | |

Numbering notes:

- `0013` is used twice. Both RFCs are cited by number elsewhere (changesets,
  RFC 0014), so neither is renumbered; cite them by number plus title.
- `0006`–`0008` and `0015`–`0017` were never written as RFC documents.
- `0010` shipped in `@nagi-js/otel`, which has since been removed from the
  repo; its `Status:` line predates the removal and is kept as history.
- RFC numbers are not GitHub issue numbers. A commit saying "rfc#13" means
  this directory's `0013`, not issue #13.
