---
"@nagi-js/core": patch
"@nagi-js/postgres": patch
---

Fold the subflow parent linkage on `FlowStartedFact` from two independently
optional fields (`parentRunId?` / `parentStepId?`) into a single optional
`parent?: ParentLink` (`{ runId, stepId }`). A run is either a root (no
`parent`) or a child (a complete `parent`); the previous shape allowed the
unrepresentable half-state of a `parentRunId` with no `parentStepId`, which the
runtime then had to guard against at every read. Two new types are exported:
`ParentLink` (the durable link persisted on the fact) and
`ParentRef extends ParentLink` (adds `attempt`, the in-process reference used
for otel span/registry lookups). `FlowStartEvent.parent` now references
`ParentRef` — its `{ runId, stepId, attempt }` shape is unchanged.

Optionality now lives only at the boundary: `startRunInternal` keeps a single
`parent?: ParentRef` (the one genuine root-vs-child fork), while the
subflow-only internals (`startChildRun`, `DispatchDeps.startChildRun`,
`executeSubflow`) take a required `parent: ParentRef`. `propagateToParent`
collapses its two `=== undefined` guards into one `parent === undefined` check.
The fact still drops `attempt` (it is re-derived from parent run state on wake),
so the durable record is unchanged in information, only in shape.

**Persisted-shape change (breaking for in-flight subflows across upgrade).**
The Postgres event log serializes/revives fact payloads structurally
(`{...rest}` → JSONB → `{...body}`), so new `flow.started` payloads round-trip
the nested `parent` object with no adapter change; only the two denormalized
column writes were repointed to `fact.parent?.runId` / `fact.parent?.stepId`
(the `parent_run_id` / `parent_step_id` columns and `listChildren` are
unchanged). However, child runs persisted **before** this release carry the old
top-level `parentRunId` / `parentStepId` keys in their payload and will revive
with `parent === undefined`, so their parent will not be woken on completion. No
migration shim is provided (pre-1.0); drain in-flight subflows before upgrading,
or backfill the payloads. The in-memory store has no cross-restart persistence
and is unaffected.
