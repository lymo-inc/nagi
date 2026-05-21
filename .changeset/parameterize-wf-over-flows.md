---
"@nagi-js/core": patch
---

`Wf` is now generic over the registered flow tuple, so `flowId` carries its
literal type through queries instead of widening to `string`.
`nagi({ flows: [videoAnalysisFlow, dealAnalysisFlow] })` (no `as const` needed —
the factory uses a `const` type parameter) yields a `Wf` whose
`queryRuns()` returns `RunSummary<"videoAnalysis" | "dealAnalysis">`. Consumers
with a closed flow set can delete hand-maintained id sets / `narrowFlowId`
helpers at the `queryRuns` projection boundary — the union is now derived from
the registration and cannot drift.

`RunSummary`, `QueryRunsResult`, `QueryRunsWhere`, and `QueryRunsOpts` each gain
a `FlowId extends string = string` type parameter, and a `FlowIdOf<TFlows>`
helper is exported. `Wf.start` is narrowed to `start<F extends TFlows[number]>`,
so starting a flow that was not registered with `nagi({ flows })` is now a
compile error rather than a runtime throw. `where.flowId` accepts only the
registered union (a typo is a compile error).

Pure typing change — **no runtime delta**, no fact-shape change, no migration.
Fully backwards-compatible: every new type parameter defaults to today's
behavior, so bare `Wf` / `RunSummary` / `queryRuns` still resolve `flowId` to
`string`. `startById(flowId: string)` is intentionally left untyped for
outbox/DLQ-replay callers holding a serialized id. The `Store` contract is
unchanged (adapters read `flow_id` as `string`); the literal narrowing is a
single documented assertion at the `wf.queryRuns` read boundary. Tracks #18.
