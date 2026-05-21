# RFC 0010 — OTel parent-span linkage for subflow runs

- **Status:** Implemented (2026-05-21 JST) — `@nagi-js/otel` 54/54 tests pass, typecheck clean. See `0010-otel-subflow-span-linkage.handoff.md`.
- **Author:** @jay (lymo-inc)
- **Created:** 2026-05-20 (JST)
- **Tracking issue:** lymo-inc/nagi#10 (follow-up — main runtime shipped in `c4e1459`)
- **Related:** `docs/research/issue-10-subflow-runtime.md`, RFC 0004 (style template), `@nagi-js/otel`
- **Decisions log:** authoritative — see "Decisions taken" below.
- **Deferred (documented in handoff):** integration tests (items 8–10), type-d tests (items 11–12), cross-process `traceparent`, `FlowStartedFact` struct migration.

## Summary

`b.subflow()` runtime ships today. Child runs execute correctly, parent
linkage is persisted on `FlowStartedFact.parentRunId / parentStepId`, cancel
cascades work. **But the OTel adapter doesn't link the child flow's root
span to anything in the parent's trace** — child traces appear as
disconnected roots.

This RFC threads parent-step linkage into `FlowStartEvent` and teaches
`otelHooks.onFlowStart` to nest the child flow span under the parent's
step span. The visible effect is a single contiguous trace tree from
top-level run down through arbitrary subflow depth.

## Motivation

Today, given:

```ts
const child = flow({ id: "transcribe", ... });
const parent = flow({
  id: "analyze",
  build: (b) => ({
    audio: b.signal({ schema: audioSchema }),
    transcript: b.subflow(child, { needs: { audio }, input: ({ needs }) => ({ url: needs.audio.url }) }),
  }),
});
```

OTel exports two unrelated traces — one rooted at `flow analyze`, one at
`flow transcribe`. Operators correlate them via `nagi.run.id` /
`nagi.parent.run.id` attributes by hand. For lymo's `videoAnalysis`-style
flows (3+ subflows, depth-2 nesting), this collapses observability.

`FlowStartedFact` already records the linkage durably. The gap is purely
in the in-process hook event: `FlowStartEvent` doesn't carry the parent
info, so the OTel adapter has nothing to anchor on.

## Decisions taken (2026-05-20, Jay)

1. **Span linkage point:** child flow root span attaches to the **parent
   step span** (the call site). Trace tree becomes
   `parent-flow → parent-step (subflow) → child-flow → child-step…`.
   Matches OTel RPC semantic conventions.
2. **Event field shape:** `FlowStartEvent.parent?: { runId, stepId,
   attempt }` — a **single optional struct**, not three separate
   optionals. Makes "half-set" structurally impossible. Diverges from
   `FlowStartedFact`'s pre-existing two-optionals shape; this RFC does
   NOT propagate that pattern.
3. **Cross-process / missing-parent fallback:** parent step span → local
   parent flow context (`flowCtxs`) → OTel `context.active()`. Graceful
   degradation; never throws, never drops the child span.
4. **`FlowStartedFact` shape:** unchanged. Persisting `parentStepAttempt`
   on the fact is YAGNI — only useful for OTel which is in-process.

## Proposed API

### `FlowStartEvent` — add optional `parent` struct

`packages/core/src/types.ts:397–399`:

```ts
export interface FlowStartEvent extends FlowEvent {
  readonly input: Json;
  /**
   * Set when this run was started as a subflow child. Undefined for
   * top-level runs (start / startById). Carried in-process only — for
   * durable linkage see FlowStartedFact.parentRunId / parentStepId.
   */
  readonly parent?: {
    readonly runId: RunId;
    readonly stepId: StepId;
    readonly attempt: AttemptNumber;
  };
}
```

One optional struct. Consumers narrow all three fields with a single
`if (event.parent) { ... }`.

### `otelHooks.onFlowStart` — anchor child span under parent step

`packages/otel/src/hooks.ts:132–139` becomes:

```ts
onFlowStart: withGuard<FlowStartEvent>("onFlowStart", (event) => {
  const parentCtx = resolveParentContext(event);
  const attrs: Attributes = { ...flowAttrs(event) };
  if (event.parent) {
    attrs["nagi.parent.run.id"] = event.parent.runId;
    attrs["nagi.parent.step.id"] = event.parent.stepId;
    attrs["nagi.parent.step.attempt"] = event.parent.attempt;
  }
  const span = tracer.startSpan(
    `${flowPrefix} ${event.flowId}`,
    { kind: SpanKind.INTERNAL, startTime: event.at, attributes: attrs },
    parentCtx,
  );
  flowCtxs.set(event.runId, trace.setSpan(context.active(), span));
}),
```

`resolveParentContext` (new internal helper):

```ts
function resolveParentContext(event: FlowStartEvent): Context {
  if (!event.parent) return context.active();
  const stepSpan = stepSpanRegistry.get(
    stepKey(event.parent.runId, event.parent.stepId, event.parent.attempt),
  );
  if (stepSpan) return trace.setSpan(context.active(), stepSpan);
  const parentFlowCtx = flowCtxs.get(event.parent.runId);
  if (parentFlowCtx) return parentFlowCtx;
  return context.active();
}
```

### Plumbing — thread `attempt` from dispatch to event

`packages/core/src/dispatch.ts:373–402` (executeSubflow): pass
`message.attempt` to `startChildRun`.

`packages/core/src/runtime.ts:330–357` (startChildRun): add
`parentStepAttempt: AttemptNumber` arg, forward to `startRunInternal`.

`packages/core/src/runtime.ts:230–325` (startRunInternal): add
`parentStepAttempt?: AttemptNumber` arg. When present (always co-present
with `parentRunId` + `parentStepId`), populate `startEvent.parent`.

## Semantics

| Scenario | Behavior |
|---|---|
| Top-level run (`wf.start` / `wf.startById`) | `event.parent === undefined`. Hook starts span under `context.active()` (current behavior, byte-equivalent). |
| Subflow child, parent step span in registry | `event.parent` set; child flow span nests under parent step span. |
| Subflow child, parent step span GONE (process restart, replay) | `event.parent` set; falls back to local parent flow context. |
| Subflow child, both parent step + parent flow gone | `event.parent` set; falls back to `context.active()`. Child span is a root in this process. Linkage queryable via attributes. |
| Depth-N subflow nesting | Each level's `event.parent` points to its immediate parent. Tree forms naturally. |
| Hook called without `parent` field (older consumers) | `event.parent` is optional; missing field is undefined; hook acts as top-level. No throw. |

## Unrepresentable invalid states

This is the heart of decision (2). Three options were considered:

**(a) `parent?: { runId, stepId, attempt }` — chosen.**
A subflow run has all three pieces of parent info, or none. The struct
makes "partial linkage" (e.g. `parentRunId` set, `parentStepId` missing)
**impossible at the type level**. One conditional spread in
`startRunInternal`, one type guard in `onFlowStart`.

**(b) Three separate optionals** — rejected.
Mirrors `FlowStartedFact`'s pre-existing shape. But the fact's two
optionals are a known soft-deficiency — they allow a half-linked state
through the type system. Propagating the pattern would compound it.

**(c) Fix `FlowStartedFact` to the struct shape too** — rejected (scope).
Cleanest end state, but touches the PG projector, the in-memory store's
`projectRunState`, and any downstream fact consumers (cancel-cascade,
list-children). Outside this RFC's blast radius. Filing follow-up: "Audit
`FlowStartedFact.parentRunId/parentStepId` for unrepresentable-states
compliance."

### What stays representable but invalid

- `event.parent.runId === event.runId` (self-parent). Caught at write time
  inside `startChildRun` by the existing `flowsById.has(child.id)` check
  + `runId` uniqueness on `tryStartRun`. Worth a runtime guard? No —
  cannot reach this state from public API; would only arise from
  in-process call-site corruption.
- `event.parent.attempt < 1`. `AttemptNumber` is currently
  `type AttemptNumber = number` — no brand, no positivity check. Inherited
  weakness, not introduced here. Skipping.

## Outbox-pattern review

**Result: N/A. No outbox concern for this change.**

Outbox concerns arise when two systems must agree on whether an effect
happened (e.g., DB write + queue enqueue, or DB write + external
notification). This RFC modifies only:

1. The type of an in-memory event object (`FlowStartEvent`).
2. The body of an in-process hook handler (`otelHooks.onFlowStart`).
3. In-process plumbing arguments (`startChildRun` / `startRunInternal`).

The subflow runtime's actual outbox surface — `store.tryStartRun(childRunId,
flow.started, concurrency)` — is the atomic transactional write of the
child's `flow.started` fact. That hop happened in `c4e1459` and is not
re-examined here. The OTel hook fires **after** that write commits
(`runtime.ts:319-324`), so the worst case from a hook failure is a
missing trace span — never a corrupted fact log.

The hook is already wrapped in `withGuard` (`hooks.ts:51–62`), which
catches and logs without rethrowing. Span-level failures are
operationally observable (the `[@nagi-js/otel] onFlowStart hook failed`
log line) and do not affect business correctness.

## What does NOT change

- `FlowStartedFact` — same two-optional shape. PG schema unchanged. No
  migration.
- `Store` / `Queue` interfaces — no surface change.
- `flowCtxs` lifecycle in `otelHooks` — child runs still register and
  clean up their own entries. (The fallback path *reads* the parent's
  `flowCtxs` entry but does not mutate it.)
- `stepSpanRegistry` — write/consume contract unchanged. New code only
  *reads* via the existing `stepKey` lookup.
- `b.subflow()` builder, `SubflowDef`, `executeSubflow`,
  `propagateToParent`, `cancelRunRecursive` — all subflow runtime
  behavior is invariant.
- All existing tests pass byte-equivalent. Top-level run traces are
  unchanged (`event.parent` is undefined → `parentCtx = context.active()`,
  same as today's `context.active()` fallback).
- Canonical flow hash. Span tree shape is observability, not protocol.

## Testing

New file: `packages/otel/src/subflow-linkage.test.ts` (or extend
`hooks.test.ts`).

### Hook-level tests (fake events, no runtime)

1. `it('nests child flow span under parent step span when event.parent is set and parent step span is registered')`
2. `it('falls back to parent flow span when parent step span is not in the registry')`
3. `it('falls back to OTel active context when neither parent step span nor parent flow context is local')`
4. `it('starts child flow span as a root when event.parent is undefined')` — back-compat
5. `it('records nagi.parent.run.id, nagi.parent.step.id, nagi.parent.step.attempt as attributes when event.parent is set')`
6. `it('does not record parent attributes when event.parent is undefined')`
7. `it('does not throw when event.parent.runId references a runId that was never registered (cross-process)')` — graceful degradation

### Integration tests (real `makeHarness` + real subflow)

In `packages/otel/src/integration.test.ts` (already exists), add:

8. `it('subflow child flow span lists the parent subflow step span as its parent via SpanContext')` — assert on `ReadableSpan.parentSpanContext.spanId`
9. `it('depth-2 subflow nesting produces a chain of three flow spans')` — grandchild → child → parent
10. `it('parent step span end time is after child flow span end time (envelope invariant)')` — span lifetime

### Type-level (`hooks.test-d.ts` or extend existing)

11. `it('FlowStartEvent.parent is optional')`
12. `it('FlowStartEvent.parent narrows to { runId, stepId, attempt } when checked')`

## Migration / compatibility

- **Source compat:** `FlowStartEvent.parent` is optional and additive.
  Existing `FlowHooks` implementations that ignore it remain valid.
- **Fact log compat:** unchanged.
- **Snapshot/hash compat:** unchanged. `FlowStartEvent` is not part of
  canonicalization.
- **Adapter compat:** `@nagi-js/postgres` does not touch
  `FlowStartEvent`. No package edit required.
- **Existing top-level runs:** `event.parent` is undefined → behavior is
  byte-equivalent to today.
- **Existing subflow runs (already shipped):** start emitting
  `event.parent` for the first time. The OTel adapter is the only known
  consumer of `onFlowStart`; behavior change is observability-only.

## Considered alternatives

### Propagate W3C `traceparent` through `FlowStartedFact`

Persist the parent's traceparent on the child's `flow.started`. Solves
cross-process linkage fully. Rejected for this RFC: couples
`@nagi-js/core` types to OTel semantics, expands `@nagi-js/core`
changeset, and the in-process linkage handles the canonical case.
File as a follow-up if/when cross-process becomes a customer ask.

### Nest under parent flow span instead of parent step span

Simpler — uses existing `flowCtxs` without needing attempt threading.
Rejected: parent step is the call site; nesting under the flow span makes
the child flow a *sibling* of its calling step, which misrepresents the
causal relationship.

### Have the OTel hook read facts directly

Have `onFlowStart` call `store.loadRunState(event.runId)` to discover
parent linkage from the fact log. Rejected: violates the hook contract
(hooks are pure pub/sub; no I/O). Also: store dependency would force
async hooks; current hooks are sync.

### Add `parentStepAttempt` to `FlowStartedFact`

Fact gains a third optional. Rejected: only consumer would be OTel, and
OTel is in-process. Persisting in-process plumbing on the durable fact
adds storage without serving any durable consumer.

## Out of scope for v1

- Cross-process trace propagation (traceparent on facts).
- Fixing `FlowStartedFact` to use the struct shape (separate audit RFC).
- `nagi.parent.run.id` exposure on step spans (only on the child flow
  span; step spans inside the child can be inferred by walking ancestry).
- Subflow parent step `durationMs` semantics — today's `step.completed`
  for a subflow step has `durationMs = wall time from step.started`
  computed in `propagateToParent`; the hook reads it. If incorrect,
  separate fix.

## Implementation order

1. **Types** — `FlowStartEvent.parent?` struct added to `types.ts`.
2. **Plumbing** — `startRunInternal` accepts `parentStepAttempt?`,
   builds `event.parent` when all three pieces are present.
3. **`startChildRun`** — accepts + forwards `parentStepAttempt`.
4. **`executeSubflow`** — passes `message.attempt` as
   `parentStepAttempt`.
5. **`@nagi-js/otel` hook** — `resolveParentContext` helper +
   `onFlowStart` rewrite + parent attributes.
6. **Tests** — hooks-level (1–7), integration (8–10), type-d (11–12).
7. **Changeset** — `@nagi-js/core` patch (`FlowStartEvent` additive
   field), `@nagi-js/otel` patch (linkage behavior).

## File index (cited)

- `packages/core/src/types.ts` — `FlowStartEvent:397`,
  `FlowStartedFact:689`, `AttemptNumber:12`, `RunId`, `StepId`.
- `packages/core/src/runtime.ts` — `startRunInternal:230`, start-event
  build `:307–312`, `startChildRun:330`, fire-hook `:319–324`.
- `packages/core/src/dispatch.ts` — `executeSubflow:373`,
  `propagateToParent:787`.
- `packages/otel/src/hooks.ts` — `otelHooks:41`, `startStepSpan:84`,
  `onFlowStart:132`, `withGuard:51`.
- `packages/otel/src/context.ts` — `stepKey:6`, `stepSpanRegistry:14`.
- `packages/otel/src/hooks.test.ts` — test patterns.
- `docs/research/issue-10-subflow-runtime.md` — prior decisions on the
  runtime piece that shipped.
