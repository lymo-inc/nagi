# RFC 0004 — Multi-name signal waits (research notes)

- **Tracking issue:** lymo-inc/nagi#4
- **Date:** 2026-05-15 (JST)
- **Scope:** `@nagi-js/core` only. No persistence-schema change; no adapter change.

Working notes that back the RFC. Map what the codebase does today, where the
step-id-as-signal-name coupling lives, and what the implementation surface looks
like.

## Current state — signal lifecycle

### Public API

`b.signal(config)` lives at `packages/core/src/builder.ts:112–132`. The
caller-visible `SignalConfig` (`packages/core/src/types.ts:166–172`) carries
exactly four fields:

```ts
interface SignalConfig<Input, N, Schema> extends StepConfigBase<Input, N> {
  readonly schema: Schema;            // explicit
  // inherited from StepConfigBase:
  readonly needs?: N;
  readonly when?: (args: {...}) => boolean;
  readonly timeoutMs?: Millis;
}
```

There is **no signal-name field today**. The step's persistence handle (the key
in the `build` return object → the step id assigned by `flow()` →
`builder.ts:346–364`) IS the signal name.

### Internal def

`SignalDef` lives at `packages/core/src/internal.ts:57–67`:

```ts
interface SignalDef {
  readonly kind: "signal";
  readonly needs: NeedsMap;
  readonly schema: StandardSchemaV1;
  readonly timeoutMs?: Millis;
  readonly when?: (...) => boolean;
  readonly parentMatch?: ParentMatchRef;
}
```

Same surface; no name field.

### Inbound resolution — `wf.signal()`

`packages/core/src/runtime.ts:381–454`:

```
async signal(runId, stepName, payload) {
  ...
  const step = flow.steps[stepName];     // ← direct lookup by step id
  if (!step) throw NagiRuntimeError(...)
  const def = getDef(step);
  if (def.kind !== "signal") throw ...
  if (runState.steps[stepName].status !== "running") throw ...
  const validated = await validate(def.schema, payload);
  await store.appendFact({ kind: "signal.received", stepId: stepName, payload });
  await store.completeStep(runId, stepName, validated, completedFact);
  await advance(...);
}
```

Three coupling points in the lookup path:
1. `flow.steps[stepName]` — direct hash lookup keyed by step id.
2. `runState.steps[stepName]` — same key reused for status check.
3. `signal.received` and `step.completed` facts carry `stepId: stepName`. The
   incoming caller-supplied "name" and the step id are the same value today.

### Dispatcher path

`packages/core/src/dispatch.ts:190–193`:

```ts
} else if (def.kind === "signal") {
  // Signals don't run; they wait. Mark `started` (already done above) and ack.
  // Completion arrives via `wf.signal()`.
  await queue.ack(message.receipt);
  return;
}
```

No name correlation in the dispatcher. The signal step parks in `running`; the
external `wf.signal()` call drives completion. Multi-name does not touch this
path.

### Canonicalize projection

`packages/core/src/canonicalize.ts:149–158`:

```ts
async function canonicalizeSignal(base, def) {
  const out = { ...base };
  if (def.when !== undefined) out.whenHash = await hashFnSource(def.when);
  if (def.timeoutMs !== undefined) out.timeoutMs = def.timeoutMs;
  out.signalSchema = await canonicalizeSchema(def.schema);
  return out;
}
```

Today only `signalSchema` (vendor, version, `validateHash`) is folded into the
hash. The signal name is implicit in `CanonicalStep.id`. Once a `names` list
is introduced, the list must be sorted and folded into the hash so equivalent
multi-name configs hash identically and routing changes move the hash.

### Fact log

`packages/core/src/types.ts:985–989`:

```ts
interface SignalReceivedFact {
  kind: "signal.received";
  runId: RunId;
  at: Date;
  stepId: StepId;
  payload: Json;
}
```

`stepId` IS the resolved signal name today because they're the same string.
Under multi-name, the step id and the incoming signal name diverge — the audit
trail needs to record both. Adding an optional `signalName?: string` to
`SignalReceivedFact` (`undefined` when name == stepId for back-compat) covers
it.

`signal.sent` (`types.ts:979–983`) is declared but no runtime code path
appends it — dead infrastructure for now; leave alone.

### Postgres persistence

`packages/postgres/src/store.ts:448–449` has a no-op switch case for
`signal.received` (the generic `fact` table absorbs it via `insertFact`). No
signal-name column exists. No persistence-schema change required.

## Tests on file

- `packages/core/src/runtime.test.ts:118–156` — `"e2e: signal full loop"`,
  asserts the happy path resolves a single-name signal and downstream sees the
  payload via `needs.review`.
- `packages/core/src/dispatch.test.ts:148–180` — asserts the dispatcher leaves
  a signal step in `running` and acks the message without invoking a handler.
- `packages/core/src/dispatch.test.ts:250–278` — `StepStartEvent.input` is
  `null` for signal steps.
- `packages/core/src/canonicalize.test.ts:254`, `:293` — assert the hash
  changes when the signal schema's `validate` source or `vendor` changes.
- `packages/core/src/builder.test.ts:116–131` — asserts `b.signal(...)` ends
  up with `kind === "signal"` after `flow()` finishes.

No file is dedicated to signal behavior; coverage is spread across four files.

## Proposed shape (from the issue)

```ts
b.signal({
  names: ['audioReady', 'recordingReady'],
  schema: z.union([audioReadySchema, recordingReadySchema]),
});
```

Single-name remains via `name: 'audioReady'` or `names: ['audioReady']`, or
omit both (default is the step id — back-compat unchanged). First arrival wins.
Late losers are a no-op + logged. The schema is the caller's discrimination
mechanism (a discriminated union).

## Decisions to settle

### 1. `name` vs `names` — both or one?

Two viable shapes:

- **A: `names` only.** `b.signal({ names: ['x'] })` for single-name; omit for
  default. One field.
- **B: `name` xor `names`.** `b.signal({ name: 'x' })` reads cleaner for the
  common case. `names: ['x','y']` for multi.

The issue text mentions both options. Shape B reads better at call sites and
matches how a person describes the step out loud ("the audioReady signal");
single-name is overwhelmingly the common case. The mutual-exclusion can be
enforced with a discriminated union in TS so neither runtime-throws nor sits
in the API surface as a foot-gun (`name` and `names` simultaneously). Going
with **B** — see `Detailed design`.

### 2. Default when both omitted

`b.signal({ schema })` (no `name`, no `names`) → name defaults to the step id.
This preserves all existing call sites byte-for-byte. The signal-name space
inside the flow is identical to the step-id space.

### 3. Name uniqueness across the flow

If `b.signal({ names: ['x','y'] })` and `b.signal({ name: 'x' })` both live in
the same flow, `wf.signal(runId, 'x', payload)` is ambiguous. Two viable
strategies:

- **Construction-time error** (preferred — matches "unrepresentable invalid
  states"): `flow()` enforces global uniqueness across signal names + step
  ids, throws on overlap. A flow with duplicate names cannot be constructed.
- **Runtime resolution error.** `wf.signal()` discovers the ambiguity and
  throws. Worse: the bad flow boots fine and only blows up on a specific
  incoming signal, possibly months later.

Going with construction-time. Cheap to detect (single pass through
`flow.steps` in `flow()` or `nagi()`).

### 4. Late losers

Step is `completed` once the first signal lands. A second `wf.signal(runId,
losingName, payload)` against the now-not-`running` step takes the existing
"step is not waiting for signal" branch at `runtime.ts:406–409` and throws
`NagiRuntimeError`. The issue text proposes "no-op + logged" for losers —
softer than today's throw.

Two failure modes that look identical from the caller's POV:
- Genuine ambiguity (caller sent the wrong name at the wrong time).
- A losing webhook arrives milliseconds after the winner.

The first should be loud; the second is operationally normal and a noisy
throw is annoying. Distinguish by checking *whether the incoming name is a
recognized alias of the resolved signal step*. If it is, treat as a late
loser → log + ack. If it isn't, throw as today (no step is or was waiting on
this name).

### 5. Audit trail — record which name resolved

Adding `signalName?: string` to `SignalReceivedFact` lets observers see which
alias triggered resolution. Optional for back-compat: when name == stepId
(single-name default) the field can stay `undefined`. When multi-name resolves,
the field is populated.

Could also append a `signal.received` fact for the *losing* name (with a flag
or a different kind). Out of scope for v1 — log line is enough.

### 6. Canonical hash inclusion

The `names` list MUST fold into the canonical hash. Two configs with the same
schema but different `names` lists route differently — they're not equivalent
flows and shouldn't snapshot-share. Specifically:

- `CanonicalStep` gets `signalNames?: readonly string[]` (sorted).
- `canonicalizeSignal` sets `signalNames` whenever names are explicit OR
  there is more than one. When `names` defaults to `[stepId]` (back-compat
  single-name path), omit `signalNames` so old flows keep hashing the same.

This last detail keeps the v0→v1 upgrade snapshot-stable for callers who
never set `name` / `names`. Their hashes don't move; their `code_version`
stays put.

## What does not change

- `SignalDef.kind === "signal"` — same kind.
- The dispatcher path for signal steps — `dispatch.ts:190–193`.
- The fact log's `signal.received` and `step.completed` shapes (modulo the
  optional `signalName` field on the former — additive).
- The Postgres schema, the queue contract, the worker loop.

## Risk surface

- **Type-level mutual exclusion.** `name` xor `names` must hold at compile
  time, not just at runtime. A discriminated union (`{ name?: string; names?:
  never } | { names: readonly [string, ...string[]]; name?: never }`) does it.
- **Construction-time uniqueness check.** Must run before `flow()` returns,
  not at first signal arrival. Implementation: a single pass in `flow()` (or
  on registration in `nagi()`) collecting `(stepId | each-signalName)` into a
  Map, throwing on duplicate.
- **Snapshot stability for single-name callers.** Discussed above — gate
  `signalNames` inclusion on "names were explicit OR length > 1."

## File map — changes for the implementation

- `packages/core/src/types.ts` — extend `SignalConfig` to the discriminated
  union; add `signalName?` to `SignalReceivedFact`.
- `packages/core/src/internal.ts` — add `names?: readonly [string, ...string[]]`
  to `SignalDef`.
- `packages/core/src/builder.ts:112–132` — normalize `name` / `names` /
  neither into a single internal representation on `SignalDef`; emit the
  uniqueness check inside `flow()` (`builder.ts:301–343`).
- `packages/core/src/runtime.ts:381–454` — `wf.signal()` lookup walks an
  index `Map<signalName, stepId>` (built once at `nagi()` boot) instead of
  doing `flow.steps[stepName]`. Late-loser branch logs instead of throws.
  Append `signalName` on the fact when name != stepId.
- `packages/core/src/canonicalize.ts:149–158` — fold sorted `names` into
  `CanonicalStep.signalNames` when present.
- `packages/core/src/index.ts` — no public-surface re-exports needed (no new
  exported helper for this feature).

No file outside `packages/core/src/` changes.
