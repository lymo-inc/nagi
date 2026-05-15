# RFC 0004 — Multi-name signal waits

- **Status:** Draft
- **Author:** @jay (lymo-inc)
- **Created:** 2026-05-15 (JST)
- **Tracking issue:** lymo-inc/nagi#4
- **Related:** RFC 0001 (content-addressed snapshot store), `@nagi-js/core`
- **Research notes:** `0004-multi-name-signal-waits.research.md`

## Summary

Let one signal step resolve on the FIRST of several named signals, by
introducing a `names: readonly [string, ...string[]]` field on
`b.signal({...})`. Today `b.signal({ schema })` couples the step id to the
signal name. A step that should advance on *either* of N upstream sources has
no one-step model — callers reach for `b.match` and duplicate the downstream
handler. This RFC adds the missing model without touching back-compat for
single-name signals.

## Motivation

Lymo's `videoAnalysis` flow waits on either a `mux.audioReady` webhook or a
`recall.recordingReady` webhook for the same downstream "we have a
transcription input" join. The current workaround (`b.match` discriminator on
`input.source`) doubles the step count and forces two near-identical
handlers when the only real difference is the signal name and payload shape.

The forcing observation: **one step, N arrival names** is a concept the
builder doesn't express. Match is a different concept (mutually exclusive
branches with their own downstream chains); signal-with-aliases is "any of
these payloads, then continue."

## Proposed API

### Single-name (today, unchanged)

```ts
review: b.signal({ schema })
// → signal name = step id ("review"); wf.signal(runId, "review", payload)
```

Existing callers see no change — neither in source nor in snapshot hash.

### Single-name, explicit

```ts
review: b.signal({ names: ["approval"], schema })
// → wf.signal(runId, "approval", payload). Step id stays "review".
```

Useful when the natural step id (e.g. `review`) differs from the external
signal name (`approval`). The single-element list is the same primitive as
multi-name, just with one entry — there is no separate singular `name` field.

### Multi-name

```ts
transcript: b.signal({
  names: ["audioReady", "recordingReady"],
  schema: z.union([audioReadySchema, recordingReadySchema]),
})
// → wf.signal(runId, "audioReady" | "recordingReady", payload). First wins.
```

Same downstream join; two upstream sources. The schema is the caller's
discrimination mechanism — the runtime validates the payload against it
unchanged.

## Semantics

| Scenario | Behavior |
|---|---|
| Caller sends a name listed in `names` | Step transitions `running → completed`; payload validated against the schema; downstream advances |
| Caller sends a name not listed AND not equal to the step id | `NagiRuntimeError` ("Flow X has no step matching signal Y") |
| Caller sends a recognized name AFTER another already resolved the step | **No-op + logged.** No throw. No second fact. The step stayed `completed`. |
| Caller sends a recognized name AFTER the step `failed`/`skipped`/never `running` | `NagiRuntimeError` as today |
| Two signal steps in the same flow declare overlapping names | **Construction-time error** in `flow()` |
| A signal step's `names` includes its own step id explicitly | Allowed; deduped internally |
| `signalNames` participates in the canonical hash | Yes — sorted list folded into `CanonicalStep` |

### "First wins, losers no-op"

Today, sending a signal to a non-`running` step throws. That's right for
single-name (only the intended sender can hit it). With aliases, a losing
upstream may genuinely race the winner and arrive milliseconds late — that's
operational normal, not a bug. Distinguish:

- The incoming name is a recognized alias of a signal step in this flow, AND
  that step is no longer `running`: late loser → `logger.info("nagi: signal
  arrived after step resolved", { runId, stepId, signalName })`, no throw.
- The incoming name has no associated step at all: throw (genuine call-site
  bug).

### Construction-time uniqueness

The signal-name space and the step-id space share one namespace per flow:
every step id is also implicitly a "name" callers can send. So uniqueness is
enforced over the union of `(stepId, ...signalDef.names)` across the entire
flat `flow.steps` map. `flow()` builds this set and throws on duplicate
insert with a message that names both conflicting sources.

This locks out the foot-gun where two signal steps quietly accept the same
external name and a deploy starts misrouting webhooks.

## Detailed design

### `SignalConfig` — single interface

`packages/core/src/types.ts:166–172`:

```ts
export interface SignalConfig<Input, N extends NeedsMap, Schema extends StandardSchemaV1>
  extends StepConfigBase<Input, N> {
  readonly schema: Schema;
  /**
   * Signal names this step accepts. Omitted → defaults to [stepId].
   * Non-empty tuple type — `names: []` is a compile error.
   */
  readonly names?: readonly [string, ...string[]];
}
```

One field, one optional. No discriminated union, no `name` / `names`
mutual exclusion to enforce — the singular-explicit case is just
`names: ["x"]`.

### `SignalDef` — normalized representation

`packages/core/src/internal.ts:57–67`:

```ts
interface SignalDef {
  readonly kind: "signal";
  readonly needs: NeedsMap;
  readonly schema: StandardSchemaV1;
  /** Resolved signal names this step accepts. Omitted when names == [stepId]. */
  readonly names?: readonly [string, ...string[]];
  readonly timeoutMs?: Millis;
  readonly when?: (args: { input: unknown; needs: Record<string, unknown> }) => boolean;
  readonly parentMatch?: ParentMatchRef;
}
```

Convention: the internal `names` field is **only** populated when the caller
explicitly supplied `names`. The default case (no `names` set) leaves it
undefined and the lookup path resolves via step id. This keeps the canonical
hash byte-identical for back-compat callers.

### Builder

`packages/core/src/builder.ts:112–132`:

```ts
function signal<N extends NeedsMap, S extends StandardSchemaV1>(
  config: SignalConfig<Input, N, S>,
): Step<InferSchemaOutput<S>> {
  const def: SignalDef = {
    kind: "signal",
    needs: (config.needs ?? {}) as NeedsMap,
    schema: config.schema,
    ...(config.names !== undefined ? { names: config.names } : {}),
    ...(config.timeoutMs !== undefined ? { timeoutMs: config.timeoutMs } : {}),
    ...(config.when !== undefined ? { when: config.when as ... } : {}),
  };
  return attachDef<InferSchemaOutput<S>>({ kind: "signal", id: "" }, def);
}
```

One conditional spread. `config.names` is already the right shape (or
undefined). No normalization, no discrimination of input shapes.

### `flow()` — uniqueness check (validate-only, no persistence)

Add a pass after `walkAndRewrite` in `flow()` (`builder.ts:301–343`) that
walks `(stepId, ...signal aliases)` and throws on collision. The `claimed`
map is **local to the check** — it falls out of scope when `flow()` returns.
No long-lived index is built; we trust that the same iteration happens
freshly inside `wf.signal()` when needed.

```ts
const claimed = new Map<string, string>(); // name → "step:<id>" | "alias:<id>"
for (const [stepId, step] of Object.entries(flatSteps)) {
  const def = getDef(step);
  const isSignal = def.kind === "signal";
  const aliases = isSignal && def.names ? def.names : null;
  const names = aliases ?? [stepId];
  for (const name of names) {
    const prior = claimed.get(name);
    if (prior !== undefined) {
      throw new NagiBuildError(
        `Flow "${flowId}": signal name "${name}" is claimed by both ` +
          `${prior} and ${aliases ? `alias:${stepId}` : `step:${stepId}`}. ` +
          `Pick one. (Signal names share a namespace with step ids.)`,
      );
    }
    claimed.set(name, aliases ? `alias:${stepId}` : `step:${stepId}`);
  }
}
```

Naming the source in the error lets the caller jump straight to the bad
code:

```
Flow "videoAnalysis": signal name "audioReady" is claimed by both
    alias:transcript and step:audioReady. Pick one.
```

### `wf.signal()` — inline lookup + late-loser path

`packages/core/src/runtime.ts:381–454`:

```ts
async signal(runId, name, payload): Promise<void> {
  const runState = await store.loadRunState(runId);
  const flow = flowsById.get(runState.flowId);
  if (!flow) throw new NagiRuntimeError(...);

  // Fast path: name === step id and that step is a signal.
  // Slow path: scan for a signal step whose explicit `names` includes `name`.
  let matchedStepId: string | undefined;
  const direct = flow.steps[name];
  if (direct && getDef(direct).kind === "signal") {
    matchedStepId = name;
  } else {
    for (const [stepId, step] of Object.entries(flow.steps)) {
      const def = getDef(step);
      if (def.kind === "signal" && def.names?.includes(name)) {
        matchedStepId = stepId;
        break;
      }
    }
  }

  if (matchedStepId === undefined) {
    throw new NagiRuntimeError(
      `Flow "${flow.id}" has no signal step accepting "${name}".`,
    );
  }
  const def = getDef(flow.steps[matchedStepId]);   // narrowed to signal above
  const stepState = runState.steps[matchedStepId];

  if (stepState?.status !== "running") {
    // Late loser? `running` is the only state a signal can resolve in.
    if (stepState?.status === "completed") {
      logger.info("nagi: signal arrived after step resolved", {
        runId, stepId: matchedStepId, signalName: name,
      });
      return;
    }
    throw new NagiRuntimeError(
      `Step "${matchedStepId}" is not waiting for signal ` +
        `(status: ${stepState?.status ?? "pending"}).`,
    );
  }

  const validated = (await validate(def.schema, payload)) as Json;
  const fact: SignalReceivedFact = {
    kind: "signal.received",
    runId,
    stepId: matchedStepId,
    payload: validated,
    at: clock.now(),
    ...(name !== matchedStepId ? { signalName: name } : {}),
  };
  await store.appendFact(runId, fact);
  await store.completeStep(runId, matchedStepId, validated, completedFact);
  await fireRuntimeHook(config.hooks?.onSignalReceived, {...});
  await advance(dispatchDeps, runId);
}
```

The scan is O(steps) per webhook arrival. At LLM-workflow scale (tens of
steps per flow, single-digit webhook arrivals per run), this is microseconds
and not on any hot path. Pre-building a boot-time `Map<flowId, Map<name,
stepId>>` was rejected as premature optimization that would have
double-bookkept against `flow.steps`.

### `SignalReceivedFact` — optional `signalName`

`packages/core/src/types.ts:985–989`:

```ts
interface SignalReceivedFact extends FactBase {
  readonly kind: "signal.received";
  readonly stepId: StepId;
  readonly payload: Json;
  /** The alias the caller sent, if different from `stepId`. */
  readonly signalName?: string;
}
```

Optional, so older facts in storage still validate. The projector and
adapters don't need to change.

### Canonicalize — fold names into hash

`packages/core/src/canonicalize.ts:149–158`:

```ts
async function canonicalizeSignal(base, def): Promise<CanonicalStep> {
  const out = { ...base };
  if (def.when !== undefined) out.whenHash = await hashFnSource(def.when);
  if (def.timeoutMs !== undefined) out.timeoutMs = def.timeoutMs;
  out.signalSchema = await canonicalizeSchema(def.schema);
  if (def.names !== undefined) {
    out.signalNames = [...def.names].sort();
  }
  return out;
}
```

And add `signalNames?: readonly string[]` to `CanonicalStep`. The key
back-compat property: `def.names` is only set when the caller explicitly
supplied `name` or `names`. Default-single-name callers leave `def.names`
undefined → `signalNames` is omitted from the canonical projection → hash
stays byte-identical.

## What does NOT change

- `SignalDef.kind`, the step kind union, and downstream typing of
  `b.signal()`'s return.
- The dispatcher (`dispatch.ts:190–193`).
- The queue contract.
- The Postgres schema (`fact` table absorbs `signal.received` generically).
- Any adapter package.
- The `signal.sent` fact (unused; left alone).
- Existing single-name call sites — neither source nor hash.

## Testing

New file: `packages/core/src/signal-multi-name.test.ts` (or extend
`runtime.test.ts`).

Pinned behaviors:

1. **Default (no name field).** `b.signal({ schema })` → `wf.signal(runId,
   stepId, payload)` resolves as today.
2. **Single explicit name.** `b.signal({ names: ["x"], schema })` (step id
   `review`) → `wf.signal(runId, "x", payload)` resolves; sending the step id
   `review` instead does NOT resolve (throws "no signal step accepting ...").
3. **Multi-name: first wins.** `names: ["a", "b"]`, send `a` first → step
   resolves with `a`'s payload. Subsequent `wf.signal(runId, "b", payload2)`
   is a no-op (no second `signal.received` fact, no throw).
4. **Multi-name: order-independent.** Same flow, send `b` first instead → step
   resolves with `b`'s payload.
5. **Audit trail.** `SignalReceivedFact.signalName === "a"` when a multi-name
   step resolves via `a`; `signalName` absent when name == stepId.
6. **Unknown name throws.** `wf.signal(runId, "neverDeclared", payload)`
   throws `NagiRuntimeError`.
7. **Construction-time uniqueness — alias vs step id collision.** A flow
   with `b.signal({ names: ["x"] })` AND a separate `x: b.signal({...})`
   throws at `flow()` build time.
8. **Construction-time uniqueness — alias vs alias.** Two signal steps with
   overlapping `names` throws at `flow()` time.
9. **Type-level empty-tuple rejection.** `// @ts-expect-error` smoke test:
   `b.signal({ names: [], schema })` is a type error (the non-empty tuple
   type forbids it).
10. **Hash invariants (extend `fingerprint.test.ts` or a new
    `canonicalize.test.ts` case):**
    - `b.signal({ schema })` vs `b.signal({ schema })` (same flow id, same
      step id) → same hash. **No movement vs pre-RFC behavior.**
    - `b.signal({ schema })` (step id `x`) vs `b.signal({ names: ["x"] })`
      (step id `y`) → different hash even though the resolved single name
      `"x"` is the same. The presence of an explicit `names` is a structural
      signal.
    - `b.signal({ schema })` (step id `x`) vs `b.signal({ names: ["x"] })`
      (step id `x`) → different hash. Same routing surface, but the explicit
      declaration is a structural intent signal worth preserving.
    - `b.signal({ names: ["a", "b"] })` vs `b.signal({ names: ["b", "a"] })`
      → same hash (sorted before folding).
    - `b.signal({ names: ["a", "b"] })` vs `b.signal({ names: ["a", "c"] })`
      → different hash.

## Migration / compatibility

- **Schema:** unchanged.
- **Fact log:** `signal.received` gets an optional `signalName` field. Old
  facts and new facts coexist; projectors ignore unknown fields.
- **Existing flows:** byte-stable. No source changes; no hash changes; no
  snapshot churn; no `code_version` movement.
- **New flows using `name` / `names`:** their hashes differ from a
  hypothetical "same step id, no name field" variant. This is correct — the
  routing surface differs.

## Considered alternatives

### Names as keys in a record

```ts
b.signal({ aliases: { audioReady: muxSchema, recordingReady: recallSchema } })
```

Couples name and per-name schema; lets the step validate dynamically based
on which name arrived. Heavier API: two failure-mode questions (what if one
arm's schema diverges? what if the union is what the downstream needs?).
Issue text explicitly punts schema discrimination to the caller. Rejected.

### Implicit name-from-schema discriminant

Inspect the schema for a discriminator field, auto-derive the name set.
Vendor-specific; breaks the schema-agnostic boundary (`StandardSchemaV1`).
Rejected.

### Match-with-shared-downstream

Add `match` ergonomics for sharing a single downstream chain. Solves a
different (broader) problem and doesn't address the case where the *only*
divergence is the signal-arrival name. Out of scope.

### Runtime ambiguity instead of construction-time

Let two signal steps share aliases; resolve by "first declared wins" or
similar. Violates the unrepresentable-invalid-states locked feedback. The
bad flow boots fine and only fails at the wrong incoming webhook —
strictly worse. Rejected.

## Out of scope for v1

- A `signal.received-loser` fact kind for losing arrivals (log line is
  enough; revisit if observability asks).
- Cancel-the-other-source on first win (would require a back-channel from
  nagi to the webhook source; not nagi's job).
- Multi-name with per-name schemas in the same step (see "alternatives").
- Public `signalNames(flowId)` helper (could be added later if callers want
  a runtime view of accepted names; not needed for the feature).

## Implementation order

1. **Types** — `SignalConfig.names?` (single optional field),
   `SignalDef.names?`, `SignalReceivedFact.signalName?`,
   `CanonicalStep.signalNames?`.
2. **Builder** — one conditional spread of `config.names` into `SignalDef`
   (only set when explicit).
3. **`flow()` uniqueness pass** — single validate-only pass over
   `(stepId, ...signal aliases)` throwing on collision; nothing persisted.
4. **`canonicalize.ts`** — fold `signalNames` into `CanonicalStep` when
   present.
5. **`wf.signal()`** — fast-path `flow.steps[name]` for `name === stepId`,
   slow-path scan for `def.names?.includes(name)`; add late-loser branch
   (log + return); attach `signalName` to fact when name != stepId.
6. **Tests** — new file (or extend `runtime.test.ts` + `canonicalize.test.ts`).
7. **Changeset** — `patch` for `@nagi-js/core` documenting the new fields
   (additive; no breaking changes).

README and public docs are Jay's to write.
