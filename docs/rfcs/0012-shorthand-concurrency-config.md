# RFC 0012 — Shorthand concurrency config (typed key + default mode)

- **Status:** Draft — decisions log COMPLETE (grilled 2026-05-21); awaiting approval before implementation
- **Author:** Claude (paired with @jay)
- **Created:** 2026-05-21 (JST)
- **Tracking issue:** #20
- **Scope:** Public API addition to the `concurrency` field on `flow()`. Backwards-compatible, `packages/core` only. Patch release.
- **Decisions log:** authoritative — see "Decisions taken" and "Open decisions" below.

## Summary

Today every flow that wants entity-keyed deduping writes the fully verbose
concurrency block, where **both** lines are mandatory:

```ts
concurrency: {
  keyFn: ({ videoId }) => videoId,   // compute the key
  mode: "cancel-in-progress",        // currently the ONLY mode that exists
},
```

This RFC reduces the dominant case (key by a single top-level input property,
default collision behavior) to a shorthand, while keeping `keyFn` as the
escape hatch for composite / computed keys. The win is real but small: it is
an ergonomics + type-safety change, not a semantics change. The runtime
cancellation path is **untouched**.

Two facts from the codebase audit reframe the issue's proposal:

1. **`ConcurrencyMode` is a single-member union** — `"cancel-in-progress"` is
   the only mode (`types.ts:231`). The issue's `mode: "hash"` example is
   aspirational; no second mode exists.
2. **`mode` is currently mandatory** in `FlowConcurrency` (`types.ts:233`).
   So half of the "4-line shape" is the forced `mode:` line, and making it
   optional is itself half the verbosity win — independent of the key sugar.

## Motivation

From issue #20: in `lymo-inc/lymo`, 4 of 4 flows use the identical 4-line
shape (`keyFn` single-property destructure + `mode: "cancel-in-progress"`). In
the single-task flows that block is a non-trivial fraction of the body. The
verbose form is not wrong — composite keys and (future) non-default modes
genuinely need it — but the dominant case pays full verbosity.

A second, quieter motivation surfaced in the prior-art survey: **no comparable
system gives you a compile-time-checked key.** Inngest, Temporal, Hatchet,
Trigger.dev, BullMQ all take stringly-typed CEL expressions / job IDs that
evaluate server-side against untyped payloads ([Inngest concurrency], [Hatchet
concurrency], [Temporal workflow ID]). Because Nagi's `keyFn` runs in-process
against a typed `Input`, a string shorthand constrained to `keyof Input` is a
differentiator the CEL-based engines structurally cannot offer — and it turns
key typos into compile errors instead of silent mis-keying.

[Inngest concurrency]: https://www.inngest.com/docs/guides/concurrency
[Hatchet concurrency]: https://docs.hatchet.run/home/concurrency
[Temporal workflow ID]: https://docs.temporal.io/workflow-execution/workflowid-runid

## Current shape (audit, for reference)

```ts
// types.ts:231-236
export type ConcurrencyMode = "cancel-in-progress";

export interface FlowConcurrency<Input = Json> {
  readonly keyFn: (input: Input) => string;
  readonly mode: ConcurrencyMode;
}
```

- `FlowConfig.concurrency?: FlowConcurrency<InferSchemaOutput<InputSchema>>`
  (`types.ts:249`) — fully typed `Input` at the `flow()` call site.
- Erased to `FlowConcurrency<Json>` on the stored `Flow` (`types.ts:272`).
- Sole runtime consumer: `runtime.ts:265-276` — `keyFn(validatedInput)` is
  called once at run start; `{ key, mode }` is passed to
  `store.tryStartRun`, which atomically cancels prior active runs sharing
  `(flowId, key)` (contract at `types.ts:396-412`). **`mode` is never branched
  on in the runtime** — the store is its only consumer.

## Decisions taken

> These are recommended calls with reasoning, resolved by the prior-art survey,
> the codebase audit, and project preferences ([[feedback_complexity_must_pay_for_itself]],
> [[feedback_unrepresentable_invalid_states]]). The genuinely uncertain forks
> are deferred to "Open decisions" and will be grilled before approval.

1. **`mode` becomes optional, defaulting to `cancel-in-progress`.** It is the
   only mode, so defaulting hides nothing today. This alone removes the
   mandatory `mode:` line from every call site. *(Ratified — Resolved Q2.)*

2. **Type the key against `keyof Input` (string-valued keys).** Issue says
   "yes if cheap"; the audit confirms it is cheap — `Input =
   InferSchemaOutput<InputSchema>` is already in scope at `types.ts:249`. A
   misspelled key becomes a compile error. **Strict: string-valued keys only**
   (`StringKeyOf<Input>`). *(Ratified — Resolved Q3.)*

3. **Key source is mutually exclusive by construction.** With surface A
   (Resolved Q1) the type is `StringKeyOf<Input> | { keyFn, mode? }` — a value
   is *either* a bare string *or* an object carrying `keyFn`. A config naming
   both a property key and a `keyFn` is structurally uninhabited; no `never`
   discriminant and no runtime guard are needed. ([[feedback_unrepresentable_invalid_states]])

4. **Top-level keys only; no dotted/nested paths.** The issue scopes this to "a
   single top-level input property." `"a.b"` reads a top-level property literally
   named `a.b` (it won't typecheck against a normal `keyof Input`), it is not a
   deep path. Composite/nested keys are exactly what `keyFn` is for.

5. **Runtime key validation is unchanged.** The derived key still flows through
   the existing non-empty-string check at `runtime.ts:267`; empty/undefined
   keys throw `NagiValidationError` as today. Whitespace-only remains accepted
   (length-only rule, preserved). No new validation surface.

6. **Second collision modes (`hash`, `serialize`, `enqueue`, …) are OUT OF
   SCOPE.** Only `cancel-in-progress` exists. Adding a mode is a separate RFC
   with its own semantics design; bolting it on here would be unearned
   complexity ([[feedback_complexity_must_pay_for_itself]]).

## Decisions resolved by grilling (2026-05-21, Jay)

### Resolved Q1 — Surface area → **A (bare string + `keyFn`)**

Ship the bare string for the dominant case; keep `keyFn` for composite/computed
keys. **The `key` field is dropped.** Its only edge over a bare string —
pairing a property-name key with an explicit `mode` — is moot while
`cancel-in-progress` is the only mode, and it is trivially additive in the RFC
that introduces a second mode. Rejected: B (issue verbatim — three forms, the
`key` field redundant today) and C (object-only — loses the punchy bare
string). Aligns with [[feedback_complexity_must_pay_for_itself]].

### Resolved Q2 — Default mode → **default `cancel-in-progress` everywhere**

Forced by Q1: a bare string cannot carry a mode, so it must default; requiring
explicit `mode` only on the `keyFn` form would be gratuitously inconsistent, so
`mode` is optional there too. Safe today because it is the only mode. The
survey's warning — the closest analogs (Inngest Singleton, Temporal, Hatchet)
never *silently* default a destructive collision policy ([Inngest Singleton],
[Temporal conflict policy]) — bites only once a second mode exists. **Recorded
constraint:** the future RFC that adds a second mode MUST make the collision
policy an explicit choice rather than silently widening this default.

### Resolved Q3 — Key value type → **strict: string-valued keys only**

`StringKeyOf<Input>` = keys of `Input` whose value is `string`. A key naming a
numeric/other-typed field is a compile error; such keys use
`keyFn: (i) => String(i.id)`. Chosen over lenient (string|number + coercion)
and loose (any key, runtime throw) because it (a) keeps invalid states
unrepresentable ([[feedback_unrepresentable_invalid_states]]), (b) preserves
the existing "keys are strings" contract, and (c) avoids the `123` vs `"123"`
collision footgun that silent coercion introduces. Accepted cost: numeric IDs
do not get the bare-string shorthand.

[Inngest Singleton]: https://www.inngest.com/docs/guides/singleton
[Temporal conflict policy]: https://docs.temporal.io/workflow-execution/workflowid-runid#workflow-id-conflict-policy

## Proposed shape (final — A + default mode + strict key typing)

```ts
// types.ts
export type ConcurrencyMode = "cancel-in-progress";

// keys of Input whose value is a string
type StringKeyOf<Input> = {
  [K in keyof Input]-?: Input[K] extends string ? K : never;
}[keyof Input];

export type FlowConcurrency<Input = Json> =
  | StringKeyOf<Input>                                    // bare string shorthand
  | {
      readonly keyFn: (input: Input) => string;
      readonly mode?: ConcurrencyMode;                    // optional, defaults
    };
```

Normalization to the internal `{ keyFn, mode }` happens once, at the
builder/flow boundary (`builder.ts:197-199`), so the runtime
(`runtime.ts:265`) keeps consuming a single canonical shape — **zero runtime
branch added**:

```ts
function normalizeConcurrency<I>(c: FlowConcurrency<I>): { keyFn: (i: I) => string; mode: ConcurrencyMode } {
  if (typeof c === "string") return { keyFn: (i) => (i as Record<string, string>)[c], mode: "cancel-in-progress" };
  return { keyFn: c.keyFn, mode: c.mode ?? "cancel-in-progress" };
}
```

The stored `Flow` continues to carry the canonical `FlowConcurrency<Json>` =
`{ keyFn, mode }` (erased), so `types.ts:272`, `runtime.ts`, and the store
contract are all unchanged.

## Unrepresentable-states analysis

| Invalid state | How it is made unrepresentable |
| --- | --- |
| Property key and `keyFn` both set (contradictory key source) | Type is `string \| { keyFn, mode? }` — a value is either a string or an object; both-at-once is uninhabited. No `never` guard. (Decision 3) |
| Misspelled key (`"videold"`) silently mis-keying runs | The bare string is typed `StringKeyOf<Input>`; a non-key string fails to compile (Resolved Q3) |
| Key referencing a non-string field that throws only at runtime | Strict typing makes it a compile error (Resolved Q3) |
| A mode value outside the union | `mode?: ConcurrencyMode` — invalid literals already rejected today |

**Still representable, accepted as invariant (not worth a type):** the bare
string still admits an empty `""` literal type only if `Input` has a key named
`""`, which is degenerate; the runtime non-empty check at `runtime.ts:267`
remains the backstop. Documented, not type-enforced — matching the precedent
set in RFC 0011's analysis.

## Outbox / crash-recovery review

**Not applicable, by construction.** The outbox pattern addresses the dual-write
hazard of committing state and emitting an event atomically. This RFC adds no
write or emit path: it changes only *how the concurrency key is spelled at the
type/builder layer*. The derived `{ key, mode }` still flows into the existing
`store.tryStartRun`, whose contract already requires atomic
cancel-prior-runs + serialize-starts within a single store transaction
(`types.ts:396-412`; in-memory and Postgres both honor it). Normalization is a
pure, synchronous transform with no I/O. Crash semantics are byte-for-byte
identical to today. Flagged here only so the review is on record.

## Behavior preservation & testing

Existing `concurrency.test.ts` (13 cases) must pass **unchanged** — that is the
primary safety net, since the runtime path is untouched. New coverage
(synthesized from the test-spec draft, finalized to decisions A + default +
strict):

- **Runtime (`concurrency.test.ts`)** — bare-string shorthand reproduces
  verbose cancel-in-progress behavior; default mode applied (string form and
  `keyFn` form with `mode` omitted); key extracted correctly from the named
  field; string and verbose forms record an identical `concurrencyKey`; `keyFn`
  form regression-guarded; runId-idempotency still wins over shorthand.
- **Type-level (`types.test-d.ts`, currently zero concurrency coverage)** —
  valid string key accepted; misspelled key `@ts-expect-error`; key constrained
  to `StringKeyOf<Input>`, not arbitrary `string` (a plain `string` var is
  rejected); **numeric-valued key rejected** (`@ts-expect-error` — strict);
  `keyFn` form still typechecks; `mode?` is the `ConcurrencyMode` union; invalid
  `mode` literal rejected.
- **Edge (`concurrency.test.ts`)** — empty/undefined input value throws
  `NagiValidationError`; whitespace-only accepted (length-only rule preserved);
  dotted string `"a.b"` only typechecks if a literal `"a.b"` key exists (no deep
  path).

## Alternatives considered

- **Ship the issue verbatim (Option B).** Three forms including the redundant
  `key` field. Rejected per "complexity must pay for itself" (Resolved Q1).
- **Object-only, no bare string (Option C).** Rejected — loses the punchiest
  form; the bare string is the core verbosity win (Resolved Q1).
- **`singleTaskFlow()` helper.** Explicitly rejected in the issue (N=1 consumer,
  3 instances — insufficient signal). Concur.
- **Lenient / loose key typing.** Rejected — coercion footgun + weaker safety
  (Resolved Q3).
- **Require explicit mode (no default).** Rejected — halves the verbosity win
  and is moot with one mode (Resolved Q2).

## Resolved questions

Grilled branch-by-branch with Jay, 2026-05-21 (JST):

1. **Surface area** → **A**: bare string + `keyFn`; the `key` field is dropped.
2. **Default mode** → **default `cancel-in-progress` everywhere** (forced by Q1);
   the future second-mode RFC must force an explicit collision-policy choice.
3. **Key value type** → **strict** `StringKeyOf<Input>` (string-valued keys
   only); numeric/other keys use `keyFn`.

No open decisions remain. **Awaiting Jay's approval of this decisions log before
implementation begins** (per the pipeline's hard gate).
