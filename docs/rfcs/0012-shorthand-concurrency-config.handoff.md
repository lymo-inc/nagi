# RFC 0012 — Implementation handoff

- **RFC:** `docs/rfcs/0012-shorthand-concurrency-config.md`
- **Issue:** lymo-inc/nagi#20
- **Author of impl:** Claude, dispatched by Jay (2026-05-21 JST)
- **Status:** Implemented + tested in `@nagi-js/core` (523/523 pass; typecheck + test:types clean). Awaiting Jay's decision on PR strategy (see "Caveats").

## What landed

### `@nagi-js/core`

- `packages/core/src/types.ts:233` — new `StringKeyOf<Input>` (the string-valued keys of `Input`, optionality stripped via `-?`).
- `packages/core/src/types.ts:237` — `FlowConcurrency<Input>` redefined as the public call-site union `StringKeyOf<Input> | { keyFn; mode? }` (was an interface with a mandatory `mode`). Name preserved so `FlowConfig.concurrency` still resolves.
- `packages/core/src/types.ts:244` — new `ResolvedConcurrency` = `{ keyFn: (input: Json) => string; mode: ConcurrencyMode }`, the canonical post-normalization shape.
- `packages/core/src/types.ts:283` — erased `Flow.concurrency` retyped from `FlowConcurrency<Json>` to `ResolvedConcurrency`.
- `packages/core/src/builder.ts:199` — `flow()` now normalizes via a new module-scope `normalizeConcurrency()` instead of casting: bare string → `{ keyFn: i => i[key], mode: "cancel-in-progress" }`; object → `{ keyFn, mode: mode ?? "cancel-in-progress" }`.
- `packages/core/src/runtime.ts` — **unchanged.** It still consumes `flow.concurrency.{keyFn,mode}` exactly as before. The bare-string key's runtime validation reuses the existing non-empty-string check (`runtime.ts:267`): an empty/undefined extracted value throws `NagiValidationError`.

### Tests

- `packages/core/src/concurrency.test.ts` — new `describe("flow concurrency shorthands")`, 9 runtime/edge tests: cancellation parity, default mode applied, key extraction, keyFn-without-mode, string-vs-verbose key equality, keyFn+mode regression guard, runId idempotency wins, empty-value validation. Existing 13 concurrency tests untouched.
- `packages/core/src/types.test-d.ts` — new `describe("Flow concurrency shorthand typing")`, 7 type assertions: valid bare key; `@ts-expect-error` for misspelled, numeric-valued, and wide-`string` keys; keyFn with/without mode; `@ts-expect-error` invalid mode literal.

### Meta

- `docs/rfcs/0012-shorthand-concurrency-config.md` — decisions-log RFC (approved by Jay after branch-by-branch grilling, 2026-05-21).
- `.changeset/shorthand-concurrency-config.md` — `patch` for `@nagi-js/core`.

## What was NOT done (intentionally)

- **The `key` field** (`concurrency: { key: "videoId" }`) from the issue — dropped (Resolved Q1). Redundant with the bare string while one mode exists; trivially additive when a second mode lands.
- **A second collision mode** (`hash` / `serialize` / …) — out of scope (Decision 6). `ConcurrencyMode` stays single-member, so a future mode RFC must (per Resolved Q2) make the collision policy an explicit choice rather than silently widen this default.
- **Numeric-key coercion** — strict typing rejects numeric keys at compile time (Resolved Q3); numeric IDs use `keyFn: i => String(i.id)`.
- **Lymo call-site migration** — the consuming flows in `lymo-inc/lymo` are not touched; this is the library-side change only.

## Verification

```
pnpm -F @nagi-js/core typecheck   # PASS (clean)
pnpm -F @nagi-js/core test:types  # 523/523, no type errors
pnpm -F @nagi-js/core test        # 523/523
```

## Caveats — PR strategy needs Jay's call

The working tree carries several other in-flight, uncommitted features (RFC 0010 otel-subflow-span-linkage, RFC 0011 next-transition-state-machine, plus `simplify-core-surface` and `wf-start-by-id` changesets). `packages/core/src/types.ts` and `runtime.ts` contain unrelated edits from those (e.g. `FlowIdOf`, generic `Wf<TFlows>`, `QueryRuns*` changes) in the same files as this RFC's concurrency additions.

Per your commit-sequencing preference, nothing has been committed and no PR has been opened. Options:

1. **Isolate this RFC** — new branch, stage only the concurrency-scoped hunks (the `types.ts` concurrency block, `builder.ts`, `concurrency.test.ts`, `types.test-d.ts`, the RFC, changeset, this handoff). Requires a careful partial stage since `types.ts` is shared with other WIP.
2. **Sequence after the others** — land whichever in-flight feature(s) you sequence first, then this atop a clean main.
3. **Bundle** — include this in a larger multi-feature commit if you are shipping them together.

Decision needed before any commit/PR.

## Files index (everything authored under this RFC)

```
docs/rfcs/0012-shorthand-concurrency-config.md          (new)
docs/rfcs/0012-shorthand-concurrency-config.handoff.md  (new)
.changeset/shorthand-concurrency-config.md              (new)
packages/core/src/types.ts                              (modified — StringKeyOf + FlowConcurrency union + ResolvedConcurrency + Flow.concurrency)
packages/core/src/builder.ts                            (modified — normalizeConcurrency)
packages/core/src/concurrency.test.ts                   (modified — +9 tests)
packages/core/src/types.test-d.ts                       (modified — +7 type tests)
```
