# Handoff — RFC 0018 (Parameterize `Wf` over registered flows / typed `flowId`)

- **RFC:** `docs/rfcs/0018-parameterize-wf-over-flows.md`
- **Tracking issue:** #18
- **Author:** Claude (paired with @jay)
- **Status:** Implemented + green locally — **NOT committed, NOT PR'd** (Jay sequences; see Caveats)
- **Date:** 2026-05-21 (JST)

## ⚠ Sequencing caveat — read first

This landed in a **multi-feature working tree**. A second feature — shorthand
concurrency config (issue #20) — is in flight in the same tree, and a release
process committed during the session (HEAD moved `f926424` → `f4b9280`:
`version packages`, `bump rc v9`). Two consequences:

1. **RFC-number collision — RESOLVED.** This RFC was originally drafted as
   `0012`, colliding with the concurrency RFC (`0012-shorthand-concurrency-config`).
   Resolved 2026-05-21 by renumbering **this** RFC to **0018** (issue-matched to
   #18, collision-proof under parallel work). The concurrency RFC keeps `0012`
   and bootstrap keeps `0013` — not touched (they may be live in another
   session; Jay can convert them to issue-matched separately if desired).
2. **All `packages/core/src/*` files are shared, and features are now
   *integrating*, not merely coexisting.** At least three RFCs are live in this
   tree (#18 here, concurrency #20 at RFC 0012, bootstrap #17 at RFC 0013). The
   bootstrap refactor already absorbed this feature's factory: `nagi()` is now
   `nagiImpl<const TFlows>` (runtime.ts:183) with my generics preserved, plus a
   new `queue.ensureSchema?.()` call. So `runtime.ts`, `types.ts`,
   `types.test-d.ts`, and `queryRuns.test.ts` all carry interleaved changes from
   multiple features. A cleanly-isolated single-feature PR may no longer be
   practical — decide whether to ship #18 / #17 / #20 together or tease apart.

## What landed (this feature only)

**`packages/core/src/types.ts`**
- `FlowIdOf<T extends ReadonlyArray<Flow>> = T[number] extends Flow<infer Id> ? Id : never` — new exported helper (mirrors `FlowInput` house style).
- `RunSummary<FlowId extends string = string>` — `flowId: FlowId` (was `string`).
- `QueryRunsWhere<FlowId extends string = string>` — `flowId?: FlowId`, key name **kept** (Q1), strict union, no `(string & {})` (Q3).
- `QueryRunsOpts<FlowId extends string = string>` — threads `QueryRunsWhere<FlowId>` into both arms of the discriminated union.
- `QueryRunsResult<FlowId extends string = string>` — `runs: ReadonlyArray<RunSummary<FlowId>>`.
- `Store.queryRuns` left **non-generic** (D7) — adapters unchanged.

**`packages/core/src/runtime.ts`**
- `Wf<TFlows extends ReadonlyArray<Flow> = ReadonlyArray<Flow>>`.
- `start<F extends TFlows[number]>` (Q2 — unregistered-flow start is now a compile error).
- `queryRuns(opts?: QueryRunsOpts<FlowIdOf<TFlows>>): Promise<QueryRunsResult<FlowIdOf<TFlows>>>`.
- `nagi<const TFlows extends ReadonlyArray<Flow>>(config: NagiConfig & { flows: TFlows }): Promise<Wf<TFlows>>` (D4 — `const` infers the tuple without `as const` for inline literals).
- `startById(flowId: string)` **unchanged** (D10).
- One localized assertion `return wf as unknown as Wf<TFlows>` (runtime.ts:~800) with the D7 trust-boundary comment.

**Tests**
- `packages/core/src/types.test-d.ts` — `describe("Wf parameterized over registered flows (RFC 0018)")`, 10 `expectTypeOf` / `@ts-expect-error` cases (both negative cases confirmed to fail without the suppression).
- `packages/core/src/queryRuns.test.ts` — `describe("wf.queryRuns — no runtime delta after RFC 0018 typing")`, 4 runtime parity cases.

## What was NOT done (intentionally)

- **No adapter changes** (`memory.ts`, `packages/postgres`): defaults make `QueryRunsResult` ≡ today's type; verified via `pnpm -r typecheck` (all packages compile).
- **`startById` not narrowed** — outbox/DLQ-replay seam (D10, outbox review).
- **No runtime guard / re-validation** in `queryRuns` — would violate "no runtime delta"; the boundary cast (D7) is the deliberate alternative.
- **No README edit** (per ownership convention).
- **No commit / changeset-consume / PR** — left for Jay's sequencing.

## Verification (local, in this mixed tree)

- `pnpm -C packages/core typecheck` (`tsc --noEmit`) → clean (exit 0).
- `pnpm -C packages/core test:types` (`vitest run --typecheck`) → **523 passed (36 files)**, **Type Errors: no errors**.
- `pnpm -r typecheck` (core, otel, postgres, pgmq) → all pass (confirms D7: adapters untouched).

## Files index

```
# this feature — docs
docs/rfcs/0018-parameterize-wf-over-flows.md          (was 0012; in HEAD as 0012, renamed → stage the rename)
docs/rfcs/0018-parameterize-wf-over-flows.handoff.md  (this file)
.changeset/parameterize-wf-over-flows.md              (patch / @nagi-js/core)

# this feature — code (ALL shared/interleaved with #17, #20 — isolate by CONTENT,
# the symbols below; do NOT wholesale-add any core/src file)
packages/core/src/runtime.ts          (Wf<TFlows>; start<F extends TFlows[number]>; generic queryRuns; nagiImpl<const TFlows> generics; D7 return cast)
packages/core/src/types.ts            (FlowIdOf; RunSummary/QueryRunsWhere/QueryRunsOpts/QueryRunsResult generics)
packages/core/src/types.test-d.ts     ("Wf parameterized over registered flows (RFC 0018)" describe block)
packages/core/src/queryRuns.test.ts   ("no runtime delta after RFC 0018 typing" describe block)

# NOT this feature — concurrency #20 (do not include in this PR)
packages/core/src/builder.ts
packages/core/src/concurrency.test.ts
packages/core/src/types.ts            (StringKeyOf/FlowConcurrency/ResolvedConcurrency hunks)
packages/core/src/types.test-d.ts     ("Flow concurrency shorthand typing" block)
.changeset/shorthand-concurrency-config.md
docs/rfcs/0012-shorthand-concurrency-config.{md,handoff.md}
```

## Suggested isolation (for a single-feature PR, once you approve)

```sh
git switch -c rfc-0018-typed-flowid
# docs/changeset are exclusively mine — safe to add whole (includes the 0012→0018 RFC rename):
git add .changeset/parameterize-wf-over-flows.md docs/rfcs/0018-parameterize-wf-over-flows.*
# every core/src file is shared/interleaved (#17, #20) — hunk-pick by content, do not wholesale-add:
git add -p packages/core/src/runtime.ts packages/core/src/types.ts \
           packages/core/src/types.test-d.ts packages/core/src/queryRuns.test.ts
# stage ONLY: FlowIdOf / RunSummary / QueryRuns* generics / Wf<TFlows> / start<F extends TFlows[number]>
# / nagiImpl generics / D7 cast + my two describe blocks. Leave #20 + #17 hunks unstaged.
# NOTE: bootstrap (#17) already wraps nagi→nagiImpl in runtime.ts, so that file may not split cleanly.
```
