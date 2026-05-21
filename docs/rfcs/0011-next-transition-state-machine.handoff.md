# RFC 0011 — Implementation handoff

- **RFC:** `docs/rfcs/0011-next-transition-state-machine.md` (Accepted 2026-05-21, Jay)
- **Author of impl:** Claude, paired with Jay (2026-05-21 JST)
- **Status:** Implemented + tested. **Not committed / no PR** — working tree is a multi-feature pile that needs Jay's sequencing decision (see "Caveats").

## What landed (in the working tree)

All under `@nagi-js/core`. No public API change (nothing here is re-exported
from `index.ts`); behavior preserved.

### RFC 0011 core — `nextTransition`

- `packages/core/src/scheduler.ts` — new pure `nextTransition(flow, runState): Transition`; `Transition` 7-arm union + `MatchPromotion` + `SkipDecision`; moved `isFlowTerminal` and `computeFlowOutput` here (pure projections); private `readyPromotions` / `flowFailureError` helpers.
- `packages/core/src/dispatch.ts` — `advance` rewritten as an executor switch over `nextTransition`; `promoteMatches` (side-effecting loop) replaced by `applyPromotions(promotions)` + `recordSkips`; `finalizeFlowCompletion` now takes a precomputed `output: Json`.
- `packages/core/src/scheduler.test.ts` — 9 new `nextTransition` unit tests (one per arm + the promote-match-over-dispatch priority edge).

### Mechanical cleanups (rode along, same internal scope)

- `dispatch.ts` — `instanceof NagiAbortError` (kept the foreign-`AbortError` name check, which `concurrency.test.ts:377` depends on); shared `markStepComplete` / `markStepFail` for the match + subflow finalize paths; `serializeError` exported.
- `runtime.ts` — reuse `serializeError()` in `startRunInternal`'s cancel path; hoist a duplicated `cancelError` literal in `cancelRunRecursive`.
- `internal.ts` + `scheduler.ts` — dropped the dead `def.needs` triple-guard (the type already guarantees `Step` refs); `needsStepIds`/`resolveNeeds`/`checkUpstream` simplified.
- `internal.ts` + `builder.ts` — split match arms into finalized `MatchArmDef` (no `_nested`) vs builder-only `PendingMatchArm`/`PendingMatchDef`.
- `dispatch.ts` — `DispatchDeps.lookupFlow`/`.startChildRun` made required; removed the "missing dep" guard in `executeSubflow`.

### Meta

- `docs/rfcs/0011-next-transition-state-machine.md` — RFC + decisions log.
- `.changeset/next-transition-internal-cleanup.md` — `patch` for `@nagi-js/core`.

## What was NOT done (intentionally)

- **`queryRuns` live-summaries cache** (audit Tier 2) and **signal-name→step-id index** (audit Tier 2) — **deferred**. Both are index/cache optimizations with no profiling evidence; deferring them is the `complexity_must_pay_for_itself` call. Revisit only if a benchmark shows the linear scans matter.
- **"Remove inner `step.canceled` write in `executeTask`"** (audit Tier 2) — **WONTFIX, audit premise was wrong.** The inner write handles "handler *returned* after cancel" (`concurrency.test.ts:273`, asserts 1 canceled / 0 completed); the outer `handleStepError` write handles "handler *threw* after cancel" (`:324`). They are mutually exclusive per attempt and both tested — not a double-write.
- **`instanceof NagiAbortError` as a full replacement** — the audit suggested replacing the whole condition; that would have dropped the native-`AbortError` (DOMException) branch that `concurrency.test.ts:377` relies on. Kept both, using `instanceof` only for our own error.

## Verification

```
pnpm -r build       # all 4 buildable packages: success (DTS clean)
pnpm typecheck      # all packages: clean
pnpm test           # core 480 pass · otel 54 · pgmq 40 · postgres 77 (+28 skipped, need DB)
```

`pnpm lint` has pre-existing failures **in other features' files** (Tier 1 test
deletions, OTel, postgres store, the `startById` `OperatorDeps` line) — none in
RFC-0011-authored code. The 6 files I edited are biome-clean except the single
`OperatorDeps.cancelRunRecursive` formatting nit, which is Jay's uncommitted
`CancelArgs` line, not mine.

## Caveats — commit/PR strategy needs Jay's call

The working tree is **not** a clean base. `git diff --stat` vs HEAD
(`c4e1459`) is 23 files, +896/−1170, and contains **four intermixed
uncommitted features** sharing the same files (`types.ts`, `runtime.ts`,
`dispatch.ts`, `builder.ts`, `scheduler.ts`, `internal.ts`):

1. **Tier 1** — `simplify-core-surface` (b.step/b.include removal, match
   single-form, `FlowCanceledFact` union). HEAD does **not** contain this.
2. **RFC 0010** — `otel-subflow-span-linkage` (+ its handoff, otel hooks/tests).
3. **`startById`** — `wf-start-by-id` (new public `Wf.startById`, `CancelArgs`,
   `parentStepAttempt` threading).
4. **RFC 0011 (this work)** — `next-transition-internal-cleanup`.

RFC 0011 **depends on Tier 1** (e.g. the `MatchArmDef` split assumes Tier 1's
positional-arm structure; the guard removals assume Tier 1's needs shape). It
cannot land as a standalone commit on `c4e1459` — it must sit atop Tier 1.

**I did not commit or open a PR.** Untangling four features into ordered,
individually-green commits is a sequencing decision (and committing/pushing is
a shared-state action). Recommended order if landing all: Tier 1 → 0010 →
startById → 0011, each its own commit, `pnpm test` green at each step.

Decision needed from Jay before any commit/PR.

## Files index (RFC-0011-authored)

```
docs/rfcs/0011-next-transition-state-machine.md          (new)
docs/rfcs/0011-next-transition-state-machine.handoff.md  (new)
.changeset/next-transition-internal-cleanup.md           (new)
packages/core/src/scheduler.ts        (modified — nextTransition + moved projections)
packages/core/src/scheduler.test.ts   (modified — +9 tests)
packages/core/src/dispatch.ts         (modified — advance executor, markStep* helpers, etc.)
packages/core/src/internal.ts         (modified — Pending* types, needs guard drop)
packages/core/src/builder.ts          (modified — PendingMatchDef construction)
packages/core/src/runtime.ts          (modified — serializeError reuse, cancelError hoist)
```
