# RFC 0021 — Core internal reorganization: split `dispatch.ts`, split `types.ts` + curate the door

- **Status:** Implemented in the working tree (2026-05-23) — not committed (Jay sequences). Accepted 2026-05-23, Jay.
- **Author:** Claude (paired with @jay)
- **Created:** 2026-05-23 (JST)
- **Tracking:** Follow-up to the `@nagi-js/core` interface/module redesign. Slices §1 (needs default-to-`T` + `optional()`), §2 (fact ownership), §3 (Store dedup + `StreamTransport`) are **implemented in the working tree**; this RFC covers the held slices §4 and §5, plus the §1 follow-up to remove `cascade` (Part C).
- **Scope:** Internal-only, implemented **in one pass** (Jay, 2026-05-23). Three parts:
  - **Part A (§4)** — pure mechanical extraction of `dispatch.ts`; no public API, no fact-shape, no behavior change.
  - **Part B (§5)** — **door curation only** (Jay, 2026-05-23): reshape `index.ts` exports to be intentional + grouped; the physical `types/` file split is **dropped**. No type removed, no runtime behavior change.
  - **Part C (§1 follow-up)** — **remove `cascade`** (Jay, 2026-05-23): it no longer gates after §1. Public API change (`OperatorSkipOpts`, `StepSkippedFact`, `StepState`, `Cascade` export). Minor.
  - Ships as a single `patch` release.
- **Decisions log:** authoritative — see "Decisions taken" under each part.

## Summary

After §1–§3, two files remain oversized and organized by *code shape* rather than
*knowledge owned*:

- `dispatch.ts` (~760 lines post-§3) is a single `makeDispatcher` closure holding
  ~20 inner functions spanning four distinct concerns (callbacks, message
  handling, run progression, subflow linkage).
- `types.ts` (~930 lines) is a flat file serving six audiences (authoring,
  adapter ports, fact log, events, read model, shared primitives), and the public
  door re-exports it wholesale via `export type * from "./types"` (`index.ts`).

Neither is a *correctness* problem — both passed §1–§3 untouched in behavior.
They are *comprehension* and *navigation* problems: a new joiner cannot find "the
code that drives a run forward" without reading the whole closure, and a workflow
author's autocomplete surfaces `ClaimToken` next to `flow`.

This RFC specifies how to split both **without changing behavior or the public
type set**, and flags the one place each split is constrained (a dependency cycle
in Part A; the single-door decision in Part B).

## Context — what already landed (§1–§3)

So the file references below are accurate against the *current* working tree:

- `facts.ts` (new) is the sole fact constructor; `state.ts`/`foldRun` the sole
  interpreter. `RunState` now carries `input` and `parent`; `isAbortRequested`
  reads the projected `aborting` tag.
- `Store` lost `completeStep`/`failStep`/`getStepOutput` (now one `settleStep`)
  and its streaming pair; `StreamTransport` is a separate port threaded through
  `DispatchDeps.streamTransport` and `NagiConfig.streamTransport`.
- `needs.x` resolves to raw `T`; `optional(step)` yields `Resolved<T>`;
  `checkUpstream` gates on it.

Neither held slice depends on the other. They can land in either order, or only
one.

---

# Part A — §4: split `dispatch.ts` into `exec/`

## Motivation

`makeDispatcher(deps)` is one closure whose inner functions share `deps` and call
each other freely. To answer "what happens when a step finishes?" a reader must
trace `dispatchMessage → interpret → advance → finalizeFlowCompletion →
propagateToParent → markStepComplete → advance` across ~600 lines, mentally
separating four concerns that are textually interleaved:

| Concern | Functions today |
| --- | --- |
| **Callbacks** | `fireHook`, `fireStepLifecycle` |
| **Message handling** (one queue message) | `dispatchMessage`, `admit`, `recordStarted`, `execute`, `executeTask`, `executeMatch`, `executeSubflow`, `runHandler`, `handleStepError`, `interpret` |
| **Run progression** (drive a run forward) | `advance`, `recordSkips`, `markStepComplete`, `markStepFail`, `applyPromotions`, `finalizeFlowCompletion`, `finalizeFlowFailure` |
| **Subflow linkage** | `executeSubflow` (spawn child), `propagateToParent` (wake parent) |

The `Dispatcher` interface it exposes is already small and deep
(`dispatchMessage`, `advance`, `propagateToParent`, `fireHook` — 4 methods). The
goal is to **preserve that interface** while making the implementation navigable.

## The constraint: one real dependency cycle

The inner functions form a DAG **except for one cycle**:

```
advance → finalizeFlow{Completion,Failure} → propagateToParent → advance
                                            → markStep{Complete,Fail}
```

`propagateToParent` (subflow "wake parent") calls `advance` and `markStep*`
(progression); `advance`'s finalizers call `propagateToParent`. Everything else
is one-directional:

- `hooks` are a leaf (depend on nothing internal).
- `message` depends on `progression.advance` + `hooks`, and **nothing depends on
  message** (only `runtime`/`worker` call `dispatchMessage` from outside).
- `progression` depends on `hooks` + `propagateToParent`.

A naïve four-way split into `message` / `progression` / `subflow` / `hooks`
(where `subflow` holds both `executeSubflow` and `propagateToParent`) re-creates
the cycle *across module boundaries* (`progression ↔ subflow`), which TypeScript
can only satisfy with late-bound indirection (a partially-initialized refs
object) — a half-built mutable holder, exactly the kind of structurally-invalid
intermediate state we avoid.

## Decisions taken (2026-05-23)

> Recommended calls with reasoning. Flag any to revise before implementation.

1. **`makeDispatcher` stays as the composition root** in `dispatch.ts`. It keeps
   the `Dispatcher` interface and `DispatchDeps`, and wires the sub-factories.
   Its public surface is unchanged; `runtime.ts`/`worker.ts` import nothing new.

2. **Three new modules under `exec/`, cut along the dependency DAG — not four.**
   - `exec/hooks.ts` — `makeHooks(deps) → { fireHook, fireStepLifecycle }`. Leaf.
   - `exec/progression.ts` — the run-progression core **including
     `propagateToParent`**: `advance`, `finalize{Completion,Failure}`,
     `markStep{Complete,Fail}`, `applyPromotions`, `recordSkips`,
     `propagateToParent`. The cycle stays **inside one module**, so no
     late-binding holder is needed.
   - `exec/message.ts` — one-message handling: `dispatchMessage`, `admit`,
     `recordStarted`, `execute`, `executeTask`/`executeMatch`/`executeSubflow`,
     `runHandler`, `handleStepError`, `interpret`. Depends on `progression` +
     `hooks`, one-directional.

3. **No standalone `subflow.ts` (diverges from the original §4 sketch).** The two
   halves of subflow linkage fire at different lifecycle points — `executeSubflow`
   (spawn) during message dispatch, `propagateToParent` (wake) during
   finalization — and share no mutable state, because the parent linkage is owned
   by the projection (`RunState.parent`, since §2) and `runtime.startChildRun`.
   Co-locating them would force the `progression ↔ subflow` cycle for zero
   information-hiding gain. So `executeSubflow` lives in `message`, `propagateToParent`
   in `progression`.

4. **`hooks.ts` is the sole place user callbacks run; the
   `Dispatcher.fireHook` escape hatch is removed.** Today `runtime.ts` reaches
   into the dispatcher via `dispatcher.fireHook(...)` (in `startRunInternal`'s
   concurrency-cancel path and `cancelRunRecursive`). After this split, `runtime`
   constructs/receives the `hooks` object directly and calls `hooks.fireHook`,
   so `fireHook` drops off the `Dispatcher` interface. (`Dispatcher` becomes
   `dispatchMessage` / `advance` / `propagateToParent` — three methods.)

5. **Sub-factories take `deps` + already-built collaborators, in dependency
   order.** No mutable refs holder:
   ```ts
   const hooks = makeHooks(deps);
   const progression = makeProgression(deps, hooks);
   const message = makeMessage(deps, hooks, progression);
   ```

6. **Pure mechanical move.** Function bodies are relocated verbatim (closing over
   `deps`/collaborators instead of the single closure). No logic change, no fact
   change, no ordering change. The existing test suite is the safety net.

## Proposed shape

```
packages/core/src/
  dispatch.ts          # makeDispatcher (composition root) + Dispatcher + DispatchDeps + shared dispatch-only types
  exec/
    hooks.ts           # makeHooks(deps)
    progression.ts     # makeProgression(deps, hooks)   — advance loop, finalize, promotions, propagateToParent
    message.ts         # makeMessage(deps, hooks, progression) — admit/execute/handleError/interpret
```

```ts
// exec/hooks.ts
export interface Hooks {
  fireHook<E>(hook: ((e: E) => void | Promise<void>) | undefined, event: E, name: string): Promise<void>;
  fireStepLifecycle<E>(stepHook, flowHook, names: readonly [string, string], event: E): Promise<void>;
}
export function makeHooks(deps: DispatchDeps): Hooks { /* fireHook + fireStepLifecycle, verbatim */ }

// exec/progression.ts
export interface Progression {
  advance(runId: RunId): Promise<void>;
  propagateToParent(childRunId: RunId, outcome: SubflowChildOutcome): Promise<void>;
}
export function makeProgression(deps: DispatchDeps, hooks: Hooks): Progression {
  // advance, finalizeFlowCompletion/Failure, markStepComplete/Fail,
  // applyPromotions, recordSkips, propagateToParent — the cycle lives here.
}

// exec/message.ts
export interface MessageHandler { dispatchMessage(message: QueueMessage): Promise<void>; }
export function makeMessage(deps: DispatchDeps, hooks: Hooks, prog: Progression): MessageHandler {
  // admit, recordStarted, execute, executeTask/Match/Subflow, runHandler,
  // handleStepError, interpret (interpret calls prog.advance).
}

// dispatch.ts
export function makeDispatcher(deps: DispatchDeps): Dispatcher {
  const hooks = makeHooks(deps);
  const progression = makeProgression(deps, hooks);
  const message = makeMessage(deps, hooks, progression);
  return {
    dispatchMessage: message.dispatchMessage,
    advance: progression.advance,
    propagateToParent: progression.propagateToParent,
  };
}
```

`SubflowChildOutcome`, `Dispatched`, `Admission`, `ExecuteTaskResult` move next to
the module that owns them (`SubflowChildOutcome` → `progression`/shared;
`Dispatched`/`Admission`/`ExecuteTaskResult` → `message`).

## Implementation plan (Part A)

1. Add `exec/hooks.ts`; move `fireHook` + `fireStepLifecycle`; export `Hooks`.
2. Update `runtime.ts` to take/construct `hooks` and call `hooks.fireHook`
   directly; drop `fireHook` from the `Dispatcher` interface and from
   `makeDispatcher`'s return. (`makeOperator` / `operator.ts` use `dispatcher`
   — check whether they call `fireHook`; if so, pass `hooks`.)
3. Add `exec/progression.ts`; move `advance`, `recordSkips`, `markStep*`,
   `applyPromotions`, `finalize*`, `propagateToParent`. Close over `deps` +
   `hooks`.
4. Add `exec/message.ts`; move `dispatchMessage`, `admit`, `recordStarted`,
   `execute`, `executeTask`/`executeMatch`/`executeSubflow`, `runHandler`,
   `handleStepError`, `interpret`. Close over `deps`, `hooks`, `progression`.
5. Reduce `dispatch.ts` to the composition root + `Dispatcher`/`DispatchDeps`.
6. `pnpm typecheck && pnpm test && pnpm lint` green; **no test file should need
   to change** (the `Dispatcher` interface and all behavior are preserved). If a
   test imports an inner function directly, that's a finding — re-export or adjust.

## Risks & behavior preservation (Part A)

- **Primary safety net:** the full existing suite must pass unchanged. This is a
  relocation, not a redesign.
- **Cycle correctness:** keeping `propagateToParent` in `progression` means the
  only cycle never crosses a module boundary — no import cycle, no late binding.
- **`fireHook` removal from `Dispatcher`:** the one observable internal-API
  change. `runtime.ts` and `operator.ts` are the only callers; both are updated
  in the same patch. Not a public (`index.ts`) export.

---

# Part B — §5: split `types.ts` + curate the `index.ts` door

## Motivation

`types.ts` mixes six audiences in one flat file, and `index.ts` ends with:

```ts
export type * from "./types";
```

which blasts every adapter-contract and fact-log type (`Store`, `ClaimToken`,
`SettleSignalResult`, all `*Fact`, `QueueMessage`, …) into the same namespace a
workflow author sees alongside `flow`/`nagi`. Per the accepted single-door
decision (Jay, Q1), we are **not** introducing `@nagi-js/core/adapter` subpaths —
so adapter types stay reachable from the main entry. The achievable win is
therefore **intentional, grouped, greppable exports** plus **file-level
modularity**, not namespace isolation. (This consciously caps principle #3 —
information hiding — at the door; that trade was accepted.)

## Decisions taken (2026-05-23)

> **Door curation only.** The physical `types/` file split is **dropped** (Jay,
> 2026-05-23): `types.ts` stays a single file. The win is an intentional,
> grouped, greppable door — not a folder.

1. **`index.ts` replaces `export type *` with explicit, grouped re-exports**,
   under audience banners (authoring / read model / adapter ports / fact log /
   events), and **keeps every type currently public** — so the public surface is
   unchanged and the `test-types` surface check stays green. The win is
   intentionality + grouping + a place to later trim, not removal:
   ```ts
   // index.ts
   export { flow, optional, nagi, /* … */ } from "...";
   // — authoring —
   export type { Flow, FlowConfig, Builder, TaskConfig, /* … */ } from "./types";
   // — read model —
   export type { RunState, StepState, RunPhase, Resolved } from "./state";
   export { unwrap, stepStateOf, /* … */ } from "./state";
   // — adapter authoring — (single door: still reachable, now grouped + intentional)
   export type { Store, StreamTransport, Queue, Clock, Trigger, ClaimToken, /* … */ } from "./types";
   export type { Fact, /* … *Fact … */ } from "./types";
   ```

2. **No type dropped, no file moved.** All exports keep coming from `types.ts`
   (and `state.ts` for the read model). Trimming genuinely-internal types and
   physically splitting `types.ts` are both deferred — higher churn, lower
   marginal value under the accepted single-door model.

> **Partially reopened by #38 (2026-09-04).** The fact-log slice of `types.ts`
> did get a physical home: `packages/core/src/facts/` owns each kind's shape,
> constructor, fold arm, and read-model row delta, grouped by lifecycle. The
> single-door decision (Q1) stands — `types.ts` re-exports the fact types and
> `index.ts` remains the only entry. The driver was not door hygiene but the
> fourth interpreter in `@nagi-js/postgres`, which now consumes `rowDeltaOf`
> instead of re-deriving fact semantics.

## Implementation plan (Part B)

1. Enumerate the current public surface: everything `export type * from "./types"`
   surfaces today.
2. Rewrite `index.ts`: drop `export type *`; add grouped explicit
   `export type { … } from "./types"` blocks under audience banners covering the
   **same** set.
3. `pnpm -r build && pnpm verify:types && pnpm typecheck && pnpm lint` green —
   `verify:types` (the `test-types/` surface check) gates that the public set is
   unchanged.

## Risks (Part B)

- **Completeness:** the explicit list must cover exactly what `export type *`
  surfaced, or a consumer / `test-types` import breaks. `verify:types` +
  `typecheck` catch any omission; when in doubt, include.
- **Low ceiling:** under the single door, autocomplete for an app author is
  unchanged. The only lever for true isolation is subpath exports — declined
  (Jay Q1), out of scope.

---

# Part C — §1 follow-up: remove `cascade`

## Motivation

Before §1, an operator skip's `cascade: "skip" | "continue"` decided whether a
*downstream* of the skipped step ran (`continue`) or was itself skipped (`skip`).
§1 moved that decision to the **consumer** via `optional()`: a bare need cascades
the skip; an `optional()` need tolerates it. `checkUpstream` no longer reads
`cascade` at all. The field is now inert — a footgun, since setting
`cascade: "continue"` looks like it does something but doesn't.

## Decisions taken (2026-05-23)

1. **Remove `cascade` entirely** (Jay, 2026-05-23). Touch points:
   - `types.ts` — drop `OperatorSkipOpts.cascade`; `OperatorSkipOpts` then has no
     fields beyond `OperatorAuditOpts`, so **collapse `Operator.skip`'s param to
     `OperatorAuditOpts`** and delete `OperatorSkipOpts`. Drop
     `StepSkippedFact.cascade`.
   - `state.ts` — drop `cascade` from the `StepState` `skipped` arm; delete the
     now-unused `Cascade` type; simplify the `step.skipped` fold (no
     `cascade ?? "skip"`).
   - `facts.ts` — drop the `cascade` param + spread from `Facts.stepSkipped`.
   - `operator.ts` — drop `const cascade = opts.cascade ?? "skip"` and the
     `cascade` argument to `Facts.stepSkipped`.
   - `index.ts` — drop the `Cascade` re-export.
   - tests — drop `cascade` assertions (e.g. `operator.test.ts`'s
     `expect(manual?.cascade).toBe("skip")`).

2. **`SkipReason` (`when-false` | `transitive` | `manual`) stays** — it records
   *why* a step skipped and is still meaningful. Only `cascade` (the dead
   *how-downstream-reacts* flag) goes.

## Risks (Part C)

- **Public API change** (drop `OperatorSkipOpts`, `StepSkippedFact.cascade`,
  `StepState.skipped.cascade`, `Cascade` export). Caught by `typecheck` +
  `verify:types`; a `patch` is acceptable in 0.1.x.
- **Persisted facts:** existing `step.skipped` facts may carry a `cascade` field;
  the fold simply ignores unknown extra fields, so old logs still project.

---

## Sequencing & changesets

One `patch` changeset for `@nagi-js/core` covering Parts A, B, and C (Jay:
implement in one pass). **Not committed by this RFC** — Jay sequences. The
working tree already carries §1–§3 plus parallel features, so re-check
`git status` before staging.

## Alternatives considered

- **Part A, four-way split with a `subflow.ts` + late-bound refs holder.**
  Co-locates the two subflow halves but needs a partially-initialized mutable
  `{ advance, markStep* }` holder to break the `progression ↔ subflow` cycle.
  Rejected: the half-built holder is a structurally-invalid intermediate state,
  and the parent-linkage knowledge already lives in the projection, so
  co-location buys nothing.
- **Part A, leave `dispatch.ts` as one module.** Defensible — it *is* one
  cohesive deep module behind a 3-method interface. Rejected only because the
  navigation cost (four concerns interleaved over ~600 lines) is real for new
  joiners; the split is pure upside if it stays behavior-preserving.
- **Part B, full subpath exports (`@nagi-js/core/adapter`).** The real
  information-hiding fix, but **already declined** (Jay Q1) for single-import-door
  ergonomics. Not reopened here.
- **Part B, curate `index.ts` only, skip the physical `types/` split.** Lower
  churn; delivers the intentional-door win without moving 930 lines. **Chosen**
  (Jay, 2026-05-23).

## Resolved (2026-05-23, Jay)

1. **`cascade` cleanup** → **remove it.** Now Part C, in scope for this pass.
2. **Part B scope** → **door curation only.** The physical `types/` split is
   dropped; `types.ts` stays one file.
3. **One pass** → yes. Parts A, B, and C land together (one RFC, one changeset).
4. **RFC number** → `0021`.
