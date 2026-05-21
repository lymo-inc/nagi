# RFC 0011 — Extract `nextTransition` from the `advance` loop

- **Status:** Accepted (2026-05-21, Jay) — implementation in progress
- **Author:** Claude (paired with @jay)
- **Created:** 2026-05-21 (JST)
- **Tracking:** Tier 2 core-simplification pass (follow-up to `c4e1459 feat: implement rfcs`)
- **Scope:** Internal-only. No public API change, no fact-shape change, behavior-preserving. Patch release.
- **Decisions log:** authoritative — see "Decisions taken" below.

## Summary

`advance()` in `dispatch.ts:531-590` is the heart of the engine: it loops,
loading run state and deciding what to do next — promote a finished match,
finalize the flow, enqueue runnable steps, record skips, or stop. Today that
decision logic is **interleaved with the I/O that acts on it** (loading state,
appending facts, enqueueing, firing hooks, finalizing). The control flow *is*
the state machine, encoded implicitly in statement ordering and early returns.

This RFC extracts the decision into a **pure function**
`nextTransition(flow, runState): Transition` living in `scheduler.ts`, and
reduces `advance()` to a thin executor that performs the side effects for
whichever `Transition` it's handed.

The win is twofold: (1) the engine's state machine becomes a single,
readable, exhaustively-typed union a new joiner can read top-to-bottom; and
(2) the decision logic becomes unit-testable against synthetic `RunState`
values with no `Store`, `Queue`, or `Clock`.

## Motivation

To understand "what does the engine do next?" today, a reader must mentally
execute `advance`'s loop, tracking:

- that `promoteMatches` runs *first* and `continue`s before termination is
  checked (line 538-539) — because a just-completed match might be the last
  step blocking termination;
- that `flowTermination.done` branches three ways, one of which
  (`isFlowTerminal(facts)` at line 544) is a silent no-op guarding against
  double-finalize;
- that `nextRunnable` returning *only* skips loops again (line 579 falls
  through), but returning any runnable step returns;
- that the whole thing is bounded by `MAX_ADVANCE_ITERS` to catch cycles.

These rules are correct but **only enforced by careful ordering**. A
well-meaning refactor that moves the termination check above match promotion,
or drops the `isFlowTerminal` guard, compiles fine and breaks subtly. The
invariants live in a programmer's head, not in a type.

`nextRunnable`, `flowTermination`, and `aggregateMatch` are *already* pure and
already in `scheduler.ts`. `nextTransition` is the missing capstone that
composes them into the single decision the loop actually makes.

## Decisions taken (2026-05-21)

> These are my recommended calls with reasoning. Flag any you want to revise
> before I implement.

1. **`nextTransition` is pure and total.** Signature
   `(flow: Flow, runState: RunState) => Transition`. No `Store`/`Queue`/
   `Clock`/`Promise`. It reads facts and step states, returns one transition.
   All hook firing, fact appending, and enqueueing stay in `advance`.

2. **One tick = one `Transition`.** `nextTransition` returns the single
   highest-priority action for the current state. `advance` performs it and
   loops to re-derive. Priority order (first match wins):
   `promote-match` → `settled`/`complete`/`fail` → `dispatch` → `skip` →
   `waiting`. This order is the load-bearing invariant; centralizing it in one
   function is the whole point.

3. **The `Transition` union (7 arms):**

   ```ts
   export type Transition =
     | { readonly kind: "promote-match"; readonly promotions: readonly MatchPromotion[] }
     | { readonly kind: "complete"; readonly output: Json }
     | { readonly kind: "fail"; readonly error: SerializedError }
     | { readonly kind: "dispatch";
         readonly runnable: readonly StepId[];
         readonly skip: readonly SkipDecision[] }
     | { readonly kind: "skip"; readonly skip: readonly SkipDecision[] }
     | { readonly kind: "settled" }   // flow already finalized — no-op
     | { readonly kind: "waiting" };  // flow live but parked on an external trigger

   export interface MatchPromotion {
     readonly matchId: StepId;
     readonly attempt: AttemptNumber;
     readonly result:
       | { readonly kind: "complete"; readonly output: Json }
       | { readonly kind: "fail"; readonly error: SerializedError };
   }
   ```

   `SkipDecision` is the existing `ScheduleDecision["skip"]` element type
   (`{ stepId, reason }`), promoted to a named export.

4. **Precomputed payloads.** Because `computeFlowOutput`, the fail-error
   extraction, and `aggregateMatch` are all pure, their results ride *inside*
   the transition (`complete.output`, `fail.error`, `promotion.result`). This
   keeps `advance` a dumb executor — it never recomputes a decision, just acts.
   Consequence: `computeFlowOutput` (currently `dispatch.ts:903`) and
   `isFlowTerminal` (`dispatch.ts:729`) move to `scheduler.ts` as they are
   pure projections and `nextTransition` needs them.

5. **`promote-match` carries *all* ready promotions, not one.** Mirrors
   today's `promoteMatches`, which promotes every eligible match in a single
   pass before looping. `advance` applies them all, then loops.

6. **Two "no automatic progress" arms: `settled` and `waiting`** (Jay's call,
   2026-05-21 — diverges from my one-arm recommendation). `settled` = the flow
   already carries a terminal `flow.*` fact (`isFlowTerminal` true — today's
   silent no-op return). `waiting` = the flow is live but parked on an external
   trigger (signal / subflow child / nothing runnable). Both cause `advance` to
   return without side effects, but they are kept distinct so a future
   observability hook can report *why* the engine parked without re-deriving
   state. The double-finalize guard is now *structural*: `nextTransition` only
   emits `complete`/`fail` when the flow is done **and** not yet terminal;
   once terminal it emits `settled`. `advance` cannot finalize twice because it
   only acts on what it's handed.

7. **`advance` keeps the `MAX_ADVANCE_ITERS` cycle guard.** Only the
   loop-continuing arms (`skip`, `promote-match`) can spin; the guard's role
   (catch an infinite skip/promote loop and fail the flow with
   `NagiCycleError`) is unchanged.

8. **`nextTransition` lives in `scheduler.ts`, not a new file.** It composes
   functions already there. No new module for one function.

## Proposed shape

### `scheduler.ts` — new pure decision function

```ts
export function nextTransition(flow: Flow, runState: RunState): Transition {
  // 1. promote any running match whose chosen-arm steps are all terminal
  const promotions = readyPromotions(flow, runState); // uses aggregateMatch
  if (promotions.length > 0) return { kind: "promote-match", promotions };

  // 2. terminal?
  const term = flowTermination(flow, runState);
  if (term.done) {
    if (isFlowTerminal(runState.facts)) return { kind: "settled" };
    if (term.failed) return { kind: "fail", error: flowFailureError(runState) };
    return { kind: "complete", output: computeFlowOutput(flow, runState) };
  }

  // 3. runnable / skip / waiting
  const input = extractInput(runState);
  const { runnable, skip } = nextRunnable({ flow, runState, input });
  if (runnable.length > 0) return { kind: "dispatch", runnable, skip };
  if (skip.length > 0) return { kind: "skip", skip };
  return { kind: "waiting" };
}
```

### `dispatch.ts` — `advance` becomes an executor

```ts
export async function advance(deps: DispatchDeps, runId: RunId): Promise<void> {
  const flow = await deps.flowFor(runId);
  for (let iter = 0; iter < MAX_ADVANCE_ITERS; iter++) {
    const runState = await deps.store.loadRunState(runId);
    const t = nextTransition(flow, runState);
    switch (t.kind) {
      case "settled":
      case "waiting":
        return;
      case "complete":
        await finalizeFlowCompletion({ deps, flow, runId, output: t.output });
        return;
      case "fail":
        await finalizeFlowFailure({ deps, flow, runId, error: t.error });
        return;
      case "dispatch":
        await recordSkips(deps, runId, t.skip);
        for (const stepId of t.runnable) await deps.queue.enqueue(runId, stepId);
        return;
      case "skip":
        await recordSkips(deps, runId, t.skip);
        continue; // re-derive after recording transitive skips
      case "promote-match":
        await applyPromotions(deps, flow, runState, t.promotions);
        continue; // re-derive after marking matches terminal
    }
  }
  // ...unchanged NagiCycleError guard...
}
```

`finalizeFlowCompletion`'s signature changes from taking `runState` to taking
the precomputed `output: Json` (decision #4). `applyPromotions` and
`recordSkips` are small extracted helpers wrapping the existing
`store.completeStep`/`failStep` + hook-fire and `store.appendFact(step.skipped)`
calls respectively — the same code that lives inline in `promoteMatches` /
`advance` today, just relocated.

## Unrepresentable-states analysis

| Invalid state today (prevented only by ordering) | After this RFC |
| --- | --- |
| Termination checked before a finished match is promoted → flow stalls with a "running" match that's actually done | Priority order is encoded once in `nextTransition`; `promote-match` is checked first, by construction |
| Double-finalize: writing a second `flow.completed`/`flow.failed` | `complete`/`fail` are only constructed when `!isFlowTerminal`; otherwise `settled`. The executor has no path to finalize a settled flow |
| `advance` recomputing a decision differently than it acts on it (e.g. recompute output after state changed) | The decision and its payload are one immutable `Transition`; no recompute |

**Still representable, accepted as invariant (not worth a type):** a
`dispatch` transition with an empty `runnable` array would be wrong, but it's
only ever constructed under `runnable.length > 0`. A `NonEmptyArray<StepId>`
type would prove it, but that's gold-plating for a single construction site.
Documented as an invariant in a code comment instead.

## Outbox / crash-recovery review

`advance` is the engine's "outbox drain": it turns the durable fact log into
queue enqueues. The relevant durability question is what happens if the
process crashes mid-tick.

- **`dispatch`/`skip` ticks** append `step.skipped` facts and/or enqueue
  messages as **separate, non-atomic** awaits (true today, unchanged here).
- Recovery is **by re-derivation, not by transactional outbox**: on restart,
  `advance` re-runs, `nextTransition` re-reads state, and `nextRunnable`
  re-emits the same decision. Skips already recorded are skipped again
  (idempotent projection); runnables already enqueued may be re-enqueued, and
  `dispatchMessage`'s `claimStep` (dispatch.ts:118) dedupes the double
  delivery.

This RFC **preserves that model exactly** — it does not change the order of
`appendFact` vs `enqueue`, and does not attempt to make them atomic. Making
the skip-fact-then-enqueue transactional is a separate concern and explicitly
**out of scope**. Flagging it here only so the review is on record: the
refactor neither improves nor regresses crash semantics.

## Behavior preservation & testing

- All existing `dispatch.test.ts`, `scheduler.test.ts`, `replay-from.test.ts`,
  and end-to-end runtime tests must pass **unchanged**. That is the primary
  safety net — this is a pure mechanical extraction.
- **New** `scheduler.test.ts` cases exercise `nextTransition` directly with
  synthetic `RunState` (built via the existing `projectFacts` helper), one per
  arm and one per priority-ordering edge (e.g. "a finished match plus an
  otherwise-runnable step yields `promote-match`, not `dispatch`").

## Alternatives considered

- **One promotion per tick** (decision #5 inverse): simpler `MatchPromotion`
  (no array) but more loop iterations and a gratuitous behavior change vs
  today's batch promotion. Rejected — no benefit.
- **`advance` recomputes payloads** (decision #4 inverse): `Transition`
  carries only `kind`, `advance` calls `computeFlowOutput` itself. Keeps
  `scheduler.ts` thinner but reintroduces the "decide and act can diverge"
  hazard and makes the transition non-self-describing. Rejected.
- **New `engine.ts` module** (decision #8 inverse): rejected — one function,
  composing existing scheduler primitives, belongs with them.

## Resolved questions (2026-05-21, Jay)

1. **Naming.** `Transition` / `nextTransition` — kept (decision #1, #8).
2. **`idle` granularity.** **Split** into `settled` + `waiting` (decision #6) —
   diverges from my one-arm recommendation; chosen for future observability.
3. **Payload placement.** **Move** `computeFlowOutput` + `isFlowTerminal` into
   `scheduler.ts` (decision #4).
```
