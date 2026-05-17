# Issue #6 — Step-scoped replay (`wf.replay(runId, { from: stepId })`)

Research + plan. Citations are `file:line` against current `main`.

## Issue recap

- Today: `wf.replay(runId, opts)` re-dispatches from the first incomplete step. No way to replay a specific step on a completed run.
- Want: add `from?: StepId` to `ReplayOpts`. Semantics per issue: "append the equivalent of `step.reset` to the fact log, re-project, re-dispatch from there. Steps downstream cascade per existing replay semantics."
- Forcing case: Lymo UI per-tab "re-run just X" affordance; read-side OpenAPI already carries `RunAction.from`.

## How replay works today

- `wf.replay` lives in `packages/core/src/runtime.ts:495-558`.
  - Loads `runState` via `store.loadRunState(runId)` (`runtime.ts:499`).
  - Hard-errors on canceled runs (`runtime.ts:506-511`).
  - Inspect mode is a no-op (`runtime.ts:512`).
  - Drift handling on `continue` mode if `flowHash` pinned (`runtime.ts:526-550`).
  - Calls `advance(replayDeps, runId)` (`runtime.ts:552`), which is the same dispatch path as a live run.
  - Drains inline if `fireHooks: false` (`runtime.ts:556-557`).
- `ReplayOpts` defined at `packages/core/src/types.ts:1067-1087`: `mode | allowDrift? | fireHooks?`.
- `ReplayMode = "inspect" | "continue"` (`types.ts:1065`).

## How "from first incomplete" is computed today

There is no explicit "fact-log walk for first incomplete" — the behavior is emergent from the scheduler:

- `advance()` → eventually `nextRunnable(...)` (`packages/core/src/dispatch.ts:407`).
- `nextRunnable` (`packages/core/src/scheduler.ts:29-72`) iterates `flow.steps` and picks any step whose state is `undefined` or `status === "pending"` (`scheduler.ts:36`). Completed/failed/skipped steps are skipped.
- So "first incomplete" is just "every step that isn't terminal". On a fully completed run, `nextRunnable` returns nothing → replay is a no-op today. (This matches the issue's claim that completed runs have no retry primitive.)

## Fact log + projection

- `FactKind` enum at `types.ts:869-882`. No `step.reset` exists today.
- `Fact` union at `types.ts:1017-1030`.
- `projectRunState` at `packages/core/src/memory.ts:246-322` is the **single** projector. Pure fold: `(runId, facts[]) → RunState`.
  - Handles `flow.*` (sets `status`, `flowHash`, etc.) and `step.started/completed/failed/skipped`.
  - `step.retried`, `signal.*`, `once.recorded`, `match.arm-selected` deliberately don't affect step-state projection (`memory.ts:303-309`); they live only in the fact log and are read directly by other code (e.g. `readSelectedArm`).
- Both adapters route through the same projector:
  - In-memory: `memory.ts:130-131`.
  - Postgres: `packages/postgres/src/store.ts:204-219` (`import { projectRunState } from "@nagi-js/core"`, then `return projectRunState(runId, facts)`).
- Pgmq is a queue-only adapter (no store), so no projection changes there.

## Cascade semantics (already in place)

- `checkUpstream` in `scheduler.ts:76-98`: if upstream state is `undefined` → "blocked"; "completed" → continue; "failed"/"skipped" → "transitive-skip".
- So **deleting a step's state via projection is enough to re-enable downstream re-runs** — but only if downstream's own state is also cleared, otherwise `nextRunnable` line 36 skips it (it's still "completed").
- Match arm selection is read from facts (`readSelectedArm` — not yet read in detail; lives in `internal.ts`). Flagged below.

## Other relevant pieces

- `StepId = string` (`types.ts:11`); no structural constraints. Validated only by membership in `flow.steps`.
- `NagiValidationError` at `runtime.ts:99`. Used elsewhere for input validation (e.g. empty runId).
- Tests for replay live in `packages/core/src/snapshot.test.ts:110-297` (drift) and `concurrency.test.ts:170` (canceled). No "completed run replay" test today — confirms the issue.
- `RunAction.from` 400 handler lives **outside this repo** (Lymo's read-side API). Not in nagi core, postgres, or pgmq. So this issue is core-only.

## Design decisions to confirm before coding

These are not implementation details — they shape semantics.

### 1. Where does cascade live?

Two options:

- **A. Projector handles cascade.** `step.reset { stepId }` in projector clears that step *plus all transitive descendants*. Requires passing the flow DAG into `projectRunState`. Breaks the current pure `(runId, facts) → RunState` signature.
- **B. Runtime expands cascade.** `replay()` computes `descendants(from, liveFlow)`, appends one `step.reset` fact per descendant (including `from` itself). Projector stays DAG-agnostic — it just deletes one step's state per fact.

**Recommend B.** Keeps projector pure, keeps the fact log explicit about what was reset (auditable), preserves the "store + memory share one projector" invariant.

### 2. What does `step.reset` actually do in projection?

Proposed: `case "step.reset": delete steps[fact.stepId]; break;` (`memory.ts` switch). After this, `nextRunnable` line 36 treats the step as runnable.

Edge cases:
- A step has `step.retried` / `signal.*` / `once.recorded` facts from a prior attempt. These don't feed `steps[]`, so a delete is sufficient for the projector. But `readSelectedArm` for match steps reads facts directly — **a reset on a match step needs to invalidate the prior `match.arm-selected` fact**, otherwise the old arm selection sticks. → simplest fix: make `readSelectedArm` return the latest `match.arm-selected` fact *after* the most recent `step.reset` for that stepId. **Need to read `internal.ts:readSelectedArm` to confirm shape before committing.**
- `once.recorded`: if a step is reset and re-run, does `step.once` deduplicate? Probably yes today (the fact still exists). Whether reset should clear it is a behavior call — flagging.

### 3. `from` on which kinds of runs?

- **Completed run** → primary use case. ✅
- **Failed run** → today, replay restarts from failed step. With `from: X` where X != failed step, what wins? Recommend: `from` always wins (explicit beats implicit).
- **Running run** → forbid (`NagiRuntimeError`)? A reset mid-flight races with in-flight workers. Recommend: forbid for now, revisit if requested.
- **Canceled run** → already forbidden at `runtime.ts:506-511`. Keep.

### 4. `from` validation

- `from` must be a key in `liveFlow.steps`. Unknown → `NagiValidationError` (per issue).
- Under drift + `allowDrift: true`, replay uses the synthesized flow (`runtime.ts:548`). `from` should be validated against the **synthesized** flow (snapshot topology), not live. Otherwise users get inconsistent semantics between drift / no-drift replays.

### 5. `step.reset` fact shape

Minimal: `{ kind: "step.reset", runId, at, stepId }`. Considered extras:
- `triggeredBy: "replay-from"` — useful for audit but adds no behavior; can defer.
- `cascadedFrom: StepId` — when runtime emits multiple resets, distinguish the user-named one from cascade-emitted ones. Useful for read-side UIs. **Recommend including.**

### 6. Inspect mode + `from`

`mode: "inspect"` returns early at `runtime.ts:512`. Should `from` work in inspect? Recommend: no — inspect today is a probe with no side effects; honor that. (Could revisit later: "what would replay-from-X dispatch?")

## Implementation plan

Files to touch:

1. **`packages/core/src/types.ts`**
   - Add `"step.reset"` to `FactKind` (`types.ts:869-882`).
   - Add `StepResetFact` interface + add to `Fact` union (`types.ts:1017-1030`).
   - Add `from?: StepId` to `ReplayOpts` (`types.ts:1067-1087`) with a docstring covering: descendant cascade, validation, drift interaction.

2. **`packages/core/src/memory.ts`**
   - Add `case "step.reset":` in `projectRunState` switch (`memory.ts:256-311`) → `delete steps[fact.stepId]`.

3. **`packages/core/src/internal.ts`** *(pending verification)*
   - Update `readSelectedArm` to find the latest `match.arm-selected` only after the most recent `step.reset` for that match stepId. Without this, a reset match step keeps its old arm.

4. **`packages/core/src/runtime.ts`**
   - In `replay()` (`runtime.ts:495-558`): after drift handling, if `opts.from` defined:
     - Resolve the effective flow (synthesized if drift+allow, else live).
     - Validate `from ∈ flow.steps` → `NagiValidationError` if not.
     - Forbid on `status === "running"` → `NagiRuntimeError`.
     - Compute transitive descendants via the flow DAG (helper, probably in `scheduler.ts` or new `dag.ts`).
     - Append `step.reset` facts (one for `from`, one per descendant) via `store.appendFact`.
     - Continue to `advance(replayDeps, runId)`.

5. **Helper: `descendantsOf(flow, stepId): readonly StepId[]`**
   - Walks `step.needs` reverse-edges. Live in `scheduler.ts` (next to existing DAG-aware code) or new file. Pure function, easily testable.

6. **Tests**
   - `packages/core/src/runtime.test.ts` or a new `replay-from.test.ts`:
     - Completed run + `from: X` re-runs X and downstream; doesn't re-run upstream.
     - `from: unknownId` → `NagiValidationError`.
     - `from` on running run → `NagiRuntimeError`.
     - `from` overrides "first incomplete" on failed run.
     - `from: matchStep` re-selects arm (covers `readSelectedArm` change).
     - `from` inside a match arm cascades within the arm only.
     - `fireHooks: false` + `from` still suppresses hooks (existing scope).
     - Drift + `allowDrift: true` + `from` validates against synthesized topology.
   - `packages/core/src/scheduler.test.ts`: add `descendantsOf` cases (linear, diamond, match arms, parentMatch nesting).
   - `packages/postgres/src/store.test.ts` (if exists): smoke-test that `step.reset` round-trips and projects identically.

7. **Changeset**
   - `.changeset/<slug>.md` — minor bump on `@nagi-js/core` (additive surface). Postgres/pgmq no version bump (no API change, behavior change is transitive through `projectRunState`).

## Things I haven't verified yet (would do in a second pass before coding)

- Exact shape of `readSelectedArm` in `internal.ts` and whether other fact readers (`once.recorded`, `signal.*`) need similar "stop at most recent reset" treatment.
- Whether `once.recorded` semantics should be reset alongside the step (probably yes for the "re-run just X" mental model, but it changes the contract).
- Whether the postgres adapter has any column / index that assumes monotonic step state (don't expect so — facts table is append-only, projection is in-process — but worth a 30-second scan).
- Whether `dispatch.ts:439` (`(await store.loadRunState(runId)).facts`) consumers care that the fact log now contains `step.reset` entries they don't recognize.

## Open questions for Jay

1. Cascade-in-runtime (Option B) — agree?
2. `cascadedFrom: StepId` on the fact — include or defer?
3. `from` on a running run — forbid (recommended) or allow with a footgun warning?
4. `step.once` and prior signal facts — reset alongside, or leave them as recorded history? (Has UX implications for "re-run just the summary" if the summary step uses `step.once`.)
5. `from` in inspect mode — no-op (recommended) or compute-without-side-effects probe?
