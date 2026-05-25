# RFC 0013 — Activity steps (research notes)

- **Tracking issue:** lymo-inc/nagi#13
- **Date:** 2026-05-25 (JST)
- **Scope:** `@nagi-js/core` (builder, types, dispatch, state, canonicalize) + one
  shared store path. No persistence-schema change; no adapter-schema change.

Working notes backing the RFC: how a step is defined and executed today, where
the transaction is held, and the exact implementation surface for a new kind.

## Current state — step kinds

Five kinds, defined in `packages/core/src/types.ts:66`:

```ts
export type StepKind = "task" | "signal" | "match" | "subflow" | "streaming";
```

Internal defs in `packages/core/src/internal.ts`:
- `TaskDef` / `StreamingTaskDef` — the "handler" defs (`HandlerDef`), have `run`.
- `SignalDef`, `MatchDef`, `SubflowDef` — no body.
- `handlerDef(def)` (`internal.ts:192`) returns the def only for `task`/`streaming`.

## Where the transaction is held

`packages/postgres/src/store.ts:343` `runStep` opens `this.db.transaction()` and
runs the **entire body** inside it (`body(trx)`), then writes the fact + releases
the lease. `executeTask` (`packages/core/src/exec/message.ts:301`) calls
`runHandler` *inside* that body, so the handler — including any LLM call — holds
the tx for its full duration. `ctx.tx` (`makeStepCtx`, `step-exec.ts:170`) is
that transaction.

The memory store mirrors this (`packages/core/src/memory.ts` `runStep`).

## Prior art for non-tx steps

`match`, `subflow`, and `signal` already commit **without** wrapping a body in
`runStep`: they use `store.appendFact` / `store.settleStep` (short txns). So "a
step kind that doesn't hold a tx around a body" is already precedented; activities
add "run a body first, *then* settle."

`store.settleStep` (`types.ts:514`) commits `step.completed | step.failed` in a
short tx (upsert + insertFact + deleteLease). It does **not** take
`step.canceled` — so the activity commit reuses `runStep` with a no-op body
(which already handles all three fact kinds via `resolveExecutionFact`) rather
than `settleStep`, to keep cancel semantics identical to `task` for free.

## Implementation surface (exhaustive)

Branch points found via grep for kind switches (non-test):

| File | Location | Change |
|---|---|---|
| `internal.ts` | `TaskDef` (79) | add `ActivityDef` (mirror), add to `StepDef` + `HandlerDef` unions |
| `internal.ts` | `handlerDef` (192) | include `"activity"` |
| `types.ts` | `StepKind` (66) | add `"activity"` |
| `types.ts` | `StepCtx` (110) | add `ActivityCtx = Omit<StepCtx, "tx">`, `ActivityConfig`, `Builder.activity` |
| `builder.ts` | `task` (45) | add `activity` fn + export in builder object |
| `state.ts` | `startTarget` (227) | `case "activity"` → `running` |
| `canonicalize.ts` | `canonicalizeStep` (108) | include `"activity"` in the task/streaming branch; widen `canonicalizeTask` param |
| `exec/message.ts` | `execute` (178) | `case "activity"` → `executeActivity`; add to `startCarriesInput` (165); add `executeActivity` (handler outside `runStep` + short commit) |
| `step-exec.ts` | `makeStepCtx` (170) | add `makeActivityCtx` (no real tx; throwing `tx` getter) |

No change needed:
- `scheduler.ts` — keys off needs/`when`; activity is non-match, falls through
  the task-like path (`scheduler.ts:86` else branch).
- `flow-registry.ts:28` — only tracks `streaming` ids.
- `signals.ts` — signal-only.
- `replay.ts` — re-dispatches via `dispatchMessage`; completed activities are
  memoized (skipped by `admit`), so no re-run.

## Execution detail — `executeActivity`

Mirror of `executeTask` with the handler hoisted out of `runStep`:

```ts
const ctx = makeActivityCtx({ runId, stepId, attempt, input, store, clock, signal: ac.signal, emitLog });
const out = await runHandler(def, { input, needs, ctx, runId, stepId }); // NO tx held
let stepAbortedHere = false;
const output = await store.runStep<Json>(runId, stepId, attempt, async () => {
  const postState = await store.loadRunState(runId);
  const { fact, abortedHere } = resolveExecutionFact({ postState, runId, stepId, attempt, output: out, at: clock.now() });
  stepAbortedHere = abortedHere;
  return { output: out, fact };           // short tx: load + fact write only
});
return { output, skipAdvance: stepAbortedHere };
```

- Handler throw → propagates to `dispatchMessage` catch → `handleStepError`
  (retry/fail/cancel), with **no** tx held during the failed handler.
- Cancellation during the (now untransacted) handler is detected by the existing
  `startCancelWatcher` + `resolveExecutionFact` post-state check, identical to
  `task`.

## Risk / validation

- Type-level: new `ActivityCtx` (no `tx`) must thread through builder inference
  without breaking `task`/`streaming` `.test-d.ts` files. Run `verify:types`.
- Canonicalization: activity hashes like a task (needs/when/timeout/retry) — a
  flow that swaps `task`→`activity` *will* change its flow hash (different
  `kind`), which is correct (it is a different step kind) and triggers a normal
  ref update.
- Run full `@nagi-js/core` suite + `@nagi-js/postgres` integration after impl.
