# Issue #8 — `ctx.signal` aborts on run cancellation

Research + plan. Citations are `file:line` against current `main`.

## Issue recap

- Today: `cancel-in-progress` cancels the prior run's *scheduling* but in-flight step handlers keep running until they return. An LLM call started before cancellation can keep streaming tokens for 30–60s.
- Want: a live `AbortSignal` in `ctx.signal` that aborts the moment the run enters `canceled` / `aborted` status. Power users opt in by passing `ctx.signal` to `fetch` / `anthropic.messages.create({ signal })`.
- Best-effort by adapter contract — handlers that ignore the signal get no benefit. That's fine.

## What already exists

- `StepCtx.signal: AbortSignal` is **already declared** (`packages/core/src/types.ts:97`) and surfaced via a type-test (`types.test-d.ts:76-78`).
- `makeStepCtx` (`dispatch.ts:541-571`) already constructs `const ac = new AbortController()` (`dispatch.ts:552`) and passes `ac.signal` to the handler ctx (`dispatch.ts:559`). **Nothing aborts `ac`.** It's a stub — handlers see a never-aborting signal today.
- Implication: there is no new public API; only wiring is missing.

## How cancellation reaches a worker today

- `wf.start()` → `store.tryStartRun(runId, fact, concurrencyArg)` (`runtime.ts:321-325`). Adapter atomically marks priors `canceled` and returns the `canceled` array of `{runId, fact}` pairs.
- In-memory: `memory.ts:97-141` (`tryStartRun` calls `appendFact` with a `flow.canceled` fact on each prior run, mutates `activeByKey`).
- Postgres: `store.ts:84-198` (same pattern; updates `workflow_run.status='canceled'`, inserts `flow.canceled` row in `fact`).
- The **fact** lands inside the new `wf.start()`'s process. Workers running the cancelled run's steps are a separate async chain (or separate process) — they learn nothing automatically.
- Pre-claim guard exists in dispatcher: `dispatch.ts:107-115` — when a *queued* message is picked up after cancellation, dispatch reads `runState.status === "canceled"`, acks-and-skips. That's why the existing test "dispatchMessage early-acks a queued step for a canceled run" (`concurrency.test.ts:180-200`) passes. But it does nothing for a step already past the claim and inside `def.run()`.

## Wakeup mechanisms

Two pieces are already in place:

- **Postgres NOTIFY**: `store.ts:515-518` emits `pg_notify(channel, runId)` after every `appendFact`. `trigger.ts:47-84` exposes `postgresTrigger({ listen, channel })` returning a `Trigger`. Subscribers get only `runId`; they must reload run state.
- **InMemoryTrigger**: `memory.ts:652-668` (`subscribe`/`fire`) but `appendFact` does *not* call `trigger.fire` (test-only sync dispatch).

So the in-memory adapter has no wakeup channel between `appendFact` and any observer. **Polling is the universal fallback.**

## Fact + status types

- `RunStatus = "pending" | "running" | "completed" | "failed" | "canceled"` (`types.ts:1114-1119`). The issue text says "cancelled/aborted" but `aborted` is not a real status today — only `canceled`. Cleanest design: trigger abort on `status === "canceled" || "failed" || "completed"` (terminal of any kind).
- `FactKind` (`types.ts:934-948`): `flow.canceled` exists; **`step.cancelled` does not.** Adding it is a fact-kind union widening (small surface).
- `StepStatus = "pending" | "running" | "completed" | "failed" | "skipped"` (`types.ts:1120-1125`). No `canceled` step status today.

## Replay nuance (open question in issue)

- `wf.replay({ from })` on a `running` run throws today (`runtime.ts:577-582`). So "abort the prior in-flight attempt" doesn't apply to `replay({ from })` until that restriction is lifted.
- This is out of scope for this PR unless we also relax the running-run restriction. **Recommend: punt to a follow-up issue.**

## Design decisions to confirm before coding

### 1. Wakeup mechanism: polling vs subscription

- **A. Polling only.** In `executeTask`, spawn a background tick that calls `store.loadRunState(runId)` every ~250ms; on terminal status, abort the controller. Works for every Store adapter without interface changes. Cost: one extra `loadRunState` per step per 250ms (cheap on in-memory; one read query on Postgres).
- **B. Trigger-driven, polling fallback.** If `deps.trigger !== undefined`, subscribe filtered by `runId`. On wake, reload state, abort if terminal. Otherwise fall back to polling.
- **C. Trigger only.** Mandatory `Trigger`. Cleanest in pg; breaks the in-memory adapter and tests.

**Recommend A for v1.** Predictable, no new Store interface, no adapter coupling. We can layer B in later without a public-API change (the `signal` semantics don't shift). Note `DispatchDeps` doesn't currently carry `trigger` (`dispatch.ts:34-55`), so B would also add plumbing.

Concrete poll interval — propose `250ms`, configurable via a new `DispatchDeps.cancelPollIntervalMs?` (or hard-coded for v1, add config later when someone needs to tune).

### 2. How is an aborted step recorded?

The issue proposes a new fact: `step.cancelled` (terminal, no retry). Alternatives:

- **A. New `step.cancelled` fact.** Pros: observers can discriminate "this step was cancelled by run-cancel" from "this step failed". Cons: widens `FactKind`, projector update, postgres migration (CHECK constraint on `kind` if any).
- **B. Reuse `step.failed`, set `error.name = "NagiAbortedError"`.** Pros: zero schema change. Cons: collapses cancel-by-abort with handler-thrown errors at the projection layer; observers need to check `error.name`.

Tradeoff: nagi already has `NagiCanceledError` (`runtime.ts:139-160`) used for the *flow*-level case. Symmetry argues for a step-level analogue. Issue prefers a dedicated fact.

**Recommend A** (matches issue intent), and:
- Add `StepStatus = "...|cancelled"` (alongside `completed/failed/skipped`).
- Projector handles `step.cancelled` like a terminal state (no retry, no re-pick).
- `flow.canceled` already wins as the terminal flow status; the in-flight step's `step.cancelled` is informational, the *flow* is canceled regardless.

Open sub-question: spelling — `step.canceled` (one L, matches `flow.canceled`) or `step.cancelled` (two L, matches issue text)? Existing code uses one-L (`flow.canceled`, `status: "canceled"`). **Recommend one-L for consistency: `step.canceled`.**

### 3. Classification logic

Inside `executeTask`'s catch:

```ts
} catch (err) {
  // If the controller aborted while we were waiting, the step is canceled,
  // not failed. This holds whether the handler threw AbortError (because it
  // honored ctx.signal) or some unrelated error (race between abort and
  // throw — we still classify as cancel, because the run is gone).
  if (ac.signal.aborted) {
    // record step.canceled, skip retry, skip onStepError
    return;
  }
  // existing failure path
}
```

After a *successful* return, also check `ac.signal.aborted`:

```ts
const out = await def.run({ ... });
if (ac.signal.aborted) {
  // handler returned but the run was canceled mid-flight.
  // Record step.canceled rather than step.completed.
}
```

This handles the case "handler didn't honor signal, returned normally, but run was already cancelled." Without this, we'd record `step.completed` on a cancelled run, leaving stale step output. Cleaner to canonicalize: any step that ran on a canceled run records `step.canceled`.

(Counter-argument: persisting `step.completed` is harmless — the run is terminal, downstream is skipped. Less code. But it pollutes the read-side projection.)

**Recommend: classify any post-cancel return as `step.canceled`.**

### 4. Retry suppression

Today: `handleStepError` decides retry based on policy + `retryAllows` (`dispatch.ts:317`). If we go with the dedicated-fact approach, the cancel path doesn't hit `handleStepError` at all — it short-circuits to `step.canceled` + ack. So retry suppression is structural, not a policy check.

For belt-and-braces: even on `step.failed`, if `runState.status === "canceled"`, suppress retry. This catches the edge where a normal error and a cancellation race. Add `attempt < policy.maxAttempts && !runIsCanceled && retryAllows(...)` to the guard.

### 5. Hooks behavior on cancel

When a step is canceled mid-flight, do we fire:
- `step.onError` / `onStepError`? The issue doesn't say. My read: **no** — it's not an error, it's a cancellation. `flow.onError` already fires (with `NagiCanceledError`) at the start of the new run that cancelled this one (`runtime.ts:361-372`). Adding another hook fire on each in-flight step would be noisy.
- A new `step.onCancel` hook? Out of scope for v1. We can add later if someone asks.

**Recommend: no hooks fire on `step.canceled` in v1.** Document it.

### 6. Always-present `signal` (issue confirms)

`signal` is already always present in `StepCtx`. No change. Handlers that don't pass it to `fetch` simply observe no benefit. ✅

## Implementation sketch

Touches `packages/core/src/dispatch.ts` only (plus `types.ts` for the new fact kind / status / projector).

```ts
// dispatch.ts — executeTask
async function executeTask(args) {
  const { deps, message, def, runId, stepId, attempt } = args;
  const { store, queue, clock } = deps;

  const runState0 = await store.loadRunState(runId);
  const input = extractInput(runState0);
  const needs = resolveNeeds(def, (id) => runState0.steps[id]?.output ?? null);

  const ac = new AbortController();
  const watcher = startCancelWatcher({ store, runId, ac, intervalMs: 250 });

  try {
    return await store.runStep<Json>(runId, stepId, attempt, async (tx) => {
      const ctx = makeStepCtx({ ..., signal: ac.signal, tx });
      const out = (await def.run({ input, needs, ctx })) as Json;
      if (ac.signal.aborted) {
        const fact: Fact = { kind: "step.canceled", runId, stepId, attempt, at: clock.now() };
        return { output: out, fact, classification: "canceled" };
      }
      const fact: Fact = { kind: "step.completed", runId, stepId, attempt, output: out, at: clock.now() };
      return { output: out, fact };
    });
  } finally {
    watcher.stop();
    await queue.ack(message.receipt);
  }
}

function startCancelWatcher({ store, runId, ac, intervalMs }) {
  let stopped = false;
  void (async () => {
    while (!stopped) {
      await new Promise(r => setTimeout(r, intervalMs));
      if (stopped) return;
      try {
        const state = await store.loadRunState(runId);
        if (state.status === "canceled" || state.status === "failed" || state.status === "completed") {
          ac.abort(new NagiStepCanceledError(runId));
          return;
        }
      } catch {} // best-effort
    }
  })();
  return { stop: () => { stopped = true; } };
}
```

Note: `Store.runStep` currently expects `{ output, fact }` (`memory.ts:203-219` and Store contract `types.ts:744-...`). Adding a `canceled` fact may need a small contract widening — needs verification. Confirm before coding.

In `handleStepError`:

```ts
if (ac.signal.aborted) {
  // canceled, not failed
  await store.appendFact(runId, { kind: "step.canceled", runId, stepId, attempt, at: clock.now() });
  await queue.ack(message.receipt);
  await advance(deps, runId); // advance will see flow.canceled and finalize
  return;
}
```

## Files to change

- `packages/core/src/types.ts`:
  - Add `"step.canceled"` to `FactKind` (`types.ts:934-948`).
  - Add `StepCanceledFact` interface to the `Fact` union (`types.ts:1098-1112`).
  - Add `"canceled"` to `StepStatus` (`types.ts:1120-1125`).
  - Confirm `Store.runStep` body return type accepts cancel.

- `packages/core/src/memory.ts`:
  - Projector switch in `projectRunState` — handle `step.canceled` (mark step canceled). Need to read `memory.ts:246-...` to insert at the right spot.
  - `runStep` body return needs to handle the new fact kind.

- `packages/core/src/dispatch.ts`:
  - Replace dead `AbortController` (`dispatch.ts:552`) wiring with `startCancelWatcher`.
  - Classify post-handler `ac.signal.aborted` → `step.canceled`.
  - In `handleStepError`, when `ac.signal.aborted` → record `step.canceled`, skip retry.
  - Suppress hooks on cancel.

- `packages/postgres/src/migrations.ts`:
  - If there's a `kind` CHECK constraint on `fact`, widen to include `step.canceled`. Need to read `migrations.ts:131-137` area to confirm.
  - If there's a `status` CHECK constraint on a step-state column or computed view, widen.

- `packages/postgres/src/store.ts`:
  - Project / write `step.canceled` rows the same way `step.failed` is handled. Need to read `store.ts:411-...` (the `kind` switch) to mirror.

- `packages/core/src/concurrency.test.ts`:
  - New test: handler that loops checking `ctx.signal.aborted`; second `wf.start()` with same key; verify handler exits within ≪ N ms (the poll interval + a slack budget).
  - New test: handler that throws `AbortError` after receiving `ctx.signal` — classified as `step.canceled` not `step.failed`.
  - New test: handler that ignores signal and returns normally on a cancelled run — `step.canceled` recorded (not `step.completed`).

- `packages/postgres/src/integration.test.ts`:
  - Mirror at least one cancel-aborts-in-flight test against real Postgres.

- `.changeset/`:
  - `patch` bump on `@nagi-js/core` (per `feedback-changeset-bump-type`).
  - `patch` on `@nagi-js/postgres` if migration touched.

## Open questions for Jay (before coding)

1. **Poll interval.** Default `250ms`? Configurable from the start, or hard-code and add config later?
2. **`step.canceled` vs reuse `step.failed`.** Confirm dedicated fact (matches issue) vs reuse-and-tag-error (less schema change). Recommend dedicated.
3. **Spelling.** `step.canceled` (one L, matches `flow.canceled`) over issue's `step.cancelled`?
4. **Hooks on cancel.** Suppress `onStepError` for canceled steps? Recommend yes.
5. **Replay-aborts-prior-attempt.** Out of scope for this PR? Recommend yes — file as #N for later, requires lifting the "running run" check at `runtime.ts:577-582`.
6. **Successful return on cancelled run.** Reclassify to `step.canceled` (recommend) or keep `step.completed`?
