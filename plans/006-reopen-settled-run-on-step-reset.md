# Plan 006: Reopen a completed/failed run on `step.reset` so replay and operator.retry finish the job

> **Executor instructions**: Follow this plan step by step. Run every
> verification command and confirm the expected result before moving to the
> next step. If anything in the "STOP conditions" section occurs, stop and
> report — do not improvise. When done, update the status row for this plan
> in `plans/README.md` — unless a reviewer dispatched you and told you they
> maintain the index.
>
> **Drift check (run first)**: `git diff --stat ced05c2..HEAD -- packages/core/src/state.ts packages/core/src/memory.ts packages/core/src/scheduler.ts packages/core/src/step-exec.ts packages/core/src/replay.ts packages/core/src/operator.ts packages/postgres/src/store.ts packages/core/src/tests/replay-from.test.ts packages/core/src/tests/operator.test.ts packages/postgres/src/integration.test.ts`
> If any in-scope file changed since this plan was written, compare the
> "Current state" excerpts against the live code before proceeding; on a
> mismatch, treat it as a STOP condition.

## Status

- **Priority**: P1
- **Effort**: M
- **Risk**: MED
- **Depends on**: plans/004-clear-buffered-signals-on-reset.md (recommended first; independent code, but the combined tests in Plan 004 assume this plan's semantics are not yet present — land 004 first, then this)
- **Category**: bug
- **Planned at**: commit `ced05c2`, 2026-09-04
- **Issue**: https://github.com/lymo-inc/nagi/issues/48

## Why this matters

`wf.replay({ from })` and `operator.retry` are the recovery tools for a run
that ended `failed`. Both write `step.reset` facts and re-dispatch the steps.
But the run-state fold never leaves a terminal phase: after the reset, steps
are `pending` while `phase` is still `failed`. Two things then go wrong:

1. **The run never completes.** `nextTransition` returns `settled` for any
   terminal run before it can emit `complete`, so when the re-run steps finish
   no `flow.completed` fact is written. `describe()`, `queryRuns`, and the
   materialized `workflow_run.status` keep saying `failed`; `onFlowComplete`
   never fires; a parent waiting on this run as a subflow is never woken.
   The existing tests (`replay-from.test.ts` "from-failed",
   `operator.test.ts` "retry-terminal") only assert that handlers re-ran.
2. **Re-run handlers are aborted after 250 ms.** The cancel watcher started
   for every task/activity polls `loadRunState` and calls
   `ac.abort(new NagiAbortError(runId, "run"))` when `isTerminalRun(state)` —
   which is true for a `failed` run. Any handler that honors `ctx.signal`
   (every `fetch`/LLM SDK call that is passed the signal) is aborted on the
   first watcher tick, classified as a normal failure, and the retry fails
   again. In the test suite handlers finish in microseconds so this is
   invisible; in production a retried LLM step cannot succeed.

The structural fix, in line with the repo's "make invalid states
unrepresentable" principle: a run with a pending step is not terminal. A
`step.reset` on a `completed`/`failed` run **reopens** it to `running`
(never a `canceled` run — `operator.retry` already rejects those).

## Current state

- `packages/core/src/state.ts`:

  ```ts
  // state.ts:81-86
  export type RunPhase =
    | { readonly tag: "pending" }
    | { readonly tag: "running" }
    | { readonly tag: "completed"; readonly output: Json }
    | { readonly tag: "failed"; readonly error: SerializedError }
    | { readonly tag: "canceled"; readonly cause: RunCancelCause };

  // state.ts:185-187
  export function isTerminalRun(state: RunState): boolean {
    return state.phase.tag !== "pending" && state.phase.tag !== "running";
  }

  // state.ts:378-381 (inside foldRun)
  case "step.reset":
    steps[fact.stepId] = PENDING;
    delete selectedArms[fact.stepId];
    break;
  ```

  (After Plan 004 this branch also has `delete bufferedSignals[fact.stepId];` — keep it.)
- `packages/core/src/scheduler.ts:232-253` — `nextTransition`:

  ```ts
  const term = flowTermination(flow, runState);
  switch (term.kind) {
    case "succeeded":
    case "failed": {
      if (isTerminalRun(runState)) return { kind: "settled" };
      if (term.kind === "failed") return { kind: "fail", error: term.error };
      return { kind: "complete", output: computeFlowOutput(flow, runState) };
    }
  ```
- `packages/core/src/step-exec.ts:100-136` — `startCancelWatcher` (the 250 ms
  poll; `CANCEL_POLL_INTERVAL_MS = 250` at line 32):

  ```ts
  const s = await store.loadRunState(runId);
  if (isTerminalRun(s)) {
    if (!ac.signal.aborted) ac.abort(new NagiAbortError(runId, "run"));
    return;
  }
  ```
- `packages/core/src/replay.ts:71-97` — `replay({ from })` throws if the run
  is `running`, else appends `step.reset` for `from` + descendants, then advances.
- `packages/core/src/operator.ts:116-172` — `retry` throws if `canceled`,
  aborts a running step, appends `step.reset` for the cascade, then advances.
- `packages/core/src/memory.ts` — `InMemoryStore`:
  - fields (lines 69-90): `facts`, `activeByKey: Map<string, RunId>`,
    `keyByActiveRun: Map<RunId, string>`, `summaries`, …
  - `appendFact` (line 94–146): pushes the fact, drives the stream hub, and on
    `flow.completed|failed|canceled` **deletes** the run's concurrency slot:

    ```ts
    // memory.ts:126-138
    if (fact.kind === "flow.completed" || fact.kind === "flow.failed" || fact.kind === "flow.canceled") {
      const slot = this.keyByActiveRun.get(runId);
      if (slot !== undefined) {
        this.keyByActiveRun.delete(runId);
        if (this.activeByKey.get(slot) === runId) {
          this.activeByKey.delete(slot);
        }
      }
    }
    ```
  - `tryStartRun` registers the slot: `const slot = \`${fact.flowId}::${concurrency.key}\`;`
    then `this.activeByKey.set(slot, runId); this.keyByActiveRun.set(runId, slot);`
    (after canceling the prior holder).
  - `describe()` (from line ~555): `const status = runStatusOf(state);` then
    walks `factList` backwards to the last `flow.completed|failed|canceled`
    to fill `completedAt`/`output`/`error`/`canceledByRunId` (lines 566-587).
- `packages/postgres/src/store.ts`:
  - `applyFactToMaterialized` (line 726+): `flow.completed` →
    `UPDATE workflow_run SET status = 'completed', output = …, completed_at = …`;
    `flow.failed` → `SET status = 'failed', error = …, completed_at = …`;
    `step.reset` (lines 782-789):

    ```ts
    case "step.reset":
      await sql`DELETE FROM ${sql.raw(this.t("step_run"))} WHERE run_id = ${runId} AND step_id = ${fact.stepId}`.execute(trx);
      await this.deleteLease(trx, runId, fact.stepId);
      await this.deleteTimer(trx, runId, fact.stepId);
      return;
    ```
  - `isUniqueViolation(err)` helper at line 1296 (checks SQLSTATE `23505`).
  - `insertFact` is called inside a transaction by `appendFact`; the
    materialization runs in the same tx.
- `packages/postgres/src/migrations.ts` — `workflow_run` has
  `status` (CHECK in pending/running/completed/failed/canceled), `output`,
  `error`, `completed_at`, `concurrency_key`, and the partial unique index
  `workflow_run_concurrency_active_uidx ON (flow_id, concurrency_key) WHERE
  concurrency_key IS NOT NULL AND status IN ('pending','running')`. Reopening
  a run whose key another active run holds **violates this index** — that is
  the correct outcome and must surface as `NagiConcurrencyConflictError`.
- `packages/core/src/errors.ts:142-158`:

  ```ts
  export class NagiConcurrencyConflictError extends Error {
    constructor({ runId, flowId, concurrencyKey }: { readonly runId: RunId; readonly flowId: string; readonly concurrencyKey: string }) …
  ```
- Tests to extend: `packages/core/src/tests/replay-from.test.ts` ("`from`
  overrides the default 'first incomplete' behavior on a failed run", lines
  ~96-133) and `packages/core/src/tests/operator.test.ts` ("retry-terminal",
  lines ~147-185). Both end with handler-count assertions and no run-status
  assertion. Harness: `makeHarness`, `h.drain()`, `h.result(runId)` →
  `{ status, stepStatus, factCount, output }`; `StepCtx` exposes `ctx.signal`
  (an `AbortSignal`) to handlers.
- Changesets: `.changeset/<slug>.md`, `patch` for `@nagi-js/core` and `@nagi-js/postgres`.

## Commands you will need

| Purpose   | Command                  | Expected on success |
|-----------|--------------------------|---------------------|
| Typecheck | `pnpm typecheck`         | exit 0 |
| Core tests| `pnpm -F @nagi-js/core test` | exit 0 |
| Postgres integration | `pnpm db:up && pnpm test:integration; pnpm db:down` | exit 0 |
| Lint      | `pnpm lint`              | exit 0 |

## Scope

**In scope** (the only files you should modify):
- `packages/core/src/state.ts` (fold `step.reset` branch)
- `packages/core/src/memory.ts` (`appendFact` reopen + slot re-registration; `describe()` terminal fields)
- `packages/postgres/src/store.ts` (`applyFactToMaterialized` `step.reset` case)
- `packages/core/src/tests/replay-from.test.ts`, `packages/core/src/tests/operator.test.ts` (extend two tests)
- `packages/core/src/tests/run-reopen.test.ts` (create)
- `packages/postgres/src/integration.test.ts` (add two tests)
- `.changeset/reopen-on-step-reset.md` (create)

**Out of scope** (do NOT touch, even though they look related):
- `packages/core/src/scheduler.ts` — `nextTransition`'s `settled` branch is
  correct once the phase is right; do not special-case resets there.
- `packages/core/src/step-exec.ts` — the cancel watcher is correct once the
  phase is right; do not weaken `isTerminalRun`.
- `packages/core/src/replay.ts`, `operator.ts` — no new facts, no new guards.
  (`operator.retry` on a `canceled` run keeps throwing; `replay({from})` on a
  `canceled` run keeps its current behavior — the fold does not reopen `canceled`.)
- `packages/core/src/types.ts` — no new fact kind (`flow.reopened` was
  considered and rejected: the reset already is the reopening event).
- `docs/OPERATIONS.md`, `README.md`.

## Git workflow

- Branch: `advisor/006-reopen-on-step-reset`
- Commit subject: `fix(core,postgres): step.reset reopens a completed/failed run so replay and operator.retry can finish it`
- Do NOT push or open a PR unless the operator instructed it.

## Steps

### Step 1: Reopen in the fold

In `packages/core/src/state.ts` `foldRun`, `step.reset` branch:

```ts
case "step.reset":
  steps[fact.stepId] = PENDING;
  delete selectedArms[fact.stepId];
  delete bufferedSignals[fact.stepId];   // present after Plan 004; add if missing
  // A run with a pending step is not settled. Reset reopens completed/failed
  // (replay, operator.retry); canceled stays canceled — retry rejects those.
  if (phase.tag === "completed" || phase.tag === "failed") {
    phase = { tag: "running" };
  }
  break;
```

**Verify**: `pnpm typecheck` → exit 0. `pnpm -F @nagi-js/core test` → run it and
note any failures; expected: all pass (the fold change only affects runs with
a reset after a terminal fact). If `concurrency.test.ts` or `describe.test.ts`
fail, see STOP conditions.

### Step 2: Extend the two existing tests to assert the run re-terminalizes

`replay-from.test.ts`, test "`from` overrides the default 'first incomplete'
behavior on a failed run" — append after `expect(bRuns).toBe(2);`:

```ts
const result = await h.result(runId);
expect(result.status).toBe("completed");
expect(result.factCount("flow.failed")).toBe(1);
expect(result.factCount("flow.completed")).toBe(1);
```

`operator.test.ts`, test "retry-terminal" — after `expect(result.stepStatus("b")).toBe("completed");`:

```ts
expect(result.status).toBe("completed");
expect(result.factCount("flow.completed")).toBe(1);
```

**Verify**: both files pass. Stash `state.ts` → both new assertions fail
(`status` is `failed`); unstash → pass.

### Step 3: In-memory store — re-register the concurrency slot on reopen, and fix `describe()`

In `packages/core/src/memory.ts`:

1. Add a persistent map next to the existing ones:
   `private readonly slotByRun = new Map<RunId, string>();`
   In `tryStartRun`, where `this.keyByActiveRun.set(runId, slot);` is, also
   `this.slotByRun.set(runId, slot);`. (Do not delete from `slotByRun` on
   terminal facts.)
2. In `appendFact`, **before** `list.push(fact)`, handle reopen:

   ```ts
   if (fact.kind === "step.reset") {
     const prior = foldRun(runId, list);
     if (prior.phase.tag === "completed" || prior.phase.tag === "failed") {
       const slot = this.slotByRun.get(runId);
       if (slot !== undefined) {
         const holder = this.activeByKey.get(slot);
         if (holder !== undefined && holder !== runId) {
           throw new NagiConcurrencyConflictError({
             runId,
             flowId: prior.flowId,
             concurrencyKey: slot.slice(slot.indexOf("::") + 2),
           });
         }
         this.activeByKey.set(slot, runId);
         this.keyByActiveRun.set(runId, slot);
       }
     }
   }
   ```

   `foldRun` is already imported in `memory.ts` (it is used by `loadRunState`);
   import `NagiConcurrencyConflictError` from `./errors`.
3. In `describe()`, only fill `completedAt` / `output` / `error` /
   `canceledByRunId` from the backwards fact walk when `isTerminalRun(state)`
   (import from `./state` if not already). A reopened run must report no
   `completedAt`.

**Verify**: `pnpm typecheck` → exit 0; `pnpm -F @nagi-js/core test` → green.

### Step 4: Postgres store — materialize the reopen

In `packages/postgres/src/store.ts` `applyFactToMaterialized`, `case "step.reset"`,
add before the existing `DELETE FROM step_run`:

```ts
try {
  await sql`
    UPDATE ${sql.raw(this.t("workflow_run"))}
       SET status = 'running', output = NULL, error = NULL, completed_at = NULL
     WHERE run_id = ${runId} AND status IN ('completed', 'failed')
  `.execute(trx);
} catch (err) {
  // workflow_run_concurrency_active_uidx: another active run holds this key.
  if (!isUniqueViolation(err)) throw err;
  const row = await sql<{ flow_id: string; concurrency_key: string | null }>`
    SELECT flow_id, concurrency_key FROM ${sql.raw(this.t("workflow_run"))} WHERE run_id = ${runId}
  `.execute(trx);
  throw new NagiConcurrencyConflictError({
    runId,
    flowId: row.rows[0]?.flow_id ?? "",
    concurrencyKey: row.rows[0]?.concurrency_key ?? "",
  });
}
```

Import `NagiConcurrencyConflictError` from `@nagi-js/core` (check it is exported
from `packages/core/src/index.ts` — it is, in the `./errors` export block).
Note: after a unique-violation error inside a Postgres transaction the tx is
aborted; the `SELECT` in the catch will itself fail with "current transaction
is aborted". If it does, move the `SELECT` **before** the `UPDATE` (fetch
`flow_id`/`concurrency_key` first, then attempt the update) — prefer that
shape from the start.

**Verify**: `pnpm typecheck` → exit 0.

### Step 5: New core tests

Create `packages/core/src/tests/run-reopen.test.ts`:

1. **Fold unit test**: facts `[flow.started, step.started(a,1), step.failed(a,1), flow.failed, step.reset(a)]`
   → `phase.tag === "running"`; and `[flow.started, …, flow.canceled(explicit), step.reset(a)]`
   → `phase.tag === "canceled"`. Use `foldRun` from `../state` and `Facts`
   from `../facts` (check constructor shapes in `facts.ts`; the explicit-cancel
   constructor is `Facts.flowCanceled` at `facts.ts:65` — read its parameter
   object before calling it).
2. **Cancel watcher does not abort a reopened run's handler**: a two-step flow
   `a → b` where `b` fails on the first attempt (`retry: { maxAttempts: 1, backoff: "fixed" }`)
   and on the second attempt waits 600 ms honoring `ctx.signal`:

   ```ts
   run: async ({ ctx }) => {
     attempts += 1;
     if (attempts === 1) throw new Error("boom");
     await new Promise<void>((resolve, reject) => {
       const t = setTimeout(resolve, 600);
       ctx.signal.addEventListener("abort", () => { clearTimeout(t); reject(ctx.signal.reason); }, { once: true });
     });
     return { ok: true };
   }
   ```

   Drive it with `makeHarness` + `h.drain()` (dispatch awaits the handler,
   and the watcher runs on real timers concurrently). After the first drain
   assert `status === "failed"`. Then `await h.wf.operator().retry(runId, "b", { actor: "ops" })`,
   `await h.drain()`, and assert `status === "completed"` and
   `stepStatus("b") === "completed"`. Set the test timeout to 5 s.
   Without Step 1 this test fails: the watcher aborts `b` at ~250 ms and the
   run stays `failed`.
3. **Reopen refuses to steal a held concurrency key**: a flow with
   `concurrency: { keyFn: () => "k", mode: "cancel-in-progress" }` (check the
   exact `FlowConfig.concurrency` shape in `packages/core/src/types.ts` and
   an example in `concurrency.test.ts`) whose single step fails
   (`maxAttempts: 1`). Start run1, drain → `failed`. Start run2 with the
   same key (it parks on a signal step or a slow handler so it stays
   `running` — simplest: make the step `b.signal({ timeoutMs: "unbounded" as const, schema })`
   in a second flow variant, or gate failure on a flag so run2 does not fail).
   Then `await expect(h.wf.operator().retry(run1, "s", { actor: "ops" })).rejects.toBeInstanceOf(NagiConcurrencyConflictError)`.
   Also assert run1 has **no** `step.reset` fact (the throw happened before the push).

**Verify**: `pnpm -F @nagi-js/core exec vitest run src/tests/run-reopen.test.ts` → 4 passed
(2 fold + 2 runtime). Stash `state.ts` + `memory.ts` → tests 1 and 2 fail; unstash → pass.

### Step 6: Postgres integration tests

In `packages/postgres/src/integration.test.ts`, inside the existing
end-to-end `describe` that has a `Wf` wired to `postgresStore` (grep for
`operator()` or `replay(` to find the operator/replay section; if none
exists, add a small block modeled on the "b.subflow — end-to-end via PG"
section), add:

1. `operator.retry on a failed run reopens it and describe() reports completed`:
   fail a one-step run (`maxAttempts: 1`), retry it with the failure flag
   cleared, drain/wait for end, then
   `expect((await wf.describe(runId))?.run.status).toBe("completed")` and
   `completedAt` defined; before the retry, after the failure,
   `run.status === "failed"`.
2. `operator.retry rejects with NagiConcurrencyConflictError when another run holds the key`
   (Postgres path through the partial unique index). Mirror Step 5.3.

**Verify**: `pnpm db:up && pnpm test:integration` → all pass (119 expected).
`pnpm db:down`.

### Step 7: Changeset

Create `.changeset/reopen-on-step-reset.md`:

```md
---
"@nagi-js/core": patch
"@nagi-js/postgres": patch
---

`step.reset` (from `wf.replay({ from })` and `operator.retry`) now reopens a
`completed`/`failed` run to `running`. Previously the run stayed terminal:
the re-run steps finished but no `flow.completed` was ever written
(`describe()`/`queryRuns` kept reporting `failed`, `onFlowComplete` never
fired, a waiting parent subflow was never woken), and the cancel watcher
aborted any re-run handler honoring `ctx.signal` after 250 ms because the run
looked terminal. Reopening a run whose concurrency key another active run
holds throws `NagiConcurrencyConflictError`. `canceled` runs are not reopened.
```

**Verify**: `pnpm lint`, `pnpm typecheck`, `pnpm test` → exit 0.

## Test plan

- Extended: `replay-from.test.ts` (from-failed), `operator.test.ts` (retry-terminal).
- New: `run-reopen.test.ts` — fold reopen/no-reopen-on-canceled, watcher
  non-abort, concurrency-key refusal.
- New: two Postgres integration tests (Step 6).
- Must stay green: `concurrency.test.ts`, `describe.test.ts`, `subflow.test.ts`,
  `streaming-replay.test.ts`, `queryRuns.test.ts`, `pruneFacts.test.ts`.
- Verification: `pnpm test` exit 0; `pnpm test:integration` exit 0 (with Docker).

## Done criteria

- [ ] `grep -n 'phase = { tag: "running" }' packages/core/src/state.ts` → a match inside the `step.reset` branch
- [ ] `grep -n "status = 'running', output = NULL" packages/postgres/src/store.ts` → 1 match
- [ ] `grep -n "slotByRun" packages/core/src/memory.ts` → ≥ 3 matches (field, set, get)
- [ ] `pnpm typecheck`, `pnpm lint`, `pnpm test` exit 0; new/extended core tests pass
- [ ] Stash check performed per Steps 2 and 5
- [ ] `pnpm test:integration` passes with the two new tests, or the plan is marked BLOCKED "integration not executed" with Docker unavailable stated
- [ ] `.changeset/reopen-on-step-reset.md` exists
- [ ] `git status` shows only in-scope files modified
- [ ] `plans/README.md` status row updated

## STOP conditions

Stop and report back (do not improvise) if:
- The fold's `step.reset` branch or `nextTransition` no longer match the excerpts.
- After Step 1 an existing test in `concurrency.test.ts`, `describe.test.ts`,
  or `subflow.test.ts` fails — report the assertion; it may encode a
  deliberate "terminal is forever" expectation that needs the maintainer.
- The Step 5.2 watcher test passes **even with `state.ts` stashed** — then
  the test is not exercising the watcher (handler resolved before the first
  250 ms tick, or `ctx.signal` is not the watcher's signal); fix the test's
  timing, and if it still cannot fail, report.
- `NagiConcurrencyConflictError` is not exported from `@nagi-js/core`'s
  `index.ts`, or its constructor shape differs from the excerpt.
- The Postgres unique-violation catch cannot recover inside the aborted
  transaction even after moving the SELECT first — report; do not add a
  savepoint.

## Maintenance notes

- After this lands, a fact log may contain `flow.failed` … `step.reset` …
  `flow.completed`. Consumers reading raw facts (there should be none; the
  runbook forbids hand SQL) must not assume one terminal fact per run.
  `describe()` in both adapters reports the **current** phase.
- A reopened child subflow that later completes will call
  `propagateToParent`; if the parent already recorded the child's failure and
  itself failed, the wake is skipped with an info log ("parent step not
  awaiting child"). Recovering the parent is a separate `operator.retry` on
  the parent's subflow step — worth a runbook sentence in a later docs pass.
- Reviewer should scrutinize the concurrency-key re-registration in
  `InMemoryStore` against the Postgres partial-index behavior: both must
  refuse when another **active** run holds the key and succeed when the
  holder is the run itself or nobody.
- Deferred: Plan 002's redelivery recovery and this reopen together still
  leave the "terminal flow facts overwrite unconditionally" fold gap
  (backlog C-06) — a `flow.canceled` racing `flow.completed` is unrelated to
  reset and is planned separately.
