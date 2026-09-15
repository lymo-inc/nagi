# Plan 002: Re-drive a run on message redelivery so a lost `advance` self-heals

> **Executor instructions**: Follow this plan step by step. Run every
> verification command and confirm the expected result before moving to the
> next step. If anything in the "STOP conditions" section occurs, stop and
> report — do not improvise. When done, update the status row for this plan
> in `plans/README.md` — unless a reviewer dispatched you and told you they
> maintain the index.
>
> **Drift check (run first)**: `git diff --stat ced05c2..HEAD -- packages/core/src/exec/message.ts packages/core/src/worker.ts docs/OPERATIONS.md packages/core/src/tests/`
> If any in-scope file changed since this plan was written, compare the
> "Current state" excerpts against the live code before proceeding; on a
> mismatch, treat it as a STOP condition.

## Status

- **Priority**: P1
- **Effort**: S
- **Risk**: LOW
- **Depends on**: plans/001-ci-and-release-gating.md (for a trustworthy green run; not a code dependency)
- **Category**: bug
- **Planned at**: commit `ced05c2`, 2026-09-04
- **Issue**: https://github.com/lymo-inc/nagi/issues/44

## Why this matters

nagi is a durable workflow engine driven by an at-least-once queue. A step's
terminal fact (`step.completed` / `step.failed`) is committed by the store,
and *then* the dispatcher calls `advance(runId)` to enqueue whatever becomes
runnable next. Today the queue message is **acked before `advance` runs**, and
when a message is redelivered for a step that is already terminal the
dispatcher acks it **without advancing**. So a worker crash, a SIGKILL, or a
single transient store error inside `advance` — after the step's fact commits
but before the next steps are enqueued — leaves the run `running` with a
completed step, no queue message, and no lease for the reaper to find. Nothing
ever re-drives it. The operations runbook already documents this exact shape
("running, no lease, no entries — advance was lost") and prescribes a manual
`operator().retry`. This plan makes redelivery re-drive the run automatically
and moves the ack after the advance so a failed advance leaves the message
redeliverable.

## Current state

- `packages/core/src/exec/message.ts` — one-message handling. `dispatchMessage`
  (lines 80–136) and `admit` (lines 138–175):

  ```ts
  // message.ts:100-136
  const admission = await admit({ flow, message });
  if (admission.tag === "skip") {
    await queue.ack(message.receipt);
    return;
  }
  const { def, state } = admission;

  await recordStarted({ flow, message, def, state });

  const startedAt = Date.now();
  // Hold the message lease for the whole handler run so a slow step (e.g. a
  // multi-minute LLM call) isn't redelivered and re-executed concurrently.
  // The store lease is extended in lock-step so the reaper sees a live lease.
  const heartbeat = startHeartbeat({ ... });
  let outcome: Dispatched;
  try {
    outcome = await execute({ flow, message, def, state });
  } catch (err) {
    outcome = await handleStepError({ flow, message, def, err });
  } finally {
    heartbeat.stop();
  }

  await queue.ack(message.receipt);          // <-- ack BEFORE advance
  await interpret({ flow, message, def, startedAt, outcome });   // <-- calls advance
  ```

  ```ts
  // message.ts:42-49
  type Admission =
    | { readonly tag: "skip" }
    | { readonly tag: "run"; readonly def: StepDef; readonly state: RunState };

  // message.ts:155-168 (inside admit)
  const preState = await store.loadRunState(runId);
  if (preState.phase.tag === "canceled") return { tag: "skip" };

  const preStep = stepStateOf(preState, stepId);
  if (
    preStep.tag === "completed" ||
    preStep.tag === "failed" ||
    preStep.tag === "skipped"
  ) {
    return { tag: "skip" };
  }

  const claim = await store.claimStep(runId, stepId, attempt);
  if (claim === null) return { tag: "skip" };
  ```

  `advance` is available in this module as `const { advance } = progression;`
  (line 78) and is called from `interpret` (lines 312, 330). `isTerminalRun`
  is already imported from `../state` (line 19).
- `packages/core/src/exec/progression.ts:94-134` — `advance(runId)` is a
  pure re-computation loop: it loads the run state, calls `nextTransition`, and
  enqueues runnable steps / finalizes. Calling it twice on the same state is
  safe: a duplicate enqueue is absorbed because `admit` → `claimStep` returns
  `null` for a step whose lease is live, and a terminal run returns `settled`
  immediately.
- `packages/core/src/worker.ts:251-284` — `dispatchSafely` catches anything
  thrown by `dispatchMessage`, logs `worker.dispatch threw uncaught`, and
  nacks the receipt. With the ack currently before `interpret`, that nack is a
  no-op on an already-acked receipt (`InMemoryQueue.nack` at
  `packages/core/src/memory.ts:939-941` returns silently for an unknown receipt).
- `packages/core/src/facts.ts` — the only fact constructor. Signature used in
  this plan (as called at `exec/progression.ts:173`):
  `Facts.stepCompleted(runId, stepId, attempt, output, at)`.
- `docs/OPERATIONS.md:21` — runbook row:

  ```
  | running, no lease, no entries     | —                              | Advance was lost — `operator().retry(runId, stepId)` re-drives   |
  ```

- Test conventions: tests live in `packages/core/src/tests/*.test.ts`, use
  vitest, and build a runtime with `makeHarness` from `./test-helpers`:

  ```ts
  import { describe, expect, it } from "vitest";
  import { flow } from "../builder";
  import { makeHarness, passthroughSchema } from "./test-helpers";

  const h = await makeHarness(f);          // { wf, store, queue, clock, deps, drain, drainOnce, result, ... }
  const runId = await h.wf.start(f, {});
  await h.drain();                          // dequeues + dispatches until the queue is empty
  const result = await h.result(runId);     // { status, stepStatus(name), factCount(kind), ... }
  ```

  `h.queue` is an `InMemoryQueue` with `dequeue({count})`, `ack`, `nack(receipt, {delayMs?})`.
  `h.store` is an `InMemoryStore` with `settleStep(runId, stepId, fact)` and `loadRunState`.
  Model new tests on `packages/core/src/tests/dequeue-resilience.test.ts`
  (queue-wrapper pattern) and `packages/core/src/tests/operator.test.ts`.
- Changesets: every behavior change ships a `.changeset/<slug>.md` with
  `"@nagi-js/core": patch` (the repo is in 0.1.x pre-release; always `patch`).
  Example: `.changeset/worker-dequeue-backoff.md`.

## Commands you will need

| Purpose   | Command                  | Expected on success |
|-----------|--------------------------|---------------------|
| Typecheck | `pnpm typecheck`         | exit 0 |
| Core tests| `pnpm -F @nagi-js/core test` | exit 0, `Tests N passed` with N ≥ 956 + new tests |
| One file  | `pnpm -F @nagi-js/core exec vitest run src/tests/durable-advance.test.ts` | all pass |
| Lint      | `pnpm lint`              | exit 0 |

## Scope

**In scope** (the only files you should modify):
- `packages/core/src/exec/message.ts`
- `packages/core/src/tests/durable-advance.test.ts` (create)
- `docs/OPERATIONS.md` (the one runbook row)
- `.changeset/durable-advance.md` (create)

**Out of scope** (do NOT touch, even though they look related):
- `packages/core/src/runtime.ts` — the plain `wf.start` path has a sibling
  gap (run row commits, then `advance` enqueues the first steps outside that
  transaction). That is tracked as backlog item C-01b in `plans/README.md`;
  it needs a Store-port change and is not part of this plan.
- `packages/core/src/worker.ts` — no change needed; `dispatchSafely`'s nack
  becomes meaningful once the ack moves, with no edits.
- `packages/core/src/exec/progression.ts`, `scheduler.ts` — `advance` is
  already idempotent; do not "optimize" it.
- `Store`/`Queue` port signatures in `packages/core/src/types.ts`.

## Git workflow

- Branch: `advisor/002-durable-advance`
- Commit subject: `fix(core): re-drive a run on redelivery so a lost advance self-heals`
- Do NOT push or open a PR unless the operator instructed it.

## Steps

### Step 1: Add a `recover` admission for an already-settled step

In `packages/core/src/exec/message.ts`, extend the `Admission` union:

```ts
type Admission =
  | { readonly tag: "skip" }
  // Step already settled but the run is still live: the advance that should
  // have followed the settle may have been lost (crash between settle and
  // enqueue). Re-drive instead of dropping the redelivery.
  | { readonly tag: "recover" }
  | { readonly tag: "run"; readonly def: StepDef; readonly state: RunState };
```

In `admit`, replace the terminal-step early return so that a terminal step on
a **non-terminal** run yields `recover`, while a terminal step on a terminal
run (completed/failed/canceled) still yields `skip`:

```ts
const preState = await store.loadRunState(runId);
if (preState.phase.tag === "canceled") return { tag: "skip" };

const preStep = stepStateOf(preState, stepId);
if (
  preStep.tag === "completed" ||
  preStep.tag === "failed" ||
  preStep.tag === "skipped"
) {
  // Settled step on a live run: re-drive. On a settled run there is nothing
  // to drive — but do NOT skip non-terminal steps of a completed/failed run:
  // operator.retry / replay reset steps on such runs and re-dispatch them.
  return isTerminalRun(preState) ? { tag: "skip" } : { tag: "recover" };
}
```

Keep the `canceled` check exactly as it is today. Do not add a broader
`isTerminalRun(preState)` early return before the step check: `operator.retry`
and `wf.replay({ from })` legitimately re-dispatch steps of a run whose phase
is still `failed`/`completed` (see `packages/core/src/tests/operator.test.ts`
"retry-terminal" and `replay-from.test.ts` "from-failed"), and a broad skip
would silently break them.

**Verify**: `pnpm typecheck` → errors only in `message.ts` about the unhandled
`recover` tag in `dispatchMessage` (fixed in Step 2), nothing else.

### Step 2: Handle `recover` and move the ack after `interpret`

In `dispatchMessage`:

```ts
const admission = await admit({ flow, message });
if (admission.tag === "skip") {
  await queue.ack(message.receipt);
  return;
}
if (admission.tag === "recover") {
  await advance(message.runId);
  await queue.ack(message.receipt);
  return;
}
const { def, state } = admission;
```

Then move the existing ack so it is the **last** statement of the function:

```ts
  } finally {
    heartbeat.stop();
  }

  await interpret({ flow, message, def, startedAt, outcome });
  // Ack last: if interpret's advance throws, dispatchSafely nacks and the
  // redelivery takes the recover path above.
  await queue.ack(message.receipt);
}
```

Keep the existing comment style (short, "why" only).

**Verify**: `pnpm typecheck` → exit 0. `pnpm -F @nagi-js/core test` → all
existing tests pass (expect 956). If any existing test fails, see STOP conditions.

### Step 3: Write the regression tests

Create `packages/core/src/tests/durable-advance.test.ts` with three tests.
Use this flow in all of them:

```ts
import { describe, expect, it } from "vitest";
import { flow } from "../builder";
import { Facts } from "../facts";
import type { AttemptNumber, QueueMessage } from "../types";
import { emptySchema, makeHarness } from "./test-helpers";

function twoStep(id: string) {
  return flow({
    id,
    input: emptySchema(),
    build: (b) => {
      const a = b.task({ run: async () => ({ v: 1 }) });
      const bStep = b.task({ needs: { a }, run: async () => ({ v: 2 }) });
      return { a, b: bStep };
    },
  });
}
```

1. **"redelivery of a settled step re-drives the run"** — start the run, take
   the message for `a` off the queue yourself (`const [msg] = await h.queue.dequeue({ count: 1 })`),
   and simulate "settled but never advanced" by writing the terminal fact
   directly:

   ```ts
   await h.store.settleStep(
     runId,
     "a",
     Facts.stepCompleted(runId, "a", 1 as AttemptNumber, { v: 1 }, new Date()),
   );
   ```

   Assert the run is still `running` and `b` is `pending`. Then redeliver
   (`await h.queue.nack(msg!.receipt)`) and `await h.drain()`. Assert
   `result.status === "completed"`, `result.stepStatus("b") === "completed"`,
   `result.factCount("step.completed") === 2`, and `result.factCount("flow.completed") === 1`.
   (Before Step 1–2 this test fails: status stays `running`.)

2. **"redelivery on a terminal run is acked without advancing"** — start,
   dequeue the `a` message, `await h.wf.cancel(runId)`, nack, drain. Assert
   status is `canceled`, `a` is `pending` (never started), and
   `(await h.queue.inspect(runId)).length === 0`.

3. **"ack happens after advance: a throwing advance leaves the message redeliverable"** —
   wrap the store so that `loadRunState` rejects **once** the first time it is
   called after `a`'s `step.completed` fact exists, then behaves normally.
   Build the harness normally, then replace the dispatcher's store for this
   test by constructing `makeDispatcher({ ...h.deps, store: flakyStore })`
   (import `makeDispatcher` from `../dispatch`) and driving it manually:

   ```ts
   const [msg] = await h.queue.dequeue({ count: 1 });
   await expect(dispatcher.dispatchMessage(msg!)).rejects.toThrow("flaky");
   // The message was NOT acked (ack is now after interpret): it is still leased.
   expect((await h.queue.inspect(runId)).length).toBe(1);
   await h.queue.nack(msg!.receipt);
   await h.drain();                     // recover path completes the run
   expect((await h.result(runId)).status).toBe("completed");
   ```

   The flaky store can be `Object.create`/spread of `h.store` with an
   overridden `loadRunState` — keep it small:

   ```ts
   let armed = false; let fired = false;
   const flakyStore = new Proxy(h.store, {
     get(target, prop, recv) {
       if (prop === "loadRunState") {
         return async (id: RunId) => {
           const s = await target.loadRunState(id);
           if (!fired && s.steps["a"]?.tag === "completed") { fired = true; throw new Error("flaky"); }
           return s;
         };
       }
       const v = Reflect.get(target, prop, recv);
       return typeof v === "function" ? v.bind(target) : v;
     },
   });
   ```

   Note that `h.drain()` uses the harness's own (non-flaky) dispatcher, which
   is what you want for the recovery half.

**Verify**: `pnpm -F @nagi-js/core exec vitest run src/tests/durable-advance.test.ts` → 3 passed.
Then `git stash` your `message.ts` change, re-run: test 1 and test 3 must
**fail**; `git stash pop`, re-run: 3 passed.

### Step 4: Update the runbook row and add the changeset

`docs/OPERATIONS.md:21` — change the diagnosis cell to:

```
| running, no lease, no entries     | —                              | Advance was lost — self-heals on next redelivery; `operator().retry(runId, stepId, { actor })` re-drives immediately |
```

(Also fixes the missing required `{ actor }` argument in that recipe —
`Operator.retry` at `packages/core/src/types.ts:1104` requires `OperatorAuditOpts`.)

Create `.changeset/durable-advance.md`:

```md
---
"@nagi-js/core": patch
---

A run whose step settled but whose follow-up `advance` was lost (worker crash
or store error between the terminal fact and the next enqueue) now self-heals:
redelivery of the step's message re-drives the run instead of being dropped,
and the message is acked only after the advance succeeds.
```

**Verify**: `pnpm lint` → exit 0. `pnpm test` → all green.

## Test plan

- New: `packages/core/src/tests/durable-advance.test.ts` — the three cases in
  Step 3 (recover on redelivery, no-op on terminal run, ack-after-advance).
- Existing suites that exercise this path and must stay green:
  `dispatch.test.ts`, `subflow.test.ts`, `flow-snapshot-gone.test.ts`,
  `heartbeat.test.ts`, `lease-reaper.test.ts`, `concurrency.test.ts`.
- Verification: `pnpm test` → exit 0, core count = 956 + 3.

## Done criteria

- [ ] `pnpm typecheck` exits 0
- [ ] `pnpm test` exits 0; `durable-advance.test.ts` has 3 passing tests
- [ ] In `exec/message.ts`, `queue.ack(message.receipt)` is the last statement of `dispatchMessage`'s run path (`grep -n "queue.ack" packages/core/src/exec/message.ts` shows the run-path ack after the `interpret(` call)
- [ ] `grep -n '"recover"' packages/core/src/exec/message.ts` returns the union member and the handler
- [ ] `docs/OPERATIONS.md` row updated; `.changeset/durable-advance.md` exists
- [ ] `git status` shows only in-scope files modified
- [ ] `plans/README.md` status row updated

## STOP conditions

Stop and report back (do not improvise) if:
- `admit`/`dispatchMessage` in `exec/message.ts` no longer match the excerpts
  above (e.g. someone already reordered the ack).
- After Step 2, any existing test fails. In particular
  `flow-snapshot-gone.test.ts` or `subflow.test.ts` failing suggests a path
  relies on ack-before-advance; report which test and the assertion.
- Test 3 cannot be made to throw *after* the step fact is committed (the
  Proxy approach doesn't intercept because `InMemoryStore` methods are called
  internally) — report rather than restructuring `InMemoryStore`.
- You find yourself needing to edit `runtime.ts`, `worker.ts`, or a port
  interface.

## Maintenance notes

- The `recover` path depends on `advance` being idempotent. Anyone changing
  `nextTransition` or `advance` so that re-running on an unchanged state has a
  side effect (a duplicate hook, a duplicate fact) must revisit this.
- Reviewer should confirm the redelivered message for a step whose **later**
  attempt is already in flight still takes the `run` branch and is rejected by
  `claimStep` — that is pre-existing behavior and a separate backlog item
  (C-09, attempt-aware claims); this plan must not widen it.
- Deferred: the plain `wf.start` enqueue-after-commit gap (C-01b) and a
  "pending run with no message" sweeper. Both need a Store-port change.
