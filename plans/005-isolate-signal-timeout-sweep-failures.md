# Plan 005: Fault-isolate each run in the signal-timeout sweep

> **Executor instructions**: Follow this plan step by step. Run every
> verification command and confirm the expected result before moving to the
> next step. If anything in the "STOP conditions" section occurs, stop and
> report — do not improvise. When done, update the status row for this plan
> in `plans/README.md` — unless a reviewer dispatched you and told you they
> maintain the index.
>
> **Drift check (run first)**: `git diff --stat ced05c2..HEAD -- packages/core/src/dispatch.ts packages/core/src/worker.ts packages/core/src/tests/signal-timeout.test.ts`
> If any in-scope file changed since this plan was written, compare the
> "Current state" excerpts against the live code before proceeding; on a
> mismatch, treat it as a STOP condition.

## Status

- **Priority**: P1
- **Effort**: S
- **Risk**: LOW
- **Depends on**: none
- **Category**: bug
- **Planned at**: commit `ced05c2`, 2026-09-04
- **Issue**: https://github.com/lymo-inc/nagi/issues/47

## Why this matters

`Dispatcher.sweepTimers` asks the store to fail every signal step whose
deadline has passed (up to 100 per sweep), then loops over the results to fire
`onStepError` and `advance` each run so the step failure propagates to
`flow.failed`. The loop has no per-run error handling: the first run whose
`flowFor` or `advance` throws — a flow snapshot that is gone after a deploy, a
transient store error — aborts the loop, and every remaining run in the batch
is skipped. The store has **already deleted their timer rows** (the port
contract says it deletes "either way"), so those runs are never swept again:
their signal step is `failed` in the store, but the run stays `running`
forever with no message, no lease and no timer. The worker logs a single
`worker: signal-timeout sweep failed` warning and moves on. One bad run
must not strand its batch-mates.

## Current state

- `packages/core/src/dispatch.ts:73-100`:

  ```ts
  async function sweepTimers(now: Date): Promise<number> {
    const timedOut = await deps.store.sweepSignalTimeouts({ now });
    for (const { runId, stepId, attempt, fireAt } of timedOut) {
      // Fire onStepError before finalizing the flow, the same ordering every
      // other failure path uses (handleStepError, markStepSettled) — so a
      // consumer's per-step error handling sees signal timeouts too.
      if (deps.hooks?.onStepError) {
        const flow = await deps.flowFor(runId);
        await hooks.fireHook(
          deps.hooks.onStepError,
          { runId, flowId: flow.id, stepId, attempt, kind: "signal",
            error: serializeError(new NagiSignalTimeoutError({ runId, stepId, fireAt })),
            at: now },
          "onStepError",
        );
      }
      await progression.advance(runId);
    }
    return timedOut.length;
  }
  ```

  `deps.emitLog` (type `EmitLog` from `./internal`) is available on
  `DispatchDeps` (line 42) and is the logging convention:
  `deps.emitLog({ level: "error", msg: "...", attrs: { ... } })`.
  `progression.advance(runId)` calls `deps.flowFor(runId)` first
  (`exec/progression.ts:96`), so a gone snapshot throws from `advance` even
  when no `onStepError` hook is configured.
- `packages/core/src/worker.ts:358-373` — the caller:

  ```ts
  private async maybeSweepTimers(): Promise<void> {
    ...
    try {
      await this.dispatcher.sweepTimers(now);
    } catch (err) {
      this.deps.emitLog({ level: "warn", msg: "worker: signal-timeout sweep failed", attrs: { error: String(err) } });
    }
  }
  ```
- `packages/core/src/types.ts:643-653` — `Store.sweepSignalTimeouts` contract:
  "…it deletes the timer row either way. Returns the steps it failed so the
  caller can advance them to flow.failed. MUST NOT itself advance or fire hooks."
  Both adapters comply (`memory.ts:420-434`, `postgres/src/store.ts:570-610`).
- `packages/core/src/tests/signal-timeout.test.ts` — existing tests build a
  harness and a dispatcher and drive the sweep directly:

  ```ts
  const h = await makeHarness(f);
  const dispatcher = makeDispatcher(h.deps);
  const runId = await h.wf.start(f, {});
  await h.drain();
  const failed = await dispatcher.sweepTimers(FAR_FUTURE());
  expect(failed).toBe(1);
  const result = await h.result(runId);
  expect(result.status).toBe("failed");
  ```

  `gatedFlow({ id, timeoutMs })` at the top of that file builds a signal step
  with a deadline and a downstream task. `spyOnLog()` from `./test-helpers`
  returns `{ onLog, entries }` and `makeHarness(f, { onLog })` wires it.
- Changesets: `.changeset/<slug>.md`, `"@nagi-js/core": patch`.

## Commands you will need

| Purpose   | Command                  | Expected on success |
|-----------|--------------------------|---------------------|
| Typecheck | `pnpm typecheck`         | exit 0 |
| Core tests| `pnpm -F @nagi-js/core test` | exit 0 |
| One file  | `pnpm -F @nagi-js/core exec vitest run src/tests/signal-timeout.test.ts` | all pass |
| Lint      | `pnpm lint`              | exit 0 |

## Scope

**In scope** (the only files you should modify):
- `packages/core/src/dispatch.ts` (`sweepTimers` body only)
- `packages/core/src/tests/signal-timeout.test.ts` (add one test)
- `.changeset/isolate-timeout-sweep.md` (create)

**Out of scope** (do NOT touch, even though they look related):
- `packages/core/src/worker.ts` — its catch stays; it now only sees a
  failure from `sweepSignalTimeouts` itself.
- The `Store.sweepSignalTimeouts` contract or either adapter — deleting the
  timer row before returning is by design; the fix is on the caller side.
- The `Dispatcher.sweepTimers` return type — keep `Promise<number>` meaning
  "runs whose signal step the store failed" (unchanged semantics).

## Git workflow

- Branch: `advisor/005-isolate-timeout-sweep`
- Commit subject: `fix(core): one failing run must not abort the signal-timeout sweep batch`
- Do NOT push or open a PR unless the operator instructed it.

## Steps

### Step 1: Wrap the per-run body

Rewrite the loop in `sweepTimers` so each run's hook + advance is isolated:

```ts
async function sweepTimers(now: Date): Promise<number> {
  const timedOut = await deps.store.sweepSignalTimeouts({ now });
  for (const { runId, stepId, attempt, fireAt } of timedOut) {
    try {
      // Fire onStepError before finalizing the flow, the same ordering every
      // other failure path uses (handleStepError, markStepSettled) — so a
      // consumer's per-step error handling sees signal timeouts too.
      if (deps.hooks?.onStepError) {
        const flow = await deps.flowFor(runId);
        await hooks.fireHook(/* unchanged */);
      }
      await progression.advance(runId);
    } catch (err) {
      // The store already failed the step and dropped its timer; this run
      // will not be re-swept. Log and keep going — the rest of the batch
      // must not be stranded behind one gone snapshot or store blip.
      deps.emitLog({
        level: "error",
        msg: "nagi: signal timeout fired but the run could not be advanced — re-drive with operator.retry",
        attrs: { runId, stepId, attempt, error: String(err) },
      });
    }
  }
  return timedOut.length;
}
```

Keep the existing hook-event object exactly as it is (do not change fields).

**Verify**: `pnpm typecheck` → exit 0; `pnpm -F @nagi-js/core exec vitest run src/tests/signal-timeout.test.ts` → existing tests pass.

### Step 2: Regression test — a gone flow for run A must not strand run B

Append to `describe("b.signal timeout", ...)` in
`packages/core/src/tests/signal-timeout.test.ts`:

```ts
it("advances the remaining runs when one run's advance throws", async () => {
  const f = gatedFlow({ id: "to-isolated", timeoutMs: 1_000 });
  const { onLog, entries } = spyOnLog();
  const h = await makeHarness(f, { onLog });

  const runA = await h.wf.start(f, {});
  const runB = await h.wf.start(f, {});
  await h.drain();

  // Simulate run A's flow snapshot being gone (deploy replaced it): flowFor
  // throws for A only. advance() calls flowFor first, so A's advance throws.
  const dispatcher = makeDispatcher({
    ...h.deps,
    flowFor: async (id) => {
      if (id === runA) throw new Error("snapshot gone (simulated)");
      return h.deps.flowFor(id);
    },
  });

  // Both timers fire; the store fails both steps regardless.
  expect(await dispatcher.sweepTimers(FAR_FUTURE())).toBe(2);

  // B finalized despite A throwing.
  const b = await h.result(runB);
  expect(b.status).toBe("failed");
  expect(b.factCount("flow.failed")).toBe(1);

  // A: step failed by the store, run not advanced, and an error log names it.
  const a = await h.result(runA);
  expect(a.stepStatus("awaitAudio")).toBe("failed");
  expect(a.status).toBe("running");
  expect(
    entries.some(
      (e) => e.level === "error" && e.attrs?.["runId"] === runA &&
        String(e.msg).includes("signal timeout fired"),
    ),
  ).toBe(true);
});
```

Order caveat: `sweepSignalTimeouts` in `InMemoryStore` returns timers in map
insertion order, so A (started first) is processed first — that is exactly the
case that used to strand B. If `LogEntry.attrs` is typed differently from
`Record<string, unknown>`, adjust the accessor (check `packages/core/src/types.ts`
`LogEntry`). `spyOnLog` and `makeDispatcher` are already imported in this file
(`makeDispatcher` at line 3; add `spyOnLog` to the `./test-helpers` import).

**Verify**: file → all pass. Stash `dispatch.ts` → the new test fails on
`b.status` (stays `running`, and the sweep rejects); unstash → pass.

### Step 3: Changeset

Create `.changeset/isolate-timeout-sweep.md`:

```md
---
"@nagi-js/core": patch
---

The signal-timeout sweep now isolates each run: a run whose advance throws
(flow snapshot gone, transient store error) is logged at `error` level and
skipped, and the remaining runs in the batch are still advanced to
`flow.failed`. Previously the first throw aborted the whole batch, and because
the store had already dropped the fired timers, the stranded runs were never
swept again.
```

**Verify**: `pnpm lint` and `pnpm test` → exit 0.

## Test plan

- New test in `signal-timeout.test.ts` (Step 2).
- Existing timeout tests in the same file, plus `signal-multi-name.test.ts`
  and `heartbeat.test.ts`, must stay green.
- Verification: `pnpm test` → exit 0; core = 956 + 1.

## Done criteria

- [ ] `sweepTimers` in `dispatch.ts` contains a per-iteration `try { ... } catch` and returns `timedOut.length`
- [ ] `pnpm typecheck`, `pnpm lint`, `pnpm test` exit 0; the new test passes
- [ ] Stash check performed: the new test fails without the `dispatch.ts` change
- [ ] `.changeset/isolate-timeout-sweep.md` exists
- [ ] `git status` shows only in-scope files modified
- [ ] `plans/README.md` status row updated

## STOP conditions

Stop and report back (do not improvise) if:
- `sweepTimers` no longer matches the excerpt (already wrapped, or signature changed).
- `DispatchDeps.flowFor` cannot be overridden by spreading `h.deps` (e.g. it
  is non-enumerable) — report; do not reach into `InMemoryStore` to simulate.
- The store's `sweepSignalTimeouts` returns fewer than 2 entries in Step 2
  (timer arming changed) — report rather than looping the sweep.

## Maintenance notes

- A run that hits this path is left `running` with a `failed` signal step and
  no timer. The runbook's "Advance was lost" row (`docs/OPERATIONS.md`) and
  Plan 002's redelivery recovery do **not** cover it (there is no message to
  redeliver); `operator().retry(runId, stepId, { actor })` is the recovery.
  A future "non-terminal run with no message/lease/timer" sweeper (backlog
  C-01b in `plans/README.md`) would close this class entirely.
- Reviewer: confirm the hook event object is byte-for-byte unchanged, since
  the OTel adapter consumes it.
