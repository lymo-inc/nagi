# Plan 004: Clear a step's buffered signal on delivery and on `step.reset`

> **Executor instructions**: Follow this plan step by step. Run every
> verification command and confirm the expected result before moving to the
> next step. If anything in the "STOP conditions" section occurs, stop and
> report — do not improvise. When done, update the status row for this plan
> in `plans/README.md` — unless a reviewer dispatched you and told you they
> maintain the index.
>
> **Drift check (run first)**: `git diff --stat ced05c2..HEAD -- packages/core/src/state.ts packages/core/src/signals.ts packages/core/src/tests/`
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
- **Issue**: https://github.com/lymo-inc/nagi/issues/46

## Why this matters

nagi's stated use case is multi-turn LLM workflows where a `b.signal` step
waits for a human or an external system. A signal that arrives before the
step is ready is parked as a `signal.buffered` fact and delivered when the
step starts awaiting. The run-state fold keeps the first buffered payload per
step **forever**: neither delivery (`signal.received`) nor a `step.reset`
(from `operator.retry` or `wf.replay({ from })`) removes it. Consequences:

- After an operator retries or replays a signal step, the re-dispatched step
  completes **instantly with the old payload** — the human never gets to
  answer again. For an LLM conversation this replays a stale turn as if it
  were fresh.
- While that stale entry exists, a genuinely new `wf.signal` for the step is
  reported as `buffered` but **no fact is written** (the decider sees an
  existing buffer and returns `fact: null`), so the new answer is silently dropped.

The fix is a two-line change in the pure fold, plus tests. Both adapters
derive `bufferedSignals` from the fold, so nothing else changes.

## Current state

- `packages/core/src/state.ts` — `foldRun` (lines 342–435) is the sole
  interpreter of the fact log. Relevant branches today:

  ```ts
  // state.ts:378-381
  case "step.reset":
    steps[fact.stepId] = PENDING;
    delete selectedArms[fact.stepId];
    break;
  ```

  ```ts
  // state.ts:403-421
  case "signal.buffered":
    // First buffered signal per step wins, mirroring the happy path where
    // the first delivered signal completes the step and later ones no-op.
    if (!(fact.stepId in bufferedSignals)) {
      bufferedSignals[fact.stepId] = {
        payload: fact.payload,
        ...compact({ signalName: fact.signalName }),
      };
    }
    break;
  case "signal.sent":
  case "signal.received":
  case "once.recorded":
  case "lease.reaped":
    // Audit-only: the reaper re-enqueues at attempt+1; the projection
    // updates when the next attempt writes its step.started/step.failed.
    break;
  ```

  `bufferedSignals` is declared at lines 351-354 as
  `Record<StepId, { readonly payload: Json; readonly signalName?: string }>`.
- `packages/core/src/signals.ts:73-120` — `decideSignal` (pure). The worker's
  consume call (no `incoming`) delivers `runState.bufferedSignals[stepId]` when
  the step is `awaitingSignal` (line 80), writing `signal.received` +
  `step.completed`. For a pre-awaiting step with an incoming signal:

  ```ts
  // signals.ts:111-114
  if (incoming === undefined) return NOOP;
  if (runState.bufferedSignals[stepId] !== undefined) {
    return { kind: "buffer", fact: null, result: { tag: "buffered" } };
  }
  ```

  Every delivery path appends `signal.received` (constructed at
  `signals.ts:86` via `Facts.signalReceived`), so keying the clear on that
  fact covers both the early-buffer consume path and the direct `wf.signal`
  delivery path.
- `packages/core/src/operator.ts:157-172` — `operator.retry` appends
  `step.reset` for the step and its descendants, then `advance`s; the
  re-dispatched signal step goes through `recordStarted` → `awaitingSignal` →
  `store.settleSignal({ runId, stepId, at })` (`exec/message.ts:262-288`).
- Both stores fold on read: `InMemoryStore.settleSignal` (`memory.ts:373-388`)
  and `postgresStore.settleSignal` (`store.ts:360-388`) call `decideSignal`
  on a freshly folded state. No adapter keeps its own buffered-signal map.
- Tests: `packages/core/src/tests/early-signal-buffering.test.ts` (harness
  pattern for buffering) and `packages/core/src/tests/decideSignal-states.test.ts`
  (pure decider tests). `operator.test.ts` shows `wf.operator().retry(runId, stepId, { actor })`.
  Test harness: `makeHarness(flow)` from `./test-helpers` → `h.wf`, `h.store`,
  `h.drain()`, `h.result(runId)`; `h.wf.signal(runId, name, payload)`.
- Changesets: `.changeset/<slug>.md`, `"@nagi-js/core": patch`.

## Commands you will need

| Purpose   | Command                  | Expected on success |
|-----------|--------------------------|---------------------|
| Typecheck | `pnpm typecheck`         | exit 0 |
| Core tests| `pnpm -F @nagi-js/core test` | exit 0 |
| One file  | `pnpm -F @nagi-js/core exec vitest run src/tests/signal-reset.test.ts` | all pass |
| Lint      | `pnpm lint`              | exit 0 |

## Scope

**In scope** (the only files you should modify):
- `packages/core/src/state.ts` (two fold branches)
- `packages/core/src/tests/signal-reset.test.ts` (create)
- `.changeset/clear-buffered-signals.md` (create)

**Out of scope** (do NOT touch, even though they look related):
- `packages/core/src/signals.ts` — `decideSignal` is correct given a correct
  projection; do not add a "consumed" flag there.
- `packages/core/src/memory.ts`, `packages/postgres/src/store.ts` — they
  fold; no adapter change is needed. Do not add a `signal.consumed` fact kind.
- `packages/core/src/types.ts` — no new fact kinds, no `RunState` shape change.

## Git workflow

- Branch: `advisor/004-clear-buffered-signals`
- Commit subject: `fix(core): clear a step's buffered signal on delivery and on step.reset`
- Do NOT push or open a PR unless the operator instructed it.

## Steps

### Step 1: Clear the buffer in the fold

In `packages/core/src/state.ts` `foldRun`:

1. `step.reset` branch — add `delete bufferedSignals[fact.stepId];`:

   ```ts
   case "step.reset":
     steps[fact.stepId] = PENDING;
     delete selectedArms[fact.stepId];
     // A reset step must wait for a fresh signal, never replay the old one.
     delete bufferedSignals[fact.stepId];
     break;
   ```

2. Split `signal.received` out of the audit-only group and clear the buffer:

   ```ts
   case "signal.received":
     // Delivered: the buffer is spent. Leaving it would make the next
     // incoming signal a silent no-op and a later reset auto-complete.
     delete bufferedSignals[fact.stepId];
     break;
   case "signal.sent":
   case "once.recorded":
   case "lease.reaped":
     // Audit-only: ...
     break;
   ```

   Confirm `SignalReceivedFact` carries `stepId` (`grep -n "SignalReceivedFact" -A 8 packages/core/src/types.ts`).

**Verify**: `pnpm typecheck` → exit 0. `pnpm -F @nagi-js/core test` → 956 passed
(no existing test asserts the buffer survives delivery; if one does, STOP).

### Step 2: Fold-level regression tests

Create `packages/core/src/tests/signal-reset.test.ts`. First, a pure fold test
(no runtime), using `foldRun` from `../state` and `Facts` from `../facts`:

```ts
import { describe, expect, it } from "vitest";
import { flow } from "../builder";
import { Facts } from "../facts";
import { foldRun } from "../state";
import type { AttemptNumber, RunId } from "../types";
import { emptySchema, makeHarness, passthroughSchema } from "./test-helpers";

const runId = "run-signal-reset" as RunId;
const at = new Date("2026-01-01T00:00:00Z");
const attempt = 1 as AttemptNumber;

describe("foldRun — buffered signals", () => {
  it("drops the buffer once the signal is received", () => {
    const state = foldRun(runId, [
      Facts.flowStarted({ runId, flowId: "f", input: {}, at }),
      Facts.signalBuffered({ runId, stepId: "s", payload: { n: 1 }, at }),
      Facts.stepStarted(runId, "s", attempt, "signal", at),
      Facts.signalReceived({ runId, stepId: "s", payload: { n: 1 }, at }),
      Facts.stepCompleted(runId, "s", attempt, { n: 1 }, at),
    ]);
    expect(state.bufferedSignals["s"]).toBeUndefined();
  });

  it("drops the buffer on step.reset", () => {
    const state = foldRun(runId, [
      Facts.flowStarted({ runId, flowId: "f", input: {}, at }),
      Facts.signalBuffered({ runId, stepId: "s", payload: { n: 1 }, at }),
      Facts.stepReset({ runId, stepId: "s", at }),
    ]);
    expect(state.bufferedSignals["s"]).toBeUndefined();
    expect(state.steps["s"]?.tag ?? "pending").toBe("pending");
  });
});
```

Check each `Facts.*` constructor's exact parameter shape in
`packages/core/src/facts.ts` before writing (`flowStarted` takes an object with
at least `runId, flowId, input, at`; `stepReset` takes `{ runId, stepId, at }` —
see `replay.ts:90-93`; `signalBuffered`/`signalReceived` take objects — see
`signals.ts:83-90` and `signals.ts:116-120`). Adjust to the real signatures;
do not change `facts.ts`.

**Verify**: `pnpm -F @nagi-js/core exec vitest run src/tests/signal-reset.test.ts` → 2 passed.
Stash `state.ts`, re-run → both fail; unstash → pass.

### Step 3: End-to-end regression test through operator.retry

Append to the same file:

```ts
function gate() {
  return flow({
    id: "signal-reset-gate",
    input: emptySchema(),
    build: (b) => ({
      answer: b.signal({
        timeoutMs: "unbounded" as const,
        schema: passthroughSchema<{ text: string }>(),
      }),
    }),
    output: (s) => s.answer,
  });
}

describe("signal step reset", () => {
  it("re-parks after operator.retry instead of replaying the old payload, and accepts a new signal", async () => {
    const f = gate();
    const h = await makeHarness(f);
    const runId = await h.wf.start(f, {});
    // Early signal: buffered, then delivered when the step starts awaiting.
    await h.wf.signal(runId, "answer", { text: "first" });
    await h.drain();
    expect((await h.result(runId)).output("answer")).toEqual({ text: "first" });

    await h.wf.operator().retry(runId, "answer", { actor: "ops" });
    await h.drain();

    const parked = await h.store.loadRunState(runId);
    expect(parked.steps["answer"]?.tag).toBe("awaitingSignal"); // not completed
    expect(parked.bufferedSignals["answer"]).toBeUndefined();

    await h.wf.signal(runId, "answer", { text: "second" });
    await h.drain();
    expect((await h.result(runId)).output("answer")).toEqual({ text: "second" });
  });
});
```

If `b.signal` in this repo requires `names` when no name is given, or
`wf.signal` requires the step id rather than a signal name, mirror
`early-signal-buffering.test.ts` (which uses `names: [...]` and signals by
name). The assertion that matters is the `awaitingSignal` tag after retry.

Note: the run's **phase** after the retry may remain `completed` — that is a
separate, known gap (Plan 006). Do not assert on `result.status` here.

**Verify**: file → 3 passed. Stash `state.ts` → the third test fails at the
`awaitingSignal` assertion (the step auto-completes with `first`); unstash → passes.

### Step 4: Changeset

Create `.changeset/clear-buffered-signals.md`:

```md
---
"@nagi-js/core": patch
---

A signal step's buffered early signal is now cleared when it is delivered and
when the step is reset (`operator.retry`, `wf.replay({ from })`). Previously
the first buffered payload lived on the projection forever: a reset signal
step completed instantly with the stale payload, and a genuinely new
`wf.signal` for that step was reported as buffered without writing a fact.
```

**Verify**: `pnpm lint` and `pnpm test` → exit 0.

## Test plan

- `packages/core/src/tests/signal-reset.test.ts`: two pure fold tests, one
  runtime test via `operator.retry` (Steps 2–3).
- Existing suites that must stay green: `early-signal-buffering.test.ts`,
  `signal-multi-name.test.ts`, `signal-timeout.test.ts`, `decideSignal-states.test.ts`,
  `replay-from.test.ts`, `operator.test.ts`.
- Verification: `pnpm test` → exit 0, core = 956 + 3.

## Done criteria

- [ ] `grep -n "delete bufferedSignals" packages/core/src/state.ts` → 2 matches (reset + received)
- [ ] `pnpm typecheck` exits 0; `pnpm test` exits 0 with 3 new passing tests
- [ ] Stash check performed: all three new tests fail without the `state.ts` change
- [ ] `.changeset/clear-buffered-signals.md` exists
- [ ] `git status` shows only in-scope files modified
- [ ] `plans/README.md` status row updated

## STOP conditions

Stop and report back (do not improvise) if:
- The `step.reset` / `signal.received` fold branches don't match the excerpts.
- An existing test fails after Step 1 — especially anything in
  `decideSignal-states.test.ts` or `early-signal-buffering.test.ts` asserting
  `bufferedSignals` is still populated after delivery.
- Step 3 cannot reach `awaitingSignal` after `operator.retry` for a reason
  unrelated to buffering (e.g. retry rejects because the run is terminal) —
  report the error; that is Plan 006 territory.
- You are tempted to add a new fact kind or a field to `RunState`.

## Maintenance notes

- Anyone adding a new path that resolves a signal step without appending
  `signal.received` (there is none today) must also clear the buffer.
- Reviewer: check that `signals.ts:112-114` ("buffer exists → fact: null") is
  now reachable only while a signal is genuinely parked and un-delivered.
- Deferred: Plan 006 makes a settled run re-open on `step.reset`, which is
  what lets `result.status` become `completed` again after the retry above.
