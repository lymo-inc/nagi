# Issue #7 — Operator interface (skip / retry / abort)

Research + plan. Citations are `file:line` against current `main` (commit `83daa47`).

- **Tracking issue:** lymo-inc/nagi#7
- **Date:** 2026-05-18 (JST)
- **Scope:** `@nagi-js/core` + `@nagi-js/postgres`. Adapters that materialize fact-derived columns need migration.
- **Status:** Research done; design decisions taken with Jay (2026-05-18). Ready for plan review.

## TL;DR

- **`abort` is ~80 % already built.** `wf.cancel(runId, opts?)` exists (`runtime.ts:115`, impl `runtime.ts:447-503`), appends `flow.canceled`, cascades to subflows. Operator `abort` is a thin re-shape that adds structured audit (`actor`, `note`) + a proper `cause` discriminator instead of the existing `concurrencyKey`-as-reason hack (`runtime.ts:472`).
- **`skip` is small and net-new.** Append a `step.skipped` fact with `reason: "manual"` plus optional `actor` / `note` / `cascade` fields; reuse the existing skipped-step projector path (`memory.ts:479-485`). One new behavior toggle: `opts.cascade: "skip" | "continue"` to control whether downstream `needs:X` cascades skip or runs with `undefined`. Type-system change required for `"continue"` (the docstring at `types.ts:128-133` and `checkUpstream` at `scheduler.ts:90-96` currently lock skips as transitive).
- **`retry` on a terminal step is a thin wrapper** over `wf.replay(runId, { from })` (`runtime.ts:676-778`). No new mechanism.
- **`retry` on a `running` step is the big rock.** `replay({ from })` rejects when `status === "running"` (`runtime.ts:741-746`). The blocker: `ctx.signal` is a never-aborting stub (`dispatch.ts:552`). Issue #8 ("ctx.signal aborts on run cancellation") is the prerequisite — see `docs/research/issue-8-ctx-abort-signal.md`. This PR can either (a) wait for #8, or (b) bundle #8's work. **Recommend bundle** — operator API is the forcing function and the surfaces overlap.
- **No new adapter capability flag.** Per Jay (2026-05-18): operator ops are always callable; audit lives in the fact, AuthN/Z lives at the HTTP/CLI layer.

## Part 1 — Research

### 1.1 Current `Wf` surface

`Wf` exposes (`types.ts:94-...`, impl `runtime.ts:519-779`):

- `start` — `runtime.ts:520`
- `signal` — `runtime.ts:570` (resolves a parked `b.signal()` step)
- `cancel` — `runtime.ts:663` ✱ **already exists**, calls `cancelRunRecursive` (`runtime.ts:447-503`)
- `worker` — `runtime.ts:667`
- `replay` — `runtime.ts:676`
- `queryRuns` — `runtime.ts:780-...`

The issue's wording ("`Wf` today exposes `start, signal, replay, queryRuns, worker`") omits `cancel`. That's a stale claim — `cancel` landed in issue #10's subflow work (see `docs/research/issue-10-subflow-runtime.md:8`).

### 1.2 Existing `wf.cancel()` — what it does, what's missing for operator audit

- Walks the run state, no-ops if terminal (`runtime.ts:452-462`).
- Appends a `FlowCanceledFact` with `canceledByRunId = runId` (self-reference) and `concurrencyKey = reason` — both fields repurposed because the fact shape was originally defined for concurrency-supersede (`runtime.ts:464-473`).
- Recursively cancels subflow children (`runtime.ts:490-493`).
- Fires `flow.onError` / `onFlowError` with a `NagiCanceledError` (`runtime.ts:475-488`).
- Does **not** abort in-flight step handlers — they run to completion. See `concurrency-groups.md:117-130` and issue #8.

Gaps for an operator-grade audit:

- `actor` (who triggered the cancel) — no field.
- `note` / structured `reason` — only the hacked `concurrencyKey` string.
- `cause: "concurrency" | "operator" | "explicit"` discriminator — implicit and inferable only by `canceledByRunId === runId` heuristic (`runtime.ts:470`).

### 1.3 Fact log + projection (current state)

- `FactKind` enum at `types.ts:934-948`.
- `Fact` union at `types.ts:1098-1112`.
- `StepSkippedFact` (`types.ts:1046-1050`) has `reason: "when-false" | "transitive"`. No `"manual"` today.
- Projector `projectRunState` at `memory.ts:429-508`. Both stores reuse it (`postgres/store.ts:24, 223`).
- Postgres mirrors facts into `step_run` / `workflow_run` via `applyFactToMaterialized` (`postgres/store.ts:378-...`). Currently writes `'skipped'` for `step.skipped` (`store.ts:433-439`); `step.reset` deletes the materialized row + clears lease (`store.ts:440-449`).

### 1.4 Step-scoped replay (current state)

- `wf.replay(runId, { from })` (`runtime.ts:676-778`):
  - Loads run state; rejects on `canceled` runs (`runtime.ts:687-692`).
  - Rejects on `running` runs (`runtime.ts:741-746`) — explicit anti-race guard.
  - Validates `from` is in the effective flow (`runtime.ts:747-754`).
  - Walks `descendantsOf(effectiveFlow, from)` (`scheduler.ts:246-280`), appends one `step.reset` fact per descendant (`runtime.ts:756-769`).
  - Calls `advance()` to re-dispatch.

So `operator.retry` on terminal steps is "validate args + call replay({ from })". On `running` steps it needs to cooperate with issue #8 (see §1.5).

### 1.5 AbortSignal stub today, issue #8 wiring

- `StepCtx.signal: AbortSignal` declared at `types.ts:97`. Surfaced at `dispatch.ts:559`.
- `makeStepCtx` creates an `AbortController` (`dispatch.ts:552`) that is **never** aborted. Handlers see a perma-pending signal.
- Issue #8's research doc (`docs/research/issue-8-ctx-abort-signal.md`) proposes a polling-based cancel watcher inside `executeTask` that calls `ac.abort()` when the run reaches a terminal status. Defaults: 250 ms poll, new `step.canceled` fact + step status. Suppresses retry and hooks on cancel.
- For operator `retry-while-running`, the prerequisite chain is:
  1. #8 lands → `ctx.signal` aborts on flow terminal status.
  2. Either lift `replay({ from })`'s running-run guard, or have operator.retry first cancel the in-flight attempt (writing `step.canceled` via #8's path) and then replay.

### 1.6 Wakeup / cross-process delivery

- Postgres `appendFact` calls `pg_notify(channel, runId)` when `notifyChannel` is set (`postgres/store.ts:81`, declared at `store.ts:44`).
- `Trigger` interface at `types.ts:930-932`. `postgresTrigger` and `InMemoryTrigger` exist (`memory.ts:652-668`, `postgres/trigger.ts`).
- `DispatchDeps` does **not** carry `trigger` today (`dispatch.ts:34-55`). Workers don't subscribe to triggers — they only poll the queue.
- Implication for operator.retry: the existing notify path covers cross-process delivery of a "step should abort" fact. Issue #8's polling design intentionally avoids the trigger dependency for portability; the same choice applies here.

### 1.7 Skip cascade semantics today

- `scheduler.ts:90-96`: when an upstream step is `failed` or `skipped`, downstream returns `"transitive-skip"`. Type-level invariant locked at `types.ts:128-133`: "`needs.x` is the unconditional `Output`, not `Output | undefined`."
- Decision (2026-05-18, Jay): add `opts.cascade: "skip" | "continue"` to `operator.skip`. `"skip"` (default) keeps current behavior. `"continue"` requires loosening the type contract for *manual* skips only — see §2.3 for the design.

### 1.8 Concurrency-supersede vs operator-abort

- `FlowCanceledFact` (`types.ts:1011-1017`) was shaped for concurrency-supersede: `canceledByRunId: RunId; concurrencyKey: string`.
- `wf.cancel()` hacks `canceledByRunId = runId` and stuffs the reason into `concurrencyKey` (`runtime.ts:470-472`).
- Open question (issue): "How does `abort` differ from `cancel-in-progress`? Likely same fact kind; different cause field." → **Yes, same kind; add a `cause` discriminator.** See §2.4.

## Part 2 — Plan

### 2.1 Public API

```ts
// packages/core/src/runtime.ts
interface Operator {
  skip(
    runId: RunId,
    stepId: StepId,
    opts: { actor: string; note?: string; cascade?: "skip" | "continue" },
  ): Promise<void>;

  retry(
    runId: RunId,
    stepId: StepId,
    opts: { actor: string; note?: string },
  ): Promise<void>;

  abort(
    runId: RunId,
    opts: { actor: string; note?: string },
  ): Promise<void>;
}

interface Wf {
  // ... existing ...
  operator(): Operator;
}
```

- `actor` is free-form (per Jay 2026-05-18). Adapter logs whatever the caller passed.
- `note` is optional human-readable context.
- `cascade` defaults to `"skip"` (back-compat with the locked transitive semantic).
- `operator()` is a factory rather than a property to leave room for a future `operator(scope)` overload (e.g., per-flow restrictions) without breaking the type.

### 2.2 Fact-log changes (decision: extend, don't add kinds)

Per Jay (2026-05-18): extend existing facts with `manual` reason + optional `actor` / `note`. No new `FactKind` entries.

`packages/core/src/types.ts`:

- `StepSkippedFact` (`types.ts:1046-1050`):
  ```ts
  readonly kind: "step.skipped";
  readonly stepId: StepId;
  readonly reason: "when-false" | "transitive" | "manual";
  readonly actor?: string;        // present iff reason === "manual"
  readonly note?: string;
  readonly cascade?: "skip" | "continue"; // present iff reason === "manual"
  ```
  Optionality keeps existing in-DB facts valid (no migration backfill needed). Could split into a discriminated union on `reason` for tighter typing — flag for §3.
- `FlowCanceledFact` (`types.ts:1011-1017`):
  ```ts
  readonly kind: "flow.canceled";
  readonly cause: "concurrency" | "operator" | "explicit"; // new, required going forward
  readonly canceledByRunId: RunId; // unchanged
  readonly concurrencyKey: string; // unchanged
  readonly actor?: string;         // present when cause !== "concurrency"
  readonly note?: string;          // optional
  ```
  Adding `cause` as required is a wire-compat consideration. Two options:
  - **A. Required on new writes, defaulted on read.** Projector treats missing `cause` as `"concurrency"` (preserves old runs' semantics). Recommend.
  - **B. Optional everywhere.** Looser but downgrades the discriminator's value.
  Recommend A.
- No changes to `FactKind` enum.

### 2.3 Skip-cascade type-system change for `cascade: "continue"`

The locked invariant at `types.ts:128-133` says `needs.x` is `Output`, not `Output | undefined`. For operator-skipped steps with `cascade: "continue"`, downstream sees `undefined` at runtime.

Three options for handling the type drift:

- **A. Document the runtime hole, don't change types.** `cascade: "continue"` is documented as "your handler may see `undefined` for any need that traces to a manually-skipped step." Tradeoff: the type system can't catch the regression at compile time. Cheapest.
- **B. Widen `NeedsOutputs<N>` to `Output | undefined` globally.** Breaks all handlers; massive churn. Reject.
- **C. Keep types locked, runtime asserts.** Treat `cascade: "continue"` as a runtime-only behavior that violates the type contract; document loudly + add a runtime warning when `undefined` flows into a `needs` resolution. Slightly safer than A.

Recommend A. The operator API is an oncall escape hatch; the type contract is for normal flow authoring. Make the docstring on `Operator.skip` carry the warning verbatim.

`packages/core/src/scheduler.ts`:

- `checkUpstream` (`scheduler.ts:78-100`) needs to know whether a skipped upstream was `cascade: "continue"` so it doesn't return `"transitive-skip"`. Read the fact from `runState.facts` (last `step.skipped` for that stepId; `cascade === "continue"` ⇒ treat as `completed` for the upstream check; output is undefined / null).
- `resolveNeeds` (`internal.ts`) currently reads `runState.steps[id]?.output ?? null`. For `cascade: "continue"` steps, `runState.steps[id]?.status === "skipped"` and `output` is absent → already resolves to `null`. The handler sees `null` not `undefined`. **Implementation note:** the docstring should say "manually-skipped needs resolve to `null` at runtime" so it matches what `resolveNeeds` actually does. **Verify** what `resolveNeeds` returns by reading `internal.ts` before final wording.

### 2.4 Abort = re-shape over `wf.cancel`

`operator.abort(runId, { actor, note })` implementation:

- Largely delegates to `cancelRunRecursive` (`runtime.ts:447`).
- Passes a structured `cause: "operator"`, `actor`, `note` to the `FlowCanceledFact` constructor.
- Removes the existing `concurrencyKey = reason` hack — that field becomes optional / "" for non-concurrency cancels.

**Compatibility risk:** any code (read-side, dashboards) that reads `FlowCanceledFact.concurrencyKey` as a reason string breaks. Confirm none exists in `packages/` (only producer is `runtime.ts:472`; consumers — need to grep for `concurrencyKey` reads outside the constructor sites).

Sub-decision: should `wf.cancel(runId, { reason })` (existing API) be deprecated in favor of `wf.operator().abort()`? Two API surfaces for the same operation is a complexity tax (per `feedback_complexity_must_pay_for_itself`). **Recommend:** keep `wf.cancel` for back-compat, redirect it internally to the same `cancelRunRecursive` with `cause: "explicit", actor: "wf.cancel", note: reason`. Operator `abort` is the structured-audit surface.

### 2.5 Retry — terminal vs running

`operator.retry(runId, stepId, { actor, note })`:

- **Terminal case** (status `completed` / `failed`): delegate to `wf.replay(runId, { mode: "continue", from: stepId })`. Audit is captured via a new `actor`-bearing variant of the existing `step.reset` fact? Or via a sidecar `step.retry-requested` audit fact?
  - **A. Extend `StepResetFact`** (`types.ts:1091-1096`) with optional `actor` / `note` (analogous to skip/abort).
  - **B. Add a separate audit fact.**
  Recommend A. Same precedent as skipped.

- **Running case**: requires #8's `ctx.signal` plumbing.
  - Append `step.abort-requested { runId, stepId, attempt, actor, note }` (new fact OR an audit-flavored extension of the existing facts — see §3).
  - The cancel watcher from #8 picks it up via the post-abort terminal-status check… but wait: #8 only aborts on *flow*-terminal status, not on per-step abort. The watcher needs widening to also abort when a `step.abort-requested` fact appears for the in-flight attempt. **This is the #8/operator interlock.**
  - Once the in-flight attempt aborts (lands `step.canceled` per #8), call `replay({ from: stepId })`. Subsequent reset cascade and re-dispatch proceeds normally.

**Build sequence:**
1. Land #8 (or its core: ctx.signal abort on flow terminal + `step.canceled` fact + projector).
2. Extend the cancel watcher to also fire on `step.abort-requested` matching `(runId, stepId, attempt)`.
3. Implement `operator.retry`-running using that hook.

Per Jay (2026-05-18): bundle the AbortSignal cross-process plumbing into this PR. Bundling = doing both #8's work and this issue's work in one PR. Confirm before splitting.

### 2.6 Adapter (Postgres) changes

**No migration required for this PR.** Verified by reading `packages/postgres/src/migrations.ts`:

- `fact.kind` column is `text NOT NULL` with **no** CHECK constraint (`migrations.ts:52`) — new kinds and new payload fields ride in the existing `payload jsonb` (`migrations.ts:54`) with zero DDL.
- `workflow_run.status` already accepts `'canceled'` (`migrations.ts:137`, migration `0003_concurrency_groups`).
- `step_run.status` already accepts `'canceled'` (`migrations.ts:188`, migration `0006_step_canceled_status`) — pre-shipped for issue #8's eventual `step.canceled` fact even though the producer code doesn't exist yet. If we bundle #8 we land the producer without a fresh migration.
- All other CHECK constraints (`workflow_run.status`, the partial unique index, etc.) are unaffected.

`packages/postgres/src/store.ts`:

- `applyFactToMaterialized` switch (`store.ts:378-...`):
  - `step.skipped`: already handled (`store.ts:433-439`). Storage of `reason`/`actor`/`note`/`cascade` is JSONB-payload-only — no schema or materializer change.
  - `flow.canceled`: already handled (`store.ts:411-424`). Adding `cause`/`actor`/`note` is JSONB-payload-only; the `workflow_run.canceled_by_run_id` column already covers the structured link.
- `serializeFactPayload` / `reviveFact` round-trip: need to confirm the implementation is a generic `JSON.stringify(fact)` rather than a hand-rolled per-kind serializer. Tracked as a verification step in §3, not a blocker.

`packages/core/src/memory.ts`:

- `projectRunState` (`memory.ts:429-508`): currently `step.skipped` projects to `status: "skipped"` (`memory.ts:479-485`). No change needed — the projector doesn't peek at `reason`.
- For `cascade: "continue"`, the projector still records `status: "skipped"`. Downstream re-runnability is decided in `nextRunnable` / `checkUpstream` (scheduler.ts), not in the projector. **Caveat:** the projector loses the `cascade` hint after projection. Solution: scheduler walks `runState.facts` to find the latest `step.skipped` for that step and read `cascade`. Cheap (facts are already in memory after `loadRunState`).

### 2.7 Files touched (summary)

- `packages/core/src/types.ts` — extend `StepSkippedFact`, `FlowCanceledFact`, `StepResetFact`; add `Operator` interface + `Wf.operator()` declaration.
- `packages/core/src/runtime.ts` — implement `operator()` factory; refactor `cancelRunRecursive` to take a structured `{ cause, actor?, note? }` payload; keep `wf.cancel` as a thin shim.
- `packages/core/src/scheduler.ts` — `checkUpstream` consults facts for `cascade: "continue"` manual skips.
- `packages/core/src/dispatch.ts` — (if bundling #8) implement cancel watcher in `executeTask`; extend it to handle per-step abort requests.
- `packages/core/src/memory.ts` — possibly tweak `projectRunState` for `step.canceled` (if #8 bundled).
- `packages/postgres/src/store.ts` — verify `applyFactToMaterialized` handles `step.canceled` (if #8 bundled); JSONB payload round-trip for new optional fields.
- `packages/postgres/src/migrations.ts` — only if a `step_run.status` CHECK constraint needs widening; need to read this file to confirm.
- Tests:
  - `packages/core/src/operator.test.ts` (new): skip with/without cascade, retry-terminal, abort vs cancel parity, audit-field round-trip.
  - `packages/core/src/concurrency.test.ts`: existing `flow.canceled` tests need the `cause` field added.
  - `packages/postgres/src/integration.test.ts`: mirror at least one operator op end-to-end.
- `.changeset/`: `minor` on `@nagi-js/core` — public API addition. **Wait:** per `feedback-changeset-bump-type`, default to `patch` in 0.1.x. Re-confirm: **`patch`** on core unless this PR ships in a 0.2 jump window.

### 2.8 Documentation

- Update `README.md` — **No.** Jay owns README (per `feedback_readme_ownership`).
- Update package-level docstrings on `Wf.operator()` (the JSDoc on the interface method is the discoverability surface).
- Add a follow-up note in `docs/research/issue-8-ctx-abort-signal.md` referencing this issue if #8 lands separately.

## Part 3 — Open questions for Jay (before coding)

1. **Bundle #8 with this PR, or split?** (Jay said "build AbortSignal cross-process plumbing" — confirming that means a single PR bundling #8's polling-based cancel watcher with operator API.)
2. **`StepSkippedFact` shape: optional fields vs discriminated union on `reason`.** Optional is migration-friendlier; discriminated union is tighter at the type level. Recommend optional for now.
3. **`FlowCanceledFact.cause` — required on new writes, defaulted on read.** Confirms §2.2's recommendation.
4. **Keep `wf.cancel(runId, { reason })` as a shim** that internally records `cause: "explicit", actor: "wf.cancel", note: reason`? Or drop it in a major-version cleanup? Recommend keep — back-compat is cheap here.
5. **`cascade: "continue"` runtime behavior — handler sees `null`** for the skipped need. **Verified** against `internal.ts:206-222` (`resolveNeeds` returns `loadOutput(upstream.id)` per local key) and `dispatch.ts:229` / `scheduler.ts:61` (both pass `(id) => runState.steps[id]?.output ?? null`). A skipped step has no `output`, so the resolver returns `null`. Decision: document "manually-skipped needs resolve to `null` at runtime" on `Operator.skip`'s docstring.
6. **`operator.retry` audit on terminal steps — extend `StepResetFact` with `actor`/`note`** (vs a separate audit fact). Recommend extend.
7. **~~Migrations check~~ — CLOSED. No migration needed.** `fact.kind` has no CHECK constraint (`migrations.ts:52`); `workflow_run.status` already accepts `'canceled'` (`migrations.ts:137`); `step_run.status` already accepts `'canceled'` (`migrations.ts:188`, pre-shipped for issue #8). Field additions are JSONB-payload-only.
8. **Subflow cascade for `abort`** — `cancelRunRecursive` already walks subflow children with reason prefix `"parent X canceled: …"` (`runtime.ts:490-493`). For `operator.abort`, do child runs inherit `cause: "operator"` and the parent's `actor`, or do they get `cause: "explicit", note: "parent X aborted by <actor>"`? Recommend the latter — keeps `cause` semantic ("who triggered this run's cancel"), parent's actor surfaces in the note.

## Part 4 — Uncertainties (not asserted)

- ~~`resolveNeeds` exact return on a `skipped` step.~~ **Closed.** `internal.ts:206-222`: `resolveNeeds` builds `{ localKey: loadOutput(upstream.id) }`. Callers (`dispatch.ts:229`, `scheduler.ts:61`) pass `(id) => runState.steps[id]?.output ?? null`. Skipped steps have no `output`, so handlers see `needs.x === null`. The cascade-"continue" docstring (§2.3) is correct.
- ~~Postgres `step_run.status` CHECK constraint.~~ **Closed.** Already allows `'canceled'` (`migrations.ts:188`). `workflow_run.status` likewise (`migrations.ts:137`). `fact.kind` has no CHECK at all (`migrations.ts:52`). **No migration in this PR.**
- **`StepResetFact` is appended one-per-cascaded-step.** Extending it with `actor`/`note` means N facts each carry the same audit. Tolerable; alternative is a single sidecar audit fact + N plain resets. Recommend the simpler N-with-audit, but it's a judgment call.
- **`Wf.operator()` factory vs property.** No strong reason for factory over property today; flagged in §2.1 as future-proofing.
- **`serializeFactPayload` / `reviveFact` round-trip.** Not yet read. Assumed to be `JSON.stringify(fact)` (the `payload jsonb` column suggests it). If hand-rolled per-kind, optional fields like `actor`/`note`/`cascade` need explicit handling. Verify before coding.

---

## Appendix — Issue-recap claims vs codebase reality

| Issue claim | Reality | File:line |
|---|---|---|
| "`Wf` exposes start, signal, replay, queryRuns, worker" | Also exposes `cancel` (added in issue #10) | `runtime.ts:115` |
| "`wf.replay(runId, { from: stepId })` works for terminal runs" | True; rejects `running` and `canceled` runs | `runtime.ts:687-692, 741-746` |
| "for stuck-running steps we need an explicit cancel-then-retry" | True; depends on issue #8 | `dispatch.ts:552` (stub AC) |
| "operator has to direct-edit `nagi.fact` / `nagi.step_run`" | Accurate — no operator API today | n/a |
| "AbortSignal mechanism (see related RFC)" | Refers to issue #8 (`docs/research/issue-8-ctx-abort-signal.md`) | `concurrency-groups.md:117-130` |
| Proposed `step.skipped.manual` fact-kind name | Decided 2026-05-18 to extend `step.skipped.reason` instead | §2.2 |
| Proposed `run.aborted` fact-kind name | Decided 2026-05-18 to extend `flow.canceled` with `cause` instead | §2.2, §2.4 |
| "Adapter capability gate: `postgresStore({ allowOperatorOps: false })`" | Rejected by Jay 2026-05-18 — gate at HTTP/CLI, not core | n/a |
| "Cascade-skip dependents, or pass `undefined`?" | Decided 2026-05-18: `opts.cascade: "skip" \| "continue"` (default `"skip"`) | §2.3 |
| "Audit `actor` shape: free-form?" | Yes, per Jay 2026-05-18 | §2.1 |
