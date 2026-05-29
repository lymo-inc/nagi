# RFC 0014: consumer-driven stability fixes (N1–N12)

- **Status:** APPROVED (Jay, 2026-05-28 JST). Decisions log §6 COMPLETE (11/11). Implementation IN PROGRESS.
- **Date:** 2026-05-28 (JST)
- **Author:** Claude (Jay-supervised synthesis from 3-agent parallel pipeline)
- **Decision owner:** Jay
- **Source:** the consumer-side RFC at `lymo/docs/rfcs/2026-05-23-nagi-usage-rc12-refactor.md` §12 catalogs 12 upstream nagi candidates (N1–N12) discovered while the consumer refactored to rc.12. Three are filed as nagi issues (#21=N5, #22=N1, #23=N3+N4); N7–N12 were discovered during the LYMO-119 stuck-run incident (2026-05-27) and the LYMO-12P canceler-bug investigation (2026-05-28); none of N7–N12 are filed yet.

> Decisions-log RFC. §1–§5 are synthesis from three parallel exploration subagents
> (researcher / codebase auditor / test-spec drafter). §6 (the decisions log) is the heart
> and is filled by Jay through the grilling loop (§10), one branch at a time.
> `OPEN` = undecided · `PROPOSED` = Claude's recommendation, NOT a decision ·
> `DECIDED` = locked by Jay with a date · `LOCKED (evidence)` = forced by a verified fact.

---

## 1. Summary

The consumer's rc.12 refactor (lymo §12) surfaced 12 upstream candidates against nagi. Two collapse on inspection (N2 by intent, N6 verified OK in rc.12). Of the remaining 10, **the verify-first audit revealed two important framing revisions** that should be locked before any work begins:

1. **N7 ("`wf.signal` throws on `step_run.status='pending'`") is partially misdiagnosed.** The "is not waiting for signal" error string **does not exist in current nagi source** (it only appears in CHANGELOG entries documenting what rc.11 *fixed*). `decideSignal` (`packages/core/src/signals.ts:54-121`) correctly buffers all pre-awaiting states (`pending` / `running` / `backoff` / `aborting`). The consumer's empirical "buffered fact written but never delivered for 43h" is **N8 alone**: the buffer write succeeds, but the buffered signal is only flushed on the *next dispatch* of the step, and a dead worker means dispatch never recurs. **N7 collapses into N8** under verification.

2. **N9 ("cross-deploy `flow_hash` undispatchability") is also partially misdiagnosed.** `flowFor` resolves by `flowId`, **not by `flowHash`** (`flow-registry.ts:45-53`). A run pinned to an old hash but with a current `flowId` still dispatches — against the *new* code's DAG. That's a silent code-version-drift bug (steps may have moved), but it's the opposite of "undispatchable." The "stuck runs after deploy" symptom in the consumer's data is also best explained by N8 alone (lease never reaped + new worker process means in-process state lost).

The non-obvious crux: **N8 (the lease autoreaper) explains the bulk of the stuck-run incidents the consumer reported.** It is the load-bearing fix. Everything else (N1 / N3 / N4 / N10 / N11 / N12) is small/additive; N5 is large but architecturally independent; N9 reduces to a typed error + documented binding.

This RFC scopes 10 fixes across **4 phases**, sequenced low-blast to high-blast. The decisions log (§6) locks: scope/sequencing, the verify-first reframe of N7/N9, the Store-ABI shape for the three ABI-touching fixes (N3, N5, N8), and the cancel/error typing strategy that ties N10/N12 together.

## 2. Motivation

- **Reliability** — N8 (lease reaper) closes the steady-state failure mode behind 55+ stuck runs over 7 days in the consumer's prod (LYMO-119 cluster, 2026-05-27). This is the highest-leverage reliability item nagi has open today.
- **Contract conformance** — N1 (pgmq under `CamelCasePlugin`) and N12 (`NagiAbortError.name`) are silent contract breakages that cause prod incidents in any consumer with non-default plugins or fetch-based LLM SDKs.
- **Ergonomics** — N3 (read API) and N4 (`RunId` constructor) eliminate the 8+-site `as RunId` cast and the direct `nagi.workflow_run`/`step_run` table coupling. Additive, no behavior change.
- **Unrepresentable invalid states** — N10 (canceler column not persisted) and N12 (`NagiAbortError.name` doesn't match WHATWG) are *structural* bugs: the error type already carries the canceler info, but it doesn't round-trip to the canonical fact / DB column. Tying typed errors to typed facts dissolves both.

## 3. Current state (audited 2026-05-28)

**Repo state.** rc.13 published; RFC 0013 (activity steps) is fully shipped (`packages/postgres/CHANGELOG.md` "Implement RFC#13"). Open RFCs under `docs/rfcs/` include 0018–0021 handoffs (numbers ahead of doc numbering — RFC doc numbers ≠ GH issue numbers per memory). RFC 0013's **Open Question 3** (lease-claim heartbeat for activities, `docs/rfcs/0013-activity-steps.md:182-200`) is N8 stated verbatim and is explicitly resolved by this RFC's D4.

**Packages:** `core` (orchestrator + flow API + Store/Queue/Operator contracts), `pgmq` (queue adapter), `postgres` (store adapter + migrations), `otel`.

**Store contract (the load-bearing interface).** Source: `packages/core/src/types.ts:492-591`. Conformed by `InMemoryStore` (`packages/core/src/memory.ts:57-437`) and `PostgresStore` (`packages/postgres/src/store.ts:50-751`). Methods touched by N3/N5/N8:

```
tryStartRun(runId, FlowStartedFact, concurrency?): { started, canceled[] }   // NO tx parameter today — N5 hinge
claimStep(runId, stepId, attempt): Promise<ClaimToken | null>                // never re-claimed; N8 root cause
settleStep(runId, stepId, StepCompletedFact | StepFailedFact)                 // never writes canceled_by_run_id on failure — N10
queryRuns(QueryRunsOpts): QueryRunsResult                                     // exists; N3 adds getRun/getStep
```

**Pure decision functions in core** (per memory `feedback-store-policy-in-core`): `decideSignal` (`signals.ts:54-121`), `stepStateOf`/`foldRun` (`state.ts:114-432`), `resolveExecutionFact` (`step-exec.ts:59-87`), `classifyFailure` (`step-exec.ts:184-206`). N7's "pending buffering" lives entirely in `decideSignal` and **already works** (verified).

**Migrations.** `packages/postgres/src/migrations.ts:1-229`. Head = `0007_prune_completed_at_idx`. Next id = `0008`. Schema:

- `nagi.workflow_run`: includes `canceled_by_run_id text` (since `0003_concurrency_groups`, `:124`) — **no FK**.
- `nagi.lease`: `(run_id, step_id, attempt) PK, token text, expires_at timestamptz`, index on `expires_at`. **No sweeper exists.**
- `nagi.step_run`: status CHECK includes `pending` (since `0006_step_canceled_status`) but **core only ever inserts step_run rows with status='running' on `step.started`** — the `pending` value is reserved, never written.

**Existing test layout.** `vitest`. Integration via `packages/core/src/tests/test-helpers.ts` (in-process `nagi()` with `InMemoryStore`/`InMemoryQueue`/`InMemoryClock`). Postgres adapter integration at `packages/postgres/src/integration.test.ts`. 38 test files under `packages/core/src/tests/`.

**`NagiAbortError`** (`packages/core/src/step-exec.ts:41-54`) is **not exported** (`index.ts:18-23` exports `NagiCanceledError`, `NagiRuntimeError`, `NagiSnapshotDriftError`, `NagiValidationError` — `NagiAbortError` is internal).

## 4. What rc.13 currently provides (relative to N1–N12)

| Capability | Current API symbol | Gap |
|---|---|---|
| Buffered signals (pre-awaiting) | `decideSignal` + `SignalDecision = noop \| buffer \| deliver` | **Already correct.** N7 collapses. |
| Multi-name signal waits | `b.signal({names, schema})` | **Already correct.** N6 verified OK. |
| Lease | `nagi.lease` table + `claimStep` | **No sweeper.** N8. |
| Transactional start | `wf.start(opts: {runId?})` | **No `tx` parameter.** N5. |
| Run/step read | `wf.queryRuns(opts)` (summary list only) | **No `getRun`/`getStep`.** N3. |
| `RunId` brand | `type RunId = string & {brand}` | **No constructor.** N4. |
| Cancel metadata persistence | `step.failed.error.cause.canceledByRunId` | **`workflow_run.canceled_by_run_id` not populated on `step.failed` path.** N10. |
| Abort error WHATWG conformance | `NagiAbortError.name = "NagiAbortError"` | **Falls outside WHATWG abort-allowlist.** N12. |
| Flow snapshot drift detection | `NagiSnapshotDriftError` on replay only | **Not detected at dispatch time** (silent code-version drift). N9. |
| pgmq receipt under plugins | `parseReceipt(row.msg_id)` | **Plugin-rewrites break it.** N1. |

## 5. Verification surface (test-spec synthesis)

Test-spec drafter enumerated **71 tests across N1–N12** (skipping N2/N6). Full list in test-drafter report (delivered to RFC author); landing target after §6 approval. Highlights:

- **N1**: 8 tests. Includes the prod 2026-05-20 regression ("CamelCasePlugin + ack succeeds") via a real Kysely+plugin harness.
- **N3+N4**: 10 tests. Split 6 (`wf.getRun`/`getStep` projection shape, null-on-missing) + 4 (`RunId.parse` / brand round-trip).
- **N5**: 10 tests. Includes tx-rollback ⇒ no orphan row, concurrency-key supersession under caller tx, backwards-compat no-tx path, type-level adapter-compat check.
- **N7**: 8 tests (after re-framing: pin that `decideSignal` buffers all pre-awaiting sub-states, including the previously-untested `step_run.status='pending'` materialized case; add regression cite to LYMO-119).
- **N8**: 10 tests. Detection, re-dispatch with `attempt+1`, live-lease guard, terminal-step skip, restart-safety, batching.
- **N9**: 7 tests. Mostly typed-error pins.
- **N10**: 6 tests. Column-write atomicity + consumer-side COALESCE fallback preservation.
- **N11**: 6 tests. Atomic supersession + invariant ("every `canceled_by_run_id` resolves to an existing row").
- **N12**: 6 tests. Rename + export + `kind` discriminator.

The full list is intentionally not inlined here — it's the implementation target, not a decision input.

## 6. Decisions log (the heart — filled by Jay via grilling)

| ID | Decision | Status | Options | Claude's lean |
|---|---|---|---|---|
| **D1** | Scope & sequencing | `DECIDED A` (Jay, 2026-05-28) | A) ONE RFC, 4 phases (low-blast → high-blast), single PR · B) split into 3 RFCs · C) ONLY N8 + N12 (the prod-incident driven fixes); everything else deferred | **A** — one RFC, 4-phase, single PR. Phase 1=N1+N12 · Phase 2=N3+N4+N10 · Phase 3=N8+N9 · Phase 4=N5. |
| **D2** | N7 scope (verification-first) | `DECIDED B` (Jay, 2026-05-28) | A) **drop independent N7 work** — `decideSignal` already correct; the empirical bug is N8 alone (verified-via-source) · B) keep N7 as an audit-pin (8 tests for the pending materialized case) under N8's umbrella · C) treat as independent fix (assume consumer's diagnosis correct) | **B** — drop N7 as a structural fix, but land its 8 tests as audit pins to ensure no regression while landing N8 |
| **D3** | N9 scope (verification-first) | `DECIDED A` (Jay, 2026-05-28) | A) typed `NagiFlowSnapshotGoneError` at dispatch (loud failure, consumer restarts) · B) full multi-version flow registry (correct but high process cost) · C) document-and-restart-only (status quo, labeled) · D) reconstruct dispatcher from `flow_snapshot.dag` (infeasible — handlers are closures) | **A** — typed error at dispatch is the smallest correct delta; matches the consumer's existing `admin-restart` ops surface; rules out the silent code-drift class |
| **D4** | N8 mechanism (the load-bearing fix) | `DECIDED E` (Jay, 2026-05-28) | A) periodic in-worker sweeper started by `nagi.run`, scanning `lease WHERE expires_at < now()` · B) boot-time reaper only (DBOS pattern) · C) exposed `wf.reapExpiredLeases()`, consumer owns cadence (like `pruneFacts`) · D) heartbeat extends store lease too (alone — no sweep) · E) **A + heartbeat-extends-store-lease (defense in depth)** | **E** — sweep covers crashes; heartbeat extension keeps lease state truthful so the sweep doesn't double-dispatch live work. Resolves RFC 0013 OQ3. |
| **D5** | N5 shape (transactional start) | `DECIDED B` (Jay, 2026-05-28) | A) `StartOpts.tx?: Tx` single param; store joins caller tx if supplied · B) two-phase `wf.startStaged(tx, …) → {started, canceled, applyOnCommit}` so hooks fire post-commit · C) new `wf.enqueueStart(tx, ...)` parallel to `wf.start` · D) keep consumer outbox forever (D2=A in consumer RFC) | **B** — concurrency-cancel hooks fire outside the store tx today (`runtime.ts:233-262`); joining a caller tx requires a post-commit moment. Two-phase is the structurally honest shape. |
| **D6** | N5 advisory-lock policy under shared tx | `DECIDED A` (Jay, 2026-05-28) | A) skip the `nagi:concurrency:${flowId}:${key}` advisory lock when joining caller tx (rely on unique-active partial index `migrations.ts:132-135`); add unique-violation retry handling · B) keep the lock on the caller's tx (safe but possibly deadlocking with caller's own locks) · C) abort if caller tx already holds locks on these names | **A** — the partial unique index is the actual invariant; the advisory lock is belt-and-suspenders. Skipping under shared tx is safe and avoids cross-codebase deadlocks. |
| **D7** | N10 fix path (canceler persistence) | `DECIDED B` (Jay, 2026-05-28) | A) materialized-update extracts canceler from `step.failed.error.cause` (store knows error shape) · B) **classify `NagiCanceledError` as `flow.canceled` (cause: concurrency), not `flow.failed`** — flips the cancel/fail discriminator at `classifyFailure` (`step-exec.ts:184-206`) · C) new `flow.failed.becauseCanceled` fact kind · D) leave it; consumer COALESCE (status quo) | **B** — the run *was* canceled. Classifying it as failed is the actual bug. `NagiCanceledError` is structurally distinguishable (typed class with `canceledByRunId`); extending `classifyFailure` to emit a `flow.canceled (concurrency)` fact makes the row's canonical status match reality. Matches "unrepresentable invalid states." |
| **D8** | N11 approach (phantom superseder) | `DECIDED A` (Jay, 2026-05-28) | A) **investigate before designing** — postgres `tryStartRun` is already atomic; frequency is 1-in-53 (consumer; investigate the actual phantom row first); FK migration deferred to follow-up · B) add FK `canceled_by_run_id REFERENCES workflow_run(run_id) ON DELETE SET NULL` (migration 0009) in this RFC · C) re-order INSERT-new-row-first then UPDATE-prior-cancel (different phantom class — worse) | **A** — N11 deferred from this RFC. Tracking: query lymo prod for orphan `canceled_by_run_id` refs to identify the actual source path. |
| **D9** | N3 read API shape | `DECIDED B` (Jay, 2026-05-28) | A) `wf.getRun(runId)` + `wf.getStep(runId, stepId)` — two methods · B) **single `wf.describe(runId)` returning nested `{run, steps, parent, children}`** — folds in the read-side child-nesting follow-up the consumer deferred (RFC §13) · C) expose `Store.loadRunState` directly (leaks internal projection types) · D) Kysely view DDL only, no method | **B** — the consumer's RFC §13 already deferred read-side child-nesting; folding it into the first read API avoids a v2 cut. RunView nests `parent?` and `children: readonly RunId[]` (nullable only at the API boundary; internals stay total via `foldRun`). |
| **D10** | N12 approach (`NagiAbortError.name`) | `DECIDED A` (Jay, 2026-05-28) | A) rename `this.name = "AbortError"`, add `kind: "run" \| "step"` discriminator, export the class from `@nagi-js/core` · B) extend `DOMException` (constructed via `new DOMException(msg, "AbortError")`) | **A** — smallest WHATWG-conforming delta. `instanceof NagiAbortError` discriminator survives via class identity, not via `.name`. Export enables typed consumer-side translation. |
| **D11** | RFC 0013 Open Question 3 resolution | `LOCKED (by D4)` | — | D4=E (sweep + heartbeat-extend-store-lease) closes OQ3: long activities heartbeat the store lease; crash-then-stuck is reaped. |
| **D12** | Versioning (Store ABI breaks) | `LOCKED (memory)` | — | per memory `feedback-changeset-bump-type`, in 0.1.x default to `patch` in changesets. Breaking changes are expected. Single `patch` changeset for the whole RFC. |

## 7. Unrepresentable-states analysis

Per memory `feedback_unrepresentable_invalid_states` — design so bad states are structurally impossible, not patched at runtime. Five candidates here:

| ID | Hazard | Current shape | Proposed structural fix | Decision |
|---|---|---|---|---|
| **H-N5** | A `flow.started` fact survives a rolled-back caller tx (orphan run row in caller's failure path) | `Store.tryStartRun` opens its own tx; can't join caller's | Make tx-joining first-class via D5; the store's own tx becomes a *fallback* for the no-tx form, not the only path | D5 |
| **H-N8** | A lease row with `expires_at < now()` exists indefinitely; the *invariant* "expired lease ⇒ step is reclaimable" is true on read but no writer enforces it | Lease just sits; `claimStep` only checks on conflict | Sweeper writes "lease.reaped" fact + new queue message; lease state means what it should | D4 |
| **H-N10** | `workflow_run.canceled_by_run_id IS NULL` while `error.cause.canceledByRunId` is set ⇒ two sources of truth disagree | `settleStep(step.failed)` ignores the canceler in the error cause | Reclassify (D7=B): `NagiCanceledError` produces a `flow.canceled (concurrency)` fact; column is populated from the fact, not from the error JSONB | D7 |
| **H-N11** | `workflow_run.canceled_by_run_id` points at a `run_id` that doesn't exist in `workflow_run` ⇒ FK invariant violated by data | No FK; investigated 1-in-53 occurrence | FK with `ON DELETE SET NULL` (D8=B, follow-up after investigation) | D8 |
| **H-N12** | A nagi-emitted abort error has `name="NagiAbortError"` ⇒ fetch SDKs key off WHATWG's `{AbortError, ResponseAborted, TimeoutError}` allowlist and treat it as a real failure → Sentry spam + wasted LLM retries | `class NagiAbortError extends Error` with custom name | Rename to `"AbortError"`, move discriminator to `kind` field (D10=A) | D10 |

**Pattern:** all five reduce to **two sources of truth, one of which is silently lossy**. The fix in each case is to **collapse onto a single source** (the tx, the fact log, the typed error class, the WHATWG name) so the bad state is unrepresentable rather than catchable.

## 8. Store-policy review (adapters vs core pure fns)

Per memory `feedback-store-policy-in-core` — "adapters own only the tx boundary (locks/persistence); decision policy → core pure fns."

The three Store-ABI-touching fixes each split cleanly into a pure decision + an adapter tx boundary:

- **N5 (transactional start)** — pure: build the cancellation set (`{prior, fact}[]` to cancel + the new `flow.started` fact). Adapter: apply on caller tx, return the staged-apply closure. Both stores can share the pure fn; pure fn is testable with no DB.
- **N8 (lease reaper)** — pure: `decideExpiredLeaseAction(lease, currentTime, stepState)` → `{kind: "reap", redispatchAttempt} | {kind: "skip", reason}`. Adapter: scan + apply. Pure fn is the unit-test surface.
- **N10 (cancel reclassify)** — pure: `classifyFailure` (already exists, `step-exec.ts:184-206`) extended to recognize `NagiCanceledError` and emit `flow.canceled` cause. Adapter: unchanged — it just persists whichever fact the classifier picked.

Cross-cutting: the existing `decideSignal` (already pure, already correct) **stays untouched**. N7's collapse into N8 means no policy change is needed in the signal path — only the *delivery* mechanism (sweep + redispatch) needs to be reliable. This is the structurally cleaner read.

## 9. Touch points (provisional — finalized after §6)

Gated on decisions; listed so blast radius is visible. Source: auditor report, cross-referenced with researcher's design space.

- **N1** (low) — `packages/pgmq/src/pgmq-queue.ts:97-107` (the `dequeue` SELECT), `:171-180` (`parseReceipt`). Fix: wrap the internal Kysely with `db.withoutPlugins()` at the queue constructor; falls back to positional indexing if `withoutPlugins` becomes unstable. Adds 1 test file extension.
- **N3+N4** (med) — `packages/core/src/runtime.ts:84-114` (Wf), `:types.ts:492-591` (Store), new `packages/core/src/run-id.ts` for the brand constructor, new tests `packages/core/src/tests/getRun.test.ts` and `runId.test.ts`. Public-API export additions in `packages/core/src/index.ts:64-181`.
- **N5** (high) — `packages/core/src/types.ts:515-528` (Store contract), `packages/core/src/runtime.ts:183-284` (startRunInternal), `packages/postgres/src/store.ts:80-190` (PG tryStartRun), `packages/core/src/memory.ts:103-180` (in-memory). Two-phase shape per D5. The `Tx` register slot (`types.ts:61-64`) is already in place.
- **N7 (under D2=B)** — no code change; add 8 tests to `packages/core/src/tests/early-signal-buffering.test.ts` pinning the `step_run.status='pending'` and other pre-awaiting sub-states.
- **N8** (high) — new `packages/core/src/lease-reaper.ts` (pure decide fn), `packages/postgres/src/store.ts` (new `sweepLeases` method), `packages/core/src/runtime.ts:560-595` (start/stop loop in `nagi.run`), `packages/core/src/step-exec.ts:139-179` (extend heartbeat to also extend store lease). Migration optional (no schema change needed; existing `lease_expires_idx` is sufficient). RFC 0013 OQ3 explicitly resolved.
- **N9** — `packages/core/src/errors.ts` (new `NagiFlowSnapshotGoneError`), `packages/core/src/flow-registry.ts:45-53` (check hash before resolve), `packages/core/src/exec/message.ts:103-140` (`admit` path catches). One test file.
- **N10** — `packages/core/src/step-exec.ts:184-206` (extend `classifyFailure` for `NagiCanceledError`), no migration. Existing tests in `concurrency.test.ts` likely need 1–2 deltas plus 6 new pins.
- **N11** — investigation first (D8=A). If FK lands as follow-up: migration `0009_canceled_by_run_id_fk` with `ON DELETE SET NULL`, plus a cleanup step for orphan rows (the 1 phantom in 7d).
- **N12** — `packages/core/src/step-exec.ts:41-54` (rename `this.name`, add `kind`), `packages/core/src/index.ts:18-23` (export). 1 test file.

**Blast radius summary:** N1, N7, N9, N10, N11, N12 are localized (1–3 files each). N3+N4 grows public API surface (additive). N5 + N8 are the structural fixes (Store ABI change, multiple adapters).

## 10. Grilling loop (decision log, appended live)

**D2 — N7 verification reframe.** Q: drop independent N7 work given `decideSignal` already buffers pending; the bug is N8 alone? A (Jay, 2026-05-28): **B — drop N7 as a structural fix, keep 8 audit-pin tests** to guard against regression when N8 lands.

**D3 — N9 verification reframe.** Q: typed error at dispatch, multi-version registry, or document-and-restart? A (Jay, 2026-05-28): **A — typed `NagiFlowSnapshotGoneError` at dispatch.** Smallest correct delta; matches consumer's existing admin-restart ops; rules out silent code-drift class.

**D4 — N8 lease reaper mechanism (load-bearing).** Q: periodic sweep, boot-only, exposed primitive, or sweep + heartbeat-extends-lease? A (Jay, 2026-05-28): **E — periodic in-worker sweep + heartbeat extends store lease.** Sweep covers crashes; heartbeat extension keeps lease state truthful (live worker = live lease) so the sweep doesn't double-dispatch live work. Resolves RFC 0013 OQ3.

**D5 — N5 shape (transactional start).** Q: single `StartOpts.tx?`, two-phase staged, parallel `enqueueStart`, or no-N5? A (Jay, 2026-05-28): **B — `wf.startStaged(tx, …) → {started, canceled, applyOnCommit}`.** Concurrency-cancel hooks fire post-commit via `applyOnCommit()`, structurally honest.

**D6 — N5 advisory-lock policy under shared tx.** Q: skip lock and rely on partial unique idx, keep lock (deadlock risk), or abort if caller holds locks? A (Jay, 2026-05-28): **A — skip advisory lock when caller tx is supplied; add unique-violation retry handling.** The partial unique index is the actual invariant.

**D7 — N10 fix path.** Q: COALESCE from error.cause, reclassify NagiCanceledError as flow.canceled, new fact kind, or status quo? A (Jay, 2026-05-28): **B — reclassify NagiCanceledError as flow.canceled (cause: concurrency)** at `classifyFailure`. The run was canceled; calling it failed was the bug. Matches "unrepresentable invalid states."

**D8 — N11 sequencing.** Q: investigate first (FK follow-up), FK in this RFC, re-order, or skip? A (Jay, 2026-05-28): **A — investigate first; defer FK migration to follow-up RFC.** Current postgres path is atomic; phantom must come from elsewhere.

**D9 — N3 read API shape.** Q: getRun+getStep, single describe (nested), Store.loadRunState exposed, or Kysely view only? A (Jay, 2026-05-28): **B — single `wf.describe(runId)` returning `{run, steps, parent?, children}`.** Folds in the consumer's deferred read-side child-nesting follow-up.

**D10 — N12 NagiAbortError.** Q: rename + kind discriminator + export, extend DOMException, or status quo? A (Jay, 2026-05-28): **A — rename `name="AbortError"`, add `kind`, export.** Smallest WHATWG-conforming delta.

**D1 — scope & sequencing.** Q: one RFC 4-phase single PR, one RFC per-phase PRs, three RFCs, or N8+N12 only? A (Jay, 2026-05-28): **A — ONE RFC, 4 phases, single PR.** Phase 1 = N1 + N12 (contract). Phase 2 = N3+N4 + N10 (additive + classifier). Phase 3 = N8 + N9 (reliability). Phase 4 = N5 (transactional start).

**DECISIONS LOG COMPLETE — 11/11 (2026-05-28). Awaiting Jay's approval to implement.**

## 11. Open questions / assumptions

- **A1** (`ASSUMED`) RFC 0013's just-shipped activity-step path is correct and stays untouched. N8's heartbeat extension applies to *both* regular task heartbeats AND activity heartbeats (the lease semantics are the same).
- **A2** (`VERIFY before locking D8`) Investigate the 1 phantom-superseder row in lymo's 7-day data: which code path inserted the row? Is it postgres-store-current, custom store, or a legacy row? D8's structural fix depends on this answer.
- **V1** (`VERIFY before locking D4`) Confirm the sweeper's claim semantics under retry: when the sweeper re-enqueues with `attempt+1`, does `claimStep` correctly write a new lease (it should — `ON CONFLICT … WHERE expires_at < now()` is the existing guard)?
- **V2** (`VERIFY before locking D5`) Confirm that `pgmqQueue.withTx(tx)` covers everything the start path needs (currently it only wraps `send` — verify the queue side has no other writes during start).

## 12. Upstream issues already filed (status)

| Issue | Covers | Status |
|---|---|---|
| [#21](https://github.com/lymo-inc/nagi/issues/21) | N5 | will close on rc.14 merge (D5=B implemented) |
| [#22](https://github.com/lymo-inc/nagi/issues/22) | N1 | will close on rc.14 merge |
| [#23](https://github.com/lymo-inc/nagi/issues/23) | N3 + N4 | will close on rc.14 merge (D9=B implemented) |
| (no issue) | N7 | DROP per D2=B (verified — `decideSignal` already correct); audit-pin tests landed in `decideSignal-states.test.ts` |
| [#25](https://github.com/lymo-inc/nagi/issues/25) | N8 | will close on rc.14 merge (D4=E implemented; resolves RFC 0013 OQ3) |
| [#26](https://github.com/lymo-inc/nagi/issues/26) | N9 | will close on rc.14 merge (D3=A implemented) |
| [#27](https://github.com/lymo-inc/nagi/issues/27) | N10 | will close on rc.14 merge (D7=B implemented) |
| [#29](https://github.com/lymo-inc/nagi/issues/29) | N11 | **OPEN — investigation required**, deferred per D8=A. Do NOT close on rc.14 merge. |
| [#28](https://github.com/lymo-inc/nagi/issues/28) | N12 | will close on rc.14 merge (D10=A implemented) |

## 13. Implementation log (live)

### Phase 1 — N1 + N12 (landed 2026-05-28, JST)

- **N1** — `packages/pgmq/src/pgmq-queue.ts`: `stripPlugins()` helper applied at constructor + `withTx` so internal SQL (`pgmq.read`, `set_vt`, etc.) runs against a plugin-free executor. Caller's `db` is untouched (their CamelCase reads of their own tables still work). `parseReceipt` gains a diagnostic hint that names CamelCasePlugin specifically when receipt is the string `"undefined"`.
- **N12** — `packages/core/src/step-exec.ts`: `NagiAbortError.name = "AbortError"` (WHATWG-conforming); `scope` field renamed to `kind`; class exported via `packages/core/src/index.ts`. Internal `instanceof NagiAbortError` discriminator in `classifyFailure` (`step-exec.ts:194`) survives the rename.
- **Tests** — 6 new tests for N1 (`pgmq-queue.test.ts` extended with a real Kysely + CamelCasePlugin harness); 6 new tests for N12 (new `tests/nagi-abort-error.test.ts`). Verified by reverting the N1 fix and watching test 2 (`receive under CamelCasePlugin`) fail — the load-bearing pin.
- **Verification** — `pnpm -F @nagi-js/{core,pgmq} typecheck` CLEAN; `pnpm -F @nagi-js/{core,pgmq} test` green (782 + 53 tests).

### Phase 2 — N3 + N4 + N10 + N7 audit-pins (landed 2026-05-28, JST)

- **N3 — `wf.describe(runId)`.** New `packages/core/src/run-view.ts` carries the locked field set (Jay-chosen shape: flat extension of `RunSummary`, optionals at the API boundary only). `Store.describe(runId): Promise<RunDescription>` added to the Store contract (`types.ts`); both `InMemoryStore` and `PostgresStore` implement. PG uses 3 SELECTs in one tx (run row, steps + LEFT JOIN lease, children); in-memory projects from facts + leases via `foldRun`. `Wf.describe` thinly delegates.
- **N4 — `RunId.parse` / `RunId.fromTrusted`.** New `packages/core/src/run-id.ts`: `RunId.parse(s)` validates (non-empty, trims-to-content) and brands; `RunId.fromTrusted(s)` brands without validation for DB-read hot paths. Single identifier carries both type + value (via re-export pattern).
- **N10 — `NagiCanceledError → flow.canceled` reclassification.** `classifyFailure` (`step-exec.ts:184-225`) extended: walks the `err.cause` chain via `instanceof NagiCanceledError` (guards against forged JSONB), returns a new `flowCanceled` outcome arm. `handleStepError` (`exec/message.ts`) gains the matching branch — emits `step.canceled` + `flow.canceled (concurrency)`. The PG `applyFactToMaterialized` already populates `canceled_by_run_id` for `cause: "concurrency"`, so the column lights up automatically (verified). Consumer's COALESCE fallback keeps working.
- **N7 audit pins (D2=B).** 8 tests in `tests/decideSignal-states.test.ts` pin buffering across `pending` / `running` / `backoff` / `aborting` states + multi-name + terminal-noop. No code change; pinning the structural invariant ahead of N8.
- **Tests** — +46 core tests (828/828 PASS) + 49 postgres tests passing (29 skipped, PG-instance-dependent). New files: `tests/describe.test.ts` (6), `tests/run-id.test.ts` (4), `tests/decideSignal-states.test.ts` (8); extended `tests/concurrency.test.ts` (+5 N10 tests).
- **Open follow-up (judgment call #6 from subagent):** hooks (`onStepError`/`onFlowError`) are NOT fired from the `flowCanceled` branch; the assumption is the supersession-side `tryStartRun` already fired them. For a `NagiCanceledError` arriving via `wf.cancel()` or operator path (not supersession), hooks may need fresh emission. Verify with a test in a follow-up; not a blocker.

### Phase 3 — N8 + N9 (landed 2026-05-28, JST)

- **N8 — lease reaper (load-bearing).** New pure fn `decideExpiredLeaseAction` (`packages/core/src/lease-reaper.ts`, body Jay-chosen: `attempt+1`, no backoff, skip terminal/live). New `Store.sweepLeases({ now, queue, limit })` + `Store.extendLease(...)` contract methods; both stores implement. PG adapter uses `FOR UPDATE OF l SKIP LOCKED` for safe concurrent sweeps; calls `pgmq.withTx(trx)` so the lease DELETE + audit fact INSERT + dispatch enqueue commit atomically. New `lease.reaped` audit fact kind. Heartbeat (`step-exec.ts`) now extends both queue VT AND store lease per tick via `Promise.all` (independent failure handling — never throws). Periodic reaper loop integrated into `nagiRun` alongside `worker.run()`; configurable `NagiConfig.reaperIntervalMs` (default 30s = ½ default lease TTL; `0` disables).
- **N9 — typed `NagiFlowSnapshotGoneError`.** New error class in `errors.ts` (carries `runId`, `flowId`, `pinnedHash`, `currentHash: string | null`). `FlowRegistry.requireForRun` extended with optional `pinnedHash`/`currentHashOf` params; **subtractive bypass**: callers that don't thread the hash (operator/cancel/signal) keep the legacy resolve-by-flowId path automatically. Dispatch boundary (`worker.dispatchSafely`) catches the new error → warn-log + nack (consumer recovers via restart).
- **V1 verified (RFC §11):** PG `claimStep` at `attempt+1` is a clean `INSERT ON CONFLICT (run_id, step_id, attempt)` — the deleted lease at `attempt` doesn't collide because `attempt` is part of the PK. Pinned by integration test.
- **No schema migration required** — `lease.reaped` is just a row in `nagi.fact`; existing `lease_expires_idx` covers the sweeper's selector.
- **Resolves RFC 0013 Open Question 3** — long activities now heartbeat the store lease too.
- **Tests** — 36 new (lease-reaper.test.ts +11, flow-snapshot-gone.test.ts +7, heartbeat.test.ts +1, integration.test.ts +1 PG sweep end-to-end, plus 16 across existing files updated for the new heartbeat signature).
- **Verification** — `pnpm -F @nagi-js/{core,postgres} typecheck` CLEAN; `pnpm -F @nagi-js/core test` 866/866 PASS; `pnpm -F @nagi-js/postgres test` 79 PASS (PG integration suite gates on `NAGI_POSTGRES_TEST_URL`).

### Phase 4 — N5 wf.startStaged (landed 2026-05-28, JST)

- **`Store.tryStartRunOnTx(tx, runId, fact, concurrency?)`** added (`types.ts`) — same write semantics as `tryStartRun` but operates on the caller's tx; does NOT fire hooks or `propagateToParent` (those are post-commit). Skips the `pg_advisory_xact_lock` under shared tx per D6=A.
- **`NagiConcurrencyConflictError`** new error class. PG `tryStartRunOnTx` catches SQLSTATE `23505` (unique_violation) once, retries SELECT-prior + INSERT-new; second violation throws this error so the consumer can retry the whole tx.
- **`Wf.startStaged(flow, input, opts: { tx, runId? })`** added — returns `{ runId, started, canceled, applyOnCommit }`. Pre-computes initial transition via `nextTransition`, enqueues entry steps via `queue.withTx(tx).enqueue(...)` so the first dispatch rides the caller's tx atomically. `applyOnCommit` is idempotent (internal `fired` flag) and fires `onFlowError`/`flow.onError` + `dispatcher.propagateToParent` for cancellations, then `onFlowStart` for the new run. Sibling `Wf.startStagedById` for parity with `start`/`startById`.
- **`wf.start` and `Store.tryStartRun` are unchanged** — fully backwards-compatible (D5=B locked the two-method, additive shape).
- **V2 verified (RFC §11):** the only queue write in `startStaged`'s pre-commit work is the entry-step enqueue. `dispatcher.advance` is NOT called pre-commit (the worker picks up the message post-commit). `pgmqQueue.withTx` covers this single enqueue. No other start-path writes exist outside the tx.
- **Tests** — 10 new in `tests/start-staged.test.ts` (886/886 core PASS) + 1 PG-gated integration test pinning that business INSERT + flow.started fact + queue message all commit (or all roll back) together. Uses `RollbackableStore` / `TxAwareQueue` / `ConflictingStore` / `NoLockPathStore` test doubles to verify each invariant in isolation.
- **Verification** — all 4 packages typecheck CLEAN; core 886/886, pgmq 53/53, postgres 80 PASS + 31 PG-gated.

---

**IMPLEMENTATION COMPLETE (2026-05-28).** All 4 phases landed (N1, N3+N4, N7 audit pins, N8, N9, N10, N12, N5). N11 deferred per D8=A. Cumulative diff: 18 modified + 11 new files, +1711/-34 lines. PR materials staged; awaiting Jay's commit-sequencing instruction.

