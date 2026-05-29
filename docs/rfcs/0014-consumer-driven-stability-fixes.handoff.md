# Handoff — RFC 0014 (consumer-driven stability fixes)

- **RFC:** `docs/rfcs/0014-consumer-driven-stability-fixes.md`
- **Source:** the consumer-side rc.12 refactor RFC at `../lymo/docs/rfcs/2026-05-23-nagi-usage-rc12-refactor.md` §12 (N1–N12)
- **Author:** Claude Opus 4.7 (1M ctx), 3-agent parallel pipeline + 4 implementation subagents, supervised by @jay
- **Tracking issues:** [#21 N5](https://github.com/lymo-inc/nagi/issues/21), [#22 N1](https://github.com/lymo-inc/nagi/issues/22), [#23 N3+N4](https://github.com/lymo-inc/nagi/issues/23), [#25 N8](https://github.com/lymo-inc/nagi/issues/25), [#26 N9](https://github.com/lymo-inc/nagi/issues/26), [#27 N10](https://github.com/lymo-inc/nagi/issues/27), [#28 N12](https://github.com/lymo-inc/nagi/issues/28), [#29 N11](https://github.com/lymo-inc/nagi/issues/29) (TRACKING ONLY — don't close on merge). N7 verified-OK, no issue filed.
- **Status:** Implemented + green locally — **NOT committed, NOT PR'd** (Jay sequences; see memory `feedback-commit-sequencing`)
- **Date:** 2026-05-29 (JST)

## What landed (cumulative)

| Candidate | Decision | Scope | Tests |
|---|---|---|---|
| N1 | strip Kysely plugins from pgmq's internal executor | `pgmq-queue.ts` | +6 |
| N3+N4 | `wf.describe(runId)` returning nested `{run, steps}` + `RunId.parse` / `fromTrusted` | `types.ts`, `runtime.ts`, `memory.ts`, `postgres/store.ts`, new `run-view.ts` + `run-id.ts` | +10 |
| N5 | new `wf.startStaged(flow, input, {tx, runId?})` returning `{started, canceled, applyOnCommit}` | `types.ts`, `runtime.ts`, `memory.ts`, `postgres/store.ts`, `errors.ts` | +10 + 1 PG int |
| N7 | audit pins only (`decideSignal` already correct) | new `decideSignal-states.test.ts` | +8 |
| N8 | lease autoreaper + heartbeat-extends-store-lease | new `lease-reaper.ts`, `step-exec.ts`, `runtime.ts`, `types.ts`, `memory.ts`, `postgres/store.ts`, `facts.ts`, `state.ts` | +11 + 1 PG int |
| N9 | typed `NagiFlowSnapshotGoneError` at dispatch | `errors.ts`, `flow-registry.ts`, `runtime.ts`, `worker.ts` | +7 |
| N10 | `NagiCanceledError` → `flow.canceled (concurrency)` reclassification | `step-exec.ts`, `exec/message.ts` | +5 |
| N12 | `NagiAbortError.name = "AbortError"` + `kind` + export | `step-exec.ts`, `index.ts` | +6 |

**Cumulative diff:** 18 modified + 11 new files, **+1711 / -34 lines** across `@nagi-js/{core,pgmq,postgres}`.

**Verification (final):** `tsc --noEmit` CLEAN on all 4 packages. Tests: **core 886/886 PASS**, **pgmq 53/53 PASS**, **postgres 80 PASS + 31 PG-gated** (run only with `NAGI_POSTGRES_TEST_URL` set; PG integration suite includes the new N5 + N8 end-to-end pins).

## Deferred / out of scope

- **N11 (phantom superseder, 1-in-53 frequency).** Postgres `tryStartRun` is already atomic; the phantom row must come from another path (custom store? legacy fact log? pre-rc.12 row?). Per D8=A: investigate first in lymo's prod data (query `workflow_run WHERE canceled_by_run_id NOT IN (SELECT run_id FROM workflow_run)`), then file a follow-up RFC with the FK migration + cleanup. Tracking task #2 in the session task list.
- **N2 (CHANGELOG/README).** By intent — maintainer is shipping these later (per memory `feedback_readme_ownership`).
- **N6 (multi-name buffering).** Already verified correct in rc.12; no code change.

## Sequencing caveats — read before committing

1. **Multi-feature tree.** Per memory `project-parallel-tree-activity`, Jay runs features + automation concurrently. Before committing, run `git status` to confirm the working tree only contains RFC 0014 work. The `docs/rfcs/0013-activity-steps.md` modification was pre-existing at session start and is **unrelated to RFC 0014** — exclude it from any 0014 commit. RFC 0014's expected commit scope:
   - 18 modified packages files (errors, types, runtime, memory, postgres/store, etc.)
   - 11 new files (`docs/rfcs/0014-*.md` + handoff, 8 test files, 3 new src files: `lease-reaper.ts`, `run-view.ts`, `run-id.ts`)
   - 1 new changeset: `.changeset/rfc-0014-stability-fixes.md`
2. **No release-related commands run.** Per memory `feedback-release-flow`, version bumps go through `pnpm version-packages` (NOT `npm version`); this handoff stops at PR creation. Don't run `changeset publish` here.
3. **Changeset is `patch`** per memory `feedback-changeset-bump-type` — we're still in 0.1.x where breaking changes ship in patches. Phase 4's N5 IS technically a Store ABI break (new required method on the Store interface), but the rc.X cadence is the right channel and consumers updating to rc.14 expect to update their custom Store impls (if any — lymo uses only the built-in postgres store).

## Known follow-ups (post-merge)

| Topic | Reason | Owner |
|---|---|---|
| `flowCanceled` outcome hooks emission | RFC §13 Phase 2 follow-up: hooks NOT fired from the new `flowCanceled` branch on the assumption supersession already fired them. For explicit `wf.cancel()` / operator paths, hooks may need fresh emission. Verify with a test. | jay |
| N11 phantom superseder root cause | Investigate orphan `canceled_by_run_id` in lymo prod; file follow-up RFC + FK migration | jay (RFC 0015?) |
| N3+N4 in-flow read-side child nesting | Already in this RFC — but only the projection shape. `wf.describe` does N+1 SELECTs (children list separate). Optimize if profiling shows it. | TBD |
| Update lymo consumer RFC §12 | Mark N1/N3+N4/N5/N7/N8/N9/N10/N12 as RESOLVED in lymo's prod doc when rc.14 lands. N11 stays OPEN. | jay |

## Test-double inventory (for reviewers)

To avoid coupling tests to PG, the implementation subagents introduced these `InMemoryStore`-based test doubles (all in `packages/core/src/tests/start-staged.test.ts`):

- `RollbackableStore` — buffers writes per-tx; never applies. Pins the rollback contract (run row + flow.started fact don't survive caller's rollback).
- `TxAwareQueue extends InMemoryQueue` — exposes a `withTx` that records the threaded `tx` reference. Pins that the entry-step enqueue rides the caller's tx.
- `ConflictingStore` — throws `NagiConcurrencyConflictError` on `tryStartRunOnTx`. Pins error propagation.
- `NoLockPathStore` — overrides `tryStartRun` to throw; only `tryStartRunOnTx` is no-op. Pins the runtime routes through the new method (NEVER through the lock-bearing one).

For the PG integration test, a `QueueStub` records `withTxArg` to verify tx threading without needing a real `pgmq` schema.

## Pipeline summary (process notes for the next RFC)

The 3-agent parallel pipeline + 4 implementation subagents pattern worked well. Key wins:

1. **The 3-agent synthesis caught a major framing error.** All three independent agents (researcher / auditor / test-spec drafter) flagged that the lymo §12 N7 ("`wf.signal` throws on pending") didn't reproduce in current nagi source — the error string only exists in CHANGELOGs documenting rc.11's fix. Without three agents converging on this, the implementation would have spent days chasing a phantom bug. The actual bug (lease never reaped → buffered fact never delivered) was N8 alone.
2. **Two grilling-time user code contributions shaped the policy heart of the design:**
   - `RunView`/`StepView` field set (D9, locked the public read contract)
   - `decideExpiredLeaseAction` policy (D4-related, locked the reaper recovery model: `attempt+1`, no backoff)
3. **N5 (D5=B two-phase staged) was the structurally honest call.** The single-`tx` param shape (D5=A) had no answer for "when do cancellation hooks fire under a caller's tx?". The staged API made the post-commit moment a first-class return value (`applyOnCommit`). Memory `feedback-store-policy-in-core` and `feedback-public-api-shape` both pointed at this — the grilling locked it explicitly.
4. **Server load 529s appeared twice** during agent dispatch (Phase 3 retry, Phase 4 retry). Retrying with the same prompt worked both times; total elapsed delay was ~7 minutes.

## Commands to verify before PR

```bash
# 1. confirm the tree only contains RFC 0014 work + pre-existing 0013 edits
git status

# 2. final verification (already passing)
pnpm -F @nagi-js/core typecheck
pnpm -F @nagi-js/postgres typecheck
pnpm -F @nagi-js/pgmq typecheck
pnpm -F @nagi-js/otel typecheck
pnpm -F @nagi-js/core test
pnpm -F @nagi-js/pgmq test
pnpm -F @nagi-js/postgres test

# 3. biome (run from repo root)
pnpm exec biome check .

# 4. (optional) versioned snapshot
pnpm version-packages    # use this, NOT npm version (memory: feedback-release-flow)
```

## PR shape (suggested)

```
title: feat: rfc#14 — consumer-driven stability fixes (N1, N3+N4, N5, N7-pins, N8, N9, N10, N12)

body:
RFC 0014 implements the upstream nagi candidates discovered during the lymo backend's rc.12 refactor (consumer RFC §12).

Closes #21 (N5), #22 (N1), #23 (N3+N4), #25 (N8), #26 (N9), #27 (N10), #28 (N12).
Tracks #29 (N11) — deferred for follow-up investigation (per RFC §6 D8=A).
N7 verified-OK against current source; audit-pin tests landed instead of code change (per §6 D2=B).

The load-bearing fix is N8 — the lease autoreaper. The consumer had 55+ stuck runs over 7 days across 9+ orgs in prod (LYMO-119 cluster); root cause was workers dying mid-step without re-dispatch. Periodic sweep + heartbeat-extends-store-lease (per RFC §6 D4=E) closes the steady-state failure mode and resolves RFC 0013 Open Question 3.

See docs/rfcs/0014-consumer-driven-stability-fixes.md for the full decisions log, unrepresentable-states analysis, and per-phase implementation notes. See the handoff doc for verification commands and follow-up tasks.

🤖 Generated with [Claude Code](https://claude.com/claude-code)
```
