# Handoff — RFC 0020 (`onLog` callback replaces the `Logger` interface)

- **RFC:** `docs/rfcs/0020-onlog-callback.md` (decisions log, approved by @jay 2026-05-21)
- **Tracking issue:** #19
- **Author:** Claude (paired with @jay)
- **Status:** Implemented + green locally (714 tests, 0 type errors). **Production code already committed to `main` in `e451bfd` — see Sequencing caveat. New test files + changeset are uncommitted. NOT PR'd.**
- **Date:** 2026-05-21 (JST)

## ⚠ Sequencing caveat — read first

This landed in a **live multi-feature working tree** (the parallel-tree pattern).
During the session, an automated/refactor commit
`e451bfd "chore: refactor core internals"` **swept the uncommitted tree onto
`main`**, bundling at least three streams of work into one commit:

1. **This feature (onLog / issue #19)** — `types.ts` (`LogEntry`/`LogLevel`),
   `internal.ts` (`EmitLog`/`makeEmit`), `runtime.ts` (`onLog` + threading),
   `dispatch.ts` (the 5 dispatch/subflow sites + `makeStepCtx` rewire +
   `consoleLogger` deletion), `worker.ts`, the RFC, and the four migrated test
   files (`runtime-run`, `signal-multi-name`, `dispatch`, `test-helpers`).
2. **Streaming-task (RFC 0019, issue #12)** — `stream-hub.ts`,
   `streaming-task.test-d.ts`, builder/dispatch/runtime stream wiring.
3. **A `step-output-always-present` refactor + the `src/*.test.ts → src/tests/`
   directory reorg.**

Consequences:

- **A cleanly-isolated single-feature PR for #19 is no longer practical** — the
  implementation is already on `main`, interleaved with #12 and the refactor
  inside `e451bfd`. There is no separable feature branch to open a PR *from*
  without history surgery.
- **Still-uncommitted concurrent work** at handoff time: `dispatch.ts`,
  `runtime.ts`, `builder.ts`, `memory.ts`, `stream-hub.ts`,
  `tests/stream-hub.test.ts` (all streaming-task #12) — **do not touch.**
- **Remaining onLog artifacts NOT yet committed** (purely additive, no overlap
  with the streaming files):
  - `packages/core/src/tests/onLog.test.ts` (untracked)
  - `packages/core/src/tests/onLog.test-d.ts` (untracked)
  - `.changeset/onlog-callback.md` (untracked)
  - `docs/rfcs/0020-onlog-callback.handoff.md` (this file, untracked)
- **RFC number:** `0020` — issue-matched to #19 (coincidentally). No collision
  with the streaming RFC, which is `0019` (it tracks issue #12).

**Decision left to Jay:** how to land the four remaining files. Options:
(a) a focused additive commit to `main` (`git add` the four by name — they don't
touch the streaming `M` files); (b) a small branch + PR carrying just those four;
(c) leave them for the next sweep. The implementation itself is already shipped.

## What shipped (decisions, all approved)

Firm: **D1** `NagiConfig.logger` → `onLog?: (entry: LogEntry) => void`. **D2**
`LogEntry { readonly level: LogLevel; readonly msg: string; readonly attrs?: Record<string, unknown> }`,
level-as-field, no timestamp. **D3** fully silent by default — `consoleLogger`
fallback deleted, no record allocated when no sink. **D4** `attrs` is `undefined`,
never `{}`. **D5** sink calls wrapped `try/catch`, fire-and-forget (returned
promises ignored). **D6** zero durability impact. **D7** `patch` changeset (rc
prerelease).

Grilled/resolved: **O1** keep the method-shaped in-step `ctx.logger` (`Logger`
interface retained + exported). **O2** auto-enrich in-step entries with
`{ runId, stepId, attempt }`, runtime-authoritative on collision (new behavior).
**O3** hard break (no `logger` shim). **O4** swallow sink throws.

## Implementation map (all in `packages/core/src/`)

- `internal.ts` — `EmitLog` type + `makeEmit(onLog?)` choke point: returns a
  no-op when `onLog` is absent (D3) else a `try/catch` wrapper (D5). Internal —
  not re-exported from `index.ts`.
- `types.ts` — `LogLevel`, `LogEntry`; `Logger` and `StepCtx.logger: Logger`
  retained (O1).
- `runtime.ts` — `NagiConfig.onLog`; `emitLog = makeEmit(config.onLog)` built in
  both `nagiImpl` and `nagiRun`; threaded as `DispatchDeps.emitLog` /
  `OperatorDeps.emitLog`; 4 direct sites rewritten.
- `dispatch.ts` — 4 sites rewritten (note: the not-in-flow `warn` emits no
  `attrs` key, D4; the hook-threw `error` keeps the conditional `stack`);
  `makeStepCtx` builds the enriching method-shaped `ctx.logger` (O2) and
  `consoleLogger` is deleted (D3).
- `worker.ts` — the uncaught-dispatch `error` site.

## Test coverage

- `tests/onLog.test.ts` — 31 `it` across record shape & level routing (all 9
  real diagnostics driven deterministically), attrs contract (incl.
  `attrs === undefined` for the not-in-flow warn, conditional `stack`),
  silent-by-default (no throw / no `console.*` / handler `ctx.logger` silent),
  single-channel, exactly-once + ordering, throw isolation (O4), and in-step
  `ctx.logger` (O1 + O2 enrich, runtime-wins-on-collision, `attempt: 2` on retry).
- `tests/onLog.test-d.ts` — type-level: `onLog` is exactly
  `((entry: LogEntry) => void) | undefined`; `LogLevel` exhaustive (not `string`);
  `attrs` values `unknown` not `any`; readonly fields; a method-shaped `Logger`
  is **not** assignable to `onLog`; the old `logger` key is a compile error
  (hard-break proof); `StepCtx["logger"]` is method-shaped.
- Migrated (already committed in `e451bfd`): `runtime-run`, `signal-multi-name`,
  `dispatch`, `test-helpers` (`spyOnLog()` helper added).

## Verification (local)

`pnpm -F @nagi-js/core test` → **48 files / 714 tests passed, 0 type errors**
(count reflects concurrent streaming tests in the tree). `biome check` clean on
the new files. Baseline before the work was 568/0.

## Consumer migration

Single call-site change (see `.changeset/onlog-callback.md`). lymo backend:
delete `adaptLogger` (`apps/backend/src/workflows/runtime.ts`) and pass
`onLog: ({ level, msg, attrs }) => logger[level](attrs ?? {}, msg)`.

## Follow-ups / not in scope

- `@nagi-js/otel` still uses `console.error` directly in `hooks.ts` `withGuard`;
  it could route through `onLog`/an adapter in a later pass (out of scope here).
- No level-filtering knob inside nagi (non-goal — host filters in `onLog`).
