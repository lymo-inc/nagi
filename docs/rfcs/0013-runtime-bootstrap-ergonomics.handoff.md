# RFC 0013 — Implementation handoff

- **RFC:** `docs/rfcs/0013-runtime-bootstrap-ergonomics.md` (Accepted 2026-05-21, Jay — decisions resolved via grill)
- **Tracking:** lymo-inc/nagi#17
- **Author of impl:** Claude, paired with Jay (2026-05-21 JST)
- **Status:** Implemented + tested. **Not committed / no PR** — the working tree
  intermixes #17 with your concurrent #18 / #20 / fold-parent-link work in shared
  files; sequencing is your call (see "Caveats").

## What landed (in the working tree)

### Change 1 — `nagi.run` (`@nagi-js/core`)

- `packages/core/src/runtime.ts` — `nagi` renamed to internal `nagiImpl`;
  new `nagiRun` (internal controller + `AbortSignal.any` merge of an optional
  external `signal`; privately-held loop promise; graceful-vs-crash branch that
  logs only true crashes via `logger`; memoized idempotent `stop()`); new
  `export const nagi = Object.assign(nagiImpl, { run: nagiRun })`; new
  `NagiRunConfig` and `RuntimeHandle<TFlows>` interfaces.
- `packages/core/src/index.ts` — re-export `type NagiRunConfig`, `type RuntimeHandle`.
- `packages/core/src/runtime-run.test.ts` (new) — 9 lifecycle tests: shape;
  worker processes a started flow with no manual drain; `stop()` awaits an
  in-flight handler; `stop()` idempotent (twice + concurrent, same memoized
  promise, never throws); graceful `stop()` never calls `logger.error`; a
  `dequeue`-throwing crash logs `logger.error` once and `stop()` still resolves;
  external-signal abort → graceful; already-aborted signal → immediately stopped;
  legacy `nagi()` + `wf.worker()` + `worker.run()` still drives a flow.

### Change 2 — auto `ensureSchema` (`@nagi-js/core`)

- `packages/core/src/types.ts` — `Queue` gains optional `ensureSchema?(): Promise<void>` (with JSDoc).
- `packages/core/src/runtime.ts` — `nagi()` awaits `config.queue.ensureSchema?.()`
  once, early in construction (before the flow loop), eager + fail-fast.
- `packages/core/src/queue-bootstrap.test.ts` (new) — 5 tests: called exactly
  once at construction; not again on `wf.start` / `wf.worker` / dispatch; a
  hookless queue (in-memory) constructs and runs end-to-end; a rejecting
  `ensureSchema` fails `nagi()` (fail-fast, error preserved); a rejecting
  `ensureSchema` yields no usable runtime (enqueue-before-schema unreachable).

### Change 3 — generic `pgmqQueue<DB>` (`@nagi-js/pgmq`)

- `packages/pgmq/src/pgmq-queue.ts` — `PgmqQueueOpts<DB = unknown>` with
  `db: Kysely<DB>`; `pgmqQueue<DB = unknown>(opts)`; a single internal erasure
  (`opts.db as unknown as Kysely<unknown>`) keeps the queue body byte-identical.
- `packages/pgmq/src/types.test-d.ts` — extended: accepts a typed `Kysely<DB>`
  with no cast, infers `DB`, result is `PgmqQueue`/assignable to `Queue`, and the
  bare form still resolves to `Kysely<unknown>`.

### Meta

- `docs/rfcs/0013-runtime-bootstrap-ergonomics.md` — RFC + decisions log (the
  grill-resolved version is the uncommitted diff over the copy your `version
  packages` commit swept into HEAD).
- `docs/rfcs/0013-runtime-bootstrap-ergonomics.research.md` — prior-art survey.
- `.changeset/runtime-bootstrap-ergonomics.md` — `patch` for `@nagi-js/core` + `@nagi-js/pgmq`.

## What was NOT done (intentionally — resolved in the grill)

- **`handle.done` deferred (N2).** Handle is `{ wf, stop }`; the loop promise is
  private. Adding `done` later is non-breaking. Crash is observable via `logger`.
- **No `ensureSchema` opt-out knob (N3).** Eager + fail-fast only; best-effort
  was rejected (it reintroduces the silent first-enqueue failure). A future
  no-`CREATE`-privilege opt-out belongs on the pgmq adapter
  (`pgmqQueue({ ensureSchema: false })`), non-breaking to add then.
- **No use-after-stop guard (N4).** `wf` stays a normal durable producer after
  `stop()`; that's the supported "produce here, drain elsewhere" topology.
- **No auto-SIGTERM handler.** Compose with the caller's:
  `process.once("SIGTERM", () => void handle.stop())`.
- **No `stop({ timeout })`.** Drain awaits in-flight indefinitely (issue non-goal).

## Verification

```
pnpm --filter @nagi-js/pgmq typecheck        # clean
pnpm --filter @nagi-js/pgmq test:types       # 41 type tests pass, no type errors
pnpm --filter @nagi-js/pgmq test             # green
pnpm --filter @nagi-js/core test             # 40 files · 551 tests pass · no type errors
pnpm --filter @nagi-js/core typecheck        # clean (tsc --noEmit)
```

The core suite green-count **includes** your concurrent #18/#20/fold-parent-link
work and the two new #17 test files together — i.e. #17 composes with your
in-flight changes (the generic `nagi<const TFlows>` and `ParentRef` refactor) and
all suites pass as one tree.

## Caveats — sequencing needs your call

1. **Intermixed shared files.** `packages/core/src/runtime.ts` (+135),
   `packages/core/src/types.ts` (+75), and `index.ts` carry **both** #17 and your
   uncommitted #18 / #20 / fold-parent-link changes. There is no clean way to
   commit #17 alone without `git add -p`-style hunk selection — your sequencing
   decision. Cleanly isolated #17 pieces: all of `packages/pgmq/*`, the two new
   `*.test.ts` files, the `0013` docs, and the changeset.

2. **Active clobber risk.** While implementing, a concurrent save to `runtime.ts`
   (the `ParentRef` refactor) overwrote Change 1 once; it was re-applied and the
   tree is green now, but further `runtime.ts` saves will clobber it again. If
   #17 isn't committed soon, consider moving it to an isolated `git worktree`.

3. **RFC numbering.** You're renaming RFCs to issue numbers (0012→0018 for #18).
   By that convention this RFC should be **`0017`**, not `0013`. It's already
   committed as `0013` in HEAD, so the rename is yours to make (RFC + research +
   handoff + the changeset's "RFC 0013" reference move together).

4. **No commit / no PR made.** Per your multi-feature-tree sequencing preference.

## Files index (#17-authored)

```
docs/rfcs/0013-runtime-bootstrap-ergonomics.md           (RFC + decisions log; grill-resolved)
docs/rfcs/0013-runtime-bootstrap-ergonomics.research.md  (prior-art survey)
docs/rfcs/0013-runtime-bootstrap-ergonomics.handoff.md   (this file)
.changeset/runtime-bootstrap-ergonomics.md               (patch: core + pgmq)
packages/core/src/runtime.ts        (modified — Change 1 + Change 2; INTERMIXED with #18/fold)
packages/core/src/types.ts          (modified — Change 2 Queue.ensureSchema; INTERMIXED with #18/#20)
packages/core/src/index.ts          (modified — NagiRunConfig + RuntimeHandle exports; INTERMIXED)
packages/core/src/queue-bootstrap.test.ts  (new — Change 2 tests)
packages/core/src/runtime-run.test.ts      (new — Change 1 tests)
packages/pgmq/src/pgmq-queue.ts     (modified — Change 3; isolated)
packages/pgmq/src/types.test-d.ts   (modified — Change 3 type tests; isolated)
```
