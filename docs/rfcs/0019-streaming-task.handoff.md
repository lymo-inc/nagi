# RFC 0019 — Implementation handoff

- **RFC:** `docs/rfcs/0019-streaming-task.md` (Accepted 2026-05-21, Jay — decisions resolved via grill)
- **Tracking:** lymo-inc/nagi#12 (note: RFC doc number 0019 ≠ issue #12; see Caveats)
- **Author of impl:** Claude, paired with Jay (2026-05-21 JST) — built via a parallel-subagent pipeline (3 research agents → RFC → grill → 3 implementation agents, diffs reviewed incrementally)
- **Status:** **Phases A–D complete + tested. Whole `@nagi-js/core` suite green (50 files · 743 tests · typecheck clean) as of 2026-05-22**, composing with your committed core refactor (`e451bfd`) and uncommitted RFC 0020 (onLog). **Not committed / no PR** — the working tree intermixes #12 with your RFC 0020 work in shared files; sequencing is your call (see "Caveats").

## What landed (in the working tree)

### Phase A — additive types + builder (`@nagi-js/core`)
- `packages/core/src/types.ts` — `StepKind` gains `"streaming"`; new `StreamEvent<C = Json>` (discriminated `chunk`/`dropped`/`retry`/`error`), `StreamingStepCtx<Input,Chunk>` (extends `StepCtx` with `emit`), `StreamingTaskConfig`; `Builder.streamingTask<N,O,C=Json>(): Step<O>`; optional `Store.subscribeStream?`/`publishChunk?` (mirrors `Queue.ensureSchema?`).
- `packages/core/src/internal.ts` — `StreamingTaskDef` (`kind:"streaming"`) + added to the `StepDef` union.
- `packages/core/src/builder.ts` — `streamingTask` in `makeBuilder` (mirrors `task`; uses the `compact()` helper from your refactor).
- `packages/core/src/canonicalize.ts` — `"streaming"` routes through `canonicalizeTask` (D8: hashes/replays as a task; chunks never affect the hash).
- `packages/core/src/dispatch.ts` — `"streaming"` routed to the task path at every `def.kind === "task"` site (later refined in Phase C).

### Phase B — in-memory broadcast hub (`@nagi-js/core`)
- `packages/core/src/stream-hub.ts` (new) — `InMemoryStreamHub`: per-`(runId,stepId)` fan-out with independent FIFO per-subscriber cursors; non-blocking `publishChunk`; `signalRetry`/`closeOk`/`closeError` (+ `closeRun` from Phase C); future-only + `replayBuffered` late-subscribe; drop-oldest backpressure surfaced as `{kind:"dropped",count}` (never drops control events). **Edge-safe, zero-dep** — plain Promise hand-off, no `node:events`. Caps `STREAM_SUBSCRIBER_BUFFER_CAP = STREAM_REPLAY_BUFFER_CAP = 256`.
- `packages/core/src/memory.ts` — `InMemoryStore` holds a hub; `subscribeStream`/`publishChunk` delegate.

### Phase C — wiring (`@nagi-js/core`)
- `packages/core/src/dispatch.ts` — `executeTask` builds a `StreamingStepCtx` for `kind:"streaming"`; `emit` publishes out-of-band via `store.publishChunk?` (never through `tx` — D3), guarded by an `emitActive` flag (emit-after-return is a no-op). Output-capture path byte-identical to `b.task`.
- `packages/core/src/memory.ts` — `appendFact` drives the hub fact-first: `step.completed`→`closeOk`, `step.failed`→`closeError`, `step.retried`→`signalRetry(attempt+1)`, `flow.completed|failed|canceled`→`closeRun`. `subscribeStream` consults durable facts and returns an empty closed stream if the step/run is already terminal (the authoritative no-hang guard).
- `packages/core/src/stream-hub.ts` — `closeOk`/`closeError` no longer create channels (leak-free for non-streaming steps); added `closeRun(runId)`.
- `packages/core/src/runtime.ts` — `Wf.subscribe<C=Json>(runId,stepId,opts?)` added + implemented (delegates to `store.subscribeStream`, O6 cast); static `stepId` validation throws `NagiRuntimeError` on a typo/non-streaming id; **D4 capability gate** throws at `nagi()` if a streaming flow is registered against a store without `subscribeStream`.

### Tests (new, isolated — purely #12)
- `packages/core/src/tests/streaming-task.test.ts` — 24 runtime tests: happy-path ordering + durable output; downstream `needs`; chunks-not-in-fact-log; fan-out; termination on complete; terminal `error` envelope on fail; retry `{attempt:2}` event; invariant guards (subscribe-after-complete/empty/typo-throws/skipped-step/closeRun); capability gate (via a capability-stripping Proxy); run+step scoping; emit-with-zero-subscribers; mid-run `replayBuffered` vs future-only.
- `packages/core/src/tests/streaming-replay.test.ts` (Phase D) — 8 tests: replay is emit-inert + durable output survives (inspect/continue modes), subscribe-after-replay empty, forced re-execution `replay(runId,{from})` re-runs the handler + recomputes output **without** re-emitting to a live subscriber (terminal-fact guard holds), retried-attempt chunks stay ephemeral.
- `packages/core/src/tests/stream-hub.test.ts` — 13 hub unit tests.
- `packages/core/src/tests/streaming-task.test-d.ts` — type-level: output-not-chunk inference, `emit` chunk typing + `@ts-expect-error`, `StreamEvent` narrowing, the **D2 guard** (a plain `b.task` ctx has no `emit`), `StepKind` membership, `wf.subscribe` Json-default + `subscribe<C>` element type + optional `{replayBuffered}` + branded-`RunId` rejection. Reconciled to your `ResolvedNeeds` refactor (downstream needs assert `Resolved<Output>`).
- `packages/core/src/tests/builder.test.ts` — added a `streamingTask` structural block (INTERMIXED with your refactor).

### Meta
- `docs/rfcs/0019-streaming-task.md` — RFC + decisions log (firm D1–D9, resolved O1–O6, unrepresentable-states table, outbox review, implementation notes).
- `docs/rfcs/0019-streaming-task.handoff.md` — this file.
- `.changeset/streaming-task.md` — `patch` for `@nagi-js/core` (per the 0.1.x patch rule — a `minor` would burn a release name).

## What was NOT done (intentional — per the resolved decisions)
- **Postgres streaming (O2).** `@nagi-js/postgres` leaves `subscribeStream` undefined on purpose; a Postgres flow + `streamingTask` throws at registration. Redis-Streams transport is a follow-up RFC (LISTEN/NOTIFY is unfit — 8 KB cap, pooled-conn drops).
- **Full type-level chunk inference on the consumer (O6).** `subscribe<C = Json>` is caller-asserted; the producer side is fully inferred. Cross-flow stepId→chunk inference was rejected (fragile, fails "complexity must pay for itself").
- **Durable streams / rewind / structured chunk schemas** — issue non-goals.

## Verification (2026-05-22, whole tree settled green)
```
pnpm -F @nagi-js/core typecheck     # clean (tsc --noEmit, no errors)
pnpm -F @nagi-js/core test          # 50 files · 743 tests pass · no type errors
# streaming subset in isolation:
pnpm -F @nagi-js/core exec vitest run \
  src/tests/streaming-task.test.ts src/tests/stream-hub.test.ts \
  src/tests/streaming-replay.test.ts --typecheck.enabled=false   # 44 tests pass
```
The full-suite green count **composes** #12 with your committed core refactor (`e451bfd`) and your uncommitted RFC 0020 work — they coexist cleanly. (Note: there was a transient mid-session window where the whole-package `tsc` went red from your then-untracked `state.ts`; that resolved once you landed the refactor + the `ResolvedNeeds` sweep, which also reconciled the streaming `.test-d` `needs` assertions to `Resolved<Output>`.)

## Caveats — sequencing needs your call
1. **Shared, intermixed files.** `types.ts`, `internal.ts`, `builder.ts`, `dispatch.ts`, `memory.ts`, `runtime.ts`, and `tests/builder.test.ts` carry **both** #12 and your concurrent state.ts/Fact refactor + RFC 0020 work. No clean `git add <file>` for #12 alone — needs `git add -p`-style hunk selection (your call). **Cleanly isolated #12 pieces:** `packages/core/src/stream-hub.ts`, the three new `streaming-task.*` / `stream-hub.test.ts` files, the `0019` docs, and the changeset.
2. **Earlier red tree — now resolved.** Mid-session your untracked `state.ts` broke the shared `tsc`; you've since landed the refactor and the tree is green. I never touched `state.ts` or any non-streaming source. Your `ResolvedNeeds` sweep edited `streaming-task.test-d.ts` (a #12 file) to assert `Resolved<Output>` — that edit is yours and is correct; flagging only so you know that file now carries a hunk from your sweep too.
3. **Active clobber risk.** Concurrent saves to `dispatch.ts`/`runtime.ts`/`memory.ts` can overwrite the streaming edits. If #12 isn't committed soon, consider moving it to an isolated `git worktree`.
4. **RFC numbering.** Used `0019` (next free above 0018; `0012` is taken by `shorthand-concurrency-config`). Your evolving convention aligns RFC docs to issue numbers — but #12's slot is occupied, so reconcile as you see fit; the rename (RFC + handoff + changeset reference) is yours.
5. **No commit / no PR made** — per your multi-feature-tree sequencing preference. PR materials are ready when you are.

## Files index (#12-authored)
```
docs/rfcs/0019-streaming-task.md           (RFC + decisions log)
docs/rfcs/0019-streaming-task.handoff.md   (this file)
.changeset/streaming-task.md               (patch: @nagi-js/core)
packages/core/src/stream-hub.ts            (new — InMemoryStreamHub; ISOLATED)
packages/core/src/tests/stream-hub.test.ts        (new — 13 hub tests; ISOLATED)
packages/core/src/tests/streaming-task.test.ts    (new — 24 runtime tests; ISOLATED)
packages/core/src/tests/streaming-replay.test.ts  (new — 8 replay/retry tests; ISOLATED)
packages/core/src/tests/streaming-task.test-d.ts  (new — type tests; carries one hunk from your ResolvedNeeds sweep)
packages/core/src/types.ts          (modified — INTERMIXED with state.ts/Fact refactor)
packages/core/src/internal.ts       (modified — INTERMIXED with PendingMatchDef refactor)
packages/core/src/builder.ts        (modified — INTERMIXED)
packages/core/src/canonicalize.ts   (modified — D8 routing; mostly isolated)
packages/core/src/dispatch.ts       (modified — emit + lifecycle; INTERMIXED)
packages/core/src/memory.ts         (modified — hub + fact-driven lifecycle; INTERMIXED)
packages/core/src/runtime.ts        (modified — Wf.subscribe + capability gate; INTERMIXED)
packages/core/src/tests/builder.test.ts  (modified — streamingTask block; INTERMIXED)
```
