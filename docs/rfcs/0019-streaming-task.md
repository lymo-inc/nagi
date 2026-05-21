# RFC 0019 — `b.streamingTask`: streaming step outputs for LLM token streaming

- **Status:** Accepted (2026-05-21, Jay — decisions resolved via grill). **Implemented: Phases A–D** (types/builder; in-memory broadcast hub; dispatch `emit` + lifecycle + `wf.subscribe` + capability gate; replay/retry/edge + type-d tests). Whole `@nagi-js/core` suite green (50 files · 743 tests · typecheck clean, 2026-05-22), composing with the committed core refactor + uncommitted RFC 0020. **Not committed / no PR** — held for your sequencing (intermixed with RFC 0020 in shared files). See `0019-streaming-task.handoff.md`.
- **Author:** Claude (paired with @jay)
- **Created:** 2026-05-21 (JST)
- **Tracking:** GitHub issue #12 (note: RFC number ≠ issue number; RFC 0012 is `shorthand-concurrency-config`)
- **Decisions log:** authoritative — see "Firm decisions" and "Open decisions" below. Implementation is blocked on Jay's approval of this log.

## Summary

Add a new builder primitive `b.streamingTask`. Inside its `run`, the handler
calls `ctx.emit(chunk)` to push **ephemeral** chunks to live subscribers, and
still `return`s a value that is captured as the **durable** `step.output` —
exactly like `b.task`. A consumer reads chunks via
`wf.subscribe(runId, stepId): AsyncIterable<Chunk>`.

The streamed chunks are deliberately **outside** the durable fact log: they are
at-most-once, fan-out, future-only-by-default, and lost on replay. The final
output is the only durable artifact, and it flows to downstream steps
(`needs: ['generate']`) untouched.

This keeps Nagi's run-as-source-of-truth model intact while giving LLM steps the
token-streaming UX that, today, forces users to bypass nagi (streaming directly
from the handler to a side channel) — which is the exact failure the issue
names.

## Motivation

`b.task` returns its output only on completion. LLM steps want to stream tokens
to an SSE client / chat UI as they generate, while still capturing the final
text for downstream nagi steps. Without a primitive, users stream out-of-band,
and those chunks never appear in `step.output`, can't be observed via run-state
hooks, and break the abstraction. Inngest (a direct competitor) shipped exactly
this primitive for exactly this reason; the survey (below) confirms it's the
right layer to own the *emit point* even when the bytes stay ephemeral.

## Scope reconciliation (important)

A standing project note says *"Nagi is for multi-turn LLM backend workflows
only; realtime/fire-and-forget is out of scope."* Token streaming is
realtime-flavored, so this RFC must justify itself against that boundary rather
than ignore it.

The reconciliation: the **step stays a durable backend workflow step** — its
final output is captured, replayable, and drives downstream steps. Streaming is
an **ephemeral read-side projection** of a step that is otherwise fully durable.
We own the *emit point* (so streams correlate to runs/steps and stay
observable), but the *transport* is thin, ephemeral, and adapter-local. We do
**not** own durable streams, rewind/seek, or resumable stream protocols (those
would drag Nagi from "engine" into "streaming infrastructure" — see
Trigger.dev in the survey). This boundary is **Open decision O1** — the root
gate for everything below.

## External-design survey (condensed)

| System | How it streams from a durable step | Durable or ephemeral? |
| --- | --- | --- |
| **Temporal** | *Refuses* to stream activity output; bytes go off-substrate, only control (signals/heartbeats) on it | n/a (off-log by design) |
| **Inngest Realtime** | `publish()` (non-durable, outside step log) vs `step.realtime.publish()` (durable, memoized); WebSocket channels keyed by runId; at-most-once; fan-out | **both**, recommends ephemeral for tokens |
| **Trigger.dev Streams v2** | Durable streams backed by S2, 28-day retention, resumable via `startIndex` | **durable** (a whole separate product) |
| **DBOS** | `event_stream_handler` out-of-band; forces in-order emission for deterministic replay | ephemeral |
| **LangGraph** | `StreamWriter` / `get_stream_writer` (≈ `ctx.emit`); checkpoints only *between* nodes | ephemeral (mid-node lost on replay) |
| **Vercel AI SDK** | `streamText` + lazy `ReadableStream` (pull backpressure); `consumeStream()` drains server-side on client disconnect; resumability bolted on via Redis | ephemeral; durability is transport-layer |
| **Cloudflare Workflows** | `step.do()` can return a stream but it counts against storage; real-time delivery is a *separate* DO/Agents layer | split by design |

**Takeaways that shaped the decisions:** (1) every event-sourced engine except
Trigger.dev keeps streamed bytes ephemeral; (2) Postgres `LISTEN/NOTIFY` has a
hard 8 KB payload cap (bytes incl. channel name) and silently drops on pooled
connections — **unsuitable as the chunk pipe**; (3) unbounded buffers are the
universal OOM path; (4) fan-out must be per-subscriber-cursored so a slow
consumer can't stall the producer; (5) emit must be **replay-inert** so a
memoized step doesn't re-stream tokens the LLM never regenerated.

## Firm decisions (my confident calls — flag any to revise)

> These align with the issue, the survey, and the project's design memories
> (unrepresentable invalid states; optionality only at the boundary; complexity
> must pay for itself). I'm treating them as settled unless you push back.

- **D1 — Ephemeral chunks, durable output only.** Chunks never enter the fact
  log, the canonical flow hash, or replay. The `step.completed` fact (with
  `output`) is written exactly as for `b.task`, via the existing
  `store.runStep` transactional path (`dispatch.ts:281-289`). Unchanged.

- **D2 — `emit` lives on a distinct `StreamingStepCtx`, not as an optional
  field on `StepCtx`.** A regular `b.task` handler's `ctx` has **no** `emit`
  member at the type level; only `b.streamingTask`'s `run` receives
  `StreamingStepCtx<Input, Chunk>` (extends `StepCtx<Input>` with
  `emit(chunk: Chunk): Promise<void>`). Calling `emit` from a non-streaming task
  is a *compile error*, not a runtime guard. (Rejects the auditor's
  `emit?: ...` optional-field suggestion — that would make misuse representable.)

- **D3 — `emit` publishes out-of-band, never through `tx`.** Because `def.run`
  runs inside `store.runStep`'s transaction (`dispatch.ts:246`), publishing
  through the txn connection would make chunks invisible until commit (fatal for
  Postgres `pg_notify`). `emit` writes to a transport handle that is independent
  of `tx`. **Stream termination is derived from the durable terminal fact**
  (`step.completed`/`step.failed`), *not* from an ephemeral "done" marker — so a
  consumer's `for await` always terminates correctly even if it missed an
  end-of-stream chunk. (See Outbox review.)

- **D4 — Capability gated at registration (throw), mirroring
  `Queue.ensureSchema?`.** `Store.subscribeStream?` is an optional method.
  At `nagi()` registration, if any registered flow contains a `streaming` step
  and `store.subscribeStream === undefined`, throw `NagiRuntimeError` naming the
  offending step and the missing capability. Fail at boot, never at first
  dispatch. (The *type-level* gating ambition is Open decision O6.)

- **D5 — In-memory transport = per-`(runId, stepId)` broadcast hub** with
  per-subscriber queues (novel infra in `memory.ts`; no shared primitive today).

- **D6 — Fan-out / broadcast semantics.** Every subscriber sees every chunk,
  each with an independent cursor. (Issue-decided; survey-validated.)

- **D7 — Late subscribers are future-only by default**, with opt-in
  `wf.subscribe(runId, stepId, { replayBuffered: true })`. `replayBuffered` is
  best-effort against a **bounded** in-process buffer that is **dropped when the
  step reaches a terminal fact** — so "subscribe after completion with
  `replayBuffered`" yields an empty, closed stream (the buffer is gone). This
  keeps the feature ephemeral and bounds memory. (Issue-decided default;
  buffer-retention call is mine.)

- **D8 — `StepKind` gains `"streaming"`; builder method is `b.streamingTask`;
  internal def is `StreamingTaskDef`.** For scheduling/needs/skip purposes a
  streaming step behaves identically to a task; only output capture gains the
  side-channel. `canonicalize.ts` and `synthesizeReplayFlow` (`runtime.ts:1109`)
  handle `"streaming"` as a task-equivalent shape so the flow hash and
  replay-from logic stay correct. (One-word kind matches the existing
  `task|signal|match|subflow` convention.)

- **D9 — Changeset is `patch`.** New public API would normally be `minor`, but
  in `0.1.x` a `minor` burns a release name (`0.1.x→0.2.0`); per the project's
  changeset rule we ship features as `patch` until 1.0.

## Resolved decisions (grilling, 2026-05-21, Jay)

> These were the open branch points. All resolved one-at-a-time. Where Jay's
> call diverged from my recommendation, it's marked **[diverged]** — those are
> deliberate, and they consistently chose *more explicit / more type-safe*.

- **O1 (root gate) — Accept streaming into the engine?** **Yes — accept into
  core, ephemeral** (matches my rec). In scope *because* the step stays durable
  (output captured, replayable, drives `needs`); streaming is an ephemeral
  read-side projection. Ships as normal public API in `@nagi-js/core` (not
  flagged experimental). Reconciles the "realtime out of scope" memory: we own
  the emit point, not durable streams.

- **O2 — v1 transport scope.** **In-memory adapter only** (matches my rec).
  `@nagi-js/postgres` leaves `subscribeStream` undefined → Postgres +
  `streamingTask` throws at registration (D4). Postgres transport (Redis Streams
  per the survey) is a **follow-up RFC**, explicitly out of scope here.

- **O3 — Backpressure.** **Non-blocking `emit`; per-subscriber bounded ring
  buffer; drop-oldest on overflow + a `{ kind: "dropped", count }` marker**
  (matches my rec). Producer never blocks on the slowest consumer; memory is
  bounded; a lagging consumer learns it lagged and can fall back to the durable
  final output.

- **O4 — Stream element shape / failure surfacing. [diverged]** **Discriminated
  envelope** `StreamEvent<C>` carrying control *and* data, rather than raw
  chunks + out-of-band control. The iterator ends when the step reaches a
  terminal fact; a terminal **failure surfaces as a final `{ kind: "error" }`
  event** (not a thrown rejection, not a silent graceful end). Chosen because
  O3's in-band `dropped` marker already forces control events into the stream,
  and an envelope makes "is this a real chunk or a framework marker?"
  *unrepresentable-to-confuse* — aligns with the unrepresentable-states memory.

- **O5 — Retry semantics. [diverged]** **Add a `{ kind: "retry", attempt }`
  event** to the envelope (my rec was "continuous, no marker"). A subscriber
  attached before the first attempt sees attempt-1 chunks, then a `retry` event,
  then the new attempt's chunks — so a chat UI can clear and re-render. A
  failed-but-will-retry attempt does **not** emit `error`; only final
  retry-exhaustion (terminal `step.failed`) emits `error`. Consistent with O4's
  explicit-control philosophy.

- **O6 — `subscribe` typing. [diverged]** **Explicit generic with Json
  default**: `subscribe<C = Json>(...): AsyncIterable<StreamEvent<C>>` (my rec
  was Json-only; the full-inference alternative was rejected as failing
  "complexity must pay for itself"). The **producer side is fully inferred**
  regardless (D2: `StreamingStepCtx<Input, Chunk>` infers `Chunk` from the `run`
  handler). The consumer names `C` once at the call site; it is caller-asserted,
  not cross-checked against the step (acceptable: `runId`/`stepId` are runtime
  values, so full inference would be fragile and only fire on literal stepIds).

## Proposed shape

### `types.ts`

```ts
export type StepKind = "task" | "signal" | "match" | "subflow" | "streaming";

// The subscription element — a discriminated control+data envelope (O4 + O5).
export type StreamEvent<C = Json> =
  | { readonly kind: "chunk"; readonly chunk: C }
  | { readonly kind: "dropped"; readonly count: number }          // O3 lag marker
  | { readonly kind: "retry"; readonly attempt: AttemptNumber }   // O5 restart
  | { readonly kind: "error"; readonly error: SerializedError };  // O4 terminal fail

export interface StreamingStepCtx<Input = unknown, Chunk = Json>
  extends StepCtx<Input> {
  // emit takes the RAW chunk; the hub wraps it as { kind: "chunk", chunk }.
  readonly emit: (chunk: Chunk) => Promise<void>;
}

export interface StreamingTaskConfig<Input, N extends NeedsMap, Output, Chunk>
  extends StepConfigBase<Input, N> {
  readonly retry?: RetryPolicy;
  readonly run: (args: {
    readonly input: NoInfer<Input>;
    readonly needs: NoInfer<NeedsOutputs<N>>;
    readonly ctx: StreamingStepCtx<NoInfer<Input>, Chunk>;
  }) => Promise<Output>;
  // onStart/onComplete/onError/onRetry identical to TaskConfig
}

// Store gains an optional capability (mirrors Queue.ensureSchema?):
export interface Store {
  // ...existing methods...
  subscribeStream?(
    runId: RunId,
    stepId: StepId,
    opts?: { readonly replayBuffered?: boolean },
  ): AsyncIterable<StreamEvent<Json>>;
  // write-side: ctx.emit pushes a raw chunk; the hub wraps + fans out.
  // dropped/retry/error events are hub/dispatch-generated, not published here.
  publishChunk?(runId: RunId, stepId: StepId, chunk: Json): void;
}
```

### `Builder` + `Wf`

```ts
// Builder<Input> gains:
streamingTask<N extends NeedsMap, O, C = Json>(
  config: StreamingTaskConfig<Input, N, O, C>,
): Step<O>;   // NB: Step<O> — the Output, not the Chunk

// Wf<TFlows> gains (O6: explicit generic, Json default):
subscribe<C = Json>(
  runId: RunId,
  stepId: StepId,
  opts?: { readonly replayBuffered?: boolean },
): AsyncIterable<StreamEvent<C>>;
```

`b.streamingTask` returns `Step<O>` so `needs` threading is *identical* to
`b.task` — downstream steps see the durable output type, never the chunk type.
The chunk type `C` lives only on the producer's `StreamingStepCtx` (inferred)
and the consumer's `subscribe<C>` (caller-asserted).

### Execution wiring (`dispatch.ts`)

`executeTask` branches on `def.kind === "streaming"`: it builds a
`StreamingStepCtx` whose `emit` calls the store's out-of-band write-side
(D3 — independent of `tx`), then runs `def.run` exactly as today. The
output-capture path (`dispatch.ts:281-289`) is unchanged. Event generation by
source:

- **`chunk`** — from `ctx.emit(raw)` during `run`; the hub wraps it.
- **`dropped`** — from a per-subscriber buffer overflow (hub-internal, O3).
- **`retry`** — emitted by the dispatch layer when it schedules a retry of a
  streaming step (it knows the attempt number), via a hub `signalRetry` call.
- **`error`** / **end** — derived from the durable terminal fact: on
  `step.failed` (retries exhausted) the hub emits a final `error` then closes;
  on `step.completed` it just closes. Termination is driven by durable state,
  not an ephemeral end-marker (D3), so consumers never hang.

## Unrepresentable-states analysis

| Invalid state | Prevented by |
| --- | --- |
| Calling `emit` from a non-streaming `b.task` handler | **Structural (compile error):** `emit` exists only on `StreamingStepCtx`, handed only to `streamingTask.run` (D2). A `b.task` `ctx` has no such member. |
| A `streamingTask` flow running against a store with no stream transport | **Registration throw (D4).** *Partially* structural — fully structural form (compile error) is Open decision O6/typed `StreamingCapableStore`. |
| Chunks polluting the durable fact log / changing the flow hash / re-emitting on replay | **Structural:** chunks never touch `store.runStep`/`appendFact`; emit is replay-inert because a memoized step's `run` is not re-invoked (D1/D8). |
| A consumer hanging forever because it missed the ephemeral "stream done" chunk | **Structural:** termination is derived from the durable terminal fact, not an ephemeral marker (D3). |
| Confusing a framework control marker (`dropped`/`retry`/`error`) with a real data chunk | **Structural:** `StreamEvent<C>` is a discriminated union (O4); a real chunk only ever arrives as `{ kind: "chunk", chunk }`, so a consumer that switches on `kind` cannot mistake one for the other. |
| Unbounded memory from a slow/absent consumer | Bounded per-subscriber buffer + drop-oldest (O3). *Not* structural — a runtime bound. |

**Still representable, accepted as invariant:** `emit` called after `run`
resolved (e.g. a dangling `setTimeout` in the handler). Recommendation: swallow
as a no-op rather than throw, since attempt/abort races make a throw
user-hostile. Documented as an invariant, not a type.

## Outbox / crash-recovery / transaction review

This feature is the **inverse of an outbox**. An outbox atomically commits state
*and* an event-to-publish so the event is never lost. Here we deliberately want
the opposite guarantee for chunks: **at-most-once, non-transactional,
fire-and-forget.** The reliability argument:

- **Chunks are not in the outbox and must not be.** Putting per-token chunks
  through the transactional fact log would (a) bloat the log, (b) on Postgres,
  defer delivery to commit (D3) — i.e. no streaming at all. So `emit` is
  explicitly *outside* `tx`.
- **The durable boundary is the terminal fact, and it already is an outbox.**
  `step.completed`/`step.failed` is the transactional commit. Consumers derive
  *both* the data they care about for replay (final output, from the fact log)
  *and* stream termination (the channel closes when the terminal fact lands)
  from this single durable event. There is no second ephemeral signal that can
  be lost and strand a consumer.
- **Crash mid-stream:** chunks emitted before the crash are gone (ephemeral, by
  design). On restart, the step's `run` re-executes (it never committed a
  terminal fact), re-emitting to whoever is currently subscribed. The final
  output, once committed, survives. This matches LangGraph (no mid-node
  checkpoints) and Inngest's recommended non-durable mode.
- **Postgres-specific (if/when O2 expands):** `pg_notify` must fire on a
  connection *outside* the step transaction, or be replaced by Redis Streams
  (survey recommendation — `LISTEN/NOTIFY`'s 8 KB cap and
  pooled-connection drop make it unfit as the chunk pipe; reserve it for a
  control signal only).

## Testing

Acceptance criteria come from the test-spec phase (~52 runtime `it` names + ~17
type-level). Groups: emit→subscribe happy path & ordering; durable-output vs
ephemeral-chunks; downstream `needs`; subscription timing (before/late/after);
fan-out; termination signaling; failure mid-stream (O4); emit lifecycle guards;
adapter capability (D4); replay (emit replay-inert); retry (O5); subscription
scoping by `(runId, stepId)`; non-goal guard tests. Type-level: `emit` chunk
type, `subscribe` return type (O6), downstream `needs` inferring **Output not
Chunk**, `StepKind` includes `"streaming"`. Files:
`packages/core/src/tests/streaming-task.test.ts` and `.test-d.ts`. The in-memory
harness (`test-helpers.ts: makeHarness`) is the vehicle; a `collect(iter)`
helper drains a subscription.

## Implementation notes (Phases A–C, 2026-05-21)

Refinements made while implementing — all consistent with the decisions above;
recorded so the log stays authoritative:

- **Lifecycle is fact-driven inside `InMemoryStore`, zero new `Store` methods.**
  `appendFact` (the single chokepoint) drives the hub: `step.completed`→`closeOk`,
  `step.failed`→`closeError`, `step.retried`→`signalRetry(attempt+1)`, and the
  run-terminal facts (`flow.completed`/`flow.failed`/`flow.canceled`)→`closeRun`.
  This realizes D3 ("termination derived from durable facts") with no Store-port
  bloat (only the Phase-A `subscribeStream?`/`publishChunk?` capability methods).
- **Closed-ness is authoritative in the fact log, not the hub.** `closeOk`/
  `closeError` no longer create channels (so non-streaming steps leak nothing);
  `subscribeStream` returns an immediately-closed empty stream when the step or
  the run is already terminal. This kills the "subscribe to a typo'd / silent /
  skipped step hangs forever" failure mode.
- **`emit` no-op after return** is enforced at the ctx boundary via an
  `emitActive` flag flipped in a `finally` (not relying on the hub).
- **`signalRetry` also clears the channel's replay buffer** so a `replayBuffered`
  late subscriber can't replay a superseded attempt's chunks (extends D7's
  ephemeral intent).
- **`wf.subscribe` fail-loud (revised minor call):** instead of "throw on unknown
  stepId" via run→flow resolution, it statically validates `stepId` against the
  set of `kind:"streaming"` step ids across registered flows and throws
  `NagiRuntimeError` on a typo / non-streaming id. Hangs from a run/flow mismatch
  are still prevented by the run-terminal `closeRun` guard.
- **Buffer caps:** `STREAM_SUBSCRIBER_BUFFER_CAP = STREAM_REPLAY_BUFFER_CAP =
  256` (item-count bounds for v1; documented, tunable later).

## Alternatives considered

- **`emit?` optional on the shared `StepCtx`** (D2 inverse): one ctx type, but
  makes "emit from a normal task" representable. Rejected — violates the
  unrepresentable-states principle.
- **Chunks in the fact log / durable streams** (D1 inverse, Trigger.dev model):
  enables resume/replay of streams but is a separate product (durable storage,
  retention, resume protocol) and breaks the ephemeral boundary. Rejected as
  non-goal.
- **Block the producer on backpressure** (O3 alt): simplest fan-out, but couples
  LLM generation to the slowest SSE client and can stall the step. Disfavored.
- **Postgres LISTEN/NOTIFY as the chunk transport** (O2 alt): 8 KB cap +
  pooled-connection drops; needs chunk-by-reference which reintroduces durable
  writes. Disfavored — Redis Streams if/when Postgres streaming ships.

## Resolved questions — quick reference (2026-05-21, Jay)

| # | Question | Decision |
| --- | --- | --- |
| O1 | Accept streaming into the engine? | **Yes** — core, ephemeral, not experimental |
| O2 | v1 transport scope | **In-memory only**; Postgres deferred (throws at registration) |
| O3 | Backpressure | Non-blocking emit; bounded ring buffer; **drop-oldest + `dropped` marker** |
| O4 | Stream shape / failure | **Discriminated `StreamEvent<C>` envelope**; failure = final `error` event |
| O5 | Retry | **Add `retry` event**; only exhaustion emits `error` |
| O6 | `subscribe` typing | **`subscribe<C = Json>`** (producer fully inferred via `StreamingStepCtx`) |

See "Resolved decisions" above for full reasoning. O4/O5/O6 diverged from my
recommendation toward more explicit / more type-safe shapes.
