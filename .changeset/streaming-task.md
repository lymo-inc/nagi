---
"@nagi-js/core": patch
---

Add `b.streamingTask` — a step primitive for LLM token streaming. Inside `run`,
the handler calls `ctx.emit(chunk)` to push ephemeral chunks to live subscribers
while still `return`ing a value captured as the durable `step.output` (identical
to `b.task` for downstream `needs`). Consumers read via
`wf.subscribe<C>(runId, stepId): AsyncIterable<StreamEvent<C>>`.

Chunks are deliberately ephemeral: they never enter the fact log, the canonical
flow hash, or replay — the final output is the only durable artifact. Delivery
is fan-out (every subscriber sees every chunk), future-only by default
(opt-in `{ replayBuffered: true }`), and non-blocking with bounded
per-subscriber buffers (drop-oldest, surfaced as a `{ kind: "dropped" }`
marker). The subscription element is a discriminated envelope
`StreamEvent<C>` (`chunk` / `dropped` / `retry` / `error`) so control markers
can never be confused with data. Stream termination is derived from the durable
terminal fact (`step.completed`/`step.failed`/run-terminal), so a consumer's
`for await` never hangs.

Streaming is an optional `Store` capability (`subscribeStream?`/`publishChunk?`):
the in-memory store implements it; a flow using `b.streamingTask` against a store
without the capability throws at registration. (`@nagi-js/postgres` streaming is
deferred to a follow-up RFC.)
