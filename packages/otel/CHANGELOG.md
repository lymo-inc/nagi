# @nagi-js/otel

## 0.1.1-rc.9

### Patch Changes

- RFCs #10, #11 implemented!
- Updated dependencies
  - @nagi-js/core@0.1.1-rc.9

## 0.1.1-rc.8

### Patch Changes

- f926424: `@nagi-js/otel` now nests a subflow child flow's root span under its
  parent's subflow-step span, producing a single contiguous trace tree from
  the top-level run down through arbitrary subflow depth. Previously each
  child run rooted its own trace, leaving operators to correlate parent and
  child via `nagi.run.id` / `nagi.parent.run.id` attributes by hand.

  `FlowStartEvent` gains an optional `parent: { runId, stepId, attempt }`
  struct (single optional, not three — by construction either all three
  parent fields are set or none are). `nagi()` populates it on every
  subflow-spawned run; top-level runs leave it `undefined`. The struct is
  in-process only; durable parent linkage continues to live on
  `FlowStartedFact.parentRunId / parentStepId` unchanged.

  `otelHooks.onFlowStart` resolves the child flow span's parent context via
  a three-step fallback: parent step span in registry → local parent flow
  context (`flowCtxs`) → OTel `context.active()`. Never throws. Records
  `nagi.parent.run.id`, `nagi.parent.step.id`, `nagi.parent.step.attempt`
  as attributes on the child flow span when `event.parent` is set so
  cross-process linkage remains queryable even when the in-process anchor
  is missing (process restart, replay).

  Top-level run traces are byte-equivalent to prior behavior. See
  `docs/rfcs/0010-otel-subflow-span-linkage.md`.

- Updated dependencies [f926424]
- Updated dependencies [b79ede2]
- Updated dependencies [f926424]
- Updated dependencies [f926424]
- Updated dependencies [f926424]
  - @nagi-js/core@0.1.1-rc.8

## 0.1.1-rc.7

### Patch Changes

- Implement RFCs #7 #9 #10
- Updated dependencies [c4e1459]
- Updated dependencies
- Updated dependencies [c4e1459]
- Updated dependencies [c4e1459]
  - @nagi-js/core@0.1.1-rc.7

## 0.1.1-rc.6

### Patch Changes

- Implement issue #5
- Updated dependencies
- Updated dependencies [735cea4]
  - @nagi-js/core@0.2.0-rc.6

## 0.1.1-rc.5

### Patch Changes

- Updated dependencies [c728826]
  - @nagi-js/core@0.1.1-rc.5

## 0.1.1-rc.4

### Patch Changes

- Realign release cohort: republish all four packages on the 0.1.x line.
  @nagi-js/core@0.2.0-rc.3 (and the otel/pgmq/postgres rc.3 cohort that
  pinned it as a workspace dep) was an unintended minor bump and will be
  unpublished from npm. No code changes — this changeset exists to produce
  a clean rc.4 cohort with core back on 0.1.x.
- Updated dependencies
  - @nagi-js/core@0.1.1-rc.4

## 0.1.1-rc.3

### Patch Changes

- fix rc tagging
- Updated dependencies
  - @nagi-js/core@0.2.0-rc.3

## 0.1.1-rc.2

### Patch Changes

- Updated dependencies [d67d361]
  - @nagi-js/core@0.2.0-rc.2

## 0.1.1-rc.1

### Patch Changes

- step hooks
- Updated dependencies
  - @nagi-js/core@0.1.1-rc.1

## 0.1.1

### Patch Changes

- Updated dependencies [2f4b9f0]
- Updated dependencies [2f4b9f0]
  - @nagi-js/core@1.0.0

## 0.1.0

### Minor Changes

- 3bceb7a: Implement the v0 `@nagi-js/otel` OpenTelemetry adapter.

  - `otelHooks(opts?)` — returns a `FlowHooks` value to pass to `nagi({ hooks })`. Maps every Nagi lifecycle event to OTel spans:
    - One `flow {flowId}` span per run (kind `INTERNAL`).
    - One sibling `step {stepId}` span per step attempt (kind `INTERNAL`). Retries become fresh sibling spans; the failed attempt's span carries `ERROR` status + `recordException`, and the flow span gets a `nagi.retry.scheduled` event with `nagi.next_attempt_at`.
    - Stable `nagi.*` attribute namespace: `nagi.flow.id`, `nagi.run.id`, `nagi.step.id`, `nagi.step.attempt`, `nagi.step.kind`, `nagi.step.duration_ms`. Plus `error.type` + `exception.*` (via `recordException`) on failure. Match-step `durationMs` is recomputed from the stashed start time (dispatch hard-codes `0`).
    - `defaultAttributes` option for every-span attrs (e.g. `deployment.environment`).
    - Custom `tracer` and span-name prefix options.
    - Adapter throws are swallowed via `console.error` — a misconfigured tracer can never crash a workflow.
  - `composeHooks(...hs)` — fan-out helper so users can wire `otelHooks()` alongside their own logger / metrics hooks. Awaits each subscriber in declaration order; per-subscriber throws are logged and do not block later subscribers.
  - `getStepSpan(ctx)` — module-level helper so user handlers can read the current step span and stamp custom attributes / open child spans. Note: the span is NOT installed as the active context in v0 (`trace.getActiveSpan()` inside handlers will not see it); this is a documented limit pending a core `Register` widening.
  - Peer-deps `@opentelemetry/api ^1.9.0` only. Never imports SDK packages — the host application wires the provider/exporter.
  - ESM-only, edge-compliant (`platform: "neutral"`, no `node:*`).
  - 40 tests pass: `compose.test.ts` (subscriber fan-out / error swallowing), `context.test.ts` (registry key derivation), `hooks.test.ts` (span hierarchy / attributes / status / retry / signal-received / out-of-order resilience) against an `InMemorySpanExporter`; `integration.test.ts` drives the hooks through a real `nagi()` runtime with a 2-step flow.
  - `onSignalSent` is intentionally not implemented — core declares the hook in `FlowHooks` but the runtime never fires it (see `Nagi Otel Package Research` §5.1).

### Patch Changes

- Updated dependencies [3bceb7a]
  - @nagi-js/core@0.1.0
