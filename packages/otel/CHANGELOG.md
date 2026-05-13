# @nagi-js/otel

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
