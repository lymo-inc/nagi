# Per-step / per-flow lifecycle hooks on step + flow config

**Date**: 2026-05-15 (JST)
**Issue**: [lymo-inc/nagi#1](https://github.com/lymo-inc/nagi/issues/1)
**Status**: research → implementation

## Goal

Add lifecycle hooks (`onStart` / `onComplete` / `onError` / `onRetry`) directly on `TaskConfig`, `StepEntryConfig`, and `FlowConfig` so side effects can be colocated with the step `run` / flow `output`. Top-level `FlowHooks` remains untouched for cross-cutting observers (Sentry, OTel, structured logging).

Primary win: `onComplete`'s event is typed as `StepCompleteEvent & { output: Output }` — the user gets `output.transcript.wordCount` typed as `number` for free, with no `switch` and no `Json` narrowing.

## Codebase map (what I needed to find before touching code)

### Type sites — `packages/core/src/types.ts`

| Type | Lines |
|---|---|
| `TaskConfig<Input, N, Output>` | 141-149 |
| `StepEntryConfig<Input, A, Needs, Output>` | 251-269 |
| `FlowConfig<Id, InputSchema, R, Output>` | 395-417 |
| `Flow<Id, InputSchema, M, Output>` | 419-429 |
| `FlowHooks` | 496-508 |
| `StepStartEvent` / `StepCompleteEvent` / `StepErrorEvent` / `StepRetryEvent` | 463-486 |
| `FlowStartEvent` / `FlowCompleteEvent` / `FlowErrorEvent` | 445-455 |
| `ReplayOpts` | 917-928 |

`Output` flows from `run`'s return type through TypeScript generic inference. In `TaskConfig` and `StepEntryConfig`, `Output` is already a free generic on the interface — `onComplete: (event: StepCompleteEvent & { output: Output })` reuses the same parameter and gets typed automatically.

### Internal def — `packages/core/src/internal.ts`

`TaskDef` (lines 25-40) is the durable form of a step. It stores `run`, `retry`, `when`, `timeoutMs`, `parentMatch`. Extending it with `onStart` / `onComplete` / `onError` / `onRetry` is the only structural change needed at runtime: every dispatch reads `getDef(step)` and now has hook fns in scope.

### Hook dispatch sites

Top-level `FlowHooks` fires from 11 sites:

**`packages/core/src/dispatch.ts`** (9 sites):
- L107 `onStepStart`
- L129 `onStepComplete` (task)
- L279 `onStepRetry`
- L303 `onStepError`
- L352 `onFlowError` (terminal failed)
- L376 `onFlowComplete`
- L431 `onFlowError` (cycle guard)
- L488 `onStepError` (match fail-fast)
- L510 `onStepComplete` (match completion)

**`packages/core/src/runtime.ts`** (2 sites):
- L245 `onFlowStart`
- L313 `onSignalReceived`

None of these are wrapped in try/catch today. A throwing hook propagates — for step hooks, into `handleStepError` (treating an observer bug as a step failure); for flow hooks, out of `advance()` into the worker's nack path. **This is a latent footgun**: the issue requirement to swallow+log is therefore an improvement to existing behavior, not just a rule for the new step-local layer.

### Builder — `packages/core/src/builder.ts`

`task()` (L75), `step()` (L176), `flow()` (L273). Each constructs the def by cherry-picking config fields. New hook fields need explicit forwarding at all three call sites (omitting fields when undefined keeps the canonical TaskDef shape unchanged for hash-equivalence).

### Canonicalize — `packages/core/src/canonicalize.ts`

`canonicalStep` (L143-184) uses an explicit allowlist: `whenHash`, `retry`, `timeoutMs`, `signalSchema`, `matchOnHash`, `matchArms`. **It does not enumerate over def fields.** Adding hooks to `TaskDef` does NOT affect the canonical DAG and therefore does NOT change `flowHash`. This matches RFC 0001's "handler code is not part of topology" — hooks are side-effect handlers and follow the same rule.

### Replay — `packages/core/src/runtime.ts:335-388`

`wf.replay()` reuses `dispatchDeps` (with `hooks: config.hooks`) when calling `advance()`. There is no current hook-suppression flag. The drift-allowed path synthesizes a flow with `liveDef` (lines 459-462) — `liveDef` will carry our new hook fields, so drift replay naturally inherits them.

For the `ReplayOpts.fireHooks?: boolean` escape hatch, the cleanest wiring is a flag on `DispatchDeps` itself. `advance()` and `dispatchMessage()` already thread `deps` everywhere; a single `fireHooks` bit on `deps` (default `true`) read inside the hook-fire helper kills both top-level and step-local hooks at once. No knob proliferation.

### Tests

`packages/core/src/dispatch.test.ts:215-269` has two existing `onStepStart` tests. Pattern: `makeHarness(f, { hooks: { ... } })` + assert event captured. `HarnessOpts.hooks` is wired to both `nagi()` and `DispatchDeps` (test-helpers.ts:177).

`packages/core/src/snapshot.test.ts:114-232` covers replay drift detection but doesn't assert hook firing. I'll extend it with a `fireHooks: false` assertion (no top-level + no step-local hook fires during replay when the flag is set).

## Design decisions

### 1. Output typing on `onComplete`

```ts
readonly onComplete?: (
  event: StepCompleteEvent & { readonly output: Output }
) => void | Promise<void>;
```

`Output` is the same generic parameter `run` uses. No new type machinery — TypeScript propagates inference through the interface.

`onStart` is *not* typed-augmented in this pass. The flow input is already typed as `Input` in `run({ input })`, but the StepStartEvent's `input: Json` field carries the post-validation Json form for cross-step uniformity (task: flow input; signal/match: `null`). The issue's API proposal also keeps `onStart` event un-augmented; matching that.

### 2. Step-local hooks fire before top-level (deterministic ordering)

```ts
await fireHook(def.onComplete, event, deps);          // step-local first
await fireHook(deps.hooks?.onStepComplete, event, deps); // then top-level
```

Two reasons:
- Predictable for users — they can reason about cause/effect (local triggers a Sentry span via top-level).
- Lets the cross-cutting layer observe what the local one did (e.g., a publishing-failure log line emitted by `onComplete` is in scope when the OTel span closes).

Both await — no parallelism. The whole hook tail runs on the dispatch thread before the next step is enqueued, matching today's serial semantics.

### 3. Hook errors are swallowed + logged

Current behavior: a throwing top-level hook propagates and corrupts the run. This is fixed alongside the addition — both step-local and top-level hooks now route through one `fireHook` helper that try/catches and logs via `deps.logger`. No hook of any layer can fail the run.

### 4. Replay defaults to firing hooks; `fireHooks: false` suppresses both layers

```ts
interface ReplayOpts {
  readonly mode: ReplayMode;
  readonly allowDrift?: boolean;
  readonly fireHooks?: boolean; // default true
}
```

Implementation: `runtime.ts` constructs `dispatchDeps` with `fireHooks: opts.fireHooks ?? true` for the replay path. `dispatch.ts`'s `fireHook` helper short-circuits when `deps.fireHooks === false`. Both step-local and top-level hooks honor the flag (a backfill-replay user wants either both to fire or neither — not a mix).

### 5. Match steps: hooks on the match config are out of scope for v1

The issue raises this as an open question. My research turned up that `promoteMatches` (dispatch.ts:488, 510) already fires top-level `onStepComplete` / `onStepError` for the whole match with the aggregated output — arms don't have their own observable lifecycle. Adding per-arm hooks would be a new design surface (output typing across arms is non-trivial: discriminator → union, guard → arm-union).

Decision: defer match-config hooks. The 38-publish-site migration from the issue uses task steps. Top-level `FlowHooks` continues to cover match observability via the existing dispatch sites. If the need surfaces post-v1, add `onComplete` to `MatchDiscriminatorConfig` / `MatchGuardConfig` as a non-breaking addition.

### 6. Signal steps: hooks on signal config are out of scope for v1

Similar reasoning — `wf.signal()` already fires top-level `onSignalReceived`. Adding `onSignalReceived` per-step config is a non-breaking follow-up; not load-bearing for the issue's use case.

## Implementation order

1. **`types.ts`** — extend `TaskConfig`, `StepEntryConfig`, `FlowConfig`, `Flow`, `ReplayOpts`. Pure additions, all optional.
2. **`internal.ts`** — extend `TaskDef` with hook fields. Stored at runtime-wide signatures (Json input/output); narrowed signatures live at the public config types.
3. **`builder.ts`** — propagate hook fields from `TaskConfig`/`StepEntryConfig` into `TaskDef`; propagate `FlowConfig` hooks into the returned `Flow`.
4. **`dispatch.ts`** — add `fireHooks?: boolean` to `DispatchDeps`; introduce a `fireHook` helper that wraps every hook call; rewrite all 9 dispatch sites to fire step-local then top-level through the helper. Add flow-level hook firing alongside the existing top-level flow hooks.
5. **`runtime.ts`** — fire flow-level `onStart` alongside top-level `onFlowStart`; thread `fireHooks` from `ReplayOpts` into the replay's `dispatchDeps`.
6. **Tests** — `dispatch.test.ts` for per-step / per-flow hooks (firing + ordering + error swallow + output typing assertion via TS structural check); `snapshot.test.ts` for `fireHooks: false`.

## Out of scope (deliberately)

- Match-config hooks (defer to v2).
- Signal-config hooks (defer to v2).
- Per-arm match hooks (decided against in §5).
- `StepLifecycleHooks` shared interface — premature unification; the three sites have different surface shapes already (signature with output, output-less, etc.). Revisit if v2 adds match/signal hooks.

## Migration in lymo-inc/lymo (downstream)

Per the issue: after this lands, delete `apps/backend/src/workflows/hooks/{on-flow-complete,on-flow-error,on-step-complete,on-step-error}.ts` and inline each webhook into its corresponding `b.step()`. Drops 4 dispatcher files; the 38-site migration becomes per-flow PRs.
