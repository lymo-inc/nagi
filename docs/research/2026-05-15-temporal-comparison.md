# What Nagi can learn from Temporal.io

**Date**: 2026-05-15 (JST)
**Status**: research
**Related**: [RFC 0004 multi-name signal waits](../rfcs/0004-multi-name-signal-waits.md)
**Sources surveyed**:
[temporalio/temporal](https://github.com/temporalio/temporal) (Go server),
[temporalio/sdk-typescript](https://github.com/temporalio/sdk-typescript),
[temporalio/samples-typescript](https://github.com/temporalio/samples-typescript),
[docs.temporal.io/encyclopedia](https://docs.temporal.io/encyclopedia).

## Why look at Temporal at all

Nagi's scope (per [`project_nagi_scope`](../../.. memory)) is **multi-turn LLM backend workflows**: long-running, signal/webhook-driven, human-in-the-loop pauses, replayable across worker restarts. Temporal solves the same correctness problem (durable execution) for a much wider workload (every kind of orchestration at Stripe / Snap / Coinbase scale). Most of Temporal's surface area is overkill for Nagi — but the *primitives* underneath have ~7 years of production hardening and answer many questions Nagi will face. This doc cherry-picks.

## TL;DR

- **Borrow**: event-log-as-source-of-truth, command/event split, `patched()`-style versioning markers, `CancellationScope`-style structured cancellation, `defineSignal` symbol-as-contract, two-mode test environment (auto-skip + manual-advance time), `typeof activities` type propagation.
- **Skip**: V8 isolate sandbox, Worker Versioning, ScheduleToStart timeouts by default, sticky-execution caching as a v1 concern, server-side persistence layer with its own matching/history/frontend services, bundler-as-runtime-dependency.
- **Direct hit on RFC 0004**: Temporal has *no* first-class "wait for any of N signals" API — users compose `Trigger + Promise.race` or two handlers + one `condition()`. Nagi's `b.signal({ names })` is *better prior art*. But: Temporal's edge-case work on buffering, late arrivals, drain-on-completion, and duplicate signals surfaces ≥6 cases RFC 0004 should pin down before merging (see §[RFC 0004 follow-ups](#rfc-0004-follow-ups)).

---

## 1. Durable execution: event-sourced history

Temporal persists an **append-only Event History per workflow execution** on the server side. The full event taxonomy lives in [`temporal/api/enums/v1/event_type.proto`](https://github.com/temporalio/api/blob/master/temporal/api/enums/v1/event_type.proto) — **61 event types**. The shape splits into four families:

| Family | Examples |
|---|---|
| Lifecycle | `WORKFLOW_EXECUTION_STARTED/COMPLETED/FAILED/TIMED_OUT/CANCELED/CONTINUED_AS_NEW` |
| Task plumbing | `WORKFLOW_TASK_SCHEDULED/STARTED/COMPLETED/FAILED/TIMED_OUT` |
| Side effects | `ACTIVITY_TASK_SCHEDULED/STARTED/COMPLETED/FAILED/TIMED_OUT/CANCELED`, `TIMER_STARTED/FIRED`, `MARKER_RECORDED` |
| External coupling | `WORKFLOW_EXECUTION_SIGNALED`, `*_UPDATE_ADMITTED/ACCEPTED/REJECTED/COMPLETED`, `START_CHILD_WORKFLOW_*`, `SIGNAL_EXTERNAL_*`, `NEXUS_OPERATION_*` |

**Key invariant**: workers never write to history. They propose *commands* inside a `WorkflowTaskCompleted` event; the server validates and materializes them as new events. This is what makes the log the single source of truth and matches Nagi's [`feedback_unrepresentable_invalid_states`](../../.. memory) principle — workflow code physically cannot mutate persisted state.

**For Nagi**: borrow the pattern, radically shrink the alphabet. Probably ~8 event kinds suffice — `RunStarted`, `StepScheduled`, `StepCompleted`, `StepFailed`, `SignalReceived`, `TimerFired`, `RunCompleted`, `RunFailed`. Resist mirroring `WORKFLOW_TASK_*` (it exists because Temporal separates decide-from-do across a network).

## 2. Determinism and replay

Workers rebuild workflow state by **re-executing the workflow function from line 1**, feeding history into the same await points. For this to work, every observable side effect must come from history, not the host.

The TS SDK enforces this by replacing globals inside a V8 workflow VM ([`packages/workflow/src/global-overrides.ts`](https://github.com/temporalio/sdk-typescript/blob/main/packages/workflow/src/global-overrides.ts)):

- `Date()` no-arg and `Date.now()` → `getActivator().now` (frozen at task start)
- `Math.random()` → seeded PRNG from a history-derived seed
- `setTimeout`/`clearTimeout` → command-backed timers (`TIMER_STARTED`/`TIMER_FIRED`)
- `WeakRef` / `FinalizationRegistry` → throw `DeterminismViolationError` (GC is observable)
- Network/file/threads → no Node globals in the isolate, so any I/O must go through `scheduleActivity`

Non-determinism caught at replay time throws a Non-Determinism Error and fails the workflow *task* (not the workflow) so you can fix-and-redeploy.

**For Nagi**: override the same five globals (Date, Math.random, setTimeout, WeakRef, FinalizationRegistry) inside a lightweight `vm.Context` — full V8 isolates are the cost center for every other ergonomic problem (Bun/Deno, Vite, AsyncLocalStorage, Next.js, browser, +150 MB of @swc/core). A lint rule + a "you called `fetch` outside a step" runtime guard buys ~95% of sandbox safety at ~1% of the complexity.

## 3. Signals, queries, updates (relevant to RFC 0004)

### The three primitives

```ts
defineSignal<Args>(name)   // client → wf, no return, async handler ok
defineQuery<Ret>(name)     // client ← wf, sync, NO mutation
defineUpdate<Ret, Args>(n) // client ↔ wf, sync validator + async handler, returns value
setHandler(def, handler | undefined, options?)
condition(predicate, timeout?): Promise<void | boolean>
```

| Aspect | Signal | Query | Update |
|---|---|---|---|
| Mutates state | yes | **no** | yes |
| Returns value | no | yes (sync) | yes (async-trackable) |
| Validator | no | no | yes (sync, can reject pre-history) |
| Adds to history | yes | no | yes |
| Buffered if no handler | yes (indefinitely) | n/a | rejected at activation end |

### How "wait for a signal" actually works

Two idioms in the TS SDK — **there is no `awaitAny([signal1, signal2])` first-class API**:

1. **Mutate-and-`condition`** (canonical). Handlers flip variables; one `condition(() => predicate)` awaits the join. See `samples-typescript/expense/src/workflows.ts` — `approveSignal` and `rejectSignal` both mutate `status`, then `await condition(() => status !== TIMED_OUT, timeout)`.
2. **`Trigger<T>` + `Promise.race`** ([`packages/workflow/src/trigger.ts`](https://github.com/temporalio/sdk-typescript/blob/main/packages/workflow/src/trigger.ts)). A `PromiseLike` with public `.resolve` / `.reject` so a signal handler can resolve a future directly. CancellationScope-aware (auto-rejects on scope cancel).

```ts
const userInteraction = new Trigger<boolean>();
setHandler(completeUserInteraction, () => userInteraction.resolve(true));
const userInteracted = await Promise.race([userInteraction, sleep('30 days')]);
```

The deeper reason Temporal hasn't built first-class disjunction: signals are stateful inputs whose effects often need to land in workflow state *regardless* of who wins. Two-handler + single-condition keeps that observable.

### Buffering, ordering, drain semantics

From [`packages/workflow/src/internals.ts`](https://github.com/temporalio/sdk-typescript/blob/main/packages/workflow/src/internals.ts):

- **Buffered indefinitely** when no handler is registered for that name; flushed in FIFO order when `setHandler` lands.
- **Signal jobs ordered ahead** of other jobs within an activation — workflow code observes signal-driven state changes before unrelated timers/activities resolve in the same task.
- **Late signals after completion**: silently dropped server-side.
- **Drain-on-completion** safety: workflow exposes `allHandlersFinished()` so async-handler workflows can `await condition(allHandlersFinished)` before returning. Each handler has a `HandlerUnfinishedPolicy` (default `WARN_AND_ABANDON`).
- **No server-side dedup for signals.** Two clients sending `approve` → two invocations. Updates support a per-call `updateId` for idempotency.

### Schema / validation

Zero runtime help for signals — `defineSignal<[ApproveInput]>('approve')` is a pure type-level brand. Updates have a synchronous `validator` slot (`ApplicationFailure` rejects before history is written). Async validation isn't supported there.

## 4. Workers, time, retries, timeouts

### Worker concurrency model

Per-slot tuning, not a single worker count ([`packages/worker/src/worker-options.ts`](https://github.com/temporalio/sdk-typescript/blob/main/packages/worker/src/worker-options.ts)):

| Knob | Default |
|---|---|
| `maxConcurrentWorkflowTaskExecutions` | 40 |
| `maxConcurrentActivityTaskExecutions` | 100 |
| `maxConcurrentLocalActivityExecutions` | 100 (in-process) |
| `maxConcurrentWorkflowTaskPolls` | min(10, executions) |
| `maxActivitiesPerSecond` | unset (client-side rate limit) |
| `maxTaskQueueActivitiesPerSecond` | unset (server-side rate limit) |

Pollers decoupled from executors so a backlog can saturate slots.

### Sticky execution

Once a worker first processes a workflow task, the server dispatches subsequent tasks for that workflow to a worker-private **sticky queue** and the worker keeps state cached in memory (`maxCachedWorkflows`). If the worker doesn't accept within `stickyQueueScheduleToStartTimeout` (SDK default 10s, server default 5s), the task falls back to the shared queue and another worker rebuilds from history. `reuseV8Context: true` (default) shares a single V8 context across cached workflows — ~2/3 memory, ~1/2 CPU savings.

**For Nagi**: skip stickiness in v1, but **design for it**. Persist commands (timer scheduled, signal awaited, activity dispatched), not state — that's what makes additive in-memory caching possible later.

### Retry policy

Activities retry by default; workflows do not. Default `RetryPolicy`:

- `initialInterval: 1s`, `backoffCoefficient: 2.0`, `maximumInterval: 100s`
- `maximumAttempts: ∞` (capped by `ScheduleToCloseTimeout`)
- `nonRetryableErrorTypes: []`

Errors marked non-retryable via `ApplicationFailure.nonRetryable(...)` from inside the activity always win over policy. Critically, retry state lives in history as `retryState=IN_PROGRESS` + backoff timer — history doesn't bloat per attempt.

### Time

`workflow.sleep(ms)` issues a `startTimer` command keyed by a deterministic sequence number; the server persists and fires `TimerFired`. A workflow can sleep months and survive restarts. Tests use `TestWorkflowEnvironment.createTimeSkipping()` — a multi-day sleep runs in milliseconds.

### Timeout matrix

Four orthogonal timeouts because each detects a different failure:

| Timeout | Default | Detects |
|---|---|---|
| `ScheduleToStart` | ∞ | Queue/capacity health (workers not draining) |
| `StartToClose` | = ScheduleToClose | Single-attempt cap (silent worker death after pickup) |
| `ScheduleToClose` | ∞ | Total wall-clock across all retries |
| `Heartbeat` | unset | Mid-execution hang (cancellation delivered via heartbeat response) |

Without heartbeats, an activity *cannot* be cancelled mid-run.

**For Nagi (LLM workloads)**: minimum is `StartToClose` (single-call cap) + `ScheduleToClose` (overall cap across retries). Skip `ScheduleToStart` unless multi-region routing materializes. Make `Heartbeat` opt-in for streaming/tool-loop steps.

## 5. Cancellation (`CancellationScope`)

Workflow cancellation is **structured**, not a raw `AbortSignal` ([`packages/workflow/src/cancellation-scope.ts`](https://github.com/temporalio/sdk-typescript/blob/main/packages/workflow/src/cancellation-scope.ts)). Scopes form a tree rooted at the workflow's main function:

- `CancellationScope.cancellable(fn)` — inherits parent cancellation
- `CancellationScope.nonCancellable(fn)` — **shield**: parent cancel doesn't propagate (wrap cleanup/compensation here)
- `CancellationScope.withTimeout(ms, fn)` — self-cancels at deadline
- `scope.cancelRequested: Promise<never>` — race-able promise that rejects on cancel
- `scope.cancel()` / `CancellationScope.current()` — manual control

Cascades down: cancel a scope, cancel all descendant timers, activities, child workflows. Child workflows have `ParentClosePolicy = TERMINATE | ABANDON | REQUEST_CANCEL`.

**For Nagi**: human-in-the-loop pauses + webhook signals + LLM calls compose *poorly* with raw `AbortController`. Adopt scope tree + shielding semantics — this is the right primitive for "user cancelled the run, but still persist the partial summary."

## 6. Versioning long-running workflows

Two independent mechanisms:

- **`patched(patchId)` / `deprecatePatch(patchId)`** ([`packages/workflow/src/workflow.ts`](https://github.com/temporalio/sdk-typescript/blob/main/packages/workflow/src/workflow.ts)). First call writes a `MARKER_RECORDED` event. On replay: marker present → return `true`; live (non-replay) → return `true` and record marker; replay of old history without marker → return `false`. Lets you branch new vs. old behavior, then later delete the old branch with `deprecatePatch` (which writes the marker unconditionally so older histories with the marker still replay cleanly).
- **Worker Versioning**: deployment-level — workers register a Build ID; server tags each workflow with the Build ID of its starting worker. `Pinned` → complete on that exact version forever. `Auto-Upgrade` → hop to current (still need `patched()` for in-flight changes).

**For Nagi**: adopt `patched()`-style markers. Skip Worker Versioning — it solves blue/green of long-lived workers, which is overkill for LLM workflows that complete in minutes/hours.

## 7. TS SDK ergonomics — what users love, what they hate

### Loved

- **`proxyActivities<typeof activities>()`** — a single generic that erases at compile time and gives end-to-end types without codegen.
- **Symbol-as-contract for signals** — `defineSignal('name')` exports one value that both handler and caller import.
- **Two-mode test env** — `createTimeSkipping()` covers ~90% of test cases with `execute()` (fast-forward) and `start() + sleep()` (manual).
- **Activity mocking via `Partial<typeof activities>`** — zero ceremony, no mock framework.

### Hated (recurring high-reaction GitHub issues)

| # | Pain |
|---|---|
| [1334](https://github.com/temporalio/sdk-typescript/issues/1334) | Bun/Deno not supported |
| [933](https://github.com/temporalio/sdk-typescript/issues/933) | 150 MB @swc/core + webpack in `dependencies` |
| [1280](https://github.com/temporalio/sdk-typescript/issues/1280) | Vite not supported |
| [1615](https://github.com/temporalio/sdk-typescript/issues/1615) | Function-name-as-workflow-type breaks under minification (Next.js 15) |
| [207](https://github.com/temporalio/sdk-typescript/issues/207) | Worker can't run in browser/edge |
| [1432](https://github.com/temporalio/sdk-typescript/issues/1432) | AsyncLocalStorage cleanup leaks inside workflows |
| [254](https://github.com/temporalio/sdk-typescript/issues/254) | "TransportError" with no context |

Recurring themes: **bundler/runtime rigidity**, **fragile name-based identification**, **sandbox abstraction leaks**.

**For Nagi**:
- Don't put a bundler in `dependencies` — if Nagi ever needs one, ship `@nagi-js/bundler` separately.
- Don't identify workflows/steps by `function.name`. Nagi's [`b.step({ name: '...' })`](../../packages/core/src/builder.ts) already does this right — structurally unrepresentable to forget the name.
- Don't impose a V8 isolate unless full replay determinism is a core promise. Favor durable checkpointing at step boundaries.

---

## RFC 0004 follow-ups

Temporal's signal prior art surfaces specific edge cases [RFC 0004 multi-name signal waits](../rfcs/0004-multi-name-signal-waits.md) should explicitly address before merging:

1. **Late losers must be silent no-ops, not errors.** Matches Temporal's `signalAfterClose` (dropped server-side). The RFC's "first wins, losers no-op + log" already gets this right; preserve it under tests.
2. **Duplicate of the *winner's* name before resolution is persisted.** Temporal invokes the handler twice; Nagi collapses to "late loser" branch. Pick a deterministic tie-break order (insertion order of `names`, matching Temporal's FIFO over `bufferedSignals`) and pin in tests. Currently silent in the draft.
3. **Signal-then-cancel.** Temporal couples handlers to `CancellationScope` via `Trigger`; on cancel the trigger rejects. Nagi has no analogous cancellation channel in the snippet — decide whether a queued-but-undelivered signal becomes a no-op fact or a `cancelled` fact. Temporal's symmetric answer: `failed` with a `CancelledError`-shaped reason.
4. **Pre-handler arrival.** Temporal buffers indefinitely + drains on `setHandler`. RFC 0004 only covers "unrecognized name → throw", not "known name, step not yet running". Buffer-and-drain is the least-surprising default for webhook fanout (Mux/Recall webhooks can race ahead of the run's step entering `running`).
5. **Namespace collision under multi-name.** The construction-time check over `(stepId ∪ ...names)` is strictly stronger than Temporal's last-registration-wins footgun — keep it, and extend to "multi-name step lists *another step's `name`*", not just its step id.
6. **Schema on a union of arrival names.** Temporal punts entirely. Today's draft is one union schema (callers discriminate). Worth deciding whether `schema` should become a name-keyed `Record<name, Schema>` — Temporal users routinely lament the lack of this. Opportunity to do better than the prior art.
7. **Webhook retry idempotency.** Temporal Updates have client-supplied `updateId` for dedup; signals have none. If Mux retries `audioReady` after a 5xx, Nagi should either expose a caller-supplied dedup key on `wf.signal(...)` or document that the user's handler must be idempotent. At least one RFC paragraph — it's a real production hazard.
8. **Drain-on-completion.** Temporal added `allHandlersFinished()` after enough users shipped workflows that returned mid-async-handler. If Nagi signal handlers ever become async (today: decode-and-validate; tomorrow: side effects), pre-bake the equivalent now rather than retrofit.

---

## Borrow / skip summary

| Pattern | Verdict | Why |
|---|---|---|
| Event-sourced history, command/event split | **Borrow** | Single source of truth; aligns with unrepresentable-invalid-states |
| Shrunken event alphabet (~8 vs 61) | **Borrow** | LLM workflows don't need WORKFLOW_TASK_* network plumbing |
| Determinism by global replacement (Date/Math.random/setTimeout) | **Borrow** | Cheap, effective |
| V8 isolate sandbox | **Skip** | Cost center for Bun/Deno/Vite/browser/+150MB deps |
| `patched()` versioning markers | **Borrow** | Smallest useful escape hatch |
| Worker Versioning (Build ID pinning) | **Skip** | For LLM workflows of minutes/hours, not months |
| `defineSignal` symbol-as-contract | **Borrow** | Beats stringly-typed lookup |
| First-class multi-name signal wait | **Improve on Temporal** | RFC 0004 is better prior art; nail the §[follow-ups](#rfc-0004-follow-ups) |
| Signal buffering + drain-on-handler-set | **Borrow** | Least-surprising default for webhook fanout |
| `condition()` + mutate-from-handler pattern | **Borrow as secondary path** | Composes with multi-name `b.signal` for stateful joins |
| Updates with sync validator | **Borrow if needed** | Right answer when "signal + query for outcome" recurs |
| Timeout matrix (Schedule/Start/Close/Heartbeat) | **Borrow `StartToClose` + `ScheduleToClose`** | Skip `ScheduleToStart`; `Heartbeat` opt-in |
| Default retry policy (1s × 2^n, jitter) | **Borrow** | Matches LLM provider semantics |
| `ApplicationFailure.nonRetryable()` in-code marker | **Borrow** | Provider error taxonomies vary too much for policy allowlists |
| `CancellationScope` tree + shielding | **Borrow** | `AbortController` composes poorly with HITL pauses |
| Sticky execution + workflow cache | **Skip in v1, design for** | Persist commands not state so this is additive |
| `TestWorkflowEnvironment` time-skipping | **Borrow** | Multi-day-sleep workflows otherwise untestable |
| `proxyActivities<typeof activities>()` typing | **Borrow shape** | Nagi's builder already does this — keep it |
| Bundler in runtime dependencies | **Skip** | #1 deploy-size complaint |

---

## Source pointers

Temporal:
- Event taxonomy: [`temporal/api/enums/v1/event_type.proto`](https://github.com/temporalio/api/blob/master/temporal/api/enums/v1/event_type.proto)
- Workflow primitives: [`sdk-typescript/packages/workflow/src/workflow.ts`](https://github.com/temporalio/sdk-typescript/blob/main/packages/workflow/src/workflow.ts) (`patched`, `deprecatePatch`, `sleep`, `uuid4`, `defineSignal`)
- Signal buffering: [`packages/workflow/src/internals.ts`](https://github.com/temporalio/sdk-typescript/blob/main/packages/workflow/src/internals.ts)
- Determinism overrides: [`packages/workflow/src/global-overrides.ts`](https://github.com/temporalio/sdk-typescript/blob/main/packages/workflow/src/global-overrides.ts)
- Cancellation: [`packages/workflow/src/cancellation-scope.ts`](https://github.com/temporalio/sdk-typescript/blob/main/packages/workflow/src/cancellation-scope.ts)
- Trigger primitive: [`packages/workflow/src/trigger.ts`](https://github.com/temporalio/sdk-typescript/blob/main/packages/workflow/src/trigger.ts)
- Worker tuning: [`packages/worker/src/worker-options.ts`](https://github.com/temporalio/sdk-typescript/blob/main/packages/worker/src/worker-options.ts)
- Samples: [`expense/src/workflows.ts`](https://github.com/temporalio/samples-typescript/blob/main/expense/src/workflows.ts), [`signals-queries`](https://github.com/temporalio/samples-typescript/tree/main/signals-queries), [`message-passing/introduction`](https://github.com/temporalio/samples-typescript/tree/main/message-passing)

Nagi files this research bears on:
- `packages/core/src/builder.ts:112-148, 498-535` (`b.signal` definition)
- `packages/core/src/runtime.ts` (replay loop)
- `packages/core/src/canonicalize.ts` (step-name identity — the Temporal #1615 lesson)
- `docs/rfcs/0004-multi-name-signal-waits.md` (in flight)
