# Issue #10 — `b.subflow()` runtime (research + plan)

- **Tracking issue:** lymo-inc/nagi#10
- **Date:** 2026-05-18 (JST)
- **Scope:** `@nagi-js/core` only. Adapters unaffected — uses existing `Store` / `Queue` surface.
- **Status:** Research done; design decisions taken (see "Decisions" below); implementation in progress.

## Decisions taken (2026-05-18, Jay)

- Output shape: **structured** `{ childRunId, output }`. Downstream consumers correlate to child via `needs.X.output.<field>` and access `needs.X.childRunId` for debugging.
- Cancel cascade: **full** — ship `wf.cancel(runId)` public API + transitive cascade in this PR.
- Registration: **strict** — referenced child flows must be passed to `nagi({ flows: [...] })` explicitly. Throws at boot if a `b.subflow(child)` references an unregistered child.
- Subflow input function: `({ input, needs }) => ChildInput` (wrapper). Parent input + parent needs both accessible.

These decisions roll up into Part 2 (Plan). Part 3 (Open questions) below should be read as "design questions Jay answered" — preserved for traceability.

---

## TL;DR

- Issue premise that "type-level `b.subflow()` exists" is **incorrect** — there are zero references to `subflow` in `packages/` (only one in `docs/rfcs/0002-record-literal-builder-api.md:253` as "future work"). Both the type-level constructor and the runtime path must be built.
- The runtime mechanism mirrors signal-wait almost exactly: dispatch the parent step → start the child run → park parent step in `running`; child's `flow.completed` writes the parent's `step.completed` directly (no second parent-side dispatch needed).
- Single load-bearing hook: `finalizeFlowCompletion` / `finalizeFlowFailure` in `packages/core/src/dispatch.ts:637,618`. Linkage carried by adding `parentRunId` + `parentStepId` to `FlowStartedFact` (`packages/core/src/types.ts:955`).
- Six surfaces touched: `types.ts` (kind, def, fact extension, builder method), `internal.ts` (new def), `builder.ts` (constructor), `dispatch.ts` (new dispatch branch + finalize hook), `runtime.ts` (start-child helper, flow registry resolution), `scheduler.ts` (no change expected — see "Scheduler" below).
- Cancellation cascade and concurrency-per-subflow are out of scope of this RFC per the issue's open-questions section, but the fact-log shape must not preclude them.

---

## Part 1 — Research

### 1.1 What exists today

Step kinds and dispatch are exactly three:

- `StepKind = "task" | "signal" | "match"` — `types.ts:68`.
- Dispatch switch — `dispatch.ts:163-198`:
  - `task` → `executeTask` → `advance()`
  - `signal` → `queue.ack(); return;` (parks; completion arrives out-of-band)
  - `match` → `executeMatch` → `advance()`
- `Builder<Input, A>` interface — `types.ts:321-402`. Methods: `task`, `signal`, `match` (two overloads), `step` (chainable), `include`.
- Concrete builder factory — `builder.ts:276-287` (`makeBuilder` returns the literal `{ task, signal, match, step, include }`).

Run lifecycle:

- `wf.start()` — `runtime.ts:259-396`. Mints `RunId` via `mintRunId()` (`runtime.ts:637`, `"run-" + crypto.randomUUID()`), atomically inserts `flow.started` via `store.tryStartRun(runId, fact, concurrencyArg)` (`runtime.ts:321`), then `advance(dispatchDeps, runId)` (`runtime.ts:394`).
- `FlowStartedFact` shape — `types.ts:955-973`: `{ kind, runId, flowId, input, at, flowHash?, codeVersion? }`. **No parent linkage today.**
- `FlowCompletedFact` shape — `types.ts:995-998`: `{ kind, runId, output, at }`.
- `FlowFailedFact` — `types.ts:1000-1003`: `{ kind, runId, error, at }`.
- `Flow<Id, InputSchema, M, Output>` — `types.ts:503-531`; helper aliases `FlowInput<F>` and `FlowOutput<F>` already exist at `types.ts:533-539`.
- `flowsById` registry — used by `wf.signal` at `runtime.ts:404`; constructed in `nagi()`. Subflow start will need the same lookup to resolve a referenced child flow.

### 1.2 Signal-wait mechanism (the template to copy)

Signal flow:

1. Dispatcher sees `def.kind === "signal"`. It has already written `step.started` (`dispatch.ts:133-139`); then does `queue.ack(message.receipt); return;` — `dispatch.ts:190-194`. Step stays `running`.
2. External `wf.signal(runId, name, payload)` (`runtime.ts:398-501`):
   - Resolves `name → (stepId, def)` via `flow.steps` and the optional `names` field (`runtime.ts:419-441`).
   - Validates `stepState.status === "running"` (`runtime.ts:444-460`).
   - Validates payload against `def.schema` (`runtime.ts:462`).
   - Appends a diagnostic `signal.received` fact (`runtime.ts:466-473`).
   - Calls `store.completeStep(runId, stepId, validated, completedFact)` — atomic `step.completed` write + persisted output update (`runtime.ts:483`).
   - Calls `advance(dispatchDeps, runId)` — `runtime.ts:500`.

Key takeaway for subflow: completion does NOT round-trip through the queue. `wf.signal` directly invokes `store.completeStep` and `advance`. The subflow's "child finished" path will do the same — no re-enqueue of the parent step.

### 1.3 `finalizeFlowCompletion` / `finalizeFlowFailure` — the wake-up chokepoint

- `finalizeFlowCompletion(args)` — `dispatch.ts:637-655`. Writes `flow.completed` via `store.appendFact`, then fires `onComplete` / `onFlowComplete` hooks. **Only place** in `advance()`'s terminal branch where a run's `flow.completed` is appended (`dispatch.ts:401`).
- `finalizeFlowFailure(args)` — `dispatch.ts:618-635`. Writes `flow.failed`, fires error hooks. Two call sites: `advance()` terminal-failed (`dispatch.ts:399`) and cycle-guard (`dispatch.ts:441`).
- Both receive `{ deps, flow, runId, ... }` — they already have `store` access. Loading the run's `flow.started` fact to read `parentRunId` is one `loadRunState` away.

This is the cleanest place to wire "if I have a parent, complete the parent's subflow step."

### 1.4 Store and Queue surfaces

- `Store` interface — `types.ts:664-831`. Relevant methods for subflow: `tryStartRun`, `appendFact`, `completeStep`, `failStep`, `loadRunState`, `claimStep`.
- `Queue` interface — `types.ts:908-914`. `enqueue(runId, stepId, opts?)` — already accepts any `runId`, so spawning a child run on the same queue is supported without adapter changes.
- Verified: PG adapter and in-memory adapter both implement the abstract `Store`/`Queue`; subflow needs no new method (uncertain on the PG side for the new optional fact fields — see Open Question #1 below).

### 1.5 Scheduler

- `scheduler.ts` is pure functions; the actual loop is `advance()` in `dispatch.ts` and the worker in `worker.ts`.
- `nextRunnable` (`scheduler.ts:31`) gates downstream steps on `runState.steps[upstream].status === "completed"`. A subflow step in `running` (waiting on its child) is treated identically to a signal step in `running` — `nextRunnable` won't enqueue its downstream. **No change needed.**
- `flowTermination` (`scheduler.ts:204`) — sees a subflow step as non-terminal while it's `running`. No change.

### 1.6 Test harness

- `makeHarness` — `test-helpers.ts`. Returns `{ wf, store, drain(), result(runId), startWorker(), waitForStep(runId, key, status), waitForEnd(runId) }`.
- Synchronous-ish path (no signals): `await h.wf.start(...); await h.drain(); const r = await h.result(runId)` — `dispatch.test.ts:51-68`.
- Worker-driven path (signals, multi-step async): `signal-multi-name.test.ts:49-61`. Will need this pattern for subflow tests because the child run advances independently.

---

## Part 2 — Plan

### 2.1 API shape

The issue's example uses an undocumented `b.flow('analysis', { steps: { ... } })` form that doesn't match today's `flow({ id, input, build })` API. **Adapt to the current builder.** Two authoring styles in today's `Builder<Input, A>` (`types.ts:300-319`); subflow needs to work in both. Sketch:

```ts
// Define the child flow first, exactly like any other flow.
const transcriptionFlow = flow({
  id: 'transcription',
  input: passthroughSchema<{ audioUrl: string }>(),
  build: (b) => ({
    fetch: b.task({ run: async ({ input }) => ({ blob: await fetchAudio(input.audioUrl) }) }),
    transcribe: b.task({ needs: { fetch }, run: async ({ needs }) => ({ text: await whisper(needs.fetch.blob) }) }),
  }),
  output: ({ transcribe }) => transcribe,
});

// Parent flow embeds it via b.subflow.
const analysisFlow = flow({
  id: 'analysis',
  input: passthroughSchema<{ videoId: string }>(),
  build: (b) => {
    const audio = b.signal({ schema: audioReadySchema });
    const transcript = b.subflow(transcriptionFlow, {
      needs: { audio },
      input: ({ needs }) => ({ audioUrl: needs.audio.url }),
    });
    const summary = b.task({
      needs: { transcript },
      run: async ({ needs }) => summarize(needs.transcript.output.text),
      // needs.transcript is { childRunId: RunId, output: <child output> }
    });
    return { audio, transcript, summary };
  },
});
```

**Subflow config:**

```ts
interface SubflowConfig<Input, N extends NeedsMap, Child extends Flow> {
  readonly needs?: N;
  readonly when?: (args: { input: Input; needs: NeedsOutputs<N> }) => boolean;
  readonly timeoutMs?: Millis;
  readonly input: (args: {
    input: Input;
    needs: NeedsOutputs<N>;
  }) => FlowInput<Child>;
}
```

**Builder method signature:**

```ts
subflow<N extends NeedsMap, C extends Flow>(
  child: C,
  config: SubflowConfig<Input, N, C>,
): Step<{ readonly childRunId: RunId; readonly output: FlowOutput<C> }>;
```

The output is intentionally `{ childRunId, output }` — issue says "Probably lift" but I'd prefer to expose both so users can correlate to child for debugging without losing direct access to the data. Cheap to revise pre-1.0.

### 2.2 Internal def

Add to `internal.ts`:

```ts
interface SubflowDef {
  readonly kind: "subflow";
  readonly needs: NeedsMap;
  readonly childFlowId: string;         // by id; lookup via flowsById at dispatch time
  readonly buildInput: (args: { input: Json; needs: Record<string, Json> }) => Json;
  readonly timeoutMs?: Millis;
  readonly when?: (args: { input: Json; needs: Record<string, Json> }) => boolean;
  readonly parentMatch?: ParentMatchRef;
}
```

`StepDef = TaskDef | SignalDef | MatchDef | SubflowDef`. `StepKind = "task" | "signal" | "match" | "subflow"`.

### 2.3 Fact-log extension

Extend `FlowStartedFact` (`types.ts:955-973`):

```ts
export interface FlowStartedFact extends FactBase {
  readonly kind: "flow.started";
  readonly flowId: string;
  readonly input: Json;
  readonly flowHash?: string;
  readonly codeVersion?: string;
  // NEW:
  readonly parentRunId?: RunId;
  readonly parentStepId?: string;
}
```

Both new fields optional + undefined for non-subflow runs → no backfill needed, no breaking change to existing fact logs. **Canonicalization is unaffected** — `flow.started` isn't part of the canonical DAG hash; it's a runtime fact.

No new fact kind. The "subflow started" event is the child's `flow.started` itself — querying by `parentRunId` finds all children.

### 2.4 Runtime — dispatch path

In `dispatchMessage` (`dispatch.ts:114-`), extend the `startEventInput` switch (`dispatch.ts:145-146`) — subflow uses `extractInput(...)` like task, since it has needs/input to derive child input.

New branch after match (`dispatch.ts:198`):

```ts
} else if (def.kind === "subflow") {
  await startChildRun({
    deps, flow, def, runId, stepId, attempt,
  });
  await queue.ack(message.receipt);
  return;
}
```

`startChildRun` (new helper, likely in `runtime.ts` because it needs `flowsById` and `wf.start`-style logic, or factored to `dispatch.ts` with `flowsById` injected through `DispatchDeps`):

1. Load `runState`; build `needs` via `resolveNeeds`; extract `input` via `extractInput`.
2. Derive child input: `def.buildInput({ input, needs })`.
3. Validate against child flow's input schema (use existing `validate(child.input, ...)` helper from `runtime.ts`).
4. `childRunId = mintRunId()`.
5. Build `FlowStartedFact` with `parentRunId = runId`, `parentStepId = stepId`, plus the standard `flowHash`/`codeVersion` for the child flow.
6. `store.tryStartRun(childRunId, fact, /* concurrencyArg derived from child.concurrency */)`.
7. `advance(deps, childRunId)` to kick off child execution. Parent's `step.started` was already written by `dispatchMessage` at line 133-139; parent step stays in `running` status.

### 2.5 Runtime — wake-parent on child completion

Modify `finalizeFlowCompletion` (`dispatch.ts:637`):

```ts
async function finalizeFlowCompletion({ deps, flow, runId, runState }) {
  const output = computeFlowOutput(flow, runState);
  await store.appendFact(runId, { kind: "flow.completed", runId, output, at: clock.now() });
  await fireHook(flow.onComplete, ...);
  await fireHook(deps.hooks?.onFlowComplete, ...);

  // NEW: parent-link wakeup.
  const startedFact = runState.facts.find(f => f.kind === "flow.started") as FlowStartedFact | undefined;
  if (startedFact?.parentRunId !== undefined && startedFact.parentStepId !== undefined) {
    await completeParentSubflowStep({
      deps,
      parentRunId: startedFact.parentRunId,
      parentStepId: startedFact.parentStepId,
      childRunId: runId,
      childOutput: output,
    });
  }
}
```

`completeParentSubflowStep` writes the parent's `step.completed` with output `{ childRunId, output: childOutput }` via `store.completeStep`, then `advance(deps, parentRunId)`. This is the precise analogue of `wf.signal`.

Symmetric branch in `finalizeFlowFailure` writes parent's `step.failed` with the child's error. Per issue: "Probably surface (lift `step.output.error`)" — I read this as: error structurally propagates, parent step fails. Implement that.

### 2.6 Replay semantics

Per issue: "Probably memo (child has its own replay)." Replay behavior:

- A replayed parent run sees its `step.started` for the subflow step in the fact log.
- The `childRunId` is recoverable: the child's `flow.started` fact carries `parentRunId === parentRunId` — query by parent run.
- If the child run is still terminal (`flow.completed` exists): re-derive the parent step's output from the child's completed fact; **do not start a new child**.
- If the child run is non-terminal (interrupted): policy decision — re-enter the child or fail the replay. **Defer to follow-up RFC.** For first cut, only memo terminal-completed children; non-terminal child during replay is an error.

This avoids cascading replay storms and aligns with the issue's stated preference.

### 2.7 Registration

`nagi({ flows: [...] })` must include any flow referenced by `b.subflow(child)`. Two options:

- **Strict, opt-in:** User passes child explicitly in `flows: [parentFlow, transcriptionFlow]`. Throws at boot if a referenced child isn't registered.
- **Auto-include:** `nagi()` walks each registered flow's steps; subflow steps' `childFlowId` are auto-added if not present.

Lean **strict** for the first cut: explicit > implicit, and detects missing children deterministically. Auto-include can land as a quality-of-life follow-up.

### 2.8 Canonicalization

Subflow step joins the canonical DAG as a new `CanonicalStep` variant:

```ts
{ kind: "subflow", id, needs, childFlowHash, when?, timeoutMs? }
```

`childFlowHash` is the child flow's canonical hash (sha256 of canonicalized child DAG) — pins parent's snapshot to the *exact topology* of the referenced child at registration time. If the child flow's topology changes, the parent's `flowHash` changes. This is the right behavior: replays of the parent should see the child topology they were started against.

Confirm: `canonicalize.ts` will need a new arm for the subflow kind. Cite: `canonicalize.ts:1-300` (haven't read in detail; will revisit during impl).

### 2.9 Tests

Five mandatory scenarios, in `subflow.test.ts`:

1. **Happy path** — parent has one subflow step; child completes; parent output references `childRunId` and `output` correctly.
2. **Child failure** — child fails; parent's subflow step transitions to `failed` with the child's error.
3. **Replay memo** — start parent, complete child, then replay parent — child is NOT re-executed; parent reuses the child's output.
4. **Nested subflow** — parent → child → grandchild. `parentRunId` chains correctly; all three runs complete.
5. **`needs` typing + runtime resolution** — subflow's `input(({ needs }) => ...)` receives correctly-typed and correctly-resolved needs.

Type-level: `subflow.test-d.ts` asserts:

- `b.subflow(child, { input: ({ needs }) => ChildInputShape })` typechecks when `ChildInputShape` matches.
- `Step` output type is `{ childRunId, output: FlowOutput<Child> }`.
- Wrong input shape is a compile error.

---

## Part 3 — Open questions (blocking; need Jay's call before coding)

1. **Adapter fact-schema migration.** PG adapter persists `flow.started` JSON. Adding two new optional fields probably needs no migration (JSONB is permissive), but I want to verify before claiming "no adapter change." **Uncertain — needs check.**
2. **Output shape: `{ childRunId, output }` vs lift output directly.** Issue says "Probably memo (lift `step.output.error`)" implying lift. I argued for the structured form above; want explicit decision.
3. **Cancellation cascade.** Default per issue is "yes — `parentRunId` cancellation cascades," but actually implementing this requires either (a) an index on `parentRunId` in the store, or (b) a scan. **Defer to follow-up?** Recommendation: defer, document the gap.
4. **Concurrency per child.** Issue motivation calls out "Different concurrency keys per sub-flow (`transcription` keyed by `audioHash`)." Child flow's own `concurrency` config applies as it would for a top-level run — confirm that's the intended semantics (no per-subflow override at the parent level).
5. **Strict vs auto-include flow registration.** Recommended strict; confirm.
6. **Child input function signature: `({ needs })` only vs `({ input, needs })`.** Issue example uses `({ audio }) => ...` — implies destructured `needs` directly. I propose the wrapped form `({ input, needs }) => ...` to keep parent input accessible. Confirm.
7. **Builder API surface.** `b.subflow(child, opts)` works in the standalone-constructor style. Does it also need a `b.step(key, ...)` chain variant — `b.step('transcript', { kind: 'subflow', child, ... })`? Recommendation: add only the standalone form for the first cut; chain variant later.
8. **Where does `startChildRun` live?** Pure dispatch helper or in runtime.ts? It needs `flowsById` — currently held in `runtime.ts`'s closure. Cleanest path: thread `flowFor(flowId)` through `DispatchDeps` (alongside the existing `flowFor(runId)` at `dispatch.ts:376`). Confirm.

---

## Part 4 — Out of scope

- `b.parallel(flows)` fan-out — separate primitive, future RFC.
- Dynamic subflow selection (`b.subflow(decideFlow(input))`) — issue explicitly excludes.
- Cancellation cascade index — see Q3 above.
- Child run output streaming back to parent during execution (partial outputs) — N/A; child is opaque to parent until terminal.

---

## File index (cited in this doc)

- `packages/core/src/types.ts` — `StepKind:68`, `Builder:321`, `Flow:503`, `FlowInput:533`, `FlowOutput:538`, `FlowStartedFact:955`, `FlowCompletedFact:995`, `FlowFailedFact:1000`, `Store:664`, `Queue:908`.
- `packages/core/src/internal.ts` — `TaskDef:29`, `SignalDef:57`, `MatchDef:103`, `StepDef`, `getDef`, `attachDef`.
- `packages/core/src/builder.ts` — `makeBuilder():276-287`, `flow():305-`, step id assignment `:346-364`.
- `packages/core/src/dispatch.ts` — dispatch kind switch `:163-198`, `advance:374`, `finalizeFlowCompletion:637`, `finalizeFlowFailure:618`, `isFlowTerminal:600`.
- `packages/core/src/runtime.ts` — `wf.start:259-396`, `mintRunId:637`, `wf.signal:398-501`, `flowsById` registry.
- `packages/core/src/scheduler.ts` — `nextRunnable:31`, `flowTermination:204`, `extractInput:286`.
- `packages/core/src/test-helpers.ts` — `makeHarness`, `passthroughSchema`, `Result`.
- `docs/rfcs/0002-record-literal-builder-api.md:253` — original "future work" note on `b.subflow`.
- `docs/rfcs/0004-multi-name-signal-waits.md` — style template followed here.
