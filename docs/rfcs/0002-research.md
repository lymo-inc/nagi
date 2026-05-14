# RFC 0002 — Pre-implementation research

- **Status:** Research
- **Author:** Claude (assisted), reviewed by @jay
- **Created:** 2026-05-14
- **Companion to:** [`0002-record-literal-builder-api.md`](./0002-record-literal-builder-api.md)

This document inventories the **current** `@nagi-js/core` builder/runtime so that the RFC 0002 implementation can be carried out as a series of localized edits rather than a from-scratch redesign. It surfaces a handful of places where the RFC's wording is slightly imprecise relative to the actual code, and proposes an implementation shape that minimizes the blast radius.

The research was performed by two parallel `code-explorer` subagents covering (a) the builder/type system and (b) the runtime/scheduler/dispatcher path, with verbatim source spot-checks of the load-bearing files.

---

## 1. Executive summary

1. **The RFC's "three-channel identity" framing slightly mischaracterizes today's code.** In the current implementation, the persisted step id comes from the **return-record key** (`collectIds()` in `builder.ts:187`), not the JS variable name. Renaming a `const upload = …` binding does **not** change the persisted id — only renaming the return-record key does. The wins RFC 0002 delivers are real but should be reframed as: (a) eliminating the return wall, (b) collapsing typed-reference + record-key into a single mention site, (c) making typos in `needs:` compile errors instead of `idByIdentity.get()` runtime throws.

2. **`b.steps(record)` cannot be a pure pass-through.** The runtime's `resolveNeeds()` (`internal.ts:149`) iterates `Object.entries(def.needs)` expecting upstream **`Step` object references** whose `.id` was stamped during `walkAndRewrite`. RFC 0002 specifies `needs: ["upload"]` — a string array. The builder method must rewrite each entry's `needs: string[]` into `needs: { upload: <sibling-Step-ref> }` before the record reaches `flow()`, using the record itself as the sibling lookup table. This is the central runtime transformation the implementation must perform.

3. **No scheduler / dispatcher / store changes are required.** Once `b.steps()` produces a `StepMap` shaped identically to today's `build(b)` return, every downstream consumer (`flow()`, `nextRunnable`, `executeTask`, `Store.*`) operates on opaque step-id strings and `Step` object references. The full data contract is documented in §4.

4. **Two RFC-vs-code naming inconsistencies — both resolved:**
   - RFC names the field `timeoutMs: number`; current `StepConfigBase` uses `timeout: Millis`. **Decision: rename `timeout` → `timeoutMs` uniformly across `TaskConfig`, `SignalConfig`, `StepConfigBase`, `TaskDef`, `SignalDef`, and all tests as part of this PR.** Keep the value type as `Millis` (the alias is fine; the field name carries the unit). Affects ~6 occurrences in `types.ts` and `internal.ts` plus a handful in builder/dispatch tests.
   - RFC's `when?: (args: { input: FlowInput }) => boolean` drops the `needs` parameter; current `when` is typed `(args: { input, needs: NeedsOutputs<N> }) => boolean`. **Decision: keep the richer current shape** — the runtime already passes both args (`scheduler.ts:56-65`).

5. **TypeScript 5.9.3 (`strict` + `noUncheckedIndexedAccess` + `exactOptionalPropertyTypes`) is in use.** The RFC's `<const S>` modifier requirement is satisfied; the `NoInfer<>` helper (TS 5.4+) used elsewhere in `types.ts` is also available. No version bump needed.

---

## 2. Current state — verbatim excerpts of the load-bearing types

The four type definitions that govern today's builder are all in `packages/core/src/types.ts`. RFC 0002's new types compose with these; reading them is a prerequisite for the implementation.

### `Step<O>` — the public handle (`types.ts:70-77`)

```ts
export interface Step<Output = unknown> {
  readonly kind: StepKind;        // "task" | "signal" | "match"
  readonly id: StepId;            // assigned by flow(); "" at construction time
  readonly __output?: Output;     // phantom — never assigned at runtime
}

export type StepOutput<S> = S extends Step<infer O> ? O : never;
export type StepMap = Readonly<Record<string, Step<unknown>>>;
```

`__output` is the type-level vehicle that carries each step's inferred output type forward to dependents. It has no runtime presence. **RFC 0002's `OutputOf<T>` type can either reuse this phantom field (via `StepOutput`) or re-derive from `T["run"]`'s return type — see §5 for the recommendation.**

### `NeedsMap` and `NeedsOutputs<N>` (`types.ts:79-82`)

```ts
export type NeedsMap = StepMap;                    // = Record<string, Step<unknown>>
export type NeedsOutputs<N extends NeedsMap> = {
  readonly [K in keyof N]: StepOutput<N[K]>;
};
```

`NeedsOutputs<N>` is what's surfaced to handlers today as `ctx.needs`. RFC 0002's `NeedsOf<S, K>` is the equivalent computation but indexed through the parent record `S` and a string-array of sibling keys.

### `StepCtx`, `TaskConfig`, `StepConfigBase` (`types.ts:91-149`)

```ts
export interface StepCtx<Input = unknown> {
  readonly input: Input;
  readonly tx: Tx;
  readonly runId: RunId;
  readonly stepId: StepId;
  readonly attempt: AttemptNumber;
  readonly signal: AbortSignal;
  readonly now: () => Date;
  readonly logger: Logger;
  once<T extends Json>(scope: string, fn: () => Promise<T>): Promise<T>;
  idempotencyKey(scope: string): string;
}

interface StepConfigBase<Input, N extends NeedsMap> {
  readonly needs?: N;
  readonly when?: (args: {
    readonly input: NoInfer<Input>;
    readonly needs: NoInfer<NeedsOutputs<N>>;
  }) => boolean;
  readonly timeout?: Millis;          // ← note: `timeout`, NOT `timeoutMs`
}

export interface TaskConfig<Input, N extends NeedsMap, Output>
  extends StepConfigBase<Input, N> {
  readonly retry?: RetryPolicy;
  readonly run: (args: {
    readonly input: NoInfer<Input>;
    readonly needs: NoInfer<NeedsOutputs<N>>;
    readonly ctx: StepCtx<NoInfer<Input>>;
  }) => Promise<Output>;
}
```

The `NoInfer<>` wrappers are deliberate: they prevent the `run` callback from leaking inference influence back onto `Input` and `N`. **The RFC 0002 `StepEntry` should mirror this pattern** — `run`'s return type infers `Output`, but `input` and `needs` types are fixed by the surrounding generics.

### `Builder<Input>` — the existing surface (`types.ts:249-288`)

```ts
export interface Builder<Input = unknown> {
  task<N extends NeedsMap, Output>(
    config: TaskConfig<Input, N, Output>,
  ): Step<Output>;

  signal<N extends NeedsMap, S extends StandardSchemaV1>(
    config: SignalConfig<Input, N, S>,
  ): Step<InferSchemaOutput<S>>;

  match<N extends NeedsMap, D extends string, Cases extends {...}>(config: ...): Step<...>;
  match<N extends NeedsMap, Arms extends ReadonlyArray<MatchArmShape<Input, N>>>(config: ...): Step<...>;
}
```

RFC 0002 adds **one new method**: `steps<const S>(record: S): /* StepMap */`. The existing three methods stay. (See §6 for whether `b.steps()` v1 accepts only tasks or extends to signal/match entries.)

### `FlowConfig` and `flow()` (`types.ts:290-321`, `builder.ts:154-184`)

```ts
export interface FlowConfig<Id extends string, InputSchema extends StandardSchemaV1, M extends StepMap, Output = unknown> {
  readonly id: Id;
  readonly input: InputSchema;
  readonly build: (b: Builder<InferSchemaOutput<InputSchema>>) => M;   // ← returns a StepMap
  output?(steps: NeedsOutputs<M>): Output;
}

export function flow<const Id extends string, InputSchema extends StandardSchemaV1, M extends StepMap, Output = unknown>(
  config: FlowConfig<Id, InputSchema, M, Output>,
): Flow<Id, InputSchema, M, Output> {
  const builder = makeBuilder<InferSchemaOutput<InputSchema>>();
  const built = config.build(builder);                                  // ← StepMap from build()

  const idByIdentity = new Map<Step<unknown>, string>();
  collectIds(built, "", idByIdentity);                                  // ← record key → step id

  const finalSteps: Record<string, Step<unknown>> = {};
  walkAndRewrite({ flowId: config.id, map: built, prefix: "", parentMatch: undefined, idByIdentity, out: finalSteps });

  return { id: config.id, input: config.input, steps: finalSteps as M, ...(config.output ? { output: config.output } : {}) };
}
```

**Key implication:** `flow()` already derives step ids from record keys. `b.steps(record)` therefore does **not** need to assign ids — it only needs to return a record-shaped `StepMap` whose entries are `Step<…>` objects whose internal `__def.needs` contains object refs to siblings (not strings).

### `walkAndRewrite()` — the runtime bridge (`builder.ts:222-260`)

```ts
function walkAndRewrite(args: WalkArgs): void {
  const { flowId, map, prefix, parentMatch, idByIdentity, out } = args;

  for (const [key, step] of Object.entries(map)) {
    const id = prefix ? `${prefix}.${key}` : key;
    const def = (step as { __def?: StepDef }).__def;
    if (def === undefined) throw new Error(`Flow "${flowId}": step "${id}" has no internal def...`);
    if (step.id !== "") throw new Error(`Flow "${flowId}": step "${id}" was produced by a different flow() call...`);

    const rewrittenNeeds: Record<string, Step<unknown>> = {};
    for (const [localKey, upstream] of Object.entries(def.needs)) {
      const upstreamId = idByIdentity.get(upstream);
      if (upstreamId === undefined) {
        throw new Error(`Flow "${flowId}": step "${id}" references an upstream step that was not returned from build()...`);
      }
      rewrittenNeeds[localKey] = { ...upstream, id: upstreamId };
    }
    // …emit out[id] = attachDef({ kind, id }, { ...def, needs: rewrittenNeeds })
  }
}
```

This is the **iron contract**: every `def.needs` value must be a `Step<unknown>` object whose **identity** is registered in `idByIdentity` (which is populated by `collectIds` walking the returned `StepMap`). If `b.steps()` synthesizes `Step` objects whose identity isn't preserved into the final returned map, `walkAndRewrite` will throw `"references an upstream step that was not returned from build()"`.

### Internal `TaskDef` (`internal.ts:25-40`)

```ts
export interface TaskDef {
  readonly kind: "task";
  readonly needs: NeedsMap;           // ← Record<localKey, Step<unknown>>, not string[]
  readonly retry?: RetryPolicy;
  readonly timeout?: Millis;
  readonly when?: (args: { input: unknown; needs: Record<string, unknown> }) => boolean;
  readonly run: (args: { input: unknown; needs: Record<string, unknown>; ctx: StepCtx<unknown> }) => Promise<Json>;
  readonly parentMatch?: ParentMatchRef;
}
```

`needs` is `NeedsMap`, a `Record<string, Step<unknown>>`. **This is the runtime shape `b.steps()` must produce.** It's not negotiable without changing `resolveNeeds`, `needsStepIds`, `walkAndRewrite`, and `scheduler.checkUpstream`, all of which iterate `Object.entries(def.needs)` and read `upstream.id`.

---

## 3. RFC 0002 requirements — restated

From `0002-record-literal-builder-api.md`:

```ts
// User-facing API
flow({
  id: "demo",
  input: passthroughSchema<{ x: number }>(),
  build: (b) => b.steps({
    a: { run: async ({ input }) => ({ y: input.x * 2 }) },
    b: { needs: ["a"], run: async ({ needs }) => ({ z: needs.a.y + 1 }) },
    c: { needs: ["a"], run: async ({ needs }) => ({ w: needs.a.y * 3 }) },
    d: { needs: ["b", "c"], run: async ({ needs }) => ({ ok: needs.b.z + needs.c.w }) },
  }),
});
```

```ts
// Type-level skeleton (from the RFC, with minor corrections noted inline)
type OutputOf<T> = T extends { run: (...args: any[]) => infer R } ? Awaited<R> : never;

type NeedsOf<S, K extends keyof S> =
  S[K] extends { needs: infer N extends ReadonlyArray<keyof S & string> }
    ? { [P in N[number]]: OutputOf<S[P]> }
    : Record<string, never>;

type ValidatedSteps<S, Input> = {
  [K in keyof S]: {
    readonly needs?: ReadonlyArray<Exclude<keyof S, K> & string>;
    readonly when?: (args: { input: Input }) => boolean;     // ⚠ should include needs (see §5)
    readonly retry?: RetryPolicy;
    readonly timeoutMs?: number;                              // ⚠ field name conflict (see §5)
    readonly run: (ctx: {
      input: Input;
      needs: NeedsOf<S, K>;
      ctx: StepCtx;
    }) => unknown;
  };
};

interface Builder<Input> {
  steps<const S>(record: S & ValidatedSteps<S, Input>): Flow<Input, S>;   // ⚠ returns StepMap, not Flow (see §5)
}
```

The RFC's signature for `Builder.steps()` returns `Flow<Input, S>`, but `b.steps()` is called **inside** `build` — its return must be assignable to `StepMap` so `flow()` can wrap it into the final `Flow<Id, InputSchema, M, Output>`. The right shape is:

```ts
interface Builder<Input> {
  steps<const S extends ValidatedSteps<S, Input>>(record: S): { readonly [K in keyof S]: Step<OutputOf<S[K]>> };
}
```

---

## 4. The runtime contract `b.steps()` must satisfy

Distilled from the runtime/scheduler/dispatch agent's full investigation:

| Consumer | What it reads | Where |
|---|---|---|
| `flow()` | `M extends StepMap` returned from `build(b)` | `builder.ts:163` |
| `collectIds()` | Record keys of the `StepMap`, recursing into `__def._nested` for matches | `builder.ts:187-205` |
| `walkAndRewrite()` | `def.needs` as `Record<localKey, Step>`, looking up `upstream.id` via `idByIdentity.get(upstream)` | `builder.ts:245-260` |
| `scheduler.checkUpstream()` | `Object.values(def.needs)`, reading `upstream.id` | `scheduler.ts:76-98` |
| `scheduler` (when-eval) | `resolveNeeds(def, loadOutput)` returning `Record<localKey, upstreamOutput>` | `scheduler.ts:56-65` |
| `dispatch.executeTask()` | `resolveNeeds(def, loadOutput)` to build the `ctx.needs` passed to `def.run({ input, needs, ctx })` | `dispatch.ts:178` |
| `dispatch.handleStepError()` | `def.retry` policy | `dispatch.ts:264` |
| `flow.output(stepOutputs)` | `Record<stepId, Json>` keyed by record key | `dispatch.ts:362-377` |
| `Store.*` | step ids as opaque strings | `memory.ts`, `postgres/store.ts` |
| `ctx.idempotencyKey(scope)` | `"nagi:" + runId + ":" + stepId + ":" + scope` | `idempotency.ts:13-17` |

**Therefore the only contracts `b.steps()` must uphold are:**

1. **Return a `StepMap`** — a `Readonly<Record<string, Step<unknown>>>` whose keys match the user's record keys verbatim.
2. **Each value is a `StepWithDef`** (has `__def: TaskDef`) — equivalent to what `b.task(…)` produces.
3. **`def.needs` is a `Record<localKey, Step<unknown>>`** where each value is an **object reference** to the sibling Step in the same returned map — so `idByIdentity.get(upstream)` will succeed in `walkAndRewrite`.
4. **Each Step has `id: ""` at construction time** (so `walkAndRewrite`'s cross-flow-sharing guard at `builder.ts:237` doesn't false-positive).

Match arm semantics, `parentMatch` annotations, and `_nested` handling are **only** relevant if `b.steps()` accepts match entries — leaving matches as `b.match(…)` separately (the RFC's recommendation) keeps `b.steps()` entirely flat.

---

## 5. Gap analysis — RFC vs. current code

### Reframe: identity channels

| RFC claim | Code reality |
|---|---|
| Step identity is split across "variable name → return-record key → persisted step id" (three channels) | Variable name has no runtime/persistence effect. The persisted id comes only from the return-record key (`collectIds`). The variable name only affects how downstream code reads `needs: { upload }` (where `upload` is the local const). |
| "Renaming the local variable changes the persisted step id (because the return record's key follows the variable)" | False as stated: the return record's key does NOT follow the variable name. `return { upload }` is shorthand for `return { upload: upload }` — renaming the const requires also updating the return-key (or using `return { upload: renamedConst }`). The id follows the **return key**, not the const. |

This isn't a fatal flaw in the RFC — the actual wins (no return wall, typo-safe needs, single mention site for the id) are real and worth doing. But the implementation PR description should reframe the motivation accurately to avoid confusing readers who go to verify.

### Field naming: `timeout` → `timeoutMs` (DECIDED — rename everywhere)

- Current: `StepConfigBase.timeout?: Millis` (where `Millis = number`).
- RFC: `timeoutMs?: number`.

**Decision: rename `timeout` → `timeoutMs` uniformly across the codebase as part of this PR.** This avoids the mixed-naming worst-case (where `b.task()` keeps `timeout` and `b.steps()` uses `timeoutMs`) and produces a self-documenting field name throughout.

The value type stays `Millis` (the alias is fine; the **field name** carries the unit). Affected files:

| File | Change |
|---|---|
| `packages/core/src/types.ts` | `StepConfigBase.timeout` → `timeoutMs` (line 138) |
| `packages/core/src/internal.ts` | `TaskDef.timeout` → `timeoutMs` (line 29); `SignalDef.timeout` → `timeoutMs` (line 46) |
| `packages/core/src/builder.ts` | Two spread-conditional blocks: `config.timeout !== undefined ? { timeout: ... } : {}` → `config.timeoutMs !== undefined ? { timeoutMs: ... } : {}` (`task` at lines 47, `signal` at line 68) |
| `packages/core/src/builder.test.ts`, `runtime.test.ts`, `dispatch.test.ts`, fixtures | Search-replace any `timeout:` usages on step configs |

Note that **no code currently enforces the timeout at runtime** (the field is read-but-not-applied in `dispatch.ts`); this is a pre-existing gap, orthogonal to RFC 0002. The rename is purely a cosmetic/API improvement.

### `when` predicate signature

- Current: `when?: (args: { input, needs: NeedsOutputs<N> }) => boolean`.
- RFC: `when?: (args: { input: FlowInput }) => boolean` — missing `needs`.

**Recommendation:** preserve the current signature. The scheduler at `scheduler.ts:56-65` passes both `input` and `resolveNeeds(def, …)` to `when`, so dropping `needs` from the type would mislead users and silently work at runtime. Use:

```ts
when?: (args: { input: Input; needs: NeedsOf<S, K> }) => boolean;
```

### `Builder.steps()` return type

- RFC: `steps<const S>(record: S & ValidatedSteps<S, Input>): Flow<Input, S>`.
- Reality: `b.steps()` is called inside `build`, so it must return a `StepMap`. `flow()` is what produces the `Flow<…>` from that `StepMap`.

**Recommendation:** the correct signature is:

```ts
steps<const S extends ValidatedSteps<S, Input>>(record: S):
  { readonly [K in keyof S]: Step<OutputOf<S[K]>> };
```

This composes cleanly with `FlowConfig.build: (b) => M extends StepMap` — `M` becomes the literal `{ a: Step<{y:number}>, b: Step<{z:number}>, … }` map, and `FlowConfig.output?(steps: NeedsOutputs<M>)` gets the same typed-record surface it has today.

### `OutputOf<T>` — phantom field vs. `run` inference

The RFC defines `OutputOf<T>` by `infer R` from `T["run"]`'s return type. The existing `StepOutput<S>` reads the phantom `__output?: O` field of `Step<O>`.

Both work, but choosing inconsistently produces friction:

- Inside `b.steps()`, entries are not yet `Step<O>` — they're raw `StepEntry` records. `OutputOf<T>` must `infer` from `run`'s return type. ✓
- Once `b.steps()` returns, each value is a `Step<O>` (with `__output: O` set by inference). Downstream type lookups (`NeedsOutputs<M>` in `flow.output`) use the phantom field via `StepOutput<…>`. ✓

So **`OutputOf<T>` (RFC-defined) and `StepOutput<S>` (existing) are complementary, not competing**. The implementation will use `OutputOf` inside the `ValidatedSteps<S, Input>` constraint and the `steps()` return-mapping, and `StepOutput` continues to govern everything outside `b.steps()`.

### Cycle / self-reference detection

- Today: missing-needs throws in `walkAndRewrite` because `idByIdentity.get(upstream) === undefined`. No explicit cycle check beyond that — a self-reference like `needs: { self }` where `self` is the step's own const is grammatically impossible (TDZ) so the issue doesn't arise.
- RFC 0002: `Exclude<keyof S, K>` in `ValidatedSteps` rejects self-references at the type level. Runtime cycle through intermediaries (A→B→A) is not detected — but it's not detected today either, and the scheduler would deadlock the same way.

**Recommendation:** match today's behavior. Type-level `Exclude<keyof S, K>` is sufficient for direct self-references; multi-hop cycles are an existing scheduler concern (consider a separate RFC for cycle detection if it becomes a real-world problem).

---

## 6. Implementation surface

Concrete edits, by file:

### `packages/core/src/types.ts` — additions only

Add the new types after `NeedsOutputs`:

```ts
// --- RFC 0002 additions ---

/** Output of a step *entry* (raw config), derived from its `run` return type. */
export type OutputOf<T> =
  T extends { readonly run: (...args: any[]) => infer R } ? Awaited<R> : never;

/** Sibling-output lookup inside a `b.steps()` entry's `run` handler. */
export type NeedsOf<S, K extends keyof S> =
  S[K] extends { readonly needs: infer N }
    ? N extends ReadonlyArray<keyof S & string>
      ? { readonly [P in N[number]]: OutputOf<S[P]> }
      : Record<string, never>
    : Record<string, never>;

/** Per-entry constraint inside `b.steps(record)`. */
export type StepsRecord<S, Input> = {
  readonly [K in keyof S]: {
    readonly needs?: ReadonlyArray<Exclude<keyof S, K> & string>;
    readonly when?: (args: {
      readonly input: NoInfer<Input>;
      readonly needs: NoInfer<NeedsOf<S, K>>;
    }) => boolean;
    readonly retry?: RetryPolicy;
    readonly timeoutMs?: Millis;   // renamed from `timeout` as part of this PR
    readonly run: (args: {
      readonly input: NoInfer<Input>;
      readonly needs: NoInfer<NeedsOf<S, K>>;
      readonly ctx: StepCtx<NoInfer<Input>>;
    }) => Promise<unknown>;
  };
};

/** The materialized StepMap returned by `b.steps(record)`. */
export type StepsResult<S> = { readonly [K in keyof S]: Step<OutputOf<S[K]>> };
```

Extend the `Builder` interface (`types.ts:249`):

```ts
export interface Builder<Input = unknown> {
  task<...>(...): Step<...>;
  signal<...>(...): Step<...>;
  match<...>(...): Step<...>;
  match<...>(...): Step<...>;

  // RFC 0002
  steps<const S extends StepsRecord<S, Input>>(record: S): StepsResult<S>;
}
```

The `const S extends StepsRecord<S, Input>` pattern: the `const` modifier preserves literal types in `needs: ["upload"]` arrays; the recursive `S extends StepsRecord<S, Input>` is the self-referential validation constraint. TS 5.0+ handles this in a single inference pass — there is precedent for this pattern in Hono's route-typing.

### `packages/core/src/internal.ts` — one small mutator export

The two-phase construction (placeholder Step → resolve needs → attach def) requires the ability to attach a def to an already-constructed Step object. Today's `attachDef` returns a **new** object, which doesn't fit the placeholder pattern.

Add:

```ts
/**
 * RFC 0002: in-place def attachment. Used by `b.steps()` so a record entry
 * can hold its sibling-Step object identity stable while the def (which
 * references those same sibling identities via `needs`) is built last.
 */
export function attachDefMut(step: Step<unknown>, def: StepDef): void {
  (step as { [DEF]: StepDef })[DEF] = def;
}
```

This keeps the `DEF` symbol module-private; `b.steps()` uses the mutator rather than reaching into the private symbol.

### `packages/core/src/builder.ts` — add `steps` to `makeBuilder`

Inside `makeBuilder<Input>()`, alongside `task` / `signal` / `match`:

```ts
function steps<const S extends StepsRecord<S, Input>>(record: S): StepsResult<S> {
  const keys = Object.keys(record) as Array<keyof S & string>;

  // PHASE 1: pre-allocate Step shells with stable object identity.
  //   These are the references that sibling `needs` will point to. The
  //   final def is attached in phase 2; until then they have no __def.
  const out = {} as { [K in keyof S]: Step<unknown> };
  for (const key of keys) {
    out[key] = { kind: "task", id: "" } as Step<unknown>;
  }

  // PHASE 2: for each entry, build the TaskDef with rewritten needs map
  //   pointing to the phase-1 shells, then attach the def in place.
  for (const key of keys) {
    const entry = record[key] as StepsRecord<S, Input>[typeof key];
    const needsMap: NeedsMap = {};
    for (const sibKey of entry.needs ?? []) {
      needsMap[sibKey] = out[sibKey as keyof S];
    }
    const def: TaskDef = {
      kind: "task",
      needs: needsMap,
      ...(entry.retry !== undefined ? { retry: entry.retry } : {}),
      ...(entry.timeoutMs !== undefined ? { timeoutMs: entry.timeoutMs } : {}),
      ...(entry.when !== undefined
        ? { when: entry.when as TaskDef["when"] }
        : {}),
      run: entry.run as TaskDef["run"],
    };
    attachDefMut(out[key], def);
  }

  return out as StepsResult<S>;
}

return { task, signal, match: match as Builder<Input>["match"], steps };
```

**Why two phases:** sibling references in `needs` must be stable object identities that survive into `flow()`'s `idByIdentity` map. The phase-1 shells are those identities. Phase 2 then builds the def (whose needs map points to those identities) and stamps it onto the shell.

**Why this works with `walkAndRewrite`:**
- `collectIds(out, "", idByIdentity)` walks `out` and registers each shell's identity under its record key.
- `walkAndRewrite` reads `def.needs[localKey]` (a shell), calls `idByIdentity.get(shell)` → returns the key. ✓
- `step.id !== ""` guard passes because shells were constructed with `id: ""`.
- `def === undefined` guard passes because `attachDefMut` set `__def`.

### `packages/core/src/index.ts` — no changes needed

`Builder` is already re-exported via `export type * from "./types"`. The new `OutputOf`, `NeedsOf`, `StepsRecord`, `StepsResult` types ride along automatically.

### `packages/core/src/builder.test.ts` — parallel coverage

Add a `describe("b.steps record-literal API")` block mirroring the existing builder tests:

1. **Step id = record key** — `b.steps({ a: { run: ... } })` produces a flow with `steps.a.id === "a"`.
2. **Needs rewrite** — `b.steps({ a: { run: ... }, b: { needs: ["a"], run: ... } })` produces a flow where `getDef(steps.b).needs.a.id === "a"`.
3. **Local needs alias** — same record key serves as the alias inside the handler (`needs.a`, not a separate key).
4. **Compile-error cases** (move to `types.test-d.ts`):
   - Typo: `needs: ["uplaod"]` is a type error.
   - Self-reference: `a: { needs: ["a"], ... }` is a type error.
   - Cross-flow sharing: still rejected at `walkAndRewrite` runtime time (no type-level guard exists today).
5. **Coexistence with `b.task`/`b.match`/`b.signal`** — `build: (b) => ({ ...b.steps({ a, b }), c: b.match({...}) })` works.

### `packages/core/src/types.test-d.ts` — type-level coverage

Mirror the existing `expectTypeOf` tests:

```ts
test("b.steps: needs.X resolves to OutputOf<S[X]>", () => {
  const f = flow({
    id: "t",
    input: passthroughSchema<{ x: number }>(),
    build: (b) => b.steps({
      a: { run: async ({ input }) => ({ y: input.x * 2 }) },
      b: { needs: ["a"], run: async ({ needs }) => ({ z: needs.a.y + 1 }) },
    }),
  });

  expectTypeOf(f.steps).toEqualTypeOf<{
    readonly a: Step<{ y: number }>;
    readonly b: Step<{ z: number }>;
  }>();
});

test("b.steps: needs typo is a compile error", () => {
  flow({
    id: "t",
    input: passthroughSchema<{ x: number }>(),
    build: (b) => b.steps({
      a: { run: async () => ({ y: 1 }) },
      // @ts-expect-error — "aa" is not a sibling key
      b: { needs: ["aa"], run: async () => ({ z: 1 }) },
    }),
  });
});

test("b.steps: self-reference is a compile error", () => {
  flow({
    id: "t",
    input: passthroughSchema<{ x: number }>(),
    build: (b) => b.steps({
      // @ts-expect-error — can't depend on yourself
      a: { needs: ["a"], run: async () => ({ y: 1 }) },
    }),
  });
});

test("b.steps: flow.output receives typed step outputs", () => {
  const f = flow({
    id: "t",
    input: passthroughSchema<{ x: number }>(),
    build: (b) => b.steps({
      a: { run: async () => ({ y: 1 }) },
      b: { needs: ["a"], run: async () => ({ z: 2 }) },
    }),
    output: (steps) => ({ total: steps.a.y + steps.b.z }),
  });

  expectTypeOf<FlowOutput<typeof f>>().toEqualTypeOf<{ total: number }>();
});
```

### `packages/core/src/runtime.test.ts` — runtime parity

Convert (or duplicate) the three canonical scenarios:

1. **Linear chain with typed needs** — `runtime.test.ts:12-33`. Verify `needs.a.doubled` flows.
2. **`when`-false skip cascade** — `runtime.test.ts:36-66`. Verify step ids `"gate"`, `"branch"`, `"after"` come from record keys; reasons `"when-false"` / `"transitive"` match today.
3. **Retry with backoff** — `runtime.test.ts:69-116`. Verify `entry.retry` propagates to `def.retry` and `handleStepError` reads it correctly.

---

## 7. Decision points the implementation will face

These should be resolved before coding begins. The recommended answer is given but the user may want to override.

| # | Decision | Status | Notes |
|---|---|---|---|
| 1 | `b.steps()` accepts only tasks (v1) | ✅ **Decided: tasks only** | Matches/signals stay on `b.match` / `b.signal`. Users mix via `{ ...b.steps({...}), choose: b.match({...}) }`. The RFC explicitly leans this way (§Edge cases). |
| 2 | Field name `timeout` vs `timeoutMs` | ✅ **Decided: rename everywhere to `timeoutMs`** | This PR uniformly renames `timeout` → `timeoutMs` across `TaskConfig`, `SignalConfig`, `StepConfigBase`, `TaskDef`, `SignalDef`, and tests. See §5 for the file-by-file change. |
| 3 | `when?` parameter shape | ✅ **Decided: keep `{ input, needs }`** | Matches the runtime (`scheduler.ts:56-65`). |
| 4 | Construction strategy: placeholders + mutator vs. mutate readonly def | **Recommended: placeholders + `attachDefMut`** | The alternative (build TaskDef with empty needs via `attachDef`, then mutate the readonly `needs` field) works at runtime but violates the `readonly` contract more loudly. The mutator is one tiny exported function vs. a code-smell mutation. Confirm during implementation. |
| 5 | Test layout: parallel `builder-steps.test.ts` vs. extend `builder.test.ts` | **Recommended: extend** | Add a `describe("b.steps")` block. One file per concern stays readable while the API is small. |
| 6 | Whether to deprecate the legacy `task/return` form now | **Recommended: no** | RFC says coexist; deprecation is a later major. |
| 7 | Whether `b.steps()` validates that all sibling refs resolve at runtime | **Recommended: no** | `walkAndRewrite` already throws on missing siblings; the type system (via `Exclude<keyof S, K>`) catches it at compile time. |
| 8 | Spread/merge support (`b.steps({ ...baseSteps, extra: {...} })`) | **Defer** (RFC open question #5) | Adds inference complexity; not needed for the lymo dogfooding case. Revisit when a real use case appears. |

---

## 8. Test strategy

| Layer | File | What to cover |
|---|---|---|
| Type-level | `types.test-d.ts` | `b.steps` return shape; `needs.X` typed lookup; typo rejection; self-reference rejection; `flow.output` typed input |
| Builder semantics | `builder.test.ts` | Record-key → step id; needs rewrite; coexistence with `b.task/match/signal`; cross-flow sharing still rejected |
| Runtime parity | `runtime.test.ts` | Re-run the linear-chain, when-skip, and retry-policy scenarios using `b.steps()` instead of `b.task()` returns |
| Scheduler / dispatcher | (no new tests) | These are id-string-only; no code change ⇒ no test change |

The runtime parity tests are the **proof** that `b.steps()` produces a `StepMap` indistinguishable from today's `build()` returns from the runtime's perspective. If they pass without changes to `scheduler.ts`, `dispatch.ts`, `runtime.ts`, or any `Store` implementation, the "no downstream changes" claim is verified.

---

## 9. Phased implementation plan (concrete)

Mapping the RFC's phases to specific files and PR boundaries:

| Phase | RFC stage | Concrete edits | Verification |
|---|---|---|---|
| **0** | `timeout` → `timeoutMs` rename (pre-req) | Rename in `types.ts` (StepConfigBase), `internal.ts` (TaskDef, SignalDef), `builder.ts` (two spread conditionals), and any test that sets `timeout:` on a step config. | Full test suite passes after the rename, before any RFC 0002 work begins. Lands as a single-commit prelude. |
| **1** | Type-level prototype | Add `OutputOf`, `NeedsOf`, `StepsRecord`, `StepsResult` to `types.ts`; extend `Builder` interface | `tsc --noEmit` clean; spot-check `types.test-d.ts` scenarios manually |
| **2** | Runtime impl | Add `attachDefMut` to `internal.ts`; add `steps` to `makeBuilder` in `builder.ts` | Existing `builder.test.ts` and `runtime.test.ts` still pass — no regression |
| **3** | Testing | Add type-level cases to `types.test-d.ts`; add runtime cases to `builder.test.ts` and `runtime.test.ts` | New tests pass; coverage of typo / self-ref / cross-flow rejection |
| **4** | Compile-time perf check (publication gate) | Author a 15-step and 50-step fixture flow; run `tsc --extendedDiagnostics`; record check time | RFC §Drawbacks calls out O(N²) at 50+ steps. **Decided: gate publication on the 50-step number being acceptable** (record in docs; if degraded, surface as an open question before phase 5). |
| **5** | Docs + changeset | Update README to lead with `b.steps`; document legacy under "Migration"; add changeset (minor — additive) | Changeset CI passes. **Jay writes the README himself per saved feedback.** |

Phase 1 and 2 can land in a single PR; phase 3 is standalone. Phase 4 is investigative — if compile time at 50 steps is fine, the result becomes a note in the docs; if it's a problem, surface as an open question before phase 5.

---

## 10. Risks & open questions

### Risks

- **TS inference depth at scale** — `<const S extends StepsRecord<S, Input>>` is a self-referential constraint. TS 5.9.3 handles this, but each new entry adds another row to `ValidatedSteps<S, Input>` and re-checks the recursion. The RFC's 50-step benchmark in phase 4 is the gate.
- **Error message quality** — `Exclude<keyof S, K>` violations produce verbose error strings like `Type '"uplaod"' is not assignable to type '"upload" | "transcribe" | "compression" | …'`. Acceptable; doesn't block. If users complain, branded types can polish later.
- **Phase-2 mutation of placeholder objects** — relies on the placeholder having no `__def` initially and being a writable plain object. The placeholder is constructed inline in `b.steps()`, so this is safe and isolated, but it's the one non-obvious bit of the implementation.

### User-resolved decisions (locked in before implementation)

| Question | Decision |
|---|---|
| Tasks-only for v1? | ✅ Yes. Matches/signals stay on `b.match` / `b.signal`. |
| Field name `timeout` vs `timeoutMs`? | ✅ Rename everywhere to `timeoutMs` as a phase-0 prelude commit. |
| `when?` parameter shape? | ✅ Keep `{ input, needs }` (matches runtime). |
| Run the 50-step `tsc` benchmark before publishing? | ✅ Yes — gate publication on the result. If the 50-step compile time is acceptable, publish; if degraded, surface as an open question before phase 5. |

### Remaining open questions (no user input needed; agent-level decisions)

1. **`b.steps((self) => …)` proxy overload?** Recommendation: no — adds surface for marginal value, RFC §Rationale already weighed and rejected this.
2. **What to do if the 50-step benchmark is bad?** If `tsc` cost spikes, the fallback is to split `StepsRecord<S, Input>` into a less-recursive shape (e.g., separate the per-entry validation from the cross-entry `needs` check). That would be a meaningful design change and should re-enter review.

---

## 11. File-level changelog summary (post-research, pre-PR)

| File | Change | Lines (approx) |
|---|---|---|
| `packages/core/src/types.ts` | (Phase 0) Rename `timeout` → `timeoutMs` in `StepConfigBase`. (Phase 1) Add 4 type aliases + 1 method to `Builder` interface. | ~+35, ~3 renamed |
| `packages/core/src/internal.ts` | (Phase 0) Rename `timeout` → `timeoutMs` in `TaskDef` and `SignalDef`. (Phase 2) Add `attachDefMut` export. | +4, ~2 renamed |
| `packages/core/src/builder.ts` | (Phase 0) Update spread conditionals in `task`/`signal` to use `timeoutMs`. (Phase 2) Add `steps` function inside `makeBuilder`; include in returned object. | +30, ~2 renamed |
| `packages/core/src/builder.test.ts` | (Phase 0) Update any `timeout:` usages. (Phase 3) Add `describe("b.steps")` block. | +80 |
| `packages/core/src/types.test-d.ts` | (Phase 3) Add 4-6 type-level cases | +60 |
| `packages/core/src/runtime.test.ts` | (Phase 0) Update any `timeout:` usages. (Phase 3) Add 3 parity scenarios. | +120 |
| `packages/core/src/dispatch.test.ts` | (Phase 0) Update any `timeout:` usages. | ~5 renamed |
| `README.md` | Updated by Jay (per saved feedback — agent does not modify) | — |
| `.changeset/*` | New minor-version changeset for `@nagi-js/core` (note: includes the `timeoutMs` rename — call it out so users get a clear migration note) | +15 |

Net change: ~340 new lines + ~12 line renames, zero edits to `scheduler.ts`, `dispatch.ts`, `runtime.ts`, `memory.ts`, `idempotency.ts`, `worker.ts`, or any package outside `core`.

**Note on the `timeoutMs` rename:** because no runtime code currently reads `def.timeout`, the rename is a pure type-surface change. Users who currently set `timeout: 30_000` on a step config will see a type error and need to rename to `timeoutMs: 30_000`. The changeset should explicitly call this out as a small breaking change to the `0.0.1-rc` API — acceptable at this pre-1.0 stage but worth surfacing.

---

## Appendix A — Files read during research

- `docs/rfcs/0002-record-literal-builder-api.md` (the RFC itself)
- `packages/core/src/types.ts` (verbatim, all 728 lines)
- `packages/core/src/builder.ts` (verbatim, all 319 lines)
- `packages/core/src/internal.ts` (verbatim, all 245 lines)
- `packages/core/src/index.ts`
- `packages/core/src/builder.test.ts`
- `packages/core/src/types.test-d.ts`
- `packages/core/src/runtime.test.ts`
- `packages/core/src/scheduler.ts`
- `packages/core/src/dispatch.ts`
- `packages/core/src/runtime.ts`
- `packages/core/src/memory.ts`
- `packages/core/src/idempotency.ts`
- `packages/core/src/test-helpers.ts`
- `package.json`, `tsconfig.base.json`

Research conducted by two parallel `feature-dev:code-explorer` subagents (agent ids `afef80e0fe73a57f4` and `aa3f4b5643be439e4`) plus main-context spot-checks of the four load-bearing files in `packages/core/src/`.
