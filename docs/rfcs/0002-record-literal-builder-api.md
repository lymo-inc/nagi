# RFC 0002 — Record-literal builder API

- **Status:** Draft
- **Author:** @jay (lymo-inc)
- **Created:** 2026-05-14
- **Related:** `@nagi-js/core`

## Summary

Replace the current builder pattern (`flow({ build: (b) => { const x = b.task(...); …; return { x, …} } })`) with a record-literal form:

```ts
flow({
  build: (b) => b.steps({
    upload:     { run: ({ ctx }) => doUpload(ctx) },
    transcribe: { needs: ["upload"], run: ({ needs }) => doTranscribe(needs.upload) },
    summary:    { needs: ["transcribe"], run: ({ needs }) => doSummary(needs.transcribe) },
  }),
})
```

The record key IS the step id. `needs: ["upload"]` references siblings by string literal. TypeScript indexed-access types resolve `needs.upload` to the typed output of the sibling. There is no return statement, no const-bound variable to track, no second source of identity for any step.

## Motivation

The current builder API has three frictions, all symptoms of the same root cause: **a step's identity is split across two channels** — the variable name (used for typed references in downstream `needs:`) and an implicit string id derived from the final return record's key.

1. **Triple-mention.** A step named `transcribe` appears as a `const transcribe = …` binding, again inside `needs: { transcribe }` of every dependent, and once more in the final `return { …, transcribe, … }`. Three places to keep in sync.
2. **The return wall.** A 14-step flow ends with a 14-name return statement. Readers parse it as a table of contents; writers parse it as boilerplate they have to maintain.
3. **Variable-name / step-id drift.** Renaming the local variable changes the persisted step id (because the return record's key follows the variable). For a system that wants to persist step ids in fact logs, snapshot stores (RFC 0001), and webhooks, this is fragile — a local refactor breaks downstream history.

The dogfooding driver: lymo's `video-analysis` flow is 14 steps. Under the current API, the build function is roughly 14 declaration lines + a 14-name return statement. Adding a step is a three-place edit; renaming one is dangerous.

## Detailed design

### The new API

`b.steps(record)` accepts an object where each key is a step id and each value is a `StepEntry`:

```ts
type StepEntry = {
  needs?: ReadonlyArray<StepId>;   // string literals referencing sibling keys
  when?: (args: { input: FlowInput }) => boolean;
  retry?: RetryPolicy;
  timeoutMs?: number;
  run: (ctx: StepCtx) => unknown;
};
```

The handler's `ctx.needs` is typed as the record-keyed lookup of outputs for the steps named in this entry's `needs:` array.

### Type-level skeleton

This is the heart of the RFC. Four type aliases, all on `@nagi-js/core`:

```ts
// 1. What a step's output is — read from its `run` function's return type.
type OutputOf<T> =
  T extends { run: (...args: any[]) => infer R } ? Awaited<R> : never;

// 2. What `needs.X` resolves to inside a step's `run` handler.
//    Walks S[K].needs and looks up each entry's OutputOf.
type NeedsOf<S, K extends keyof S> =
  S[K] extends { needs: infer N extends ReadonlyArray<keyof S & string> }
    ? { [P in N[number]]: OutputOf<S[P]> }
    : Record<string, never>;

// 3. The validated shape of a steps record — each entry constrained
//    relative to the whole record S.
type ValidatedSteps<S, Input> = {
  [K in keyof S]: {
    readonly needs?: ReadonlyArray<Exclude<keyof S, K> & string>;
    readonly when?: (args: { input: Input }) => boolean;
    readonly retry?: RetryPolicy;
    readonly timeoutMs?: number;
    readonly run: (ctx: {
      input: Input;
      needs: NeedsOf<S, K>;
      ctx: StepCtx;
    }) => unknown;
  };
};

// 4. The public API. The `const` modifier on S preserves
//    string-literal inference in `needs: ["upload"]` arrays without
//    requiring `as const` at the call site.
interface Builder<Input> {
  steps<const S>(record: S & ValidatedSteps<S, Input>): Flow<Input, S>;
}
```

A few subtleties:

- **Self-referential generics.** `ValidatedSteps<S>` references `S` for every entry's needs and run signature. TS handles this fine; the cost is at type-instantiation time on each call site.
- **Disjoint constraints.** `Exclude<keyof S, K>` enforces that a step cannot depend on itself.
- **Strict literal inference.** `<const S>` (TS 5.0+) preserves the string literal types in needs arrays. Without it, `needs: ["upload"]` would widen to `string[]` and `NeedsOf` would degrade to `Record<string, never>`.
- **`run` return type.** Inferred independently from `needs`. The two-pass nature (infer the record shape including outputs, then validate handlers against the inferred shape) is what TypeScript's higher-order type inference handles in this construct.

### How this compares to today

| Concern | Current API | Proposed API |
|---|---|---|
| Step identity | Variable name → return-record key → persisted step id (three channels) | Record key (one channel) |
| Typed `needs` | `needs: { upload }` (typed reference) | `needs: ["upload"]` (string literal, type-resolved) |
| Return statement | Required, 1 name per step | None |
| Typo-safe refs | Compile error (variable not in scope) | Compile error (`Exclude<keyof S, K>` violation) |
| Refactor: rename variable | Changes persisted step id (breaking) | No effect — the variable doesn't exist |
| Refactor: rename step id | Find/replace across declaration + needs + return | Find/replace across record key + needs |
| Add a step | New `const` line + `needs:` ref + return-record entry | New record entry |
| Remove a step | Delete `const` + return-record entry + check no needs refs | Delete record entry + check no needs refs |

### Worked example

A typical mid-sized DAG (5 steps) under both APIs:

**Today:**
```ts
flow({
  id: "demo",
  input: passthroughSchema<{ x: number }>(),
  build: (b) => {
    const a = b.task({ run: async ({ input }) => ({ y: input.x * 2 }) });
    const b1 = b.task({ needs: { a }, run: async ({ needs }) => ({ z: needs.a.y + 1 }) });
    const c = b.task({ needs: { a }, run: async ({ needs }) => ({ w: needs.a.y * 3 }) });
    const d = b.task({ needs: { b: b1, c }, run: async ({ needs }) => ({ ok: needs.b.z + needs.c.w }) });
    return { a, b: b1, c, d };
  },
});
```

**Proposed:**
```ts
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

Eight lines vs five. Each step exactly one line. No const, no return. Notice how `b: b1` (today's awkward "I had to rename to avoid the `b` collision with the builder") simply becomes `b` in the proposed form — the record key is in its own namespace, no clash with builder identifiers.

### Edge cases

**Empty `needs`.** Omit the field. `NeedsOf` resolves to `Record<string, never>`; the handler sees `ctx.needs: {}`.

**Cyclic references.** Compile error today: `needs: ["self-id"]` is rejected by `Exclude<keyof S, K>`. Runtime cycle check (for cycles via intermediaries: A→B→A) stays in `flow()` validation and throws `NagiValidationError`.

**`when` predicate.** Lives on the same entry. Skipped steps cascade to dependents (same semantics as today; see existing scheduler docs).

**Output reducer at the flow level.** The existing `flow({ output })` callback continues to work; it receives the full inferred outputs record (analogous to today's return-record-typed argument).

**Matches and signals.** Open question — see below. Possible answers: `b.steps({ ... })` accepts only tasks; `b.match` and `b.signal` continue as separate builder methods. Or: extend `StepEntry` with a `kind` field and put everything in the record. Lean toward the former for the first iteration.

**Conditional types for `run`'s return type at the flow level.** The flow's overall output type is `{ [K in keyof S]: OutputOf<S[K]> }`. This composes with `flow({ output })` cleanly.

### Backwards compatibility

The current `flow({ build: (b) => { const x = b.task(...); return { x, … } } })` API is *not* removed by this RFC. `b.steps(record)` is a new method on the builder; existing flows continue to type-check and execute. Users can migrate at their pace.

A long-tail deprecation can come in a later major version. The two APIs share the same underlying `Step` representation, so a single runtime path serves both.

## Rationale and alternatives

### Why a record literal?

The fundamental design question: where does a step's identity live?

- **Variable name** (today). Tight binding to a local lexical name. Loses one-to-one mapping under refactors. Forces a return statement to expose the bindings.
- **Positional id** (`b.task("upload", { ... })`, considered in design discussion). Decouples variable from id but introduces a second channel — the variable still exists for downstream typed references.
- **Record key** (this proposal). Single channel. Indexed-access types make the key carry both runtime identity and compile-time type lookup.

The record key wins because it collapses the two channels into one. The only place "upload" appears is the record key and any sibling that references it in `needs:`. There's nothing else to keep in sync.

### Why string literals in `needs:`?

The alternative is typed references — `needs: [upload]` where `upload` is a value of type `Step<UploadOutput>`. To make that work, we'd need the record literal to be a "forward-referenceable" structure, which JavaScript object literals are not (each value is eagerly evaluated). The two paths to make it work — a function-wrapped builder that exposes a `self` proxy, or a method chain — re-introduce some of the visual weight we're trying to remove.

String literals work because TypeScript can carry them at the type level. With `const S` inference, `needs: ["upload"]` preserves the literal type `readonly ["upload"]`, which `NeedsOf` then walks to the typed output. We get the same compile-time safety as a typed reference, without giving up the record-literal shape.

### Why not auto-derive `needs:` from handler destructuring?

i.e., `run: ({ needs: { upload } }) => …` and have nagi figure out the deps. Two paths considered:

(a) **Runtime function parsing** (`Function.prototype.toString()`). Brittle under minification, hostile to bundlers, surprising to users when it breaks.

(b) **Build-time codegen** (Babel/SWC plugin). Adds a compiler step nagi doesn't currently require. Closes off "use nagi in a script with no build step" workflows.

Either path is significant infrastructure for a modest ergonomic gain (saving the `needs: [...]` declaration). Not worth it.

### Why not method chain (`b.task(...).task(...)`)?

A `.task("upload", ...).task("transcribe", { needs: ["upload"], ... })` chain accumulates types via each call's return type, similar to Drizzle's query builder. Two reasons to prefer the record literal:

1. **Reads top-to-bottom under both forms**, but the method chain visually nests; for a 14-step DAG the indentation/dot-chain gets noisy.
2. **The record literal is the data**. The chain is data + control flow. Record literals are easier to serialize, inspect in DevTools, and reason about as a static description of the DAG — which is exactly what's wanted for RFC 0001's canonicalization step.

### Why not an array (`b.steps([{ id: "upload", ... }, ...])`)?

TS inference at scale over arrays-of-objects (with each later element depending on earlier element types) requires variadic tuple types and recursive type inference. Possible, but compile-time performance degrades on larger arrays. A record literal is keyed access, which TypeScript handles efficiently.

### Why string-literal `needs`, not typed references via a `self` proxy?

Considered: `b.steps((self) => ({ upload: ..., transcribe: { needs: [self.upload], ... } }))`. The proxy carries typed references; you write `self.upload` instead of `"upload"`. Pro: catches typos via missing-property errors instead of string-mismatch errors (slightly nicer error messages). Con: introduces a proxy abstraction with implicit semantics, and the `self.X` form is *longer* than the bare `"X"` while gaining little. We get the same compile-time safety from `Exclude<keyof S, K>` constraints.

## Drawbacks

- **TS type complexity.** Four self-referential generics. Comparable in complexity to Hono's path-param inference (which ships at scale), but it's still real surface area to maintain. New contributors will need to understand it.
- **Compile-time perf at scale.** Self-referential indexed-access types are O(N²) in the worst case. For 14 steps, fine. For 50+, IDE responsiveness in `tsc --noEmit` and language-server hover-info may degrade. Benchmark at 50 and 100 steps before publishing.
- **Error message quality.** TS errors on `Exclude<keyof S, K>` violations can be verbose. Example: `Type '"uplaod"' is not assignable to type '"upload" | "transcribe" | "compression" | …'`. Acceptable but could be polished with branded types if needed.
- **Co-existing APIs.** Until the legacy `build` API is deprecated, the docs and types carry both forms. Adds a "which form should I use?" question for new users.

## Implementation plan

### Phase 1 — type-level prototype
- Sketch the four type aliases in a sandbox file.
- Validate with 5-step, 15-step, and 50-step example flows.
- Measure `tsc` time at each scale; tune if needed.

### Phase 2 — runtime implementation
- Add `Builder.steps(record)` to `@nagi-js/core`.
- Internally, transform the record into the existing `Step` representation so the rest of nagi (scheduler, dispatcher, store) is unchanged.
- Update `flow()` to accept either the legacy `build` return or the new `b.steps()` return.

### Phase 3 — testing
- Unit tests on type-level constraints (typo rejection, cycle rejection, needs typing).
- Integration tests reusing the existing `runtime.test.ts` scenarios with the new API.
- Type-only test file (`types.test-d.ts`) asserting handler-side `needs.X` is correctly typed.

### Phase 4 — docs
- README: add the new API as the primary recommendation; keep the legacy form documented under "Migration" / "Legacy".
- Migration guide: side-by-side examples.

### Phase 5 — changeset
- Major-or-minor version: this is additive, so minor. Mark legacy `build` as soft-deprecated; remove no earlier than the next major.

## Open questions

1. **Naming.** `b.steps(...)` vs `b.dag(...)` vs `b.tasks(...)`. `steps` matches the existing internal terminology (`step_run`, `step_id`); leaning toward that.
2. **Matches and signals.** Do they live inside the same record? Or are `b.match` and `b.signal` separate builder methods that compose with `b.steps`? Leaning toward separate methods for the first iteration.
3. **`needs: ReadonlyArray<…>` vs `Set<…>`-like.** Should ordering in the array matter for anything? Today's scheduler doesn't care; canonicalization (RFC 0001) sorts. Probably leave as array for ergonomics.
4. **Conditional output reducer typing.** `flow({ output: (steps) => … })` needs `steps` typed as `{ [K in keyof S]: OutputOf<S[K]> }`. Should work out of the box but worth confirming with the prototype.
5. **Spread/merge between steps records.** Useful for composing flows ("base steps + extension steps"). Adds inference complexity; defer.

## Future possibilities

- A small codegen tool that consumes an OpenAPI-like YAML/JSON description and emits the `b.steps({...})` form. Lets non-TS tooling describe flows declaratively.
- Higher-order helpers like `b.parallel({ ... })` for "all these run independently from the same upstream" patterns, expanding to multiple `b.steps` entries under the hood.
- A `b.subflow(otherFlow)` primitive that embeds another flow as a step. Composes with this API more cleanly than with today's builder.

## Prior art

- **Hono** — path-param inference (`app.get("/users/:id", c => c.req.param("id"))`) demonstrates that self-referential generics over string literals work at scale in real-world TypeScript libraries.
- **Drizzle ORM** — method-chained builder with type accumulation. We considered this and rejected for reasons noted in "Why not method chain."
- **Effect-ts** — pipe-based composition with strong type inference. Different mental model; not a direct fit for DAG description.
- **GitHub Actions / Argo Workflows** — flat YAML maps with `needs:` arrays. Same shape as this proposal, minus the type safety.
