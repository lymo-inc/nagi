---
"@nagi-js/core": patch
"@nagi-js/postgres": patch
---

Simplify the core public surface — three breaking changes that collapse parallel APIs into a single canonical shape. Net: −250 production LOC, −3.5 KB bundle, −2.7 KB d.ts.

**`FlowCanceledFact` is now a discriminated union by `cause`.** The previous shape had an optional `cause` plus an always-required `concurrencyKey` that was abused to carry the `reason` string on explicit cancels. The new shape is three concrete arms:

- `{ cause: "concurrency", canceledByRunId, concurrencyKey }`
- `{ cause: "explicit", reason, note? }`
- `{ cause: "operator", actor, reason, note? }`

`Store.tryStartRun`'s returned `canceled[].fact` is now typed `FlowCanceledByConcurrencyFact` — adapters writing concurrency cancel facts must set `cause: "concurrency"` explicitly. Adapter persistence that previously stored `canceledByRunId` unconditionally should null it out on non-concurrency arms (see the postgres adapter for an example).

**`b.step` chain API and `b.include` are removed.** The single canonical way to declare a step is `b.task({ needs: { key: stepRef }, ... })`. Migration:

```ts
// Before
build: (b) =>
  b
    .step("a", { run: async () => ({ v: 1 }) })
    .step("b", { needs: ["a"], run: async ({ needs }) => needs.a.v + 1 })

// After
build: (b) => {
  const a = b.task({ run: async () => ({ v: 1 }) });
  const c = b.task({ needs: { a }, run: ({ needs }) => needs.a.v + 1 });
  return { a, c };
}
```

`StepEntryConfig`, `BuildResult`, `BuilderAccumulator`, `AsStepMap`, and the `Builder<Input, A>` second generic parameter are removed. `FlowConfig.build` is now typed `(b: Builder<Input>) => R extends StepMap`.

**`b.match({ on, cases })` discriminator form is removed.** Only `b.match({ arms: [...] })` remains. Migration:

```ts
// Before
b.match({
  on: ({ input }) => input.kind,
  cases: {
    a: (b1) => ({ x: b1.task({ run: ... }) }),
    b: (b1) => ({ y: b1.task({ run: ... }) }),
  },
})

// After
b.match({
  arms: [
    { when: ({ input }) => input.kind === "a", build: (b1) => ({ x: b1.task({ run: ... }) }) },
    { when: ({ input }) => input.kind === "b", build: (b1) => ({ y: b1.task({ run: ... }) }) },
    // ...or use { otherwise: true } for the fallback arm
  ],
})
```

`MatchDiscriminatorConfig` and `MatchDiscriminatorOutput` types are removed. The internal `MatchDef` no longer carries a `mode` field; `matchArms()` helper is dropped (just read `def.arms` directly). Match arms identified by case-key (e.g. `m.a.x`) are now positionally identified (`m.arm0.x`, `m.otherwise.y`); flow snapshots will rehash. The `CanonicalStep.matchMode` and `matchOnHash` fields are removed (no longer meaningful with single-arm semantics).
