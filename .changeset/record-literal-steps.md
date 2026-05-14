---
"@nagi-js/core": minor
---

Add `b.step()` chainable task builder (RFC 0002). Replaces the previously
proposed `b.steps({...})` record literal. The chain delivers full type
inference for sibling references without manual annotation:

```ts
flow({
  id: "demo",
  input: passthroughSchema<{ start: number }>(),
  build: (b) =>
    b
      .step("a", { run: async ({ input }) => ({ doubled: input.start * 2 }) })
      .step("b", {
        needs: ["a"],
        run: async ({ needs }) => ({
          // needs.a is typed as { doubled: number }
          next: needs.a.doubled + 1,
        }),
      }),
});
```

The first argument is the persisted step id; the config follows the
standalone `b.task` shape plus a `needs: ["sibling"]` tuple of accumulator
keys. Each `.step(key, config)` extends the builder's accumulator type, so:

- Typo in `needs: [...]` → compile error
- Duplicate chain key → compile error
- `needs.<sibling>` access inside `run` / `when` is fully typed

The chain coexists with `b.task` / `b.signal` / `b.match` — pre-built steps
enter the chain via `b.include(key, step)`:

```ts
build: (b) => {
  const route = b.match({ ... });
  return b
    .step("a", { ... })
    .step("b", { needs: ["a"], ... })
    .include("route", route);
}
```

`flow()` accepts either a chain return or a plain `StepMap` (back-compat for
existing `b.task` / `b.signal` / `b.match` patterns).

**Also breaking:** `timeout` field on task/signal configs renamed to
`timeoutMs` for unit clarity. Affects `TaskConfig`, `SignalConfig`, and
the internal `TaskDef` / `SignalDef`. Replace `timeout: 30_000` with
`timeoutMs: 30_000` in any step config. No runtime behavior change.
