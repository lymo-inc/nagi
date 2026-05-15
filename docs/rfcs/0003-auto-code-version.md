# RFC 0003 — Auto-compute `codeVersion` from registered flows

- **Status:** Draft
- **Author:** @jay (lymo-inc)
- **Created:** 2026-05-15 (JST)
- **Tracking issue:** lymo-inc/nagi#3
- **Related:** RFC 0001 (content-addressed snapshot store), `@nagi-js/core`
- **Research notes:** `0003-auto-code-version.research.md`

## Summary

Auto-compute `codeVersion` from the structural fingerprint of the registered
flows when the caller omits it. Today `NagiConfig.codeVersion` is optional but
defaults to `undefined`, which loses the per-deploy audit signal that
`workflow_run.code_version` is supposed to carry. Compute the default by
folding the already-computed per-flow `flowHash`es into a single SHA-256.

## Motivation

`NagiConfig.codeVersion` was introduced (alongside `flowHash`, RFC 0001) to
capture handler-code identity at run start. Drift on **topology** is detected
by `flowHash`; `codeVersion` is the matching process-wide audit signal,
written to `flow.started` facts and `workflow_run.code_version`.

In practice, every consumer has to invent a strategy for filling it in. The
obvious-looking options all have failure modes:

| Strategy | Failure mode |
|---|---|
| Build ID / git SHA from CI | False-positive drift on every deploy, even when no flow changed. Couples runtime audit to CI plumbing. |
| Per-flow declared version | Manual bumps; easy to forget → silent stale audit. |
| Source hash | Non-deterministic across rebuilds (whitespace, minification, bundler version). |
| Omit `codeVersion` | Loses the audit signal entirely (`code_version` is `null`). |
| Structural fingerprint of flows | Correct. But every consumer reinvents the canonical-form-and-hash code and has to reach into the public `Flow["steps"]` surface, which doesn't expose every field. |

Same dispatcher-shape that motivated #1: nagi has perfect visibility into the
flow graph at construction time and already canonicalizes every flow for the
snapshot store. The library should absorb the boilerplate.

## Proposed API

```ts
// Default: auto-compute
const wf = await nagi({ store, queue, flows });

// Explicit override (no fingerprinting; advanced cases like forced cutovers)
const wf = await nagi({ store, queue, flows, codeVersion: "manual-tag-v3" });
```

`NagiConfig.codeVersion` stays `?: string`. The change is purely in `nagi()`'s
behavior when the value is omitted.

### Semantics

- **`codeVersion` omitted:** runtime computes a SHA-256 over a deterministic
  serialization of the registered flows' canonical DAGs. The computed value
  is stored on the runtime and reused for every run started by that process.
  Two processes booted with the same flow code produce the same value. Two
  deploys where the flow code didn't change produce the same value.
- **`codeVersion` supplied:** taken as-is. No fingerprinting. Useful for forced
  cuts (e.g. `"cutover-2026-05-15"`).
- **No hybrid mode in v1.** The `{ tag, ... }` augmented-shape variant raised
  in the issue is out of scope.

### What "structural" means

Reuse the existing `canonicalize()` definition from RFC 0001 unchanged. Per
flow it hashes:

- `flowId`, `inputSchema` (vendor, version, `validate` source hash)
- For each step (sorted by step id):
  - `id`, `kind`, `needs` (sorted)
  - `whenHash` (if a `when` predicate is set)
  - Signal steps: `signalSchema` (vendor + version + `validate` hash)
  - Match steps: `matchMode`, `matchOnHash` (discriminator), arms with
    `id` / `whenHash` / sorted `stepIds`
  - `retry` (normalized), `timeoutMs` (if set)

What's **not** in the hash:

- `run` function bodies (would re-hash on minification; would also re-hash on
  no-op refactors)
- Comments, JSDoc, anything not load-bearing for replay determinism
- Lifecycle hooks (`onStart`, `onComplete`, `onError`, `onRetry`)

### Aggregation across flows

```ts
const entries = [...flowHashById.entries()].sort(([a], [b]) =>
  a < b ? -1 : a > b ? 1 : 0,
);
const codeVersion = await sha256Hex(stableStringify(entries));
```

Already-deterministic inputs; equivalent flow sets produce byte-identical
hashes. Adding a flow, renaming a flow id, or changing any step's structural
projection moves the result. Editing a `run` body does not.

## Why this lives in nagi, not consumer code

1. **Internal access.** Step ids assigned by `b.step()` (including the
   namespaced ids inside match arms — `route.hot.validate` etc., assigned in
   `builder.ts:341–359`), deduped `needs` edges, and signal-schema internals
   aren't part of the public `Flow["steps"]` surface. Consumer-side
   fingerprinting can't reach them reliably.
2. **Consistency with `flowHash`.** `flowHash` is the per-flow snapshot key;
   auto-`codeVersion` is a fold over the same per-flow hashes. They use the
   same canonicalization rule by construction — they cannot drift apart.
3. **Boilerplate absorption.** Every backend would otherwise re-implement the
   canonicalize-and-hash dance. `nagi()` already does the per-flow half at
   boot; finishing the fold costs nothing.
4. **Matches #1.** RFC 0001 moved hook dispatch from "every consumer rebuilds
   it" to "engine handles it." Same instinct.

## Use cases

- **Default-of-defaults backend.** `await nagi({ store, queue, flows })` —
  no `codeVersion` knob to think about. Audit signal is meaningful by default.
- **CI cutover.** Operator overrides with a tag to force an audit divergence
  across all in-flight runs from a specific deploy point:
  `codeVersion: "cutover-2026-05-15"`.
- **Multi-tenant SaaS.** Same binary, multiple tenants, identical flow code.
  Auto-fingerprint matches across tenants — no per-tenant noise in audit
  dashboards.

## Detailed design

### Public surface

Two changes in `@nagi-js/core`:

1. New export: `fingerprintFlows(flows: ReadonlyArray<Flow>): Promise<string>`.
2. `nagi()` behavior change: if `config.codeVersion === undefined`, await
   `fingerprintFlows(config.flows)` and use the result for the rest of the
   runtime's lifetime.

`NagiConfig` does **not** change. The JSDoc on `codeVersion` is updated to
note the auto-default.

### `fingerprintFlows` implementation

Lives in `packages/core/src/canonicalize.ts` alongside the existing
canonicalization primitives.

```ts
export async function fingerprintFlows(
  flows: ReadonlyArray<Flow>,
): Promise<string> {
  const entries: Array<readonly [string, string]> = [];
  for (const f of flows) {
    const dag = await canonicalize(f);
    entries.push([f.id, await sha256Canonical(dag)]);
  }
  entries.sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0));
  return sha256Hex(stableStringify(entries));
}
```

This duplicates the per-flow `canonicalize + sha256Canonical` work that
`nagi()` already does on its boot loop (`runtime.ts:147–149`). Two options:

- **Option A (chosen): duplicate the loop.** ~2 extra hashes per boot for
  typical workloads (single-digit flows). Trivial cost. Keeps the helper
  self-contained, makes it usable in tests without booting a runtime, and
  keeps `nagi()`'s boot loop readable.
- **Option B: factor the per-flow loop into a shared helper that returns
  `{ flowHashById, codeVersion }`.** Saves the duplicate work but couples
  the helper to the snapshot-upsert side effects in the boot loop. Not worth
  it.

`sha256Hex` is currently private (`canonicalize.ts:249–253`). Either export
it or inline its 4 lines into `fingerprintFlows`. Either works; the latter
keeps the public surface smaller.

### `nagi()` change

In `packages/core/src/runtime.ts`, after the per-flow canonicalize loop
(lines 137–170) and before the `dispatchDeps` object (lines 183–193), resolve
the effective `codeVersion`:

```ts
const codeVersion =
  config.codeVersion ?? (await fingerprintFlows(config.flows));
```

Then change the `flow.started` fact construction at `runtime.ts:235–237` to
read from the resolved local instead of `config.codeVersion`:

```ts
// before
...(config.codeVersion !== undefined ? { codeVersion: config.codeVersion } : {}),

// after
codeVersion,
```

`codeVersion` is now always defined, so the conditional spread collapses to a
plain field. `FlowStartedFact.codeVersion` stays `?: string` for snapshot
backwards-compat (older facts may still have `undefined`).

If we want to keep the field optional in the type but always populated for new
runs, leave the type alone and just always include the field on emission.
That's the chosen shape.

### Logging (optional, deferred)

Could log `codeVersion` at boot when auto-computed. Skipping for v1 — easy to
add when an operator asks.

## Testing

New file: `packages/core/src/fingerprint.test.ts`.

Pinned behaviors:

1. **Deterministic.** Same `flows` array → same hash across calls and across
   processes.
2. **Order-invariant.** `fingerprintFlows([a, b])` ===
   `fingerprintFlows([b, a])` (flows sorted by id internally).
3. **Topology-sensitive.** Adding a step, removing a step, adding a `needs`
   edge, flipping a `when` predicate, changing a signal schema's `~standard`
   metadata — each moves the hash.
4. **Body-insensitive.** Editing a `run` function body without changing
   structure does **not** move the hash.
5. **Single-flow → multi-flow.** Adding a second flow with a different id
   moves the hash.
6. **Default integration.** `nagi({ store, queue, flows })` (no
   `codeVersion`) produces runs whose `flow.started` fact has
   `codeVersion === await fingerprintFlows(flows)`.
7. **Explicit override.** `nagi({ ..., codeVersion: "x" })` produces
   `codeVersion: "x"` on the fact, with no fingerprinting.

The existing `snapshot.test.ts` test ("pins flow_hash + code_version onto the
flow.started fact") stays as-is — it passes an explicit `codeVersion` and
should keep working byte-for-byte.

## Migration / compatibility

- **Schema:** unchanged. `workflow_run.code_version` already accepts strings,
  including SHA hex.
- **Existing runs:** unaffected. Drift detection uses `flowHash`, not
  `codeVersion`.
- **Existing callers with explicit `codeVersion`:** unaffected; the explicit
  value is used as-is.
- **Existing callers omitting `codeVersion`:** the audit field flips from
  `null` to a SHA-256 hex string. Any dashboard that filters on `code_version
  IS NULL` to find "un-tagged" deploys will see those rows disappear. Note
  in CHANGELOG.

## Considered alternatives

### Keep optional, ship `createFlowsFingerprint` as a utility

```ts
import { createFlowsFingerprint, nagi } from "@nagi-js/core";
const wf = await nagi({ ..., codeVersion: await createFlowsFingerprint(flows) });
```

Works — and is mostly what we already have, since `fingerprintFlows` will
exist as a callable export anyway. The objection is the same one in the
issue: it's two extra lines of boilerplate every consumer copies, plus a
foot-gun (forget the `await`, get `"[object Promise]"` as your audit value).
The default-on path is strictly nicer.

### `codeVersion: 'auto'` sentinel

```ts
const wf = await nagi({ ..., codeVersion: 'auto' });
```

Adds a magic string to the API surface. `undefined → auto` reads better and
matches the TypeScript idiom. Rejected.

### Per-flow `codeVersion`

A `codeVersion` per flow lets some flows audit-drift independently of others.
Each flow already has its own `flowHash`, which is exactly that signal for
topology. Adding a per-flow `codeVersion` duplicates `flowHash` for the
audit field and pushes snapshot-storage complexity for unclear benefit.
**Out of scope.**

### Use `flowHash` of a single "registry flow" instead of fold-then-hash

Define a synthetic top-level `CanonicalRegistry` and run `sha256Canonical`
over it. Functionally equivalent to the fold. The fold is shorter and reads
as exactly what it is. Either is fine; going with the fold.

## Out of scope for v1

- Per-flow `codeVersion`.
- Configurable hash algorithm.
- Drift-aware migration tooling (`wf.migrateSnapshots(oldVersion, newVersion, fn)`).
- Reading the auto-computed `codeVersion` back from the runtime
  (`wf.codeVersion`). Could be added later; not needed for the feature.
- Logging the computed value at boot.

## Implementation order

1. **`fingerprintFlows`** in `packages/core/src/canonicalize.ts`, plus unit
   tests covering the seven behaviors above (1–5 in `fingerprint.test.ts`).
2. **`nagi()` change** in `packages/core/src/runtime.ts` — resolve
   `codeVersion` once, plumb into `flow.started`. Integration tests cover
   behaviors 6 and 7.
3. **Export** `fingerprintFlows` from `packages/core/src/index.ts` (cheap; it
   was implementation-complete in step 1, this just publishes it).
4. **CHANGELOG entry** noting the `code_version IS NULL` flip for callers
   who previously omitted the field.

README and public docs are Jay's to write.
