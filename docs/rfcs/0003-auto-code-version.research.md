# RFC 0003 — Auto-compute `codeVersion` (research notes)

- **Tracking issue:** lymo-inc/nagi#3
- **Date:** 2026-05-15 (JST)
- **Scope:** `@nagi-js/core` only; downstream packages unaffected.

These are the working notes that back the RFC. They map what the codebase already
does, where the proposal's premise is slightly out of date, and what the real
implementation surface is.

## Current state of `codeVersion`

### Public API

`NagiConfig.codeVersion` is **already optional** (not required, as the
issue text states).

```ts
// packages/core/src/runtime.ts:34–51
export interface NagiConfig {
  readonly flows: ReadonlyArray<Flow>;
  readonly store: Store;
  readonly queue: Queue;
  // …
  /**
   * Handler-code identifier — typically a git SHA from `process.env.GIT_SHA`
   * or your build's bundle hash. Persisted on `workflow_run.code_version` and
   * on every `flow.started` fact for runs started by this process. Captures
   * handler-body drift orthogonally to the topology hash. See RFC 0001
   * "Topology vs handler code."
   */
  readonly codeVersion?: string;
}
```

So the API change the issue describes (make it optional) is already done. The
substantive gap is the second half of the proposal: **auto-compute a stable
structural fingerprint when omitted, instead of leaving `codeVersion`
undefined.**

### Where `codeVersion` flows

- **Set on `flow.started` fact** at `start()` time
  (`packages/core/src/runtime.ts:235–237`):
  ```ts
  ...(config.codeVersion !== undefined
    ? { codeVersion: config.codeVersion }
    : {}),
  ```
- **Persisted on `FlowStartedFact`** (`packages/core/src/types.ts:828–832`).
- **Projected onto `RunState.codeVersion`** by the in-memory fact projector
  (`packages/core/src/memory.ts:199–208`, `:256–263`;
  `packages/core/src/types.ts:962`).
- **Persisted on `workflow_run.code_version`** in Postgres
  (`packages/postgres/src/store.ts:90, 92, 293, 295`; column declared in
  `migrations.ts:119–120`).
- **Never compared anywhere.** No drift check, no validation, no replay
  guard. It is strictly audit metadata.

### `codeVersion` vs `flowHash` — two different drift signals

`flowHash` already does the heavy lifting the issue alludes to when it says
"drift fires when topology changes."

| Signal | Scope | Computed from | Used for |
|---|---|---|---|
| `flowHash` | Per-flow | `sha256Canonical(canonicalize(flow))` | Content-addressed snapshot store; **drift detection at replay** (`NagiSnapshotDriftError`) |
| `codeVersion` | Process-wide | Supplied by caller (today) | **Audit only** — persisted on `flow.started` and `workflow_run.code_version` |

Drift detection lives in `wf.replay()`
(`packages/core/src/runtime.ts:357–414`): it compares the snapshot's pinned
`flowHash` against the live `flowHashById.get(...)` for that flow and throws
`NagiSnapshotDriftError` on mismatch. `codeVersion` does **not** participate.

This matters for issue #3: auto-computing `codeVersion` from registered flows
**does not change** drift behavior. It only fills in a meaningful default value
for the audit field. Topology drift is already handled by `flowHash` —
auto-`codeVersion` is the matching audit signal at the process level.

## Building blocks already in place

`packages/core/src/canonicalize.ts` already exports exactly what the
fingerprint needs:

- `canonicalize(flow: Flow): Promise<CanonicalDag>` — produces a
  `CanonicalDag` for one flow with:
  - `flowId`
  - `inputSchema` (vendor, version, `validateHash` of `~standard.validate`)
  - `steps` sorted by id, each with `id`, `kind`, `needs` (sorted),
    `whenHash` (if any), and kind-specific extras (`signalSchema` for signals;
    `matchMode` / `matchOnHash` / `matchArms` for matches; `retry` / `timeoutMs`
    when present)
- `sha256Canonical(dag: CanonicalDag): Promise<string>` — SHA-256 via
  `crypto.subtle.digest` over a deterministic JSON serialization
  (`stableStringify`).

`nagi()` is **already async** (`packages/core/src/runtime.ts:133`) and
**already calls both** at boot for every registered flow
(`packages/core/src/runtime.ts:147–149`):

```ts
const dag = await canonicalize(f);
const flowHash = await sha256Canonical(dag);
flowHashById.set(f.id, flowHash);
```

So a single per-flow SHA-256 is **already on the boot path**. The auto-
`codeVersion` is a roll-up over `flowHashById`.

## Deviation from the issue text — fields covered

The issue lists these fields as part of the canonical form:

> Flow `id`; per step: `id`, `kind`, `needs` sorted, `when` predicate (if any),
> `signal` name (for signal steps); sorted by step id within a flow; flows
> sorted by id.

Reusing the existing `canonicalize()` covers all of those **and more**:
`inputSchema`, `retry`, `timeoutMs`, match-step internals (`mode`, `on`-hash,
arm `whenHash`, arm `stepIds`). The "more" matters: the issue says retry /
timeout should be **excluded** because they're "operational, doesn't change
replay correctness." But the existing `canonicalize()` **includes** them —
because retry and timeout edits *do* change replay correctness in nagi's
model (they alter step boundaries and resume semantics under failure).

This is a deliberate scope difference, and it is the *right* one for nagi: the
existing `canonicalize()` is the authoritative definition of "structural" for
this codebase. We should reuse it as-is rather than re-derive a thinner
definition specific to `codeVersion`. If the canonical form's coverage changes
in the future, both `flowHash` and auto-`codeVersion` should change together —
not drift apart.

One field the issue text mentions that doesn't apply: "`signal` name". Signal
steps are identified by their **schema's `~standard.vendor` + `~standard.version`
+ `validateHash`**, not by a free-form name. `canonicalize()` already captures
this as `signalSchema`. No change needed.

## Test fixtures pinning current behavior

- `packages/core/src/snapshot.test.ts:88–107` — "pins flow_hash + code_version
  onto the flow.started fact." Passes an explicit `codeVersion: "abc1234"`,
  asserts it flows through to `RunState.codeVersion`. This test does **not**
  require `codeVersion` to be supplied; it just verifies it propagates when it
  is. Safe to keep as-is; auto-compute behavior is additive.

No other tests reference `codeVersion`. The proposed change does not need to
modify any existing test.

## Hash strategy — fold over `flowHashById`

The proposal says "flows sorted by id." We have a `Map<flowId, flowHash>`
already populated at boot. The natural roll-up is:

```ts
const entries = [...flowHashById.entries()].sort(([a], [b]) =>
  a < b ? -1 : a > b ? 1 : 0,
);
const codeVersion = await sha256Hex(stableStringify(entries));
```

This is a fold over already-deterministic inputs. Equivalent flow sets produce
byte-identical `entries` arrays → byte-identical hashes. Adding or removing a
flow, renaming a flow id, or changing any step's structural projection all
change the result. `run`-body edits, retry-policy *additions* unrelated to
topology, comments — none of these change `flowHash` for any individual flow,
so they don't change the fold either.

A slightly cleaner alternative is to define a top-level `CanonicalRegistry`
type and run `sha256Canonical` over it. Either works; the fold is shorter,
shares the format with what's already on disk (since each `flowHash` is what
goes into `flow_snapshot.flow_hash`), and reads as "this is the hash of the
set of flow hashes." Going with the fold.

`sha256Hex` is currently private inside `canonicalize.ts`
(`canonicalize.ts:249–253`). Either export it, or add a tiny public helper
that wraps the fold. The cleaner shape: add an exported
`fingerprintFlows(flows: ReadonlyArray<Flow>): Promise<string>` that does
both the per-flow canonicalize+hash and the fold — then `nagi()` can also use
that helper internally without duplicating the loop. (Today it canonicalizes
each flow inline because it also writes the snapshot row; we keep that and
just call `fingerprintFlows` with the already-computed `flowHashById` *or*
extract the helper and refactor. See plan doc for the chosen shape.)

## Decisions and open questions

### Settled (matches the proposal)

- **Default behavior is auto-compute.** No magic-string sentinel; `undefined`
  is the trigger.
- **Explicit override is taken as-is**, with no augmentation.
- **One global `codeVersion`, not per-flow.** Per-flow versioning is out of
  scope; the existing `flowHash` already covers per-flow drift.
- **SHA-256, no algorithm knob.**

### Resolved by codebase inspection

- **Sync vs async.** `nagi()` is already async — no breaking change required.
- **Hashing library.** Reuse `crypto.subtle` via the existing helpers in
  `canonicalize.ts`. No new dependency.
- **Canonical form coverage.** Reuse the existing `canonicalize()` definition
  rather than reinventing a thinner one. Treat `flowHash` and auto-
  `codeVersion` as derived from the same canonical projection so they cannot
  drift apart.

### Surfacing for the maintainer (Jay)

1. **Public utility?** The issue says "exposing the auto-fingerprint as a
   public utility" is out of scope for v1. Implementation note: even if we
   don't *publish* it, we'll want it as an internal export for tests. Easy to
   promote later if users ask.
2. **Hybrid mode.** The issue raises and tentatively rejects
   `codeVersion: { tag: 'v3.1' }` augmenting the auto-hash. Confirming
   rejection for v1 — plain `string | undefined`.
3. **Logging.** The runtime currently has no log line that announces the
   computed `codeVersion`. We *could* `logger.info("nagi codeVersion", { codeVersion })`
   at boot when auto-computed, so operators can correlate. Marginal; deferring
   unless asked.

## Risk surface

Tiny. The change:

- Adds one boot-time computation that reuses already-running primitives
  (`canonicalize` + `sha256Canonical`) — no new failure modes.
- Affects one field (`codeVersion`) that is *only* used for audit metadata —
  not for drift detection, not for replay, not for storage keying.
- Has no schema impact (`code_version` column stays the same; values are now
  hashes instead of nullable strings, which the column already accepts).
- Has no behavior change for callers who already pass `codeVersion` explicitly.

The one thing to be careful about: callers who have built dashboards or alerts
that key off "`code_version IS NULL`" to detect un-tagged deploys will lose
that signal once the auto-default lands. This is desirable (the whole point
of the issue), but worth one line in CHANGELOG.

## File map

Files that need to change for the implementation:

- `packages/core/src/canonicalize.ts` — add (and export) `fingerprintFlows`.
- `packages/core/src/runtime.ts` — if `config.codeVersion === undefined`,
  compute it via `fingerprintFlows` and use it for the rest of the runtime's
  lifetime. Plumb into `flow.started` fact the same way explicit
  `codeVersion` is plumbed today.
- `packages/core/src/index.ts` — re-export `fingerprintFlows` if we decide to
  surface it (optional).
- Tests — new test file (`fingerprint.test.ts`) or extension of
  `snapshot.test.ts`. See plan doc.

Files that do **not** need to change:

- `packages/postgres/src/*` — column is already nullable, accepts any string.
- `packages/pgmq/src/*`, `packages/otel/src/*` — no `codeVersion` references.
- `README.md` — owned by Jay; this change is for him to document.
- Any consumer code — the change is strictly additive.
