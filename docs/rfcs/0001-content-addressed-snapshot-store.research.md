# RFC 0001 — Implementation research

Research notes preparing to implement [0001-content-addressed-snapshot-store.md](./0001-content-addressed-snapshot-store.md). Goal: enumerate the exact files, signatures, and decision points an implementer needs before writing code.

This is descriptive, not prescriptive. Sections marked **Decision point** flag places where Jay's input changes the design — not just the implementation.

---

## 1. Surface area at a glance

| Phase | Touches | New artifacts |
|---|---|---|
| 1. Schema + canonicalization | `@nagi-js/postgres`, `@nagi-js/core/types`, new `canonicalize.ts` | `flow_snapshot`, `flow_ref` tables; `workflow_run.flow_hash`, `workflow_run.code_version` columns; `canonicalize(flow)`, `sha256Canonical(dag)` |
| 2. Runtime wiring | `core/runtime.ts` `nagi()` boot, `wf.start()` | `nagi()` becomes async; `tryStartRun` signature change; `flow_ref.updated` global fact |
| 3. Replay semantics | `core/runtime.ts` `wf.replay()` | `NagiSnapshotDriftError`, `ReplayOpts.allowDrift`, snapshot-aware flow resolver |
| 4. Tooling | `core/diff.ts` (new), `core/index.ts` exports | `diffSnapshots(a, b)`, `SnapshotDiff` type |
| 5. Migration + docs | `.changeset/`, README (Jay) | Major bump for `@nagi-js/core`, minor for `@nagi-js/postgres` |

---

## 2. Postgres layer

### 2.1 Migration system — how to add the new migration

**File:** `packages/postgres/src/migrations.ts:4-7, 16, 19, 108, 114-143`.

Shape:

```ts
export interface Migration {
  readonly id: string;                       // "0001_init", "0002_snapshot_tables"
  readonly sql: (schema: string) => string;  // schema interpolated by raw template literal
}
```

Migrations live in a `readonly Migration[]` array, run in array order, and are protected by a `<schema>.schema_migrations` ledger (one row per applied `id`, inserted inside the same transaction as the DDL). ID format enforced by `migrations.test.ts:15`: `/^\d{4}_[a-z][a-z0-9_]*$/`.

**Where to slot in the new work:** append one new migration object — `"0002_snapshot_tables"` — that contains all of: `CREATE TABLE flow_snapshot`, `CREATE TABLE flow_ref`, `ALTER TABLE workflow_run ADD COLUMN flow_hash`, `ALTER TABLE workflow_run ADD COLUMN code_version`, the two new indexes.

`migrations.test.ts:39` counts `CREATE TABLE IF NOT EXISTS` (`>= 6`) and PKs (`>= 6`) — `>=` is forward-compatible. The new migration must use `IF NOT EXISTS` on its CREATEs and (Postgres 16-safe) `ADD COLUMN IF NOT EXISTS` on the ALTER.

### 2.2 House style for the new DDL

From the existing migration (migrations.ts:19-89):

- Text PKs (`run_id text PRIMARY KEY`), not UUID.
- `jsonb` for structured payloads.
- `timestamptz NOT NULL DEFAULT now()` for "inserted at" timestamps; no default when the runtime supplies the value.
- `CREATE INDEX IF NOT EXISTS <table>_<purpose>_idx`.
- **No FK constraints declared in DDL today** (not even `step_run.run_id → workflow_run.run_id`).

The RFC explicitly declares `flow_ref.flow_hash REFERENCES flow_snapshot(flow_hash)` and `workflow_run.flow_hash REFERENCES flow_snapshot(flow_hash)`. **This is a new pattern for this codebase.** It is acceptable — the RFC is authoritative — but the implementer should be aware that mirroring this in subsequent tables sets a precedent.

### 2.3 Store adapter — interface lives in `core`

The `Store` interface is defined in **`packages/core/src/types.ts:454-543`** (not in `@nagi-js/postgres`). The `PostgresStore` class at `packages/postgres/src/store.ts:46-396` is one implementation; `InMemoryStore` at `packages/core/src/memory.ts` is the other.

Adding a method to `Store` means updating both implementations.

Current shape (relevant subset):

```ts
appendFact(runId: RunId, fact: Fact): Promise<void>;
loadRunState(runId: RunId): Promise<RunState>;
tryStartRun(runId: RunId, fact: FlowStartedFact): Promise<{ readonly started: boolean }>;
// ... plus claimStep, completeStep, failStep, getStepOutput, recordOnce, getOnce, runStep
```

New methods the RFC implies:

```ts
upsertSnapshot(args: { flowHash: string; flowId: string; dag: Json }): Promise<void>;
getRef(flowId: string): Promise<string | null>;
setRef(flowId: string, flowHash: string): Promise<void>;
loadSnapshot(flowHash: string): Promise<{ flowId: string; dag: Json } | null>;
```

Plus one of the variants in §3.3 below for the global fact.

### 2.4 Concurrency — `setRef` atomicity

Pattern already in the codebase at `store.ts:344-358` (`upsertStepCompleted`):

```sql
INSERT INTO ${schema}.flow_ref (flow_id, flow_hash, updated_at)
VALUES ($1, $2, now())
ON CONFLICT (flow_id) DO UPDATE
  SET flow_hash = EXCLUDED.flow_hash, updated_at = now()
```

`upsertSnapshot` is the inverse — `flow_hash` is PK and rows are immutable, so `ON CONFLICT (flow_hash) DO NOTHING` (mirror `tryStartRun` at store.ts:87-95).

The `jsonb()` helper (store.ts:427-429) — ``sql`${JSON.stringify(value)}::jsonb` `` — must be reused for the `dag` column.

### 2.5 Integration test pattern

Existing pattern at `packages/postgres/src/integration.test.ts:28-47`:

- Gated by `NAGI_POSTGRES_TEST_URL` env var (`describe.skip` when unset).
- Each suite gets a fresh schema `nagi_test_<uuid7-hex>` via `migrate(db, { schema })` in `beforeAll`.
- `DROP SCHEMA … CASCADE` in `afterAll`.
- Postgres 16-alpine on port 5433 via `docker-compose.test.yml`.

New tests for snapshot store mirror this exactly: one new `describe` block, inline `sql\`SELECT…\`.execute(db)` for assertions.

---

## 3. Core runtime

### 3.1 Boot path — `nagi({ flows })`

**File:** `packages/core/src/runtime.ts:24-33` (`NagiConfig`), `88-121` (boot).

The complete current registration is:

```ts
const flowsById = new Map<string, Flow>();
for (const f of config.flows) {
  if (flowsById.has(f.id)) {
    throw new NagiRuntimeError(`Duplicate flow id "${f.id}" passed to nagi()`);
  }
  flowsById.set(f.id, f);
}
```

The hook point is immediately inside this loop. For each flow:

1. `const dag = canonicalize(f)`
2. `const flowHash = await sha256Canonical(dag)`
3. `await config.store.upsertSnapshot({ flowHash, flowId: f.id, dag })`
4. `const previousHash = await config.store.getRef(f.id)`
5. If different: `await config.store.setRef(f.id, flowHash)` then append `flow_ref.updated` global fact.

**Decision point A — `nagi()` becomes async.** Today `nagi(config)` returns `Wf` synchronously. The snapshot upsert is an `await`, so `nagi()` must now return `Promise<Wf>`. Every consumer's wiring (`const wf = nagi({...})`) becomes `const wf = await nagi({...})`. This is a breaking change for anyone who hasn't already adopted top-level await.

Alternatives — record but don't recommend without Jay's signal:

- (a) Lazy initialization on first `wf.start()` / `wf.replay()` — keeps `nagi()` sync. Cost: every entry point now does a one-time async check; subtle race if two start calls fire concurrently before init completes.
- (b) Eager async with explicit `await wf.ready()` — `nagi()` returns immediately, work happens in the background, callers await a separate promise. Cost: API noise; tests forget to await.
- (c) Bite the bullet, return `Promise<Wf>`. RFC implicitly assumes this.

### 3.2 `wf.start()` — call site for ref resolution

**File:** `packages/core/src/runtime.ts:124-182`. The relevant lines:

```ts
const fact = {
  kind: "flow.started" as const,
  runId, flowId: flow.id, input: validated, at: startedAt,
};
const { started } = await config.store.tryStartRun(runId, fact);  // line 163
```

`FlowStartedFact` (types.ts:611-616) does not carry `flowHash` or `codeVersion` today. Both need adding. `tryStartRun(runId, fact)` is the call signature that must change.

**Decision point B — `tryStartRun` API shape.** Two choices:

| Option | Signature | Trade-off |
|---|---|---|
| Extend `FlowStartedFact` | `tryStartRun(runId, fact)` where `fact.flowHash` and `fact.codeVersion` now exist | Minimal call-site churn; positional args stay positional. Pollutes the fact type with persistence-layer concerns. |
| Switch to options object | `tryStartRun({ runId, flowId, flowHash, codeVersion, input, at })` | Cleaner separation; matches the RFC's own pseudocode (line 96). Forces a wider change in `store.ts`. |

The RFC's pseudocode in §4 ("At `wf.start`") uses an options object. Mild evidence for the second option; not decisive.

### 3.3 Global fact — `flow_ref.updated`

**Problem.** Every `Fact` today (types.ts:606-609 `FactBase`, 685-697 `Fact` union) is per-run with mandatory `runId`. The new `flow_ref.updated` event has no run.

**Decision point C — where does the global event live?**

| Option | Storage | Pros | Cons |
|---|---|---|---|
| (i) Sentinel `run_id` | Reuse `fact` table with `run_id = '__global__'` | Zero schema churn | Type system has to special-case the sentinel; existing per-run queries need filtering |
| (ii) Separate `nagi.global_fact` table | New table `(fact_id, kind text, payload jsonb, at timestamptz)` | Clean separation; queryable independently | One more table; second fact API method |
| (iii) Inline on `flow_ref` itself | `flow_ref` row already encodes the latest pointer; don't store a fact at all | Simplest | Loses the audit trail (which the RFC explicitly mentions: "the `flow_ref.updated` fact captures the order") |

The RFC doesn't pick — it says "appends a `flow_ref.updated` fact." (ii) seems to best honor the audit-trail goal without bending the existing fact union.

### 3.4 `wf.replay()` — drift detection

**File:** `packages/core/src/runtime.ts:263-276`. The complete current implementation:

```ts
async replay(runId: RunId, opts: ReplayOpts = { mode: "continue" }): Promise<void> {
  const runState = await config.store.loadRunState(runId);
  const flow = flowsById.get(runState.flowId);
  if (!flow) {
    throw new NagiRuntimeError(
      `Run ${runId} references flow "${runState.flowId}" which is not registered with nagi().`,
    );
  }
  if (opts.mode === "inspect") return;
  await advance(dispatchDeps, runId);
}
```

New behavior slots between line 268 (`flowsById.get`) and line 275 (`advance`):

1. Read `runState.flowHash` (new field on `RunState`, plumbed from `workflow_run.flow_hash`).
2. `const snapshot = await config.store.loadSnapshot(runState.flowHash)`.
3. Compute live hash: `const liveHash = await sha256Canonical(canonicalize(flow))`.
4. If `liveHash !== runState.flowHash`: throw `NagiSnapshotDriftError` unless `opts.allowDrift`.

**Decision point D — flow resolver during drift-allowed replay.** When `allowDrift: true`, the RFC says: "Use the snapshot for scheduling decisions, execute handlers from liveFlow." That requires either:

- (d1) Building a synthetic `Flow` from `snapshot.dag` for scheduling, then looking up handler functions on `liveFlow` by step ID per dispatch. The handler resolver is `dispatchDeps.flowFor` (runtime.ts:100-109); it currently returns the live flow unconditionally.
- (d2) Punting on this in v0 — throw on drift always; document `allowDrift` as a v0.3 feature. The RFC's Phase 3 calls it out but doesn't sequence it as strict-required.

(d2) is the smaller v0 if Jay wants to land Phase 1-3 quickly.

### 3.5 Error type

Existing pattern at runtime.ts:72-86:

```ts
export class NagiRuntimeError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "NagiRuntimeError";
  }
}
```

`NagiSnapshotDriftError` follows the same shape with `readonly runId`, `readonly expected: string`, `readonly actual: string` fields. Defined in runtime.ts, re-exported from `index.ts`.

### 3.6 `ReplayOpts.allowDrift`

`types.ts:723-727`:

```ts
export interface ReplayOpts {
  readonly mode: ReplayMode;
}
```

Add `readonly allowDrift?: boolean`. Default `undefined`/`false`.

### 3.7 `NagiConfig.codeVersion`

`runtime.ts:24-33` — append `readonly codeVersion?: string`. Threaded into `tryStartRun` only; no other use sites.

---

## 4. Canonicalization (new file: `packages/core/src/canonicalize.ts`)

### 4.1 Input shape — what `canonicalize` walks

`flow.steps` is a flat `Record<string, Step>` after `flow()` runs (builder.ts:154-184 hoists nested match-arm steps with dotted IDs). Each step has a hidden `__def` (TaskDef / SignalDef / MatchDef, defined in `internal.ts:25-95`).

`needs` lives in `def.needs` as a `Record<localKey, Step>`. The canonical needs list extracts upstream step IDs via the existing helper `needsStepIds(def)` at `internal.ts:133-146` — reuse it.

### 4.2 Per-step canonical fields by kind

| Field | Task | Signal | Match (discriminator) | Match (guard) |
|---|---|---|---|---|
| `id` | ✓ | ✓ | ✓ | ✓ |
| `kind` | ✓ | ✓ | ✓ | ✓ |
| `needs` (sorted) | ✓ | ✓ | ✓ | ✓ |
| `when` (hash) | ✓ if defined | ✓ if defined | — | per-arm |
| `retry` (normalized) | ✓ if defined | — | — | — |
| `timeoutMs` | ✓ if defined | ✓ if defined | — | — |
| `signal schema` (hash) | — | ✓ | — | — |
| `match.on` (hash) | — | — | ✓ | — |
| `match.arms` | — | — | sorted arm ids + nested step ids | sorted arm ids + when hashes + nested step ids |

`run` is **never** part of the hash (RFC §"Topology vs handler code").

### 4.3 Retry normalization

`DEFAULT_RETRY` at `dispatch.ts:50-55`:

```ts
{ maxAttempts: 3, backoff: "exponential", initialDelayMs: 1_000, maxDelayMs: 60_000 }
```

Canonical retry form: fill in `initialDelayMs` and `maxDelayMs` from defaults when absent; drop `retryOn` (function — not serializable). If `def.retry` is fully undefined, the canonical entry is also `undefined` (don't materialize the global default — that would make every step's hash sensitive to changes in `defaultRetry`).

**Decision point E — should `nagi({ defaultRetry })` participate in the hash?** Arguments either way:

- **Yes**: change in `defaultRetry` actually changes how every step behaves on failure. Topology-vs-behavior boundary leaks here.
- **No**: `defaultRetry` is a runtime-level concern, not a per-flow concern. Hashing it would mean changing the env's retry default invalidates every flow snapshot.

Implicit RFC default: **no**, only per-step `retry` is hashed.

### 4.4 `when` predicate hashing — RFC Open Question #1

Predicates are raw functions (`internal.ts:30, 47, 70`):

```ts
when?: (args: { input: unknown; needs: Record<string, unknown> }) => boolean
```

No AST. No DSL. `Function.prototype.toString()` is the only handle.

**Decision point F — predicate hash strategy for v0.**

| Option | Behavior | Caveat |
|---|---|---|
| (f1) `sha256(fn.toString())`, warn once | Best-effort source hash; breaks under minification (whitespace/identifier changes flip the hash even though logic is unchanged) | False positives on drift in prod builds. The RFC accepts this as the v0 trade-off. |
| (f2) No predicate hashing in v0 | Drop `when` from the canonical form entirely | Predicate edits become silently invisible — worse than topology-only. |
| (f3) Hash function name + arity only | `sha256(fn.name + ":" + fn.length)` | Almost worthless — anonymous functions all collide. |
| (f4) Build-time transformer (Babel/SWC plugin) | Author-time source captured into a stable annotation | New build dependency; out of scope for v0 per RFC. |

The RFC §"Canonicalization rules / 3" picks (f1) explicitly: "best-effort with a warning." Recommend matching the RFC and documenting the gap.

### 4.5 Schema serialization — RFC Open Question #2

`StandardSchemaV1` (types.ts:14-52) exposes runtime-stable only: `version: 1` and `vendor: string` (e.g., `"zod"`, `"valibot"`). The `types` field is a TS-only phantom — undefined at runtime. There's no `.toJSON()` / `.toJsonSchema()` contract.

**Decision point G — what goes into the schema hash?**

| Option | Hash input | What it catches |
|---|---|---|
| (g1) `{vendor, version}` only | Only library swap | Nearly nothing — adding a required field doesn't change the hash |
| (g2) `{vendor, version, validateSource: validate.toString()}` | Library + parser source | Breaks under minification (same problem as predicates); somewhat noisy but catches most real edits |
| (g3) Per-vendor introspection (Zod's `_def`, Valibot's `entries`, etc.) | Real schema structure | Best fidelity; invasive — requires per-vendor code paths in `@nagi-js/core` |
| (g4) Opt-in via `flow({ inputSchemaHash: "user-supplied-string" })` | User-supplied | Honest about the limitation; same discipline problem as the version-suffix alternative |

The RFC §"Canonicalization rules / 5" leaves this open. (g2) parallels the predicate decision and is the most consistent v0 default.

### 4.6 Hash algorithm

`crypto.subtle.digest("SHA-256", bytes)` — Web Crypto, edge-compatible with `tsup` `platform: "neutral"` (tsup.config.ts). Returns `ArrayBuffer`; convert to hex string for storage (text PK). Reference impl pattern:

```ts
async function sha256Hex(input: string): Promise<string> {
  const bytes = new TextEncoder().encode(input);
  const digest = await crypto.subtle.digest("SHA-256", bytes);
  return Array.from(new Uint8Array(digest)).map(b => b.toString(16).padStart(2, "0")).join("");
}
```

JSON serialization for hashing: `JSON.stringify` with deterministic key ordering. Since `canonicalize` already produces a struct with sorted keys/arrays, a stable stringify pass at the top should suffice (or a helper that walks the object and emits keys in sorted order).

---

## 5. `diffSnapshots` (Phase 4)

**New file:** `packages/core/src/diff.ts`.

RFC §"Behavior changes / 5. Diff" specifies:

```ts
type SnapshotDiff = {
  addedSteps: StepId[];
  removedSteps: StepId[];
  changedEdges: Array<{ from: StepId; to: StepId; before: "needed" | "absent"; after: "needed" | "absent" }>;
  changedPredicates: Array<{ stepId: StepId; field: "when" | "retry" | ... }>;
};
```

This is a pure function over two `CanonicalDag` values — no I/O, easy to unit-test exhaustively. Build last.

---

## 6. Testing plan

### 6.1 Unit tests (`packages/core/src/canonicalize.test.ts`)

Byte-stability invariants — every one of these should produce the **same** hash:

- Reorder step keys in the `build()` return object.
- Reorder `needs` keys in a task's needs map.
- Add cosmetic whitespace inside a `when` predicate body (note: this will *fail* under `Function.prototype.toString()` — document explicitly).
- Add a comment inside a handler `run` body.

Byte-difference invariants — every one of these should produce a **different** hash:

- Add a new step.
- Remove a step.
- Add a `needs` edge.
- Flip a `when` predicate (different function body).
- Change `retry.maxAttempts`.
- Change `flow.id`.
- Change `timeoutMs`.

### 6.2 Type tests (`packages/core/src/canonicalize.test-d.ts`)

`CanonicalDag` shape stability — `expectTypeOf<CanonicalDag>().toMatchTypeOf<...>()`. Pattern lives in `types.test-d.ts`.

### 6.3 Integration tests (`packages/postgres/src/integration.test.ts` — extend existing file or new `snapshot.integration.test.ts`)

- `upsertSnapshot` deduplicates: insert same `(flowHash, dag)` twice; only one row.
- `setRef` is last-writer-wins under concurrent calls.
- New run gets `workflow_run.flow_hash` populated.
- Replay against a snapshot whose hash matches live → proceeds.
- Replay against a snapshot whose hash differs from live, default opts → throws `NagiSnapshotDriftError`.
- Replay with `allowDrift: true` → proceeds (assuming decision point D resolves toward d1).

### 6.4 In-memory store parity

`InMemoryStore` (memory.ts) must implement the same new methods. Test it via the same canonicalize/replay flows in a unit test that doesn't need Postgres.

---

## 7. Changeset

`@nagi-js/core`: **major** — `nagi()` becomes async; `wf.replay()` throws on drift by default; `Store` interface gains methods; `ReplayOpts` and `NagiConfig` gain fields.

`@nagi-js/postgres`: **minor** — additive (new tables, columns, migration).

Format example:

```md
---
"@nagi-js/core": major
"@nagi-js/postgres": minor
---

Add content-addressed snapshot store. Every run is pinned to the exact DAG topology
that existed when it started. Replays read the pinned snapshot, not the current
in-memory flow. See RFC 0001.

Breaking:
- `nagi()` now returns `Promise<Wf>` (was `Wf`).
- `wf.replay()` throws `NagiSnapshotDriftError` when the live flow's hash differs
  from the pinned snapshot. Pass `replayOpts.allowDrift: true` to restore the
  previous best-effort behavior.
```

---

## 8. Decisions — resolved

| # | Decision | Chosen | Notes |
|---|---|---|---|
| A | `nagi()` async | **`Promise<Wf>`** | RFC default. Breaking change called out in changeset. |
| B | `tryStartRun` signature | *(implementer's call)* | Lean toward options object to match RFC §4 pseudocode. |
| C | Global fact storage | **New `nagi.global_fact` table** | `(fact_id text PK, kind text, payload jsonb, at timestamptz)`. New `Store.appendGlobalFact` method. |
| D | `allowDrift` behavior | **Synthetic-flow scheduling** | Build a `Flow` from `snapshot.dag` for scheduling; resolve handlers from live flow by step ID. Phase 3 includes the snapshot-aware `flowFor` resolver. |
| E | `defaultRetry` in hash | *(implementer's call — RFC implicit: no)* | Only per-step `retry` is hashed. |
| F | `when` hashing | **`sha256(fn.toString())` + one-time warning** | Matches RFC §Canonicalization/3. Warning fires from `canonicalize` when any predicate is encountered, documented as a known limit. |
| G | Schema hashing | *(implementer's call — propose g2)* | `sha256({vendor, version, validate.toString()})` parallel to predicate strategy. |

Implementation proceeds against these choices.

---

## 9. File-by-file change inventory (for implementation kickoff)

**New:**
- `packages/core/src/canonicalize.ts` — `canonicalize(flow): CanonicalDag`, `sha256Canonical(dag): Promise<string>`, types.
- `packages/core/src/canonicalize.test.ts` — invariant tests.
- `packages/core/src/canonicalize.test-d.ts` — type tests.
- `packages/core/src/diff.ts` — `diffSnapshots(a, b)`.
- `packages/core/src/errors.ts` *(optional split)* — or extend `runtime.ts` with `NagiSnapshotDriftError`.

**Modified:**
- `packages/core/src/types.ts` — extend `Store` interface (4-5 new methods), `ReplayOpts` (`allowDrift`), `RunState` (`flowHash`), `FlowStartedFact` (`flowHash`, `codeVersion`).
- `packages/core/src/runtime.ts` — `NagiConfig.codeVersion`, async boot, `wf.start()` ref resolution, `wf.replay()` drift check, `NagiSnapshotDriftError`.
- `packages/core/src/memory.ts` — `InMemoryStore` gets the new methods.
- `packages/core/src/index.ts` — re-export `canonicalize`, `diffSnapshots`, `NagiSnapshotDriftError`, types.
- `packages/postgres/src/migrations.ts` — append `0002_snapshot_tables`.
- `packages/postgres/src/migrations.test.ts` — bump expected count or rely on `>=`.
- `packages/postgres/src/store.ts` — implement new methods on `PostgresStore`.
- `packages/postgres/src/integration.test.ts` — add snapshot-store integration tests (or split into new file).

**Not modified:**
- `README.md` — Jay writes documentation himself.
- `packages/pgmq/*`, `packages/otel/*` — no changes needed.
