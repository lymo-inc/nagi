# RFC 0001 — Content-addressed snapshot store

- **Status:** Draft
- **Author:** @jay (lymo-inc)
- **Created:** 2026-05-14
- **Related:** `@nagi-js/core`, `@nagi-js/postgres`

## Summary

Add a content-addressed snapshot store to nagi so every workflow run is pinned to the exact DAG topology that existed when the run started. Replays read the pinned snapshot, not whatever's in source today. The model is borrowed from git: hash the canonical form of a flow, dedupe by hash, name the current head with a mutable ref, and treat each run as a frozen pointer into the snapshot graph.

This RFC describes the new tables, the canonicalization rules, the runtime behavior, and the known limits (handler-code drift) along with their mitigation.

## Motivation

nagi today persists `workflow_run.flow_id` (e.g., `"video-analysis.v1"`) on each run, and assumes the caller's deployed code matches at replay time. That assumption fails in three concrete ways:

1. **Silent drift on replay.** `wf.replay(runId)` re-dispatches from the first incomplete step using the *current* flow definition. If the DAG shape changed between the original run and the replay — a step removed, a `needs` edge moved, a `when` predicate flipped — the replay diverges from the original execution path without any signal to the operator.
2. **Debug archaeology.** "What did this run from last month execute?" today requires checking out the git commit that was deployed at the original run's `started_at`, then reading the flow file. Tooling-heavy, error-prone, and only works if the deployment SHA was recorded somewhere outside nagi.
3. **No safe-rollout primitive.** Deploying a new DAG shape doesn't isolate in-flight runs from the change. There's no way to say "the runs already in flight finish on the old shape; new runs use the new shape." Today both groups run against whatever's currently in the source tree.

The dogfooding driver is the lymo workflows refactor: 16 jobs across 5 templates, with workflow runs that can take hours and replays that may happen weeks later. The legacy in-house runner sidestepped the problem by storing definitions in DB rows (a row from yesterday is a snapshot of yesterday's definition). Moving to nagi without an equivalent primitive would be a strict regression — and one that's expensive to bolt on later, since it changes the on-disk schema and the replay semantics.

## Detailed design

### Mental model — what we're borrowing from git

| Git primitive | nagi analogue |
|---|---|
| **Content-addressed objects.** Every tree/blob is stored by sha256 of its content; identical content dedupes to the same hash. | Hash the *flow's structural shape* (step ids, needs edges, `when` predicate locators, retry policy). Same hash → same DAG → same row in `flow_snapshot`. |
| **Commits as snapshots.** A commit pins a tree hash; history is a DAG of commits. | A `flow_snapshot` row keyed by hash. `workflow_run.flow_hash` FKs into it. Past topology is reachable by hash. |
| **Refs as branches.** `main`, `feature/x` are mutable pointers to commits. | `flow_ref(flow_id)` is the published-name pointer; the underlying hash rotates as code changes. Past runs stay pinned to whatever hash was current when they started. |
| **Diff between trees.** `git diff <a> <b>` reads two trees and shows structural change. | `nagi.diffSnapshots(hashA, hashB)` reads two `flow_snapshot` rows and reports the structural delta (added/removed steps, changed edges, flipped predicates). |
| **Revert as forward-merge.** `git revert <sha>` creates a *new* commit that undoes the diff — never rewrites history. | Reverting a flow change is redeploying the previous source. The next nagi boot computes a hash (which matches the old one due to deduping), `flow_ref` flips back, the fact log records the transition. No history is mutated. |

The parts that transfer cleanly: **content-addressing, snapshots, refs, diff.** The parts that don't transfer (three-way merge, rebase) are out of scope — workflow definitions don't conflict the way text does.

### Schema additions

Both tables live in nagi's existing configurable schema (default `nagi`), alongside `workflow_run`, `step_run`, `fact`, etc.

```sql
-- A deduped store of flow definition snapshots, keyed by content hash.
-- Same canonical DAG → same hash → same row.
CREATE TABLE nagi.flow_snapshot (
  flow_hash   text PRIMARY KEY,        -- sha256 of canonicalized DAG JSON
  flow_id     text NOT NULL,           -- the ref name: "video-analysis.v1"
  dag         jsonb NOT NULL,          -- canonical serialization
  recorded_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX flow_snapshot_by_id ON nagi.flow_snapshot (flow_id, recorded_at DESC);

-- The mutable pointer "which hash is the currently published version of this flow_id?"
-- Updated when a process boots with code that produces a different hash.
CREATE TABLE nagi.flow_ref (
  flow_id    text PRIMARY KEY,
  flow_hash  text NOT NULL REFERENCES nagi.flow_snapshot(flow_hash),
  updated_at timestamptz NOT NULL DEFAULT now()
);
```

### New column on `workflow_run`

```sql
ALTER TABLE nagi.workflow_run
  ADD COLUMN flow_hash text REFERENCES nagi.flow_snapshot(flow_hash);
-- nullable in v0 so existing runs survive migration; nagi populates on all new starts.
CREATE INDEX workflow_run_flow_hash_idx ON nagi.workflow_run (flow_hash);
```

After a soak period (one or two minor versions), `flow_hash` becomes `NOT NULL`. Backfill for legacy runs is best-effort — they pin to whatever the current `flow_ref` was at backfill time, with a `flow_snapshot.recorded_at < workflow_run.started_at` invariant violation logged.

### Behavior changes

**1. At `nagi({ flows })` registration.** The runtime canonicalizes each registered flow (see canonicalization rules below), computes a sha256, and:

```ts
for (const flow of flows) {
  const dag = canonicalize(flow);
  const hash = sha256(JSON.stringify(dag));
  await store.upsertSnapshot({ flowHash: hash, flowId: flow.id, dag });
  const previousHash = await store.getRef(flow.id);
  if (previousHash !== hash) {
    await store.setRef(flow.id, hash);
    await store.appendGlobalFact({ kind: "flow_ref.updated", flowId: flow.id, from: previousHash, to: hash });
  }
}
```

Concurrency: two processes booting simultaneously may race on `setRef`. The `setRef` operation must be atomic (e.g., `INSERT … ON CONFLICT DO UPDATE`); the last writer wins and the `flow_ref.updated` fact captures the order.

**2. At `wf.start(flow, input, opts?)`.** The runtime resolves the current `flow_ref`, persists `workflow_run.flow_hash = <ref>`, and the run is pinned to that hash for its lifetime.

```ts
const flowHash = await store.getRef(flow.id);
await store.tryStartRun({ runId, flowId: flow.id, flowHash, input, ... });
```

**3. At `wf.replay(runId)`.** Critical semantic change. The runtime loads `workflow_run.flow_hash`, fetches the corresponding `flow_snapshot.dag`, and executes against *that* topology — not the current in-memory `flow` object.

```ts
const { flowHash } = await store.loadRun(runId);
const snapshot = await store.loadSnapshot(flowHash);
const liveFlow = registry.get(snapshot.flowId);
// Validate: the live flow's hash must match the snapshot's hash, OR we use the snapshot's frozen topology.
if (canonicalize(liveFlow).hash !== flowHash) {
  // Live code drift detected. Behavior depends on the topology-vs-handler-code resolution; see below.
  if (replayOpts.allowDrift) {
    // Use the snapshot for scheduling decisions, execute handlers from liveFlow.
  } else {
    throw new NagiSnapshotDriftError({ runId, expected: flowHash, actual: liveHash });
  }
}
```

The default is `allowDrift: false` for safety; callers opt in to drift explicitly. (See "Topology vs handler code" for why this matters.)

**4. Inspect.** SQL query: `SELECT dag FROM nagi.flow_snapshot WHERE flow_hash = (SELECT flow_hash FROM nagi.workflow_run WHERE run_id = $1)`. No git checkout required.

**5. Diff.** Helper exposed on `@nagi-js/core`:

```ts
export function diffSnapshots(a: FlowSnapshot, b: FlowSnapshot): SnapshotDiff;
type SnapshotDiff = {
  addedSteps: StepId[];
  removedSteps: StepId[];
  changedEdges: Array<{ from: StepId; to: StepId; before: "needed" | "absent"; after: "needed" | "absent" }>;
  changedPredicates: Array<{ stepId: StepId; field: "when" | "retry" | ... }>;
};
```

Feeds into admin UIs ("what changed between v1 and v2 of this flow?") and into the changeset workflow for nagi users who care about flow-shape drift.

**6. Revert.** No new runtime concept. To revert: redeploy with the previous source. The next boot's canonicalization produces a hash that matches the old `flow_snapshot` row (because of content dedup); `flow_ref` flips back; the `flow_ref.updated` fact captures the transition. In-flight runs already pinned to the *new* hash finish on the new shape; runs that haven't started yet pin to the reverted hash.

### Canonicalization rules

The hash must be byte-stable across cosmetic changes that don't affect execution and byte-different across changes that do. The canonical form is:

```ts
{
  flowId: string;                          // "video-analysis.v1"
  inputSchema: SerializedSchema;           // see "Schema serialization" below
  steps: Array<{
    id: StepId;                            // alphabetically sorted by id
    kind: "task" | "match" | "signal";
    needs: StepId[];                       // alphabetically sorted
    when?: { sourceHash: string };         // sha256 of the predicate's source text
    retry?: NormalizedRetryPolicy;
    timeoutMs?: number;
  }>;
}
```

Specifically:

1. **Step order.** Sort by `id` lexicographically. Insertion order in the source is ignored.
2. **Needs order.** Sort needs arrays lexicographically. Removes false-positives from edge reordering.
3. **`when` predicates.** Stored as a hash of the predicate's source text, not the function reference. Captured at build time via either (a) a build-time transformer that reads the source, or (b) `Function.prototype.toString()` at registration with a warning that it's best-effort. (See "Open questions" — this is the spiciest part of canonicalization.)
4. **Retry / timeout / other policies.** Normalized to a canonical form (fill in defaults explicitly, drop undefined fields).
5. **Input schema.** Schemas are serialized via their `StandardSchemaV1` introspection if available; otherwise hashed by source text. Schema changes that affect runtime validation MUST change the hash. (See "Open questions.")
6. **No handler bodies.** The handlers' source code is *not* part of the hash. See "Topology vs handler code" below.

The full JSON form is stored in `flow_snapshot.dag` so it's queryable without recomputing.

### Topology vs handler code — the known limit

Hashing the DAG shape gets you "same topology"; it doesn't get you "same handler behavior." Two scenarios where they diverge:

- **Behavior change without topology change.** Edit a handler's prompt or computation. DAG unchanged. Hash unchanged. Old runs replayed today execute the new behavior.
- **Topology change without behavior change.** Reorder steps in a way that canonicalization doesn't normalize (shouldn't happen, but bugs in canonicalization could cause it). New hash, same behavior.

Two paths:

(α) **Hash topology + handler source.** Walk every step's `run` function, hash `fn.toString()` along with the topology. Breaks under bundling/minification (function bodies change without semantic change). Could work with source-map awareness or build-time injection of pre-bundled source, but adds infrastructure.

(β) **Topology hash + separate `code_version` field.** Hash topology only. Persist a free-form `code_version` (typically a git SHA) on `workflow_run`, sourced from `nagi({ codeVersion: process.env.GIT_SHA })`. "Replay against original topology *and* original code" = check out the SHA and replay. Two-step time-travel.

**This RFC proposes (β).** Topology hash is in nagi; `code_version` is in `nagi()` options and `workflow_run.code_version`. The README documents the topology-vs-handler-code gap as a known limit. (α) is left as a future RFC if a use case demands it.

### Drift detection at replay

When `wf.replay(runId)` runs, it compares the snapshot's hash against the live flow's hash:

| Live hash matches snapshot | `code_version` matches | Result |
|---|---|---|
| ✅ | ✅ | Replay proceeds normally. |
| ✅ | ❌ | Topology unchanged; handler code may have changed. Replay proceeds; warn in `onStepStart` event payload. |
| ❌ | – | Topology has changed since the run. Default: throw `NagiSnapshotDriftError`. Caller can pass `replayOpts.allowDrift = true` to execute scheduling decisions from the snapshot using live handlers (best-effort). |

This gives operators a knob: by default we fail-loud; sophisticated callers can override after reading the diff.

## Rationale and alternatives

### Why content-addressing?

The simplest version of the feature is: stamp every run with a `flow_version` string. That works only if the operator remembers to bump it on every meaningful change. Discipline breaks under deadline; the system degrades silently.

Content-addressing makes drift mechanically detectable, not discipline-dependent. Every code change that affects DAG topology produces a new hash; the system *cannot* pretend the change didn't happen.

### Alternatives considered

**A. Version-suffix-in-id only.** Encode the version in `flow.id` ("video-analysis.v1" → ".v2") and rely on git history for the actual definition.
- Pro: zero new tables, zero canonicalization.
- Con: relies on discipline; in-flight runs aren't isolated from cross-version refactors that keep the same id.

**B. Temporal-style version gates in user code.** Users write `if (version >= 2) { newBehavior() } else { oldBehavior() }` inside handlers. Engine doesn't see definitions at all.
- Pro: maximum flexibility.
- Con: painful, viral; "version-gate spaghetti" is a well-documented Temporal anti-pattern. Definitions and runtime decisions tangle.

**C. Full code snapshot.** Serialize every step's `run` function's `.toString()` into `flow_snapshot.code`. Replay literally evals the stored code.
- Pro: full reproducibility.
- Con: hostile to bundlers, security review nightmare (executing stored code), giant blob storage, brittle under module-system changes.

**D. Snapshot only on explicit user request.** Add a `wf.snapshot(flow)` API; users decide when to freeze.
- Pro: low overhead when not used.
- Con: same discipline problem as (A) — easy to forget.

Content-addressing wins by being automatic, byte-stable, and bounded in scope (topology only).

## Drawbacks

- **Two new tables** in the migration set. Adds surface area for nagi's `migrate()` function and one more thing to think about when integrating with managed-DB tools.
- **Canonicalization is non-trivial.** `when`-predicate hashing is the spiciest part; see "Open questions."
- **The handler-code gap is real.** Operators may *think* they have full reversibility and be surprised when a handler edit changes behavior without changing the hash. The README must be explicit; we accept this as a known limit.
- **`wf.replay()` semantics change.** Today it implicitly uses current code. The new default throws on drift. This is a breaking change for anyone relying on the old behavior — handled via a major version bump and an `allowDrift` escape hatch.
- **Compile-time perf.** Canonicalization runs on every `nagi()` boot. For most users (handful of flows) it's microseconds. For users with hundreds of flows, it's worth measuring.

## Implementation plan

### Phase 1 — schema and canonicalization (in `@nagi-js/postgres` and `@nagi-js/core`)
- Add `flow_snapshot` and `flow_ref` tables to the migration set.
- Add `flow_hash` and `code_version` columns to `workflow_run`.
- Implement `canonicalize(flow): CanonicalDag` in `@nagi-js/core`.
- Implement sha256 hashing via `crypto.subtle` (edge-safe) for the canonical form.
- Unit tests on canonicalization: byte-stability under reordering, byte-difference under semantic change.

### Phase 2 — runtime wiring
- Hook canonicalization into the `nagi({ flows })` boot path.
- Wire `flow_hash` resolution into `wf.start()`.
- Implement `flow_ref.updated` fact-log entries.

### Phase 3 — replay semantics
- Update `wf.replay()` to load by `flow_hash`.
- Implement drift detection + `NagiSnapshotDriftError`.
- Document `replayOpts.allowDrift`.

### Phase 4 — tooling
- Implement and export `diffSnapshots(a, b)`.
- Add admin queries to the README ("how do I see what changed?").

### Phase 5 — migration / docs
- README section: "Versioning and reversibility."
- Migration guide for existing nagi users (snapshot backfill on first boot post-upgrade).
- Mark v0.2.0 as the version that introduces this.

## Open questions

1. **`when`-predicate canonicalization.** Hashing `.toString()` at registration is best-effort and breaks under minification. A build-time transformer (Babel/SWC plugin) would be cleaner but adds an opt-in compilation step. Which path?
2. **Schema serialization.** Different `StandardSchemaV1` implementations (Zod, Valibot, ArkType) have different introspection capabilities. Do we standardize on a serialized form, or hash the source text as a fallback?
3. **Cross-flow chained runs.** If flow A starts flow B from inside a handler, the hash of A doesn't capture B's hash transitively. Do we track the dependency graph at the snapshot level?
4. **Snapshot retention.** Should `flow_snapshot` rows ever be pruned? In principle no (immutability is the point), but extreme-churn users might want a TTL on snapshots that have no live runs pinned to them.
5. **`allowDrift` granularity.** Per-call (`replayOpts.allowDrift = true`) is proposed. Should there also be a per-flow setting ("this flow's topology may drift; replay best-effort")?

## Future possibilities

- A web UI bundled with `@nagi-js/postgres` (or as a separate `@nagi-js/admin`) that surfaces the snapshot graph, lets operators diff snapshots visually, and shows which `code_version` was active for a given run.
- An `@nagi-js/cli` command `nagi diff <runId-a> <runId-b>` that prints a structured snapshot diff at the terminal.
- Handler-code hashing (the α path) as an opt-in for users who can afford the build-time integration.

## Prior art

- **Git** — the obvious one. Content-addressed object store + mutable refs + diff. This RFC is most of git's model applied to workflow DAGs.
- **Temporal / Cadence "workflow versioning."** Their solution is user-side version gates in handler code. Famously painful; this RFC is, in part, a deliberate rejection of that approach.
- **Airflow DAG versioning.** Airflow versions DAGs via filename / DAG_ID rename. Light; same problems as alternative (A).
- **Nix / IPFS / OCI image manifests.** All content-addressed; same lineage as git.
