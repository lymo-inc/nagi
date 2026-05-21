# RFC 0012 — Parameterize `Wf` over registered flows (typed `flowId`)

- **Status:** Draft (2026-05-21) — decisions log pending Jay's approval. **Do not implement until approved.**
- **Author:** Claude (paired with @jay)
- **Created:** 2026-05-21 (JST)
- **Tracking:** issue #18
- **Scope:** `@nagi-js/core` public types only. Pure typing change, **no runtime delta**, no fact-shape change, no migration. Patch release. `@nagi-js/postgres` touched only if D7 goes the "generic Store" way (it does not, by recommendation).
- **Decisions log:** authoritative — see "Decisions taken". Items marked **⚠ OPEN** are the grilling branches; everything else is a recommended call you can still veto.
- **RFC number:** sequential after 0011 (the 0011 precedent decoupled RFC# from issue#). Tracking issue is #18; file is `0012`. Rename to `0018` if you prefer issue-matched numbering.

## Summary

`Flow<Id extends string>` (types.ts:258) already carries its id at the type
level. That information is **lost** the moment flows aggregate through
`nagi({ flows: [...] })`, because `NagiConfig.flows` is `ReadonlyArray<Flow>`
(runtime.ts:52) and `Wf` is non-generic (runtime.ts:71). So `wf.queryRuns()`
returns `RunSummary` with `flowId: string` (types.ts:503), and any consumer
with a closed flow set must hand-narrow at the projection boundary with a
hand-maintained id set (`FLOW_IDS` + `narrowFlowId` in lymo).

This RFC threads the registered flow tuple through `Wf<TFlows>` and derives the
literal-id union with `FlowIdOf<TFlows>`, so `wf.queryRuns()` returns
`RunSummary<"videoAnalysis" | "dealAnalysis" | ...>` automatically. The
hand-maintained set in consumers becomes derivable and cannot drift from the
actual registration.

## Motivation

The pain (from issue #18, lymo `apps/backend/src/workflows/read.ts`):

```ts
const FLOW_IDS: ReadonlySet<FlowId> = new Set([
  "videoAnalysis", "dealAnalysis", "productContextGeneration", "dealScoring",
]);
function narrowFlowId(value: string): FlowId {
  if (FLOW_IDS.has(value as FlowId)) return value as FlowId;
  throw new Error(`unknown nagi flow id: ${value}`);
}
```

This set is hand-maintained and has **no compile-time linkage** to the actual
`nagi({ flows: [...] })` registration. Add a flow → edit registration AND every
consumer-side narrowing. The type information already exists; the library just
drops it.

## Decisions taken (2026-05-21)

> Recommended calls with reasoning. Flag any you want to revise. The three
> **⚠ OPEN** items are the grilling branches.

1. **`Wf<TFlows extends ReadonlyArray<Flow> = ReadonlyArray<Flow>>`.** Default
   parameter preserves today's behavior exactly: bare `Wf` → `flowId: string`.
   Purely additive.

2. **`FlowIdOf<T extends ReadonlyArray<Flow>> = T[number] extends Flow<infer Id> ? Id : never`,
   exported.** Distributive (not head-recursive), so no TS2589
   "excessively deep" risk at any realistic flow count. Mirrors the existing
   `FlowInput<F>` extractor (types.ts:275) — house style.

3. **Tuple, not map (`Wf<readonly Flow[]>`, not `Wf<Record<id, Flow>>`).**
   `flows: [...]` is already a tuple; a map forces a breaking entry-point
   change for cosmetic gain. The id already lives *inside* `Flow<Id>`, so a map
   keyed by id is redundant and invites key/`.id` divergence. (Prior art —
   tRPC/Inngest/Temporal — uses maps *because their id is the key and isn't in
   the value*; nagi's situation is the inverse.) Duplicate-id detection, the
   map's one real advantage, is handled at runtime already (D12).

4. **`const` type parameter on `nagi`:**
   `nagi<const TFlows extends ReadonlyArray<Flow>>(config: NagiConfig & { flows: TFlows })`.
   TS 5.0 `const` infers the narrow tuple from an inline array literal **without
   requiring `as const`** — *provided the constraint stays `readonly`* (it does:
   `ReadonlyArray<Flow>`). Hard caveat to document: `const` only narrows array
   literals **written at the call site**. A hoisted `const flows = [...]; nagi({ flows })`
   still widens to `Flow[]` and needs `as const` — this is lymo's
   `runtime.ts` case. So `const` removes the ceremony for the inline path and is
   strictly additive for the hoisted path (default param → `flowId: string`).

5. **Defaulted generics on the projection types**, all `= string`:
   `RunSummary<FlowId extends string = string>`,
   `QueryRunsResult<FlowId extends string = string>`,
   `QueryRunsWhere<FlowId extends string = string>`, and
   `QueryRunsOpts<FlowId extends string = string>` (the discriminated union at
   types.ts:487 must thread the param into both arms). Bare usage anywhere is
   unchanged.

6. **⚠ OPEN — Q1: `QueryRunsWhere` filter key name.** The shipped code uses
   `flowId?: string` (types.ts:482). The issue text proposes renaming it to
   `flow?: FlowId`. Rename is a **breaking** change (existing callers,
   `queryRuns.test.ts:171`, lymo, and the `memory.ts:272` / `store.ts` runtime
   filter all key on `flowId`). **Recommendation: keep `flowId`** — add the
   generic, do not rename. The issue text predates the shipped implementation.

7. **D7 — narrowing boundary (the irreducible cast).** Keep `Store.queryRuns`
   **non-generic** (`Promise<QueryRunsResult>`, `flowId: string`) and localize
   the `string → FlowId` assertion as a **single boundary cast** inside
   `wf.queryRuns` (runtime.ts:730). Rationale: the DB column `workflow_run.flow_id`
   is `text`; `PostgresStore` reads it as `string` (store.ts:617) and *cannot*
   prove the persisted value is a registered id. Threading generics through
   `Store` would just relocate the same unavoidable cast into every adapter and
   add generic noise to the `Store` contract. One honest cast at the read
   boundary is the minimal-blast-radius choice (adapters untouched). See
   "Unrepresentable-states analysis" — this is the one state the type system
   genuinely cannot rule out, so a localized assertion is correct, not a smell.
   *(Confident recommendation, not grilling — flagging because it involves an
   `as`, and you are opinionated about casts.)*

8. **⚠ OPEN — Q3: filter-input strictness.** Should the `where` filter accept a
   strict `FlowId` union (typo-proof, autocomplete; dynamic callers must cast)
   or `FlowId | (string & {})` (autocomplete preserved **and** accepts a runtime
   string)? **Recommendation: strict `FlowId`** — aligns with "make invalid
   states unrepresentable"; a filter on a non-registered flow is a bug worth a
   compile error. Depends on Q1's key name.

9. **⚠ OPEN — Q2 (scope): also constrain `start`?** The issue proposes
   `start<F extends TFlows[number]>` (today: `start<F extends Flow>`,
   runtime.ts:72), which makes `wf.start(unregisteredFlow, …)` a **compile
   error** instead of a runtime throw (`flowsById` miss, runtime.ts:185).
   Backwards-compatible via the default (`TFlows[number]` → `Flow` for bare
   `Wf`). **This expands scope from read-side to write-side.** Recommendation:
   *lean yes* — it's the same single-source-of-truth win on the write path and
   directly serves "unrepresentable invalid states" — but it is scope beyond the
   issue's title, so it's your call.

10. **`startById(flowId: string)` stays `string` — NOT narrowed.** Its docstring
    (runtime.ts:78-89) targets transactional-outbox reconcilers, DLQ replay, and
    admin CLIs replaying a serialized `runId` — callers that hold a *runtime*
    string and validate against the registered schema at runtime. Narrowing it
    would defeat its entire purpose. Explicit non-goal (see "Outbox review").

11. **No branding of `FlowId`.** Brands solve "don't mix two same-shaped ids,"
    which isn't nagi's problem (we want a literal union, which `const`/`as const`
    already deliver). Branding also has a known generic-inference widening bug
    (TS#61093) that could *reintroduce* the widening we're fixing. Skip.

12. **Duplicate flow ids: no compile-time guard.** `nagi()` already throws at
    runtime on dupes (runtime.ts:185-189); the type-level union dedups
    naturally. A compile-time guard is the only thing a map registry would buy,
    and it doesn't pay for itself (D3).

13. **Empty `flows: []` → `FlowIdOf<[]>` = `never`** (natural fallout of the
    distributive form). A nagi with zero flows is degenerate — it can't start
    anything — so `never` on its query results is harmless. Not special-cased to
    `string`. (Flagging for visibility; revert to `string` fallback if you'd
    rather.)

## Proposed shape

### `packages/core/src/types.ts`

```ts
export type FlowIdOf<T extends ReadonlyArray<Flow>> =
  T[number] extends Flow<infer Id> ? Id : never;

export interface RunSummary<FlowId extends string = string> {
  readonly runId: RunId;
  readonly flowId: FlowId;          // ← was string
  readonly status: RunStatus;
  readonly startedAt: Date;
  readonly completedAt: Date | null;
  readonly input: Json;             // stays Json (D10)
}

export interface QueryRunsWhere<FlowId extends string = string> {
  readonly flowId?: FlowId;         // key name per Q1 (D6); strictness per Q3 (D8)
  readonly status?: RunStatus | ReadonlyArray<RunStatus>;
  readonly input?: Record<string, Json>;
}

export type QueryRunsOpts<FlowId extends string = string> =
  | { readonly where?: QueryRunsWhere<FlowId>; readonly latest: true;
      readonly limit?: never; readonly cursor?: never }
  | { readonly where?: QueryRunsWhere<FlowId>; readonly latest?: false;
      readonly limit?: number; readonly cursor?: string };

export interface QueryRunsResult<FlowId extends string = string> {
  readonly runs: ReadonlyArray<RunSummary<FlowId>>;
  readonly cursor: string | null;
}

// Store contract stays NON-generic (D7) — adapters read flow_id as string:
//   queryRuns(opts: QueryRunsOpts): Promise<QueryRunsResult>;  // unchanged
```

### `packages/core/src/runtime.ts`

```ts
export interface Wf<TFlows extends ReadonlyArray<Flow> = ReadonlyArray<Flow>> {
  start<F extends TFlows[number]>(            // Q2/D9 — narrows to registered flows
    flow: F, input: FlowInput<F>, opts?: StartOpts,
  ): Promise<RunId>;
  startById(flowId: string, input: unknown, opts?: StartOpts): Promise<RunId>; // D10 unchanged
  // signal / cancel / worker / replay / operator / pruneFacts — unchanged
  queryRuns(
    opts?: QueryRunsOpts<FlowIdOf<TFlows>>,
  ): Promise<QueryRunsResult<FlowIdOf<TFlows>>>;
}

export async function nagi<const TFlows extends ReadonlyArray<Flow>>(
  config: NagiConfig & { flows: TFlows },
): Promise<Wf<TFlows>> {
  // body unchanged; wf.queryRuns localizes ONE cast (D7):
  //   return store.queryRuns(opts) as Promise<QueryRunsResult<FlowIdOf<TFlows>>>;
}
```

`NagiConfig.flows` stays `ReadonlyArray<Flow>` for the non-generic default
path; the tuple is captured by the `& { flows: TFlows }` intersection on
`nagi`'s parameter so the literal survives.

## Unrepresentable-states analysis

| State | Today | After this RFC |
| --- | --- | --- |
| Consumer's `FLOW_IDS` set drifts from the actual `nagi({ flows })` registration | Representable & silent — two hand-synced lists | **Unrepresentable** — the union is *derived* from the registered tuple; one source of truth |
| `wf.queryRuns({ where: { flowId: "typoFlow" } })` | Compiles (string) | **Unrepresentable** if Q3 = strict union — compile error |
| `wf.start(flowNotRegistered, …)` | Compiles, throws at runtime (runtime.ts:185) | **Unrepresentable** if Q2 = yes — compile error |
| A persisted `flow_id` value that isn't a registered flow id | Representable | **Still representable** — see below |

**The one irreducible state (D7).** Persistence erases the literal type: the
`workflow_run.flow_id` column is `text`, so on read-back the type system cannot
prove the value is a member of `FlowIdOf<TFlows>`. We *cannot* design this away
without a runtime guard (which would change behavior — issue mandates "no
runtime delta") or a schema the DB can't express. Per house philosophy, the
runtime guard is a last-resort fallback we explicitly decline here; instead we
**localize a single boundary cast** at `wf.queryRuns` and document it as the
trust boundary ("persisted flowIds were, by construction, registered flowIds at
write time"). Confined to one site, reviewable, and honest about being an
assertion.

## Outbox / crash-recovery review

This is a read-side typing change with **no write path, no fact append, no
atomicity surface** — so most of the outbox question is N/A. Two points on
record:

- **`startById` is the outbox/replay entry point** and is deliberately left
  `string`-typed (D10). Its docstring (runtime.ts:78-89) names the exact
  callers: "transactional-outbox reconcilers, queue consumers replaying DLQs,
  admin CLIs replaying a runId." Those callers hold a *serialized* flow id from
  a durable store and validate it against the registered schema at runtime.
  Narrowing `startById` to `FlowIdOf<TFlows>` would force them to cast at every
  call — net negative. The typed path is `start` (D9); the runtime-string path
  is `startById`. Keeping them split is the correct read/write-vs-replay seam.
- **No change to the `appendFact` → `enqueue` ordering** or the
  re-derivation-based recovery model (RFC 0011). `queryRuns` is a pure
  projection over the fact log; the cast in D7 reads already-committed rows and
  introduces no new failure mode.

## Behavior preservation & testing

Mechanism: `expectTypeOf` (vitest) in `*.test-d.ts` + inline `// @ts-expect-error`
for negatives (existing pattern, types.test-d.ts:33 / :211 / :255). Runtime
tests via `describe/it` + `expect` (queryRuns.test.ts).

- **Type-level** (new, in `types.test-d.ts`): `nagi({ flows: [a,b] })` →
  `queryRuns().runs[].flowId` is `"a" | "b"`; bare `Wf`/`RunSummary` →
  `string`; single-flow tuple → that one literal (not widened);
  `FlowIdOf<typeof flows>` resolves to the union; `@ts-expect-error` on a
  non-member `where.flowId`; `start` still infers `FlowInput<F>` (+ Q2's
  `@ts-expect-error` on an unregistered flow if adopted); `nagi({ flows: [a,b] })`
  narrows **without** `as const` (guards the `readonly`-constraint requirement
  from D4).
- **Runtime** (queryRuns.test.ts — assert no delta): same `{ runs, cursor }`
  shape/values as before; `flowId` value equals the registered id string;
  existing filter/latest/cursor behavior identical.
- **Regression:** all existing `*.test.ts` and `*.test-d.ts` pass unchanged;
  `Store.queryRuns(opts): Promise<QueryRunsResult>` (un-narrowed) still
  satisfies the interface (D7).

Commands: `pnpm test` (vitest run), `pnpm test:types` (vitest --typecheck),
`pnpm typecheck` (tsc --noEmit).

## Alternatives considered

- **Map registry `Wf<Record<id, Flow>>`** — rejected (D3): breaking entry-point
  change, redundant with `Flow<Id>`, only buys duplicate detection (already at
  runtime).
- **Generic `Store<FlowId>`** — rejected (D7): relocates the same unavoidable
  cast into every adapter and pollutes the contract; adapters fundamentally read
  `string` from `text`.
- **Branded `FlowId`** — rejected (D11): wrong tool, known inference-widening
  bug.
- **Rename `flowId` → `flow`** — see Q1 (D6); recommended against (breaking, no
  functional gain).
- **Runtime narrowing guard in `queryRuns`** — rejected: violates "no runtime
  delta" and the house preference against runtime guards over structural typing.

## Open questions — to resolve by grilling

- **Q1 (D6):** keep `flowId` or rename to `flow`?
- **Q2 (D9):** also constrain `start<F extends TFlows[number]>` (write-side
  scope), or read-side only?
- **Q3 (D8):** strict `FlowId` filter vs `FlowId | (string & {})`?
