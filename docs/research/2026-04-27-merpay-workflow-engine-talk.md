# Research — Merpay in-house workflow engine talk (2026-04-27)

Source: `2026_04_27_Background_Job_Talk.pdf`, 36 slides, Japanese. Title: 「内製ワークフローエンジンの設計とメルカリでの活用事例」 — *Designing an in-house workflow engine and Mercari's use cases*. Speaker: 南祐希 / @sapuri, Merpay.

Goal of this document: distill what the talk says, map each idea against Nagi's current shape, and surface the design questions Jay needs to decide before any of this becomes an RFC.

This is descriptive, not prescriptive. Sections marked **Decision point** flag places where Jay's judgment changes the design.

---

## 0. Design lens — make invalid states unrepresentable

Before evaluating any pattern from this talk, the bar is: *can the bug class it addresses be made impossible by construction?* Recovery sweepers, polling reconciliation, and runtime guards are last-resort fallbacks, not defaults. The aspiration is the Rust/Haskell sense of "wrong states won't compile".

This lens significantly downgrades one of the talk's headline ideas (the Recovery Worker — see §6) and elevates the question of where transactional boundaries sit relative to the queue/store split (see §5 G1).

It also fits Nagi's existing posture: `nextRunnable()` is a pure function (`scheduler.ts:29`), `runStep` owns an atomic scope for handler + completion fact (`dispatch.ts:181-184`), and `@nagi-js/pgmq` exposes `withTx(tx)` precisely so a queue write commits-or-rolls-back with the handler (`pgmq-queue.ts:44-52`). The library already leans this way; the principle is "lean harder."

---

## 1. The talk in one paragraph

Merpay needed Saga-style eventual consistency for multi-service payment flows (JPY balance / Mercoin / Pub/Sub) and rejected both 2PC ("locks too long") and the off-the-shelf engines (GCP Workflows: HTTP-only, YAML-not-Go; Cadence/Temporal: no Cloud Spanner support, too much ops surface). They built an in-house Go SDK that mirrors Temporal's API shape but is dramatically smaller: **DB-persisted execution state × in-memory queue × worker-managed execution.** Maintained part-time by a few engineers. Same library now runs in three production case studies, two on Spanner and one on Postgres.

---

## 2. Their architecture (slides 13–20)

Seven components. The deck redraws the same diagram five times, adding one component at a time — useful for us because the layering matches Nagi's own decomposition surprisingly closely.

| Component | Role | Nagi analogue |
|---|---|---|
| **Manager** (`wm`) | SDK entrypoint user code calls. Wraps Activity/Workflow registration and invocation. | `Wf` from `runtime.ts` — `Wf.start`, `Wf.signal`, `Wf.worker` |
| **Engine Server** | gRPC service: `Create`, `Complete`, `List`. Persists step I/O and state; **returns the prior stored result on retry** (free idempotency). | `Store` interface in `packages/core/src/types.ts:454+` — `appendFact`, `claimStep`, `completeStep`, `getOnce`, `recordOnce` |
| **Channel** | In-memory pub/sub: `WorkflowStarted/Completed`, `ActivityStarted/Completed`. Allowed to lose events; durability lives in the store. | `Queue` interface (`@nagi-js/pgmq` for Postgres; in-memory for tests) |
| **Workers** | Goroutines that subscribe to the Channel and dispatch via `reflect.ValueOf(fn).Call(args)`. | `packages/core/src/worker.ts:20` `makeWorker` (concurrency-bounded, abortable) |
| **Registry** | In-memory map of registered functions. | `Flow` definitions registered with `nagi({ flows: [...] })` |
| **Recovery Worker** | Periodic sweeper: `List`s incomplete workflows from Spanner and re-publishes Channel events to wake them. | **No direct analogue today.** See §6. |
| **Spanner (Execution Logs)** | Source of truth. Workflow + Activity rows with I/O, status. | `workflow_run`, `step_run`, `fact` tables (Postgres adapter) |

Deployment shape: the engine ships **as a library in the same pod as the application**, plus a gRPC sidecar to Spanner. There is no separate cluster to operate. This matches the bias Nagi already has (caller-driven, library-in-process).

---

## 3. The nine ideas worth lifting

Numbered for cross-reference from §7 (the adoption table).

**(1) Persist step I/O; return stored output on retry.** The dominant idempotency primitive — completed steps return their stored result instead of re-running. Nagi already has this via `once()` (`packages/core/src/idempotency.ts:25-41`) over `store.getOnce` / `store.recordOnce`. The Merpay version is implicit at the *step* level, not opt-in per-effect.

**(2) Error taxonomy as a runtime contract.** Three kinds: `Completable` (finalise as failed — e.g., insufficient balance), `Retryable` (retry immediately), `Incompletable` (default; halt, Recovery Worker retries later). Opted-in via an `ErrorMarshaler` interface. **The contrarian rule:** *a workflow never completes unless the handler explicitly returns a Completable error.* Inverts the JS/TS default. Nagi today: errors are captured as `step.failed` facts (`types.ts:849+`); `RetryPolicy.retryOn` exists (`types.ts:123`); no taxonomy on the failure side.

**(3) Recovery Worker as the durability primitive — not the queue.** Channel can lose events; the periodic sweep over the store is what guarantees forward progress. Nagi today has the queue, but no sweeper. See §6.

**(4) Saga DSL with compensations.** First-class `saga.AddCompensation(fn, args)` registers reverse work; `saga.ExecuteWait` runs compensations in reverse on terminal failure (slide 21). Nagi has no compensation concept today.

**(5) Signal for long-running and human-in-the-loop flows.** `wm.Signal("approval").Receive(...)` blocks; external code calls `wm.Signal("approval").Send(ctx, result, workflowIdemKey)`. Used for multi-day eKYC review. Nagi has `SignalStep` already (`types.ts:299+`); the deck validates this design as the right shape for human review.

**(6) Fire-and-forget child workflow.** Split synchronous response from async side-effects (used in SIM activation: respond to user, continue Pub/Sub work in a child). Nagi has no child-workflow concept.

**(7) Determinism rules + a custom linter.** Activity args must be deterministic. They ship a `magicianlint` with per-mistake Analyzers, plus an **LLM-backed Analyzer** that flags transitive non-determinism static analysis can't trace (e.g., a randomly-generated ID passed three frames deep into an Activity argument).

**(8) Store-agnostic core.** Same engine runs on Spanner (Mercari/Merpay) and Postgres (Mercari Global EC). Argues for keeping Nagi's `Store` interface narrow enough that SQLite / MySQL / Spanner adapters are tractable.

**(9) AI as the substitute for community.** In-house frameworks have no Stack Overflow. Sourcegraph-MCP-style code search plus a strong linter recover most of the missing affordances. Directly relevant to Nagi as a young OSS library.

---

## 4. Where Nagi already aligns

Citations are to current `main`:

- **Step I/O memoised in the store.** `packages/core/src/idempotency.ts:25-41` (`makeOnce`) reads `store.getOnce`, falls back to `fn()`, persists via `store.recordOnce`. Same shape as Merpay's Engine Server returning the stored result.
- **Stable external-API idempotency key.** `packages/core/src/idempotency.ts:13-18` returns `nagi:<runId>:<stepId>:<scope>` — Stripe-friendly, identical across retries.
- **Pure scheduler.** `packages/core/src/scheduler.ts:29` `nextRunnable()` is documented as "Pure function; the caller persists the resulting facts and enqueues messages." Structurally the same separation Merpay draws between Engine Server (persistence) and Workers (dispatch).
- **Lease-based step claim.** `Store.claimStep` (`types.ts:589`) gives at-most-one-worker-per-step across processes — Merpay's equivalent is the Engine Server's `Create` returning the prior result on retry.
- **Signal as a step kind.** `types.ts:299+` already models `b.signal` as a first-class step. Direct match for §3(5).
- **Hook-based observability.** `FlowHooks` (`types.ts:418+`) + `@nagi-js/otel` adapter — matches the deck's implicit "instrument the lifecycle, don't bake it in" stance.
- **Snapshot + drift detection.** `runtime.ts:344+` (`synthesizeReplayFlow`, `NagiSnapshotDriftError`) addresses a problem the talk *never mentions*: workflow-code versioning. Nagi is ahead here.

---

## 5. Gaps in Nagi, named

The gaps the talk surfaces, ranked by how cleanly they'd slot into the current code:

| # | Gap | Effort estimate | Touches |
|---|---|---|---|
| G1 | **Same-transaction ack** for the incoming dispatch message. Today `runStep` is atomic (handler + `step.completed` fact + lease release commit together) but `queue.ack(message.receipt)` happens *after* `runStep` returns (`dispatch.ts:211`), outside its transaction. Folding the ack into the `runStep` tx via `queue.withTx(tx).ack(receipt)` makes "queue says done, store doesn't" structurally impossible for PGMQ topology. See §6. | S | `dispatch.ts:184-211`, possibly small `Store.runStep` signature tweak |
| G2 | **Error taxonomy** — `Completable` / `Retryable` / `Incompletable` as typed surfaces. Currently `RetryPolicy.retryOn` is the only knob. **Jay endorsed (2026-05-14): "powerful concept we haven't thought of."** | S–M | `types.ts`, `dispatch.ts`, error-throwing in step handlers; touches public API |
| G3 | **"Default = do not finalise"** semantic — unknown errors leave the run resumable instead of marking `flow.failed`. **Jay endorsed (2026-05-14).** | M | `dispatch.ts` `handleStepError`; semantic change, needs RFC |
| G4 | **Compensations / Saga combinator.** `b.compensate(stepId, fn)` registers reverse work; runs in reverse on terminal failure. | M | new builder method, new fact kind, runtime traversal |
| G5 | **Fire-and-forget child run.** `ctx.startChild(flowId, input)` that returns immediately and runs under a fresh `runId`. | M | `runtime.ts`, queue semantics |
| G6 | **Determinism lint.** ESLint rule pack flagging `Date.now()`, `Math.random()`, `crypto.randomUUID()`, env reads, network calls inside step handlers. Type-system version: a branded `Deterministic<T>` parameter type that the lint upgrades to a compile error. | S (static rules) / M (LLM-augmented) / M (type-level) | new `@nagi-js/eslint-plugin` package; `types.ts` for the branded variant |
| G7 | **Recovery sweeper (fallback only).** Periodic re-enqueue of stale runs. **Demoted per §0 design lens** — useful only for topologies where the queue cannot share a transaction with the store (external SQS, separate Postgres clusters). For the PGMQ blessed path, G1 makes this unnecessary by construction. | S (if scoped to non-co-located queues) | new `recovery.ts`; only ships if non-PGMQ adapters become supported |
| G8 | **Versioning policy.** Already partly solved by snapshot + drift detection (`runtime.ts:344+`). The talk **does not address this**; Nagi is already ahead. | — | already shipped |

---

## 6. Queue/store divergence — make it unrepresentable, not recoverable

Under the §0 lens, the talk's Recovery Worker is the wrong primary fix. It is *recovery from* an invalid state ("queue's view of done diverges from store's view") rather than *prevention of* that state. The right question is: can we make the divergence unrepresentable?

### 6.1 The exact hazard

Today's dispatch flow (`dispatch.ts:181-211`):

```ts
const output = await store.runStep(runId, stepId, attempt, async (tx) => {
  const out = await def.run({ input, needs, ctx });
  const fact: Fact = { kind: "step.completed", ... };
  return { output: out, fact };  // committed atomically with lease release
});
await queue.ack(message.receipt);  // ← outside the runStep tx
```

`runStep` is genuinely atomic: handler output, `step.completed` fact, and lease release commit together (`dispatch.ts:181-184`). But `queue.ack` runs *after* — a worker crash between these two lines leaves the store consistent (step is `completed`) while the queue still considers the message in-flight. After the visibility timeout, another worker dequeues the same message.

This converges safely today only because the second worker's `claimStep` (or, more precisely, the dispatcher's pre-execution check that the step is not already completed) makes the redelivery a no-op. So **the system is convergent but not invariant** — there is a real divergence window of size ≤ `vtSeconds`. Convergence-by-retry is not "unrepresentable"; it's "self-healing", which is one rung down.

### 6.2 Construction-level fix

`@nagi-js/pgmq` already exposes the mechanism (`pgmq-queue.ts:44-52`):

> `withTx(tx)` — Returns a `Queue` whose operations execute on the supplied transaction. Pass `ctx.tx` to enqueue messages atomically with the handler's domain writes — *the pgmq message commits with `step.completed` or rolls back with the handler.*

The fix is to use this for the *incoming* message's ack, not just for follow-on enqueues:

```ts
const output = await store.runStep(runId, stepId, attempt, async (tx) => {
  const out = await def.run({ input, needs, ctx });
  const fact: Fact = { kind: "step.completed", ... };
  await queue.withTx(tx).ack(message.receipt);  // joins the same transaction
  return { output: out, fact };
});
// no separate ack
```

After this change, the divergence window is gone: either the message is deleted from pgmq AND the step is marked completed, or neither happens. The bug is no longer "self-healing"; it is "structurally impossible". This is the §0 bar.

The same pattern applies symmetrically to `nack` (failure path) and to the match-step ack at `dispatch.ts:246` and to the unknown-step ack at `dispatch.ts:70/83/89`.

### 6.3 The cost — topology constraint

The fix only works when the queue and store share a transactional substrate. PGMQ + Postgres in the same database does. SQS + Postgres does not. So the cleanest framing is:

- **Blessed topology (PGMQ co-located with `@nagi-js/postgres` store):** same-transaction ack. Zero divergence by construction. No sweeper needed.
- **External-queue topology:** divergence is unavoidable, since Nagi cannot transact across that boundary. *Only here* does a sweeper become a defensible fallback — and the user pays for it explicitly by choosing a non-co-located queue.

This argues for documenting PGMQ + Postgres as the recommended path and treating other queue adapters as opt-in trade-offs, not equal peers.

### 6.4 Decision point — same-tx ack as default

**Decision point.** Adopting §6.2 means a small surgery on `dispatch.ts` and likely a tiny addition to the `Store.runStep` callback shape (so the callback can ack before returning). Two sub-choices:

1. **(A)** Add `tx` to the existing callback's argument list (already there in spirit — `runStep` already passes `tx`); the dispatcher calls `queue.withTx(tx).ack(receipt)` inside the callback. No store-interface change. Cleanest.
2. **(B)** Extend the callback's return shape to `{ output, fact, acks: readonly Receipt[] }` and have `runStep` itself perform the acks inside the tx. Decouples dispatcher from queue type, but bleeds Queue concerns into Store.

(A) is the natural extension. (B) is over-abstracted.

---

## 7. Decision point — adoption verdict per idea

This is the part of the doc that only Jay can author. The talk gives us nine candidate lessons (§3). Each needs a verdict against Nagi's actual roadmap and the locked scope of "multi-turn LLM backend workflows only" (per `project_nagi_scope.md` in memory). Fill the verdict column with one of:

- **Adopt-now** — file an RFC in the next sprint
- **Adopt-later** — agreed in principle, not this quarter
- **Reject** — explicitly out of scope for Nagi
- **Already shipped** — Nagi has this

```
| # | Idea                                          | Verdict           | One-line rationale |
|---|-----------------------------------------------|-------------------|--------------------|
| 1 | Persist step I/O, return on retry             | Already shipped   | `once()` covers this; step-level not effect-level — open question whether that gap matters |
| 2 | Error taxonomy (Completable/Retryable/Incomp.) | Endorsed (Jay)   | "Powerful concept we haven't thought of." Specific verdict (Adopt-now vs Adopt-later) and breaking-change timing — TODO |
| 3 | Recovery sweeper as the **durability primitive** | Reject          | Recovery-by-sweep is "self-healing" not "unrepresentable"; §0 bar not met. The real fix is §6.2 same-tx ack. Sweeper remains acceptable as a *fallback* for non-co-located queue topologies (G7), not as the default mechanism |
| 3' | Same-transaction ack (§6.2 / G1)              | TODO              | Construction-level replacement for idea 3. Likely Adopt-now under the §0 lens, but Jay decides |
| 4 | Saga DSL with compensations                   | TODO              | TODO |
| 5 | Signal for human-in-the-loop                  | Already shipped   | `b.signal` already exists |
| 6 | Fire-and-forget child workflow                | TODO              | TODO |
| 7 | Determinism rules + lint (LLM-augmented?)     | TODO              | TODO. Under §0, prefer the type-level variant in G6 (branded `Deterministic<T>`) over a pure-runtime lint — the lint becomes the diagnostic, not the enforcement |
| 8 | Store-agnostic core (SQLite/MySQL adapters)   | TODO              | TODO |
| 9 | AI-as-community (docs MCP, examples corpus)   | TODO              | TODO |
| 10 | "Default = do not finalise" semantic (G3)    | Endorsed (Jay)   | Adopted in principle 2026-05-14. Specific landing — TODO |
```

Sketch of how each verdict shapes the next ~quarter:

- If **G1 / idea 3' = Adopt-now**, it's the smallest surgery (one tx scope change in `dispatch.ts`) and eliminates a real divergence window. Highest leverage-to-effort ratio in the table.
- **G2 + G3 / ideas 2 and 10** are now both endorsed — together they amount to a deliberate "error model" RFC. It's a public-API breaking change — wants a major bump and a migration note. Worth bundling.
- If **idea 4 (Saga) = Reject**, that's a meaningful scope statement worth making explicit: Nagi handles step-level idempotency but compensations are the caller's job. Cleanly defensible given the "LLM backend" framing — most LLM steps are read-only or already idempotent via `once()`.
- If **idea 7 (lint) = Adopt-now**, prefer the type-level form (branded `Deterministic<T>` on handler parameters, so non-determinism fails to typecheck). The ESLint rules become a diagnostic layer for the cases the type system can't catch — closer to the §0 lens than a pure-runtime check.

---

## 8. Open questions

1. **Step-level vs effect-level memoisation.** Merpay memoises whole Activities; Nagi memoises *named effects* inside a step via `once(scope, fn)`. The latter is finer-grained (you can do multiple `once()`s per step) but requires the user to name each effect. Is this strictly better, or does a step-level default-on memoise reduce the user's surface area? Worth deciding before publicising a 1.0 API.

2. **Blessed topology declaration.** §6.3 implies PGMQ-with-Postgres is the only topology that meets the §0 bar. Should the README/docs call it out as the recommended path and treat external queues (SQS, Redis) as opt-in with documented divergence windows — or keep adapter parity and pay the sweeper tax everywhere?

3. **`Completable` as a sum-type return, not a thrown error.** The talk's `ErrorMarshaler` is awkward in idiomatic TS, and a thrown error is structurally the "I don't know what happened" path — exactly the kind of state §0 wants narrowed. A `Result<Ok, Completable | Retryable>` return type makes the handler's intent typecheck-enforced, with `throw` reserved for genuinely unknown errors that default-to-Incompletable. Worth a small spike before any RFC on G2/G3.

4. **Branded `Deterministic<T>` for handler inputs.** If step-handler arguments were typed `Deterministic<Input>` (a branded wrapper opaque to the user), then `Date.now()`, `Math.random()`, and untyped externals could only enter the handler via an explicit `unsafeFromNonDeterministic(...)` boundary. That makes the lint G6 fall out of the type system instead of needing a separate static analysis pass. Cost: ergonomic friction at the call site; benefit: full §0 coverage on the determinism question.

5. **Versioning narrative.** Nagi has snapshot+drift; the talk has nothing. This is a story worth telling in the README — but only Jay writes the README.

---

## Appendix — slide map

| Slides | Topic |
|---|---|
| 1–4 | Intro, agenda |
| 5–7 | Why distributed transactions hurt; three concrete inconsistency examples |
| 8–9 | Saga + compensation pipeline diagram |
| 10 | Build-vs-buy (rejected GCP Workflows, Cadence/Temporal) |
| 11–12 | "DB persistence × in-memory queue × worker" mantra |
| 13–20 | Architecture, redrawn five times (one component per slide) |
| 21 | Saga DSL code sample |
| 22–24 | Error taxonomy (`Completable` / `Retryable` / `Incompletable`) + `ErrorMarshaler` |
| 25 | (intermission) |
| 26–29 | Case studies: SIM activation, Global EC (Postgres), eKYC (Signal) |
| 30 | (intermission) |
| 31–35 | Operating an in-house framework: AI/MCP for code search, LLM-augmented linter |
| 36 | Wrap |
