# RFC 0013 — Runtime bootstrap ergonomics (`nagi.run` + auto queue init + pgmq typing)

- **Status:** Accepted (2026-05-21, Jay) — decisions resolved via grill; pending implementation approval
- **Author:** Claude (paired with @jay)
- **Created:** 2026-05-21 (JST)
- **Tracking issue:** lymo-inc/nagi#17
- **Related:** RFC 0003 (auto `codeVersion` — same "nagi() absorbs boilerplate" instinct), `@nagi-js/core`, `@nagi-js/pgmq`
- **Research notes:** `0013-runtime-bootstrap-ergonomics.research.md`
- **Decisions log:** authoritative — see "Decisions taken" below. Items marked **[GRILL]** are unresolved and gate implementation.

## Summary

Three independent, backwards-compatible changes that remove the boilerplate every
consumer pays on day one of adopting nagi:

1. **`nagi.run({...})`** — a turnkey worker lifecycle. Collapses the current
   four-step bootstrap (`nagi()` → `new AbortController()` → `wf.worker({signal})`
   → `worker.run().catch(graceful-vs-crash)`) and its three retained module-level
   references into a single `await` returning a handle with one idempotent
   `stop()`.
2. **Auto queue-schema bootstrap** — an optional `ensureSchema?()` on the queue
   contract that `nagi()` invokes once at construction, closing the
   "forgot to provision the pgmq queue → runtime error on first enqueue" trap.
3. **Generic `pgmqQueue<DB>`** — a pure typing change erasing the
   `db as unknown as Kysely<unknown>` cast every consumer writes.

The legacy `nagi()` + `wf.worker()` + `worker.run()` path is untouched; all three
changes are additive.

## Motivation

The reference consumer (`apps/backend/src/workflows/runtime.ts` in lymo/lymo)
demonstrates each paper cut. Today's bootstrap:

```ts
const wf = await nagi({ flows, store, queue, logger });
const abort = new AbortController();
const worker = wf.worker({ signal: abort.signal });
const loop = worker.run().catch((err) => {
  if (abort.signal.aborted) return;          // distinguish graceful shutdown
  logger.error({ err }, "worker exited unexpectedly");
});
// later, on SIGTERM:
abort.abort();
await loop;
```

| Paper cut | Failure mode it invites |
|---|---|
| Hold `worker` + `abort` + `loop` as separate refs | They can desync — `abort()` without `await loop` leaks an in-flight handler; the "was this graceful?" check is re-written (and mis-written) per consumer |
| `queue.ensureSchema()` is a separate manual step | Forgetting it throws on first enqueue — and did, in lymo/lymo `b6c2e7b05`. The error surfaces hours later in low-volume systems, far from the missing call |
| `db as unknown as Kysely<unknown>` at every `pgmqQueue` callsite | An unsafe double-cast that erases the consumer's schema type, copy-pasted with a justifying comment |

nagi already owns the queue contract and has full visibility into the runtime at
construction — the same argument as RFC 0003. The library should absorb this.

## Codebase grounding (verified)

- `nagi()` is a bare `export async function nagi(config): Promise<Wf>`
  (`packages/core/src/runtime.ts:180`), exported from
  `packages/core/src/index.ts:31`. It registers flows, writes snapshots/refs, and
  computes `codeVersion`. **It performs no `enqueue` at construction** — runs are
  enqueued later via `wf.start()` → `startRunInternal`. So `ensureSchema` placement
  inside `nagi()` is not ordering-critical for correctness.
- `worker.run()` (`packages/core/src/worker.ts:39-56`) already implements graceful
  drain: `while (!aborted())` then `await drain()`. It resolves cleanly on abort
  and **never throws on graceful shutdown**. Per-message errors are caught, logged,
  and nacked inside `dispatchSafely` (`:103-114`); `sleep()` swallows the abort
  rejection (`:126-130`). The **only** path that rejects `run()` is
  `queue.dequeue()` throwing while not aborted (`:47`). That is the "true crash."
- `drain()` (`:116-119`) polls `inFlight` with an **unsignalled** sleep, so a stuck
  handler hangs `stop()` indefinitely — accepted (see Non-goals; matches the issue's
  "no force-abort" stance).
- The core `Queue` contract (`packages/core/src/types.ts:550`) has no
  `ensureSchema`. `PgmqQueue.ensureSchema()` **already exists** but is **required**
  and **never called by `nagi()`** (`packages/pgmq/src/pgmq-queue.ts:26,58-65`);
  `pgmq.create()` is idempotent. `PgmqQueueOpts.db` is `Kysely<unknown>` (`:18`).
- `InMemoryQueue` (`packages/core/src/memory.ts:586`) implements `Queue` without
  `ensureSchema` — the ideal "adapter without the hook" test fixture.
- `test-helpers.ts:158-175` already implements `startWorker() → { stop }` (internal
  controller + held loop promise + abort-and-await). **`nagi.run` is the promotion
  of this proven fixture into the public, logger-aware SDK surface.**
- Node engine floor is `>=22` (`package.json` engines, `mise.toml`) — `AbortSignal.any` is available.

## Decisions taken (2026-05-21)

> These are my recommended calls with reasoning. Items tagged **[GRILL]** are the
> ones I want your explicit sign-off on before implementing; the rest I'll treat as
> accepted unless you veto.

1. **Name: `nagi.run`, attached as a static via `Object.assign`.**
   `export const nagi = Object.assign(nagiImpl, { run })`, where `nagiImpl` keeps the
   name `nagi` (for the existing `"... passed to nagi()"` error messages). `nagi`
   stays callable AND gains `.run`. Rationale: Graphile Worker's top-level entry is
   literally `run()`; `nagi.start()` would collide with `wf.start(flow, input)`;
   `nagi.spawn()` is unidiomatic. (Resolved N1, 2026-05-21: `nagi.run`.)

2. **`nagi.run` config = `NagiConfig` + lifecycle extras.**
   ```ts
   export interface NagiRunConfig extends NagiConfig {
     readonly worker?: WorkerConfig;   // concurrency, pollIntervalMs — forwarded to wf.worker()
     readonly signal?: AbortSignal;    // optional external shutdown signal
   }
   ```

3. **Handle shape = `{ wf, stop }`. (Resolved N2, 2026-05-21: minimal; `done` deferred.)**
   ```ts
   export interface RuntimeHandle {
     readonly wf: Wf;
     stop(): Promise<void>;   // abort internal controller + await loop; idempotent; resolves cleanly even after a crash
   }
   ```
   The loop promise is held **privately** by `stop()` (which awaits it); it is not
   exposed. A crash is observable only via `logger.error` (decision 4). `done` (the
   Graphile/Temporal/pg-boss lifecycle promise) was considered and **deferred** — it
   is non-breaking to add later if a supervisor ever needs programmatic crash
   detection, and "complexity must pay for itself" until then.

4. **Graceful-vs-crash is owned once, inside `nagi.run`.**
   ```ts
   const done = worker.run();
   done.catch((err) => {
     if (signal.aborted) return;                       // graceful — swallow
     config.logger?.error("nagi.run: worker exited unexpectedly", { error: String(err) });
   });
   ```
   The internal `.catch` guarantees the rejection is always handled (no
   `unhandledRejection`) even if the consumer ignores `done`; the original `done`
   still rejects for a consumer who *does* await it. Graceful shutdown logs nothing
   at `error`/`warn` (decision 8).

5. **External signal merged via `AbortSignal.any`.**
   ```ts
   const internal = new AbortController();
   const signal = config.signal
     ? AbortSignal.any([internal.signal, config.signal])
     : internal.signal;
   const worker = wf.worker({ ...config.worker, signal });
   ```
   Aborting either source drains the worker. `stop()` aborts `internal`. One
   composite per `nagi.run` call (runtime-lifetime), so the `AbortSignal.any`
   listener-leak class of bugs doesn't apply on Node ≥22. (The research doc's
   conservative alternative — own the controller + a single `{ once: true }`
   forwarding listener — has identical *observable* behavior; not grilling an
   invisible impl detail.)

6. **`stop()` is idempotent by memoizing the shutdown promise.**
   ```ts
   let stopping: Promise<void> | undefined;
   const stop = () => (stopping ??= (async () => {
     internal.abort();             // idempotent at the AbortController level too
     try { await done; } catch { /* graceful, or crash already logged in decision 4 */ }
   })());
   ```
   Second/concurrent `stop()` calls return the same promise — no double-abort, no
   double-log, never throws.

7. **`ensureSchema?()` is optional on the core `Queue` contract; `nagi()` invokes it
   once, eagerly, fail-fast. (Resolved N3, 2026-05-21.)**
   `types.ts:550` gains `ensureSchema?(): Promise<void>`. `nagi()` calls
   `await config.queue.ensureSchema?.()` early (right after `const clock = …`,
   before the flow loop). Adapters without the hook (e.g. `InMemoryQueue`) are
   unaffected. **Eager** (not lazy-on-first-enqueue) so a misconfigured queue fails
   at boot, not hours later at first dispatch. **Fail-fast** (a rejection rejects
   `nagi()`). Best-effort was rejected: swallowing the error re-introduces the silent
   enqueue-before-schema failure this change exists to kill. No opt-out knob in v1; if
   a no-`CREATE`-privilege deployment ever needs one, it belongs on the **pgmq
   adapter** (`pgmqQueue({ ensureSchema: false })` → no hook → `nagi()` skips it),
   non-breaking to add then.

8. **Graceful shutdown is silent at `error`/`warn`.** Only a true crash (decision 4)
   logs `error`. `stop()`, external abort, and an already-aborted signal log nothing
   at `error`/`warn`. An optional `logger.debug("nagi.run: worker stopped")` is fine.

9. **No auto SIGTERM handler.** `nagi.run` does not install process signal handlers
   (surprising behavior from a library). The handle composes with the caller's:
   `process.once("SIGTERM", () => void handle.stop())`. Documented, not done for you.

10. **`pgmqQueue<DB = unknown>` generic.**
    ```ts
    export interface PgmqQueueOpts<DB = unknown> { readonly db: Kysely<DB>; /* …unchanged… */ }
    export function pgmqQueue<DB = unknown>(opts: PgmqQueueOpts<DB>): PgmqQueue { /* … */ }
    ```
    `DB = unknown` default keeps every existing callsite compiling untouched.
    `buildQueue` keeps its internal `Kysely<unknown>` erasure cast — runtime is
    byte-identical. Pure typing change.

11. **`use-after-stop` is a non-case — no guard, no hazard-doc. (Resolved N4, 2026-05-21.)**
    Verified: `wf.start()` → `advance()` → `queue.enqueue` is decoupled from the
    worker, which polls the durable queue independently (`runtime.ts:356`,
    `worker.ts:47`). So after `stop()`, `wf` is simply "a durable producer with no
    co-located consumer" — nagi's canonical "produce here, drain elsewhere" topology.
    A post-stop `wf.start()` durably enqueues and is drained by whatever worker next
    polls. A guard would be *wrong* — it would reject the legitimate distributed
    pattern. One-line doc only: `nagi.run` co-locates exactly one worker; `stop()`
    removes it; `wf` remains a normal producer.

## Proposed shape

### Change 1 — `packages/core/src/runtime.ts`

```ts
export interface NagiRunConfig extends NagiConfig {
  readonly worker?: WorkerConfig;
  readonly signal?: AbortSignal;
}

export interface RuntimeHandle {
  readonly wf: Wf;
  stop(): Promise<void>;
}

async function nagi(config: NagiConfig): Promise<Wf> { /* unchanged + decision 7 */ }

async function run(config: NagiRunConfig): Promise<RuntimeHandle> {
  const wf = await nagi(config);
  const internal = new AbortController();
  const signal = config.signal
    ? AbortSignal.any([internal.signal, config.signal])
    : internal.signal;
  const worker = wf.worker({ ...config.worker, signal });

  const done = worker.run();
  done.catch((err) => {
    if (signal.aborted) return;
    config.logger?.error("nagi.run: worker exited unexpectedly", { error: String(err) });
  });

  let stopping: Promise<void> | undefined;
  const stop = () =>
    (stopping ??= (async () => {
      internal.abort();
      try { await done; } catch { /* graceful or already-logged crash */ }
    })());

  return { wf, stop };   // loop promise (done) stays private — held by stop()
}

export const nagi = Object.assign(_nagiImpl, { run });
```

### Change 2 — `packages/core/src/types.ts` + `runtime.ts`

```ts
// types.ts — Queue contract gains one optional method
export interface Queue {
  // …enqueue / dequeue / ack / nack / extend (unchanged)…
  ensureSchema?(): Promise<void>;
}

// runtime.ts — early in nagi(), before the flow loop:
await config.queue.ensureSchema?.();
```

`PgmqQueue` already satisfies this (its `ensureSchema` is already implemented; this
just makes the core contract aware of it and calls it automatically).

### Change 3 — `packages/pgmq/src/pgmq-queue.ts`

```ts
// before
export interface PgmqQueueOpts { readonly db: Kysely<unknown>; /* … */ }
export function pgmqQueue(opts: PgmqQueueOpts): PgmqQueue;

// after
export interface PgmqQueueOpts<DB = unknown> { readonly db: Kysely<DB>; /* … */ }
export function pgmqQueue<DB = unknown>(opts: PgmqQueueOpts<DB>): PgmqQueue;
```

## Unrepresentable-states analysis

| Invalid state today (prevented only by discipline) | After this RFC |
| --- | --- |
| Forget `queue.ensureSchema()` → `flow.started` is written, then `enqueue` throws on the missing `pgmq.q_nagi` table → run is stuck `running` forever, no message in flight | `nagi()` calls `ensureSchema?()` at construction; a queue whose schema can't be provisioned fails **boot**, before any run starts. The enqueue-before-schema window is closed by construction (decision 7) |
| The consumer's `.catch` logs a graceful `AbortError` as a crash, or logs a crash without awaiting the loop (leaked in-flight handler) | The graceful/crash branch is constructed once inside `nagi.run` (decision 4): `signal.aborted ⇒ swallow`, else `⇒ logger.error`. A consumer cannot mislabel it |
| `worker` + `abort` + `loop` held as three refs that can desync (abort without await; await without abort) | One `RuntimeHandle`; `stop()` aborts **and** awaits in one idempotent call. You cannot abort-without-draining |
| `db as unknown as Kysely<unknown>` — an unsafe cast that erases the schema type at every callsite | `pgmqQueue<DB>` accepts `Kysely<DB>` directly; the cast is structurally unnecessary (decision 10) |

**Still representable, accepted as invariant (not worth a type):**

- A second `nagi.run(sameConfig)` spawns a second independent worker (double queue
  consumption). **Not** prevented — it is the documented way to get two workers (a
  non-goal of single-handle orchestration). A "started" flag would forbid the
  intended use. Rejected.
- `wf.start()` after `stop()` enqueues work no co-located worker drains. **Not an
  invalid state** — it's the durable "produce here, drain elsewhere" topology;
  another worker (or a fresh `nagi.run`) drains it. No guard, no special doc
  (decision 11).

## Outbox / crash-recovery review

Per house convention, recording the durability review even though this RFC does not
touch the durable path:

- **The dispatcher's enqueue/persist ordering is unchanged.** `advance()`'s
  `dispatch` arm (`dispatch.ts:546-548`) still does `recordSkips` then a sequential
  `queue.enqueue` per runnable step — **non-atomic**, recovery by re-derivation on
  restart (not a transactional outbox). This RFC adds nothing to that path and
  neither improves nor regresses it.
- **`ensureSchema` is not part of the outbox.** It is a one-shot, idempotent DDL
  provisioning step (`pgmq.create` is idempotent), run once at construction —
  orthogonal to the per-run enqueue-vs-fact-write ordering. The
  enqueue-before-schema bug it fixes is a *missing-precondition* bug (the table
  didn't exist), **not** an atomicity bug. No transactional coupling is introduced
  or needed.
- **`nagi.run` changes no durability semantics.** It is an in-process lifecycle
  wrapper over `worker.run()`. On crash/restart, the durable fact log plus
  re-derivation recover exactly as before; the held `done` promise and the internal
  controller are process-local and vanish with the process.

## Behavior preservation & testing

The legacy path (`nagi()` + `wf.worker({signal})` + `worker.run()`) keeps working —
existing `worker`/runtime/`dispatch` tests must pass **unchanged**. New coverage
(full enumeration in the test-spec; key files below):

- **`packages/core/src/runtime-run.test.ts`** (new) — `nagi.run` lifecycle: returns
  `{ wf, stop }`; `wf` processes work via the internal worker with no manual
  drain; `stop()` awaits an in-flight handler before resolving; `stop()` idempotent
  (twice + concurrent, no throw, no double-log); graceful `stop()` never calls
  `logger.error`; a `dequeue`-throwing crash calls `logger.error` exactly once;
  external-signal abort triggers the same graceful path; already-aborted signal
  yields an immediately-stopped runtime; legacy path unchanged.
- **`packages/core/src/queue-bootstrap.test.ts`** (new, or folded into runtime
  tests) — `nagi()` calls `ensureSchema` exactly once at construction, never
  per-enqueue/per-worker; an adapter without the hook constructs and runs; a
  rejecting `ensureSchema` fails `nagi()` (fail-fast — N3).
- **`packages/pgmq/src/types.test-d.ts`** (extend) — `Kysely<DB>` accepted with no
  cast, `DB` inferred from `db`, result assignable to `Queue`; update the existing
  `:26` assertion (was `Kysely<unknown>`).
- **New test fixture**: a `Queue` whose `dequeue()` throws, to exercise the crash
  branch (the in-memory queue never throws there today).

## Alternatives considered

- **`{ wf, stop, done }` (add `done`)** — exposes the loop promise so a supervisor
  can `await handle.done` instead of subscribing to the logger. **Deferred (N2)** —
  non-breaking to add later; not needed by the reference consumer in v1.
- **Free `runRuntime(config)` export** instead of a `nagi.run` static — avoids
  `Object.assign`. Rejected: `nagi.run` reads better and `Object.assign` is trivial;
  the bare `runRuntime` is the fallback if you dislike mutating the function object.
- **Lazy `ensureSchema` on first enqueue** — defers config errors to first dispatch
  (hours later in low-volume systems). Rejected in favor of eager fail-fast (relates
  to **N3**).
- **`AbortSignal.any` vs manual `{ once: true }` listener** — identical observable
  behavior; `any()` is cleaner on Node ≥22. Chosen `any()`.
- **`nagi.start()` / `nagi.spawn()`** — `start` collides with `wf.start(flow)`;
  `spawn` is unidiomatic for "run a worker." Rejected (relates to **N1**).

## Resolved questions (2026-05-21, Jay — via grill)

All four open decisions are resolved; the decisions log above is final.

- **N1 — Name.** ✅ `nagi.run` (static via `Object.assign`).
- **N2 — Handle shape.** ✅ `{ wf, stop }` (minimal). The loop promise stays private;
  `done` deferred (non-breaking to add later). Crash → `logger.error` only.
- **N3 — `ensureSchema` failure policy.** ✅ Eager + fail-fast, no opt-out knob.
  Best-effort rejected (reintroduces the silent bug). Future opt-out, if ever needed,
  lives on the pgmq adapter (`pgmqQueue({ ensureSchema: false })`).
- **N4 — `use-after-stop`.** ✅ Non-case: no guard, no hazard-doc. It's the durable
  "produce here, drain elsewhere" topology; a guard would be wrong.

## Non-goals (from the issue)

- Multi-worker orchestration inside `nagi.run` (one worker per call; call it twice
  for two).
- Graceful drain richer than "stop pulling new jobs, await in-flight" — no
  `stop({ timeout })` force-abort in v1 (the `drain()` hang is the caller's deadline
  to own, via the external signal).
- Generalizing `ensureSchema` into a versioned migration framework — it is a
  one-shot idempotent bootstrap.

## Implementation order

1. **Change 3** (pure typing, isolated): `pgmqQueue<DB>` + `PgmqQueueOpts<DB>`;
   update `types.test-d.ts:26`. Lowest risk, no runtime delta.
2. **Change 2**: `ensureSchema?()` on `Queue` (`types.ts:550`) + the one-line
   `nagi()` call; tests with a spy queue + the hook-less `InMemoryQueue`.
3. **Change 1**: `NagiRunConfig` / `RuntimeHandle` + `run()` + the `Object.assign`
   export; `runtime-run.test.ts` + the `dequeue`-throwing fixture.
4. **Exports** (`packages/core/src/index.ts`: `NagiRunConfig`, `RuntimeHandle`),
   **changeset** (`patch` for `@nagi-js/core` + `@nagi-js/pgmq`), **handoff doc**.

README and public docs are Jay's to write.
