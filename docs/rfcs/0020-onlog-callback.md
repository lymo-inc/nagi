# RFC 0020 — `onLog` callback: replace the `Logger` interface with a structured-record sink

- **Status:** Decisions log COMPLETE (O1–O4 resolved 2026-05-21) — **awaiting Jay's final approval before implementation**
- **Author:** Claude (paired with @jay)
- **Created:** 2026-05-21 (JST)
- **Tracking:** GitHub issue #19 (RFC doc number ≠ issue number is coincidental here; they happen to match)
- **Decisions log:** authoritative — see "Firm decisions", "Open decisions (resolved)", and "Resolved questions". Implementation is **blocked on Jay's approval of this log.**

## Summary

Replace nagi's four-method `Logger` interface in `NagiConfig` with a single
structured-record callback:

```ts
type LogLevel = "debug" | "info" | "warn" | "error";
interface LogEntry {
  readonly level: LogLevel;
  readonly msg: string;
  readonly attrs?: Record<string, unknown>;
}
interface NagiConfig {
  // ...existing fields...
  readonly onLog?: (entry: LogEntry) => void; // replaces `logger?: Logger`
}
```

nagi produces a structured record per diagnostic; the **host** decides how to
render it (to pino, bunyan, stderr JSON, a no-op, …). Omitting `onLog` makes
nagi **completely silent** — no console fallback, no record allocation.

The same change forces a decision about the **in-step logger** (`ctx.logger`
handed to step handlers) — today a separate, non-silent surface — which is the
real structural work and the subject of the open decisions below.

## Motivation

`nagi({ ..., logger })` accepts `Logger` (`types.ts:81-86`), whose methods are
**message-first** `(message, attrs?)`. Production Node loggers (pino, bunyan,
fastify) are **object-first** `(obj?, msg, ...args)`. Every consumer with a
pino-style logger writes the same ~7-line adapter (the issue quotes lymo's
`adaptLogger`). The deeper smell: `Logger` conflates *what nagi wants to say* (a
record: `level | msg | attrs`) with *how the host formats it* (pino vs console).
A single `onLog(entry)` separates the two; the call site collapses to one line:

```ts
onLog: ({ level, msg, attrs }) => logger[level](attrs ?? {}, msg),
```

## External-design survey (condensed)

| System | Shape | Record / args | Level repr | Default when absent | In-step logger? |
| --- | --- | --- | --- | --- | --- |
| **pino** | method interface | object-first `info(obj?, msg)` | label↔number, threshold | writes **stdout** | n/a (it *is* the logger) |
| **OTel `diag`** | method interface, globally set | `(message, ...args)` | `DiagLogLevel` enum | **silent no-op** until `setLogger` | no |
| **OTel Logs SDK** | record emit (`LogRecord`) | `{severityNumber, body, attributes, …}` | `severityNumber` field | dropped (no exporter) | no |
| **Prisma** | event-emitter `$on(level, cb)` | `LogEvent{timestamp,message,target}` | event name | **silent** | no |
| **Drizzle** | single-method iface / bool | `logQuery(query, params)` | none | **silent** | no |
| **Temporal** | method iface + Sinks | message-first `(msg, meta?)` | `LogLevel` strings | `DefaultLogger` → **stderr** | **yes**, method-shaped |
| **Inngest** | method iface, passed to handlers | object-first `(meta?, msg)` | methods | `ConsoleLogger` → **console** | **yes — `ctx.logger`**, replay-wrapped |
| **Effect** | **single callback** `Logger.make((opts)=>…)` | `{logLevel, message, date, fiberId, …}` | **`logLevel` field** | console | yes (same `Effect.log*`) |
| **Kysely** | **single callback** / `LogLevel[]` | `(event: LogEvent)` w/ `level` field | **`level` field** | nothing unless set | no |
| **Vercel AI SDK** | OTel + lifecycle callbacks; catches integration throws | `onChunk`, `onStepFinish` | OTel | telemetry opt-in | callbacks |

**Takeaways that shaped the decisions:** (1) the single-`onLog(entry)` callback
with **level-as-a-field** is idiomatic and is exactly what the two systems
closest to nagi's "emit-and-route" model do — **Effect** and **Kysely**;
nagi's `LogEntry` is structurally identical to Kysely's `LogEvent`. (2) Method
interfaces dominate the *different* "host hands the library a logger it calls
many times with bound child context" case (pino/Temporal/Inngest) — which is
precisely what the **in-step** logger is, hence O1 below. (3) Object-first vs
message-first has **no winning method signature**; a record callback removes the
argument-order war entirely. (4) **Silent-by-default** is the convention for an
*embedded* library's opt-in diagnostics (Prisma/Drizzle/Kysely/OTel `diag`);
stderr-fallback is reserved for top-level processes (pino/Temporal worker).
(5) A throwing sink must be **isolated** (AI SDK catches integration throws);
treat `onLog` as synchronous fire-and-forget.

## Codebase grounding (verified)

- `Logger` interface: `types.ts:81-86`. **Publicly exported** via
  `export type * from "./types"` (`index.ts:37`).
- `NagiConfig.logger?: Logger`: `runtime.ts:68`. Threaded as `DispatchDeps.logger`
  (`dispatch.ts:54`) and `OperatorDeps.logger` (`runtime.ts:897`), assembled at
  `runtime.ts:466-478` / `603-609`.
- **9 internal diagnostic call sites**, all guarded with `?.` (so config-level
  logging is *already* silent when absent):

  | # | Site | Level | Message |
  | - | --- | --- | --- |
  | 1 | `runtime.ts:414` | info | `nagi: cancel skipped — run already terminal` |
  | 2 | `runtime.ts:543` | info | `nagi: signal arrived after step resolved` |
  | 3 | `runtime.ts:822` | error | `nagi.run: worker exited unexpectedly` |
  | 4 | `runtime.ts:945` | info | `nagi: operator.skip noop — step already terminal` |
  | 5 | `dispatch.ts:74` | error | `nagi hook "<name>" threw — swallowed` (spreads `stack` only if defined) |
  | 6 | `dispatch.ts:98` | warn | `dispatch: step "<id>" not in flow "<flow>"; ack and skip` (**no attrs**) |
  | 7 | `dispatch.ts:800` | info | `nagi: subflow wake skipped — parent run already terminal` |
  | 8 | `dispatch.ts:813` | info | `nagi: subflow wake skipped — parent step not running` |
  | 9 | `worker.ts:107` | error | `worker.dispatch threw uncaught` |

  There is **no `debug`-level diagnostic** anywhere (the only `.debug` is inside
  `consoleLogger` itself).
- **In-step `ctx.logger: Logger`** (non-optional): `types.ts:96`. Built in
  `makeStepCtx` as `logger: args.logger ?? consoleLogger()` (`dispatch.ts:701`),
  where `consoleLogger()` writes to `console.*` (`dispatch.ts:707-714`). **This is
  the one non-silent path today**, and it does **no** run/step context
  enrichment — when `config.logger` is supplied, `ctx.logger` is the *same
  instance*, not a child.
- No adapter (`otel`/`pgmq`/`postgres`) references `Logger`. `@nagi-js/otel`
  uses `console.error` directly (`hooks.ts:56-61`).
- Tests touching the logger: `dispatch.test.ts:417,529`,
  `signal-multi-name.test.ts:14-31,168`, `runtime-run.test.ts:16-18,119-175`,
  `test-helpers.ts:89,134`.
- Pattern to mirror for boot-time validation: optional capability `Queue.ensureSchema?`
  (`types.ts:588-591`, called `runtime.ts:206` via `?.()`), errors are
  `NagiRuntimeError` (`runtime.ts:128-133`) / `NagiValidationError`
  (`runtime.ts:119-126`). Inline `??` for defaults.

## Firm decisions (my confident calls — flag any to revise)

> These align with the issue, the survey, and the design memories
> (*unrepresentable invalid states*, *optionality only at the boundary*,
> *complexity must pay for itself*). Treating as settled unless you push back.

- **D1 — Config boundary becomes `onLog(entry: LogEntry)`.** `NagiConfig.logger`
  is removed/renamed to `onLog`. All 9 internal sites become
  `deps.onLog?.({ level, msg, attrs })`. (Survey: Effect/Kysely precedent; kills
  the object-first/message-first adapter tax.)

- **D2 — `LogEntry = { readonly level: LogLevel; readonly msg: string; readonly attrs?: Record<string, unknown> }`**,
  `LogLevel = "debug" | "info" | "warn" | "error"`. Level is a *field*, not a
  method name (survey-canonical). All fields `readonly`; the only optional is
  `attrs?`, living exactly at the host boundary (memory:
  *optionality-at-the-boundary*). **No `time`/timestamp field** — the host's
  logger stamps its own time; imposing a clock on the record is needless coupling
  (revisit if you disagree).

- **D3 — Completely silent by default; the `consoleLogger()` fallback is deleted.**
  Omitting `onLog` ⇒ no console, no stderr, and **no `LogEntry` is even
  constructed** (guard `if (!onLog) return;` before building the record — free,
  and covers the hot path). Config-level is already silent; this additionally
  removes `consoleLogger()` (`dispatch.ts:701-714`) so the in-step surface is
  silent too. (Survey: embedded-library convention.)

- **D4 — `attrs` is never coerced to `{}`.** "No attrs" is represented as
  `attrs === undefined` (anchor: site #6 emits no attrs; site #5 spreads `stack`
  only when present). One canonical representation of emptiness.

- **D5 — A throwing/slow sink cannot corrupt the engine.** Every `onLog` call is
  wrapped `try { onLog(entry); } catch { /* swallow */ }`, mirroring the existing
  "hook threw — swallowed" pattern (`dispatch.ts:74`). `onLog` is **synchronous
  fire-and-forget**: a returned promise is ignored (never awaited), so a slow
  sink can't be *awaited* into the hot path. (Heavy sinks must self-buffer —
  documented invariant.) *(Flagged: see O4 if you'd rather propagate.)*

- **D6 — Pure observability-surface change; zero durability impact.** Logs never
  touched `store`, the fact log, the canonical flow hash, or replay
  reconstruction. `StepKind`, scheduling, `needs`, and replay are untouched. (See
  Outbox review.)

- **D7 — Changeset is `patch`, authored into the active prerelease.** New public
  API would normally be `minor`, but in `0.1.x` `minor` burns a release name; the
  repo is mid-rc (`.changeset/pre.json` present). Ship as `patch`. (Memory:
  *changeset-bump-type*.)

## Open decisions (resolved 2026-05-21 — all chose the recommendation)

> ✅ **O1 → keep method-shaped `ctx.logger`** · ✅ **O2 → auto-enrich (reserved
> keys)** · ✅ **O3 → hard break** · ✅ **O4 → swallow.** Full rationale in
> "Resolved questions" at the bottom. Originals retained below for the record.

- **O1 (root fork) — In-step `ctx.logger` surface shape.**
  - **Rec — keep it method-shaped** (`ctx.logger.{debug,info,warn,error}(msg, attrs?)`,
    i.e. the `Logger` interface *survives* as the in-step type, its methods
    internally building a `LogEntry` and calling the host `onLog`). Survey:
    Temporal/Inngest both hand handlers a method-shaped logger because handler
    code is logging-dense and methods read better (`ctx.logger.info("retrying", { attempt })`).
  - **Alt — record callback** `ctx.log(entry: LogEntry)`, deleting the `Logger`
    interface entirely for maximum symmetry with the config boundary and one
    fewer exported type.
  - *Consequence:* this decides whether `Logger` stays in the public API at all.

- **O2 — Does the in-step logger funnel into `onLog`, and does it auto-enrich?**
  - **Funnel:** in-step logs route through the *same* host `onLog` sink (single
    choke point) — recommended; otherwise the two surfaces diverge.
  - **Enrich (NEW behavior):** the runtime attaches `runId`/`stepId`/`attempt`
    to `entry.attrs` before forwarding. **Today there is no enrichment** — the
    issue's "the runtime already adds runId/stepId" is *false* (verified:
    `makeStepCtx` passes the bare instance / console). **Rec — funnel + enrich**
    (auto-attribution is the entire reason to have a distinct in-step logger),
    with caller-supplied `attrs` taking precedence on key collision.
  - *Coupled to O1.* If O1=record-callback, "enrich" still applies to `ctx.log`.

- **O3 — Back-compat strategy.**
  - **Rec — hard break:** remove `logger`, add `onLog`. nagi is pre-1.0 and
    mid-rc; cleanup over a compat shim (memory: *complexity must pay for itself*).
  - **Alt — soft break:** accept `logger?` for one cycle, bridge it to `onLog`,
    deprecate, remove next major. Adds a bridge + deprecation path + a window
    where both are representable.

- **O4 — Throw isolation: swallow vs propagate (confirms D5).**
  - **Rec — swallow** (a logging bug must never fail a workflow step; matches the
    hook-swallow precedent).
  - **Alt — propagate** the sink's throw (host opts into "my logger is critical"),
    which couples workflow success to logging success.

## Proposed shape

### `types.ts`

```ts
export type LogLevel = "debug" | "info" | "warn" | "error";

export interface LogEntry {
  readonly level: LogLevel;
  readonly msg: string;
  readonly attrs?: Record<string, unknown>;
}

// O1=A (method-shaped in-step logger retained):
export interface Logger {
  debug(msg: string, attrs?: Record<string, unknown>): void;
  info(msg: string, attrs?: Record<string, unknown>): void;
  warn(msg: string, attrs?: Record<string, unknown>): void;
  error(msg: string, attrs?: Record<string, unknown>): void;
}
// O1=B alternative: delete `Logger`; StepCtx gains `readonly log: (entry: LogEntry) => void;`

export interface StepCtx<Input = unknown> {
  // ...unchanged fields...
  readonly logger: Logger;       // O1=A   (or `log: (e: LogEntry) => void` for O1=B)
}
```

### `runtime.ts`

```ts
export interface NagiConfig {
  // ...existing fields, with `logger?: Logger` removed...
  readonly onLog?: (entry: LogEntry) => void;
}
```

### Internal emit helper (single choke point — supports D3/D5/O2)

```ts
// constructed once from config.onLog; threaded as deps.emitLog
function makeEmit(onLog?: (e: LogEntry) => void) {
  if (!onLog) return () => {};                    // D3: no alloc, fully silent
  return (e: LogEntry) => { try { onLog(e); } catch { /* D5 swallow */ } };
}
```

The 9 internal sites call `deps.emitLog({ level, msg, attrs })`. The in-step
`ctx.logger` (O1=A) is a small object whose four methods build a `LogEntry`
(merging enriched `runId`/`stepId`/`attempt` per O2) and call the same
`emitLog`. When `onLog` is absent, `emitLog` is the no-op and `ctx.logger`'s
methods do nothing.

## Unrepresentable-states analysis

| Invalid state | Prevented by |
| --- | --- |
| Host depending on a positional arg order (object-first vs message-first) | **Structural:** `onLog` takes one record; there are no positional args. The arg-order war is unrepresentable. |
| Emitting/handling a non-existent level (`trace`, `fatal`) | **Structural:** `level` is the closed `LogLevel` union; anything else is a compile error (preserved from the old interface). |
| nagi writing to console/stderr behind the host's back | **Structural after D3:** the `consoleLogger()` fallback is deleted; the host's `onLog` is the only sink. (*Today this is representable* via the fallback — this RFC removes it.) |
| `attrs` ambiguity (`{}` vs "no attrs") | **D4:** `undefined` is the single representation of "no attrs"; `{}` only when explicitly passed. |
| In-step logs missing run/step correlation | **If O2=enrich:** structural (runtime always attaches). Otherwise representable (handler must pass them by hand). |
| Passing a method-shaped `Logger` where `onLog` is expected | **Structural (compile error):** a 4-method object is not assignable to `(entry: LogEntry) => void`. This is the type-level proof of the break. |

**Still representable, accepted as runtime invariants:** (a) a sink that
*throws* — type-valid, neutralized at runtime by D5; (b) a sink that *blocks
synchronously* — we never `await`, but a CPU-blocking sink still blocks the
caller; documented as "onLog must be cheap; offload heavy work."

## Outbox / crash-recovery / replay review

Logging is the **deliberate inverse of an outbox**: at-most-once, non-durable,
fire-and-forget. The reliability reasoning:

- **`onLog` is not a DB write and is never coupled to a transaction.** It's a
  plain function call, independent of any `tx`/`store.runStep` boundary, so there
  is no "visible only after commit" hazard and nothing to lose if a step's
  transaction rolls back. (Contrast the outbox, which *wants* event+state
  atomicity — here we want the opposite.)
- **Replay is inert for free.** A memoized/completed step's `def.run` is **not
  re-invoked** on normal replay (output is read from the fact log), so its
  `ctx.logger` calls don't re-fire — no Inngest-style replay-wrapper needed.
  Internal diagnostics fire only during *live* dispatch, not during state
  reconstruction.
- **Intentional re-execution (`replay-from`, retries) re-logs, by design.** When
  a handler genuinely re-runs, it re-emits — that's a real execution, not a
  memoized replay, and "at-most-once *per attempt*" is the correct contract.
- **Crash mid-step:** in-flight entries were already delivered (fire-and-forget);
  on restart the step re-executes and re-logs. No stranded consumer, because
  there is no durable log channel to be inconsistent with.

## Behavior preservation & testing

Acceptance criteria from the test-spec phase (~58 runtime `it` + ~22
type-level), grouped: record shape; per-level routing (anchored to the 9 real
sites); `attrs` passthrough vs `undefined` (site #6) vs conditional keys (site
#5); silent-by-default (incl. the `consoleLogger`-removal regression tests);
single-channel/no-hidden-writes; exactly-once ordering; throw-isolation (O4);
in-step surface (O1=A vs O1=B groups); enrichment (O2); back-compat (O3, only if
soft-break). Type-level: `onLog` is exactly `(e: LogEntry) => void`; `LogEntry`
readonly + `attrs?`; `LogLevel` exhaustive; a `Logger` object is **not**
assignable to `onLog` (the break); `attrs` values are `unknown` not `any`. Files:
`packages/core/src/tests/onLog.test.ts` and `onLog.test-d.ts`. Existing logger
tests to migrate: `runtime-run.test.ts` (`spyLogger`→`spyOnLog`),
`signal-multi-name.test.ts` (`memoryLogger`), `dispatch.test.ts`,
`test-helpers.ts` (`HarnessOpts.logger`→`onLog`).

## Alternatives considered

- **Just match pino's signature** (`(obj?, msg, ...args)`): removes pino users'
  adapter but bakes in "consumers own a Logger-shaped object" and still forces an
  adapter on message-first hosts. Rejected — `onLog` is shorter at the call site
  *and* format-agnostic.
- **Drop logging entirely, route via `FlowEvent`/`StepEvent`:** no event exists
  for internal diagnostics ("worker claimed message X", "schema migrated"). These
  are operational signals, not domain events. Rejected (per the issue).
- **In-level filtering knob inside nagi:** non-goal; the host filters in `onLog`
  (`if (e.level === "debug") return`). Cheap, full control at the host.
- **Keep `consoleLogger()` fallback** (no D3): convenient zero-config logs, but
  makes nagi write behind the host's back and breaks "silent by default."
  Rejected.

## Resolved questions (filled during grilling, 2026-05-21)

All four open decisions resolved with Jay; each landed on the recommended branch.

- **O1 → Keep the method-shaped in-step logger.** `ctx.logger` stays
  `Logger` (`.debug/.info/.warn/.error(msg, attrs?)`); the `Logger` interface
  **remains exported**. Its methods internally build a `LogEntry` and call the
  shared emit choke point. Rationale: handler code is logging-dense and methods
  read better; matches Temporal/Inngest. The config boundary (`onLog`) and the
  in-step boundary (`ctx.logger`) are deliberately *different shapes* for their
  *different jobs* (route-once vs. call-often-with-bound-context).

- **O2 → Auto-enrich with reserved correlation keys.** Every `ctx.logger.*`
  call merges `{ runId, stepId, attempt }` into `entry.attrs` before forwarding.
  **Runtime keys are authoritative** — a handler-supplied `attrs.runId` cannot
  clobber the real one (runtime wins on collision). This is *new* behavior
  (today there is no enrichment); it is the reason `ctx.logger` exists as a
  distinct surface rather than an alias for `onLog`. Internal diagnostics that
  already pass `runId`/`stepId` in their `attrs` (sites #1–4, #7–8) are
  unaffected — enrichment applies to the *in-step* logger, not the engine-level
  emit.

- **O3 → Hard break.** `NagiConfig.logger` is **removed** and replaced by
  `onLog`; no bridge, no deprecation cycle. Pre-1.0 + mid-rc makes this cheap,
  and a compat shim would reintroduce a representable-but-wrong state
  (both fields set). Consumers migrate in one line. lymo's `adaptLogger` shim is
  deleted in the same change.

- **O4 → Swallow.** Confirms **D5**: every `onLog` invocation is wrapped in
  `try/catch` and a throw is swallowed; a returned promise is ignored (never
  awaited). A logging bug must never fail or retry a workflow step. Mirrors the
  existing "hook threw — swallowed" precedent.

### Consolidated final API

```ts
export type LogLevel = "debug" | "info" | "warn" | "error";
export interface LogEntry {
  readonly level: LogLevel;
  readonly msg: string;
  readonly attrs?: Record<string, unknown>;
}
export interface Logger {                       // retained for ctx.logger (O1)
  debug(msg: string, attrs?: Record<string, unknown>): void;
  info(msg: string, attrs?: Record<string, unknown>): void;
  warn(msg: string, attrs?: Record<string, unknown>): void;
  error(msg: string, attrs?: Record<string, unknown>): void;
}
export interface NagiConfig {
  // ...existing fields; `logger?: Logger` REMOVED (O3)...
  readonly onLog?: (entry: LogEntry) => void;   // D1
}
export interface StepCtx<Input = unknown> {
  // ...unchanged fields...
  readonly logger: Logger;                       // funnels → onLog, enriched (O2)
}
```

**Implementation is unblocked once Jay approves this completed log.**
