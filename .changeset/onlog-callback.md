---
"@nagi-js/core": patch
---

Replace the four-method `Logger` interface in `NagiConfig` with a single
structured-record callback `onLog?: (entry: LogEntry) => void` (RFC 0020).

**Breaking:** `NagiConfig.logger` is removed. nagi now produces a structured
record per diagnostic and the host decides how to render it, so the
object-first/message-first adapter every pino/bunyan consumer wrote disappears:

```ts
// before
nagi({ ..., logger: adaptLogger(pino) })
// after — one line, format-agnostic
nagi({ ..., onLog: ({ level, msg, attrs }) => pino[level](attrs ?? {}, msg) })
```

`LogEntry` is `{ readonly level: "debug" | "info" | "warn" | "error"; readonly msg: string; readonly attrs?: Record<string, unknown> }`.

Other behavior changes:

- **Silent by default.** Omitting `onLog` makes nagi completely silent — the
  in-step `consoleLogger` fallback is removed, so nagi never writes to
  `console.*` behind the host's back, and no `LogEntry` is allocated when there
  is no sink.
- **In-step `ctx.logger` stays method-shaped** (`ctx.logger.info(msg, attrs)`)
  and now auto-enriches every entry with `runId` / `stepId` / `attempt`
  (runtime-authoritative: a handler cannot clobber the real ids), funneling into
  the same `onLog` sink.
- **A throwing `onLog` is swallowed** — a logging bug can never fail or retry a
  workflow step.
- `attrs` is `undefined` (never `{}`) when a diagnostic carries none.
