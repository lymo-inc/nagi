---
"@nagi-js/core": patch
"@nagi-js/postgres": patch
---

Fix: run events and the stream `error` event no longer carry payloads.

With `postgresStore({ listener })` configured (required for `b.streamingTask`
and `wf.watchRun` / `wf.watchRuns`), every fact write called `pg_notify`
**inside the fact's transaction** with a JSON payload embedding the step's or
flow's full `output`, or the full `error` (stack and cause). PostgreSQL
rejects a NOTIFY payload of 8000 bytes or more ("payload string too long"),
which aborted the transaction. A step returning more than ~8 KB — ordinary
for LLM text — could therefore never be recorded as completed; it retried
until it failed. A flow output over ~8 KB could never commit
`flow.completed`, and a terminal `step.failed` with a large serialized error
had the same problem via the stream `err` frame. This happened even when
nobody was watching: the NOTIFY fires whenever a listener is configured.

**Breaking change**: `RunEvent`'s `flow.completed`, `flow.failed`,
`step.completed` and `step.failed` members no longer carry `output` /
`error`; the stream `StreamEvent` `error` member is now `{ kind: "error" }`
with no `error` field. Run events and the stream error event are now
reference-only on every store — identity and status (run, step, attempt,
type), never payloads.

**Migration**: a consumer that needs an output or error calls
`wf.describe(runId)`, which still returns the full payload via
`RunView.output` / `RunView.error` / `StepView.output` / `StepView.error`.
