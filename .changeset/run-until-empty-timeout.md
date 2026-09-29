---
"@nagi-js/core": patch
---

**Breaking:** `worker.runUntilEmpty({ deadline })` is now `worker.runUntilEmpty({ timeoutMs })`. `timeoutMs` is a duration, measured on the injected clock from the call. `deadline` was an absolute epoch-ms timestamp, but nothing said so: `{ deadline: 10_000 }` meant 1970 and returned `{ processed: 0 }` at once. Replace `{ deadline: Date.now() + n }` with `{ timeoutMs: n }`. Passing `deadline` is now a type error; untyped callers that still pass it get an unbounded drain.
