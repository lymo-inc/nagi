---
"@nagi-js/core": patch
---

Internal: every retry/backoff decision now lives in one module (`retry.ts`).
`DEFAULT_RETRY` was defined twice with the same values — once for execution
(`step-exec.ts`) and once for flow hashing (`canonicalize.ts`) — so a change
to one either silently re-hashed every flow (stranding in-flight runs as
snapshot-gone) or silently never took effect at runtime. It is now defined
once and imported by both; the canonical values are pinned by test and are
byte-identical to before, so **no flow hash changes**.

The four backoff curves (step retry, dequeue outage backoff, snapshot-gone
redelivery budget, lease-reap re-dispatch) are named pure functions with one
signature shape, covered by a single table-driven test. Policy resolution
(`handler.retry` → `nagi({ defaultRetry })` → built-in) is `resolveRetry`.

No public API change: `defaultSnapshotGonePolicy` is still exported from the
package root with identical behavior.
