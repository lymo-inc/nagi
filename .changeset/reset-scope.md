---
"@nagi-js/core": patch
---

Non-cascading single-step rerun. `operator.retry(runId, stepId, { actor, scope: "step" })` resets ONLY the named step, leaving completed descendants untouched — the regenerate-one-output shape. `wf.replay(runId, { mode, from, scope })` takes the same control. `scope` defaults to `"cascade"`, so existing callers are unaffected.

Exposed as a control marker on the existing methods rather than a second `rerunStep` method: the two behaviors differ only in which steps the reset covers, and both call sites now resolve that through one `resetSetOf(flow, stepId, scope)` so they cannot drift apart.

Under `"step"` the descendants keep outputs derived from the step's PREVIOUS output. That is the contract, not a bug — callers who need a consistent run want `"cascade"`.

The origin `step.reset` fact now carries `scope` for an isolated rerun (omitted for `"cascade"`, so existing facts keep their shape). Recording the operator's intent beats inferring it: a leaf step has no descendants, so a cascading retry on a leaf is otherwise indistinguishable from an isolated one.
