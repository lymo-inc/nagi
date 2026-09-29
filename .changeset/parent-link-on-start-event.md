---
"@nagi-js/core": patch
---

`FlowStartEvent.parent` is now a `ParentLink` (`{ runId, stepId }`), and the `ParentRef` type is removed. Its extra `attempt` field existed only for the removed `@nagi-js/otel` span keys and was never stored with the child run.
