---
"@nagi-js/core": patch
---

`FlowErrorEvent` (flow `onError` and the global `onFlowError`) now carries `status: "failed" | "canceled"` and the run's `input`. A flow's own `onError` gets `input` typed by its input schema. A handler can tell a cancel from a failure without comparing `error.name` to `"NagiCanceledError"`, and act on the run's input without calling `describe()` and parsing it again. The input is read only when an error hook is registered.
