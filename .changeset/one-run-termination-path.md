---
"@nagi-js/core": patch
---

Every way a run ends now fires the flow error hooks and wakes a parked parent subflow step the same way:

- Cancel paths now fire `flow.onError` and `onFlowError`, and a handler that throws `NagiCanceledError` wakes its parent right away instead of waiting for the lease reaper.
- A run failed by snapshot-gone handling now fires `onFlowError`.
- A canceled child's parent sees the same canonical error whichever path canceled it; operator aborts now read `was canceled by <actor>: <reason>`.
