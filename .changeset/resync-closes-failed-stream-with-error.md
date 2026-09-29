---
"@nagi-js/core": patch
"@nagi-js/postgres": patch
---

After a `listener` reconnect, the Postgres store ends each stream whose close frame was lost the way the frame would have: with a final `{ kind: "error" }` event when the step failed, not a plain end. Late subscribers are unchanged and still get an empty stream.

For adapter authors: `isStreamOver(state, stepId)` is replaced by `streamEndOf(state, stepId)`, which returns `"open" | "ok" | "error"`. `isStreamOver(...)` is `streamEndOf(...) !== "open"`.
