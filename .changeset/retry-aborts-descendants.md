---
"@nagi-js/core": patch
---

Fix `replay({ from })` on a live run: it previously only aborted `from`
itself if it was running, leaving other running descendants in the reset set
free to keep executing and settle with stale inputs once they returned, and
leaving a parked subflow step's old child free to keep running and — via its
terminal fact — settle the *new* generation's parent step with its stale
output.

`replay({ from })` now aborts every member of the reset set that is `running`
(under one shared 30s deadline for the whole batch, not one per step) and
cancels the child of every reset step that was `awaitingChild`, after writing
the resets and before re-dispatching. `propagateToParent` also now checks
that the waking child is the parent step's *current* generation before
settling it, guarding against any stale child wake, not only ones `replay`
leaves behind.
