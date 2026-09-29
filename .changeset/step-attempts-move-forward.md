---
"@nagi-js/core": patch
"@nagi-js/postgres": patch
---

Fix: a step's projected state (`loadRunState`) and its `describe()` view now apply the same attempt rules, so they can no longer disagree.

After a lease reap, the re-dispatched attempt's `step.started` was ignored by the projection: the step kept reading as the dead attempt, so `operator.retry` wrote its abort request for that attempt and the live handler never saw it: the retry waited for the handler to finish on its own, or timed out. A start now supersedes the step whenever its attempt is newer than the one in flight, in both the projection and the read model.

A duplicate `step.started` no longer moves the view's `startedAt`, and a `step.retried` or `step.abort-requested` for an attempt that is not in flight no longer changes the step in either. A test now explores every single-step fact history against both and fails on any disagreement.
