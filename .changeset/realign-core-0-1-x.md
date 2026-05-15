---
"@nagi-js/core": patch
"@nagi-js/otel": patch
"@nagi-js/pgmq": patch
"@nagi-js/postgres": patch
---

Realign release cohort: republish all four packages on the 0.1.x line.
@nagi-js/core@0.2.0-rc.3 (and the otel/pgmq/postgres rc.3 cohort that
pinned it as a workspace dep) was an unintended minor bump and will be
unpublished from npm. No code changes — this changeset exists to produce
a clean rc.4 cohort with core back on 0.1.x.
