---
"@nagi-js/core": patch
---

`nagi({ codeVersion })` now auto-defaults to a structural fingerprint of the
registered flows when omitted. The audit field on `workflow_run.code_version`
and `flow.started` facts is meaningful by default and shifts only when flow
topology changes — not on every deploy. Explicit `codeVersion: string` is
unchanged and still taken as-is. Exposes `fingerprintFlows(flows)` for
callers who want to compute or compare the value directly. See RFC 0003.

Behavior note: callers who previously omitted `codeVersion` will see
`code_version` flip from `NULL` to a SHA-256 hex string for runs started
after upgrading. Any dashboard filtering on `code_version IS NULL` to detect
un-tagged deploys should be updated.
