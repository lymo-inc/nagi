---
"@nagi-js/core": patch
---

`flow()`'s `concurrency` config gains a bare-string shorthand and an optional
`mode`. `concurrency: "videoId"` is now equivalent to
`{ keyFn: (i) => i.videoId, mode: "cancel-in-progress" }`, and `mode` may be
omitted on the object form (defaults to `"cancel-in-progress"`). The existing
`{ keyFn, mode }` form is unchanged.

The string shorthand is typed against the string-valued keys of the flow's
input (`StringKeyOf<Input>`), so a misspelled or non-string key is a compile
error; composite / computed keys continue to use `keyFn`. Internally the config
normalizes to a canonical `{ keyFn, mode }` at the builder boundary, so the
runtime cancellation path (`store.tryStartRun`) is untouched and dedupe / crash
semantics are identical. See `docs/rfcs/0012-shorthand-concurrency-config.md`.
