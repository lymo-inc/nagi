---
"@nagi-js/core": minor
"@nagi-js/postgres": minor
---

`wf.queryRuns({ where, latest, limit, cursor })` — discover runs by their
`flow.started.input` without touching nagi's storage schema directly.

```ts
// Most recent run for a video
const { runs } = await wf.queryRuns({
  where: { input: { videoId: "abc-123" } },
  latest: true,
});

// All running deal-analysis runs, paginated
let cursor: string | null = null;
do {
  const page = await wf.queryRuns({
    where: { flowId: "deal-analysis", status: "running" },
    limit: 50,
    ...(cursor !== null ? { cursor } : {}),
  });
  for (const r of page.runs) /* … */
  cursor = page.cursor;
} while (cursor !== null);
```

- `where.input` is JSONB containment — the stored input is a superset of the
  filter object (recursive on nested objects). Same semantics as Postgres
  `jsonb @> jsonb`.
- `where.status` accepts one `RunStatus` or an array.
- `latest: true` returns at most one run and is incompatible with `limit` /
  `cursor` at the type level (discriminated union); the runtime guards it
  too for JS-only callers.
- Results are ordered `(startedAt DESC, runId DESC)`.
- Cursors are opaque base64url; pass back the previous page's `cursor` to
  resume.

`@nagi-js/postgres` ships migration `0004_query_runs_input_idx` adding a
GIN index (`jsonb_path_ops`) on `workflow_run.input` so containment
queries scale on busy tables.

`Store` gains a `queryRuns(opts)` method — third-party adapters must
implement it (signature in `@nagi-js/core/types`). Existing adapters not
backed by a queryable input column should reject calls or scan, depending
on their model. The in-memory adapter scans its fact log.
