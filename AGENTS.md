# Working on nagi

nagi is a durable workflow engine for multi-turn LLM backend workflows. It is
not a general job queue, a realtime or fire-and-forget system, or a scheduler.
Packages: `@nagi-js/core` (runtime, builder, in-memory adapters, the
`@nagi-js/core/testing` Store conformance suite), `@nagi-js/postgres` (Store),
`@nagi-js/pgmq` (Queue).

## Commands

Toolchain is pinned in `mise.toml` (node 22, pnpm 10.33.0).

```sh
pnpm install --frozen-lockfile
pnpm lint                 # biome, warnings are errors
pnpm -r build             # core must build before typecheck/tests of dependents
pnpm typecheck
pnpm test                 # Postgres suites skip without a database
pnpm db:up && pnpm test:integration && pnpm db:down
pnpm verify               # everything CI runs: lint, build, typecheck, test, publint, test-types
```

`pre-commit` runs lint-staged; `pre-push` runs typecheck and the full test suite.

## Design rules

- **Make invalid states unrepresentable.** Prefer a shape that cannot hold the
  bad state over a sweeper, guard or reaper that cleans it up afterwards.
- **Optionality only at the boundary.** `undefined`/`null`/optional fields
  belong in public APIs, facts and events; past the one fork point, internals
  are precise and total.
- **Policy lives in core.** Adapters own the transaction boundary and
  persistence; what a fact means (row changes, releases, closes) is decided by
  pure functions in core and enforced by `storeContract`.
- **Complexity must pay for itself.** No speculative seams, parallel API
  variants or single-implementation ports (see RFC 0021 and PR #55).
- **Public API shape:** explicit discriminated unions (`{ tag: ... }`) over
  sentinel values, without type machinery that doesn't earn its keep.
- **Single door:** everything public is exported from `packages/core/src/index.ts`
  (plus `./testing`).
- **Comments:** only load-bearing WHY, and the MUST contracts on the
  `Store`/`Queue` ports. No JSDoc narrative or banners.

## Changes, changesets, releases

- Every user-visible change to a published package gets a changeset. While on
  0.1.x the bump is always `patch`; `minor` jumps to 0.2.0.
- Version with `pnpm version-packages`, never `npm version` (the `version`
  script name is an npm lifecycle hook).
- After `changeset publish`, push the new tags by name
  (`git push origin refs/tags/@nagi-js/<pkg>@<version>`), not `--tags`.
- Don't commit, push or open PRs unless asked; the maintainer sequences
  multi-feature trees. Several sessions may share one checkout, so check
  `git status` before staging and stage only your own files.
- The root `README.md` is maintainer-written; flag needed changes instead of
  editing it.
- If a pnpm 11 binary rewrites `packageManager` in `package.json`, revert only
  that line.

## Decided — don't re-propose

- A `Trigger` adapter, `Clock.schedule`, cron firing or leader election (#11, #55).
- A bundled SSE/WebSocket server package (#16 Layer 2). Transports are
  contracts; the consumer owns the socket.
- Splitting the single-door exports, removing `RunState.facts`, per-package
  LICENSE copies, `minor` bumps in 0.1.x, `b.signal` without `timeoutMs`.
- Retention or redaction of stored inputs and errors as a defect; it is a
  product policy (`pruneFacts` `keepSummary` is by design).

RFCs are indexed in `docs/rfcs/README.md`; the audit backlog and plan status
live in `plans/README.md`; runbook material is in `docs/OPERATIONS.md`.
