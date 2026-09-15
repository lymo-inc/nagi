# Plan 001: Add CI and make lint/verify/release actually gate on tests

> **Executor instructions**: Follow this plan step by step. Run every
> verification command and confirm the expected result before moving to the
> next step. If anything in the "STOP conditions" section occurs, stop and
> report — do not improvise. When done, update the status row for this plan
> in `plans/README.md` — unless a reviewer dispatched you and told you they
> maintain the index.
>
> **Drift check (run first)**: `git diff --stat ced05c2..HEAD -- package.json mise.toml .husky packages/core/src/tests/signal-timeout.test.ts .github`
> If any in-scope file changed since this plan was written, compare the
> "Current state" excerpts against the live code before proceeding; on a
> mismatch, treat it as a STOP condition.

## Status

- **Priority**: P1
- **Effort**: S
- **Risk**: LOW
- **Depends on**: none
- **Category**: dx
- **Planned at**: commit `ced05c2`, 2026-09-04
- **Issue**: https://github.com/lymo-inc/nagi/issues/43

## Why this matters

This repo publishes four public npm packages (`@nagi-js/core`, `@nagi-js/postgres`,
`@nagi-js/pgmq`, `@nagi-js/otel`) and has **no CI at all** — there is no
`.github/` directory. The only automated gate is a local `husky` pre-push hook,
which `git push --no-verify` bypasses. The `pnpm verify` script that guards
`pnpm release` runs lint, build, publint and a types-resolution check, but
**not** `typecheck` and **not** `test`. And `pnpm lint` exits 0 on warnings, so
the one existing biome warning has survived indefinitely. Every other plan in
`plans/` assumes a reproducible green baseline; this plan creates it. It also
makes the Postgres integration suite (34 tests that silently skip without a
database URL) run somewhere other than one laptop.

## Current state

- `package.json` — root scripts. Relevant lines today:

  ```json
  "scripts": {
    "build": "pnpm -r build",
    "test": "pnpm -r test",
    "test:integration": "NAGI_POSTGRES_TEST_URL=postgres://postgres:postgres@localhost:5433/nagi_test pnpm -F @nagi-js/postgres test",
    "typecheck": "pnpm -r typecheck",
    "lint": "biome check .",
    "format": "biome format --write .",
    "db:up": "docker compose -f docker-compose.test.yml up -d --wait",
    "db:down": "docker compose -f docker-compose.test.yml down -v",
    "verify": "pnpm lint && pnpm -r build && pnpm verify:publint && pnpm verify:types",
    "verify:publint": "pnpm -r --parallel exec publint",
    "verify:types": "tsc -p test-types/tsconfig.json",
    ...
    "release": "pnpm verify && changeset publish",
  ```

  `"packageManager"` at line 7 is `"pnpm@11.25.0"` and `engines.node` is `>=22`.
- `mise.toml` — pins `node = "22"` and `pnpm = "10.33.0"`. This disagrees with
  `packageManager`; running pnpm 11 against this repo silently rewrites the
  `packageManager` field (observed during the audit).
- `.husky/pre-push` — `pnpm typecheck` then `pnpm test`. Keep it.
- `docker-compose.test.yml` — `postgres:16-alpine`, password `postgres`, db
  `nagi_test`, host port 5433, no pgmq extension. The integration suite in
  `packages/postgres/src/integration.test.ts:16-17` is gated:

  ```ts
  const url = process.env["NAGI_POSTGRES_TEST_URL"];
  const d = url ? describe : describe.skip;
  ```

  Per-test schemas are `nagi_test_<uuid7>` so one database serves the suite.
- `packages/core/src/tests/signal-timeout.test.ts:140` — the single biome
  warning (`lint/correctness/noUnusedVariables`):

  ```ts
  const runId = await h.wf.start(f, {});
  await h.drain();
  await dispatcher.sweepTimers(FAR_FUTURE());
  ```

  `runId` is never read afterwards in that test.
- `biome.json` — biome 2.4.14; `biome ci` and `biome check` both accept
  `--error-on-warnings` (verified with `pnpm exec biome ci --help`).
- Baseline at `ced05c2`: `pnpm typecheck` exit 0, `pnpm test` all green
  (core 956, otel 54, pgmq 53, postgres 83 passed / 34 skipped), `pnpm lint`
  exit 0 with 1 warning.

Repo conventions to match: conventional-commit subjects with a scope, e.g.
`fix(worker): dequeue failure must not kill the poll loop — back off and retry`,
`feat(core,pgmq): wf.inspectQueue + operations runbook — triage without SQL`.
Comment policy is aggressive minimalism: no banner comments, no narration —
only load-bearing "why" comments.

## Commands you will need

| Purpose   | Command                  | Expected on success |
|-----------|--------------------------|---------------------|
| Install   | `pnpm install --frozen-lockfile` | exit 0 |
| Typecheck | `pnpm typecheck`         | exit 0, four `Done` lines |
| Unit tests| `pnpm test`              | exit 0, no `failed` |
| Lint      | `pnpm lint`              | exit 0, `Found 0 warnings` (after step 1) |
| Integration | `pnpm db:up && pnpm test:integration` | exit 0, postgres `Tests 117 passed` (0 skipped) |
| Full gate | `pnpm verify`            | exit 0 |

## Scope

**In scope** (the only files you should modify):
- `.github/workflows/ci.yml` (create)
- `package.json` (root — `scripts.lint` and `scripts.verify` only)
- `mise.toml` (the pnpm pin only)
- `packages/core/src/tests/signal-timeout.test.ts` (one identifier at line 140)
- `.changeset/` — no changeset needed; nothing published changes.

**Out of scope** (do NOT touch, even though they look related):
- `.husky/*` — the local hooks are fine as they are.
- `docker-compose.test.yml` — CI uses a GitHub `services:` container, not compose.
- Any `packages/*/package.json` — `provenance: false` and the version fields are
  the maintainer's release decisions.
- `pnpm-lock.yaml`, `packages/postgres/package.json` — a parallel dependency
  bump may be in flight in the working tree; do not stage or revert them.
- Adding a publish/release workflow — releases stay manual (`pnpm release`).

## Git workflow

- Branch: `advisor/001-ci-and-release-gating`
- One commit per step is fine; subject style: `chore(ci): add GitHub Actions workflow`,
  `chore: make lint and verify fail on warnings and run tests`.
- Do NOT push or open a PR unless the operator instructed it.
- Before committing, run `git status` and stage only in-scope files.

## Steps

### Step 1: Clear the one lint warning

In `packages/core/src/tests/signal-timeout.test.ts` line 140, rename the unused
binding so biome's unused-variable rule is satisfied without changing behavior:

```ts
await h.wf.start(f, {});
```

(Drop the `const runId =` entirely — the value is not used in that test.)

**Verify**: `pnpm lint` → `Found 0 warnings` (or no warning summary at all) and exit 0.

### Step 2: Make lint fail on warnings and make verify run typecheck + tests

Edit root `package.json` scripts:

```json
"lint": "biome check --error-on-warnings .",
"verify": "pnpm lint && pnpm typecheck && pnpm test && pnpm -r build && pnpm verify:publint && pnpm verify:types",
```

Leave every other script unchanged.

**Verify**: `pnpm verify` → exit 0. Then temporarily confirm the gate bites:
`echo 'const unused = 1;' > /tmp/nagi-lint-probe.ts && pnpm exec biome check --error-on-warnings /tmp/nagi-lint-probe.ts; echo exit=$?` →
non-zero exit (then delete the probe file). If biome refuses a path outside the
repo, skip this probe — Step 1's before/after already demonstrates the flag.

### Step 3: Align the pnpm pin

Read `"packageManager"` from root `package.json`. Set `mise.toml`'s `pnpm`
value to exactly that version string (currently `11.25.0`). Do not change
`packageManager` itself.

**Verify**: `grep packageManager package.json; grep pnpm mise.toml` → same version in both lines.

### Step 4: Add the CI workflow

Create `.github/workflows/ci.yml`:

```yaml
name: CI

on:
  push:
    branches: [main]
  pull_request:

concurrency:
  group: ci-${{ github.ref }}
  cancel-in-progress: true

jobs:
  check:
    runs-on: ubuntu-latest
    timeout-minutes: 15
    steps:
      - uses: actions/checkout@v4
      - uses: pnpm/action-setup@v4
        # No `version:` — pnpm/action-setup reads package.json#packageManager.
      - uses: actions/setup-node@v4
        with:
          node-version: 22
          cache: pnpm
      - run: pnpm install --frozen-lockfile
      - run: pnpm lint
      - run: pnpm typecheck
      - run: pnpm test
      - run: pnpm -r build
      - run: pnpm verify:publint
      - run: pnpm verify:types

  integration:
    runs-on: ubuntu-latest
    timeout-minutes: 15
    services:
      postgres:
        image: postgres:16-alpine
        env:
          POSTGRES_PASSWORD: postgres
          POSTGRES_DB: nagi_test
        ports:
          - 5432:5432
        options: >-
          --health-cmd "pg_isready -U postgres -d nagi_test"
          --health-interval 2s
          --health-timeout 5s
          --health-retries 20
    env:
      NAGI_POSTGRES_TEST_URL: postgres://postgres:postgres@localhost:5432/nagi_test
    steps:
      - uses: actions/checkout@v4
      - uses: pnpm/action-setup@v4
      - uses: actions/setup-node@v4
        with:
          node-version: 22
          cache: pnpm
      - run: pnpm install --frozen-lockfile
      - run: pnpm -F @nagi-js/postgres test
```

Notes for the executor:
- The integration job deliberately uses `pnpm -F @nagi-js/postgres test`
  with the env var set, not the root `test:integration` script (which hardcodes
  port 5433 for the local compose file).
- `@nagi-js/postgres` depends on `@nagi-js/core` via `workspace:*` and its
  tests import from `@nagi-js/core`'s `dist`? **Check**: run
  `grep -n '"main"\|"exports"' packages/core/package.json` and
  `cat packages/postgres/tsconfig.json`. If the postgres tests resolve core
  through `dist/`, add `- run: pnpm -F @nagi-js/core build` before the test
  step in the `integration` job (and keep the `check` job order, which builds
  after unit tests — if unit tests in adapters also need core's `dist`, move
  `pnpm -r build` before `pnpm test` in the `check` job). Decide by running
  `rm -rf packages/core/dist && pnpm -F @nagi-js/postgres test` locally: if it
  fails with a module-resolution error, the build step is required.

**Verify**: `pnpm exec biome check --error-on-warnings .github` → exit 0 (biome
lints YAML as unknown → ignored; this only confirms nothing else broke). Then
validate YAML syntax: `node -e "require('node:fs').readFileSync('.github/workflows/ci.yml','utf8')" && python3 -c "import yaml,sys; yaml.safe_load(open('.github/workflows/ci.yml'))"` → no error (skip the python line if PyYAML is unavailable).

### Step 5: Run the full local equivalent of CI

`pnpm db:up && pnpm test:integration && pnpm db:down` (requires Docker). If
Docker is unavailable, say so in your report — do not mark the plan DONE
without either this run or a green CI run on the branch.

**Verify**: `pnpm test:integration` → postgres `Tests 117 passed`, `0 skipped`.

## Test plan

No new test files. The deliverable is the gate itself:
- `pnpm lint` fails on any warning (Step 2).
- `pnpm verify` runs typecheck and unit tests (Step 2).
- CI runs lint, typecheck, unit tests, build, publint, types check, and the
  Postgres integration suite against a real database (Step 4).

## Done criteria

- [ ] `.github/workflows/ci.yml` exists and matches Step 4 (plus the optional build step if required)
- [ ] `pnpm lint` exits 0 with zero warnings
- [ ] `pnpm verify` exits 0 and its command string contains `pnpm typecheck && pnpm test`
- [ ] `grep -c "" packages/core/src/tests/signal-timeout.test.ts` unchanged ±1 line; `pnpm test` green
- [ ] `mise.toml` pnpm version equals `package.json#packageManager` version
- [ ] Integration suite ran green locally or in CI (state which)
- [ ] `git status` shows no modified files outside the in-scope list (ignore pre-existing `pnpm-lock.yaml` / `packages/postgres/package.json` changes that were already present before you started)
- [ ] `plans/README.md` status row updated

## STOP conditions

Stop and report back (do not improvise) if:
- Root `package.json` scripts do not match the "Current state" excerpt (someone
  already changed `lint`/`verify`).
- `pnpm test` is not green at your starting commit — the baseline moved; report
  the failing test rather than fixing it here.
- `pnpm install --frozen-lockfile` fails because `pnpm-lock.yaml` is out of
  sync with a manifest — a dependency bump is mid-flight in the tree; do not
  run a non-frozen install to "fix" it.
- Making lint fail on warnings surfaces more than the single warning at
  `signal-timeout.test.ts:140` — list them; do not fix unrelated files.

## Maintenance notes

- If a pgmq-backed integration suite is added later (see backlog item A-08 in
  `plans/README.md`), the `integration` job needs a pgmq-enabled Postgres image
  (`postgres:16-alpine` has no `pgmq` extension) — add a second service or
  switch images at that point.
- Reviewers should check that the `verify` script still runs `publint` and the
  `test-types` resolution check last; those catch packaging regressions that
  unit tests cannot.
- Deliberately deferred: a publish workflow with npm provenance. The packages
  set `provenance: false` and releases are manual by the maintainer's choice.
