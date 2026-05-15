import type { Kysely } from "kysely";
import { sql } from "kysely";

export interface Migration {
  readonly id: string;
  readonly sql: (schema: string) => string;
}

/**
 * v0 schema for `@nagi-js/postgres`. Each migration's `sql` builds a script
 * for a configurable schema name — the default in `migrate()` is `nagi`.
 *
 * Inline strings, never `fs.readFileSync` — preserves edge-runtime safety
 * (the adapter must be importable on Cloudflare Workers / Deno Deploy).
 */
export const migrations: readonly Migration[] = [
  {
    id: "0001_init",
    sql: (schema) => `
      CREATE SCHEMA IF NOT EXISTS ${schema};

      CREATE TABLE IF NOT EXISTS ${schema}.workflow_run (
        run_id       text        PRIMARY KEY,
        flow_id      text        NOT NULL,
        status       text        NOT NULL CHECK (status IN ('pending','running','completed','failed')),
        input        jsonb       NOT NULL,
        output       jsonb,
        error        jsonb,
        started_at   timestamptz NOT NULL DEFAULT now(),
        completed_at timestamptz
      );
      CREATE INDEX IF NOT EXISTS workflow_run_flow_status_idx
        ON ${schema}.workflow_run (flow_id, status);

      CREATE TABLE IF NOT EXISTS ${schema}.step_run (
        run_id       text        NOT NULL,
        step_id      text        NOT NULL,
        attempt      integer     NOT NULL,
        status       text        NOT NULL CHECK (status IN ('pending','running','completed','failed','skipped')),
        output       jsonb,
        error        jsonb,
        started_at   timestamptz,
        completed_at timestamptz,
        PRIMARY KEY (run_id, step_id, attempt)
      );
      CREATE INDEX IF NOT EXISTS step_run_lookup_idx
        ON ${schema}.step_run (run_id, step_id, attempt DESC);

      CREATE TABLE IF NOT EXISTS ${schema}.fact (
        run_id  text        NOT NULL,
        fact_id text        NOT NULL,
        kind    text        NOT NULL,
        at      timestamptz NOT NULL,
        payload jsonb       NOT NULL,
        PRIMARY KEY (run_id, fact_id)
      );
      CREATE INDEX IF NOT EXISTS fact_run_id_idx
        ON ${schema}.fact (run_id, fact_id);

      CREATE TABLE IF NOT EXISTS ${schema}.lease (
        run_id     text        NOT NULL,
        step_id    text        NOT NULL,
        attempt    integer     NOT NULL,
        token      text        NOT NULL,
        expires_at timestamptz NOT NULL,
        PRIMARY KEY (run_id, step_id, attempt)
      );
      CREATE INDEX IF NOT EXISTS lease_expires_idx
        ON ${schema}.lease (expires_at);

      CREATE TABLE IF NOT EXISTS ${schema}.timer (
        run_id  text        NOT NULL,
        step_id text        NOT NULL,
        fire_at timestamptz NOT NULL,
        PRIMARY KEY (run_id, step_id)
      );
      CREATE INDEX IF NOT EXISTS timer_fire_at_idx
        ON ${schema}.timer (fire_at);

      CREATE TABLE IF NOT EXISTS ${schema}.dedupe (
        run_id      text        NOT NULL,
        step_id     text        NOT NULL,
        scope       text        NOT NULL,
        value       jsonb       NOT NULL,
        recorded_at timestamptz NOT NULL DEFAULT now(),
        PRIMARY KEY (run_id, step_id, scope)
      );
    `,
  },
  {
    id: "0002_snapshot_tables",
    sql: (schema) => `
      CREATE TABLE IF NOT EXISTS ${schema}.flow_snapshot (
        flow_hash   text        PRIMARY KEY,
        flow_id     text        NOT NULL,
        dag         jsonb       NOT NULL,
        recorded_at timestamptz NOT NULL DEFAULT now()
      );
      CREATE INDEX IF NOT EXISTS flow_snapshot_by_id_idx
        ON ${schema}.flow_snapshot (flow_id, recorded_at DESC);

      CREATE TABLE IF NOT EXISTS ${schema}.flow_ref (
        flow_id    text        PRIMARY KEY,
        flow_hash  text        NOT NULL REFERENCES ${schema}.flow_snapshot(flow_hash),
        updated_at timestamptz NOT NULL DEFAULT now()
      );

      CREATE TABLE IF NOT EXISTS ${schema}.global_fact (
        fact_id text        PRIMARY KEY,
        kind    text        NOT NULL,
        at      timestamptz NOT NULL,
        payload jsonb       NOT NULL
      );
      CREATE INDEX IF NOT EXISTS global_fact_kind_idx
        ON ${schema}.global_fact (kind, at DESC);

      ALTER TABLE ${schema}.workflow_run
        ADD COLUMN IF NOT EXISTS flow_hash    text REFERENCES ${schema}.flow_snapshot(flow_hash);
      ALTER TABLE ${schema}.workflow_run
        ADD COLUMN IF NOT EXISTS code_version text;
      CREATE INDEX IF NOT EXISTS workflow_run_flow_hash_idx
        ON ${schema}.workflow_run (flow_hash);
    `,
  },
  {
    id: "0003_concurrency_groups",
    sql: (schema) => `
      ALTER TABLE ${schema}.workflow_run
        ADD COLUMN IF NOT EXISTS concurrency_key   text;
      ALTER TABLE ${schema}.workflow_run
        ADD COLUMN IF NOT EXISTS canceled_by_run_id text;

      ALTER TABLE ${schema}.workflow_run
        DROP CONSTRAINT IF EXISTS workflow_run_status_check;
      ALTER TABLE ${schema}.workflow_run
        ADD CONSTRAINT workflow_run_status_check
        CHECK (status IN ('pending','running','completed','failed','canceled'));

      -- Defense-in-depth: even if application code skips the advisory lock,
      -- this index makes "two active runs sharing a concurrency key" a
      -- unique-violation at insert time. Excludes terminal rows and rows
      -- without a key (the common case for flows that don't declare
      -- concurrency).
      CREATE UNIQUE INDEX IF NOT EXISTS workflow_run_concurrency_active_uidx
        ON ${schema}.workflow_run (flow_id, concurrency_key)
        WHERE concurrency_key IS NOT NULL
          AND status IN ('pending','running');
    `,
  },
];

export interface MigrateOpts {
  /** Schema name; default `nagi`. */
  readonly schema?: string;
}

/**
 * Apply pending migrations idempotently. Maintains a `<schema>.schema_migrations`
 * table; migrations whose ids are already recorded there are skipped.
 *
 * Each migration runs inside its own transaction, so a partial failure leaves
 * the bookkeeping table consistent.
 */
export async function migrate<DB>(
  db: Kysely<DB>,
  opts: MigrateOpts = {},
): Promise<{ applied: readonly string[]; skipped: readonly string[] }> {
  const schema = opts.schema ?? "nagi";
  assertValidSchema(schema);

  const applied: string[] = [];
  const skipped: string[] = [];

  // Bootstrap the bookkeeping table outside any user migration. Idempotent.
  await sql
    .raw(
      `CREATE SCHEMA IF NOT EXISTS ${schema};
       CREATE TABLE IF NOT EXISTS ${schema}.schema_migrations (
         id         text        PRIMARY KEY,
         applied_at timestamptz NOT NULL DEFAULT now()
       );`,
    )
    .execute(db);

  for (const m of migrations) {
    await db.transaction().execute(async (tx) => {
      const existing = await sql<{ id: string }>`
        SELECT id FROM ${sql.raw(`${schema}.schema_migrations`)}
         WHERE id = ${m.id}
      `.execute(tx);
      if (existing.rows.length > 0) {
        skipped.push(m.id);
        return;
      }
      await sql.raw(m.sql(schema)).execute(tx);
      await sql`
        INSERT INTO ${sql.raw(`${schema}.schema_migrations`)} (id) VALUES (${m.id})
      `.execute(tx);
      applied.push(m.id);
    });
  }

  return { applied, skipped };
}

/**
 * Reject schema names with anything other than `[A-Za-z_][A-Za-z0-9_]*` to
 * guard against SQL injection — the schema name is interpolated raw into
 * every DDL string. Kysely's `sql` tag cannot bind identifiers, only values.
 */
function assertValidSchema(schema: string): void {
  if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(schema)) {
    throw new Error(
      `@nagi-js/postgres: invalid schema name "${schema}". Must match /^[A-Za-z_][A-Za-z0-9_]*$/.`,
    );
  }
}
