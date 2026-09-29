import type { Kysely } from "kysely";
import { sql } from "kysely";

export interface Migration {
  readonly id: string;
  readonly sql: (schema: string) => string;
}

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

      CREATE UNIQUE INDEX IF NOT EXISTS workflow_run_concurrency_active_uidx
        ON ${schema}.workflow_run (flow_id, concurrency_key)
        WHERE concurrency_key IS NOT NULL
          AND status IN ('pending','running');
    `,
  },
  {
    id: "0004_query_runs_input_idx",
    sql: (schema) => `
      CREATE INDEX IF NOT EXISTS workflow_run_input_gin_idx
        ON ${schema}.workflow_run USING gin (input jsonb_path_ops);
    `,
  },
  {
    id: "0005_subflow_parent_link",
    sql: (schema) => `
      ALTER TABLE ${schema}.workflow_run
        ADD COLUMN IF NOT EXISTS parent_run_id  text;
      ALTER TABLE ${schema}.workflow_run
        ADD COLUMN IF NOT EXISTS parent_step_id text;
      CREATE INDEX IF NOT EXISTS workflow_run_parent_run_id_idx
        ON ${schema}.workflow_run (parent_run_id)
        WHERE parent_run_id IS NOT NULL;
    `,
  },
  {
    id: "0006_step_canceled_status",
    sql: (schema) => `
      ALTER TABLE ${schema}.step_run
        DROP CONSTRAINT IF EXISTS step_run_status_check;
      ALTER TABLE ${schema}.step_run
        ADD CONSTRAINT step_run_status_check
        CHECK (status IN ('pending','running','completed','failed','canceled','skipped'));
    `,
  },
  {
    id: "0007_prune_completed_at_idx",
    sql: (schema) => `
      CREATE INDEX IF NOT EXISTS workflow_run_completed_at_idx
        ON ${schema}.workflow_run (completed_at)
        WHERE completed_at IS NOT NULL
          AND status IN ('completed','failed','canceled');
    `,
  },
  {
    id: "0008_canceled_by_run_id_fk",
    sql: (schema) => `
      UPDATE ${schema}.workflow_run w
         SET canceled_by_run_id = NULL
       WHERE w.canceled_by_run_id IS NOT NULL
         AND NOT EXISTS (
               SELECT 1 FROM ${schema}.workflow_run s
                WHERE s.run_id = w.canceled_by_run_id
             );

      CREATE INDEX IF NOT EXISTS workflow_run_canceled_by_idx
        ON ${schema}.workflow_run (canceled_by_run_id)
        WHERE canceled_by_run_id IS NOT NULL;

      ALTER TABLE ${schema}.workflow_run
        DROP CONSTRAINT IF EXISTS workflow_run_canceled_by_fk;
      ALTER TABLE ${schema}.workflow_run
        ADD CONSTRAINT workflow_run_canceled_by_fk
        FOREIGN KEY (canceled_by_run_id)
        REFERENCES ${schema}.workflow_run(run_id)
        ON DELETE SET NULL;
    `,
  },
  {
    // step_run becomes one row per step (its latest attempt), updated in place.
    // Of the per-attempt rows, a settled one wins, then the highest attempt —
    // what the fold would have made of the same facts.
    id: "0009_step_run_per_step",
    sql: (schema) => `
      DELETE FROM ${schema}.step_run s
       USING (
         SELECT run_id, step_id, attempt,
                row_number() OVER (
                  PARTITION BY run_id, step_id
                  ORDER BY (status IN ('completed','failed','canceled','skipped')) DESC,
                           attempt DESC
                ) AS rn
           FROM ${schema}.step_run
       ) ranked
       WHERE s.run_id = ranked.run_id
         AND s.step_id = ranked.step_id
         AND s.attempt = ranked.attempt
         AND ranked.rn > 1;

      ALTER TABLE ${schema}.step_run DROP CONSTRAINT IF EXISTS step_run_pkey;
      ALTER TABLE ${schema}.step_run ADD CONSTRAINT step_run_pkey
        PRIMARY KEY (run_id, step_id);
      DROP INDEX IF EXISTS ${schema}.step_run_lookup_idx;
    `,
  },
  {
    // Checked at commit: code older than c50101f writes the victim's
    // canceled_by_run_id before inserting the superseder in the same tx.
    // ON DELETE SET NULL stays immediate (referential actions never defer).
    id: "0010_canceled_by_fk_deferrable",
    sql: (schema) => `
      ALTER TABLE ${schema}.workflow_run
        ALTER CONSTRAINT workflow_run_canceled_by_fk
        DEFERRABLE INITIALLY DEFERRED;
    `,
  },
  {
    // Commit-causal read order across processes. Nullable with a default, so
    // no rewrite and no backfill: old rows (seq NULL) keep fact_id order and
    // precede every new fact.
    id: "0011_fact_seq",
    sql: (schema) => `
      CREATE SEQUENCE IF NOT EXISTS ${schema}.fact_seq;
      ALTER TABLE ${schema}.fact ADD COLUMN IF NOT EXISTS seq bigint;
      ALTER TABLE ${schema}.fact
        ALTER COLUMN seq SET DEFAULT nextval('${schema}.fact_seq');
      ALTER SEQUENCE ${schema}.fact_seq OWNED BY ${schema}.fact.seq;
    `,
  },
];

export interface MigrateOpts {
  readonly schema?: string;
}

export async function migrate<DB>(
  db: Kysely<DB>,
  opts: MigrateOpts = {},
): Promise<{ applied: readonly string[]; skipped: readonly string[] }> {
  const schema = opts.schema ?? "nagi";
  assertValidSchema(schema);

  // Session-level lock: it and the migrations must share one connection.
  return db.connection().execute(async (conn) => {
    const key = `nagi:migrate:${schema}`;
    await sql`SELECT pg_advisory_lock(hashtext(${key}))`.execute(conn);
    try {
      const applied: string[] = [];
      const skipped: string[] = [];

      await sql
        .raw(
          `CREATE SCHEMA IF NOT EXISTS ${schema};
           CREATE TABLE IF NOT EXISTS ${schema}.schema_migrations (
             id         text        PRIMARY KEY,
             applied_at timestamptz NOT NULL DEFAULT now()
           );`,
        )
        .execute(conn);

      for (const m of migrations) {
        await conn.transaction().execute(async (tx) => {
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
    } finally {
      await sql`SELECT pg_advisory_unlock(hashtext(${key}))`.execute(conn);
    }
  });
}

const MAX_SCHEMA_LEN = 56; // 63-byte identifier limit minus "_stream"/"_events"

function assertValidSchema(schema: string): void {
  if (
    !/^[A-Za-z_][A-Za-z0-9_]*$/.test(schema) ||
    schema.length > MAX_SCHEMA_LEN
  ) {
    throw new Error(
      `@nagi-js/postgres: invalid schema name "${schema}". Must match /^[A-Za-z_][A-Za-z0-9_]*$/ and be at most ${MAX_SCHEMA_LEN} characters.`,
    );
  }
}
