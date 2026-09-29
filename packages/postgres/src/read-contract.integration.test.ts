import { Kysely, PostgresDialect, sql } from "kysely";
import pg from "pg";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { migrate } from "./migrations";
import { uuidv7 } from "./uuidv7";

const url = process.env["NAGI_POSTGRES_TEST_URL"];
const d = url ? describe : describe.skip;

// The SQL read contract in docs/OPERATIONS.md ("Reading runs with SQL").
// Consumers query and join these columns directly; a migration that drops,
// renames or retypes one must fail here first.
const CONTRACT: Record<string, Record<string, string>> = {
  workflow_run: {
    run_id: "text",
    flow_id: "text",
    status: "text",
    input: "jsonb",
    output: "jsonb",
    error: "jsonb",
    started_at: "timestamp with time zone",
    completed_at: "timestamp with time zone",
    canceled_by_run_id: "text",
    parent_run_id: "text",
    parent_step_id: "text",
  },
  step_run: {
    run_id: "text",
    step_id: "text",
    attempt: "integer",
    status: "text",
    output: "jsonb",
    error: "jsonb",
    started_at: "timestamp with time zone",
    completed_at: "timestamp with time zone",
  },
  lease: {
    run_id: "text",
    step_id: "text",
    attempt: "integer",
    expires_at: "timestamp with time zone",
  },
  timer: {
    run_id: "text",
    step_id: "text",
    fire_at: "timestamp with time zone",
  },
};

d("@nagi-js/postgres — SQL read contract", () => {
  let db: Kysely<unknown>;
  let schema: string;

  beforeAll(async () => {
    db = new Kysely<unknown>({
      dialect: new PostgresDialect({
        pool: new pg.Pool({ connectionString: url }),
      }),
    });
    schema = `nagi_contract_${uuidv7().replace(/-/g, "").slice(0, 16)}`;
    await migrate(db, { schema });
  }, 30_000);

  afterAll(async () => {
    if (!db) return;
    await sql.raw(`DROP SCHEMA IF EXISTS ${schema} CASCADE`).execute(db);
    await db.destroy();
  }, 30_000);

  it("keeps every contract column with its type", async () => {
    const { rows } = await sql<{
      table_name: string;
      column_name: string;
      data_type: string;
    }>`
      SELECT table_name, column_name, data_type
        FROM information_schema.columns
       WHERE table_schema = ${schema}
    `.execute(db);
    for (const [table, columns] of Object.entries(CONTRACT)) {
      const actual = Object.fromEntries(
        rows
          .filter((r) => r.table_name === table)
          .map((r) => [r.column_name, r.data_type]),
      );
      expect(actual, table).toMatchObject(columns);
    }
  });

  it("serves input containment from the GIN index", async () => {
    const { rows } = await sql<{ indexdef: string }>`
      SELECT indexdef FROM pg_indexes
       WHERE schemaname = ${schema} AND tablename = 'workflow_run'
    `.execute(db);
    expect(
      rows.some((r) => /USING gin \(input jsonb_path_ops\)/.test(r.indexdef)),
    ).toBe(true);
  });

  it("keeps step_run at one row per step", async () => {
    const { rows } = await sql<{ indexdef: string }>`
      SELECT indexdef FROM pg_indexes
       WHERE schemaname = ${schema} AND indexname = 'step_run_pkey'
    `.execute(db);
    expect(rows[0]?.indexdef).toMatch(/\(run_id, step_id\)$/);
  });
});
