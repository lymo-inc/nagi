import { Kysely, PostgresDialect, sql } from "kysely";
import pg from "pg";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { migrate, migrations } from "./migrations";
import { uuidv7 } from "./uuidv7";

const url = process.env["NAGI_POSTGRES_TEST_URL"];
const d = url ? describe : describe.skip;

d("@nagi-js/postgres — concurrent migrate()", () => {
  const ids = migrations.map((m) => m.id);
  let db: Kysely<unknown>;
  let pool: pg.Pool;
  let schema: string;

  beforeAll(() => {
    pool = new pg.Pool({ connectionString: url });
    db = new Kysely<unknown>({ dialect: new PostgresDialect({ pool }) });
    schema = `nagi_migrate_${uuidv7().replace(/-/g, "").slice(0, 16)}`;
  });

  afterAll(async () => {
    if (!db) return;
    await sql.raw(`DROP SCHEMA IF EXISTS ${schema} CASCADE`).execute(db);
    await db.destroy();
  }, 30_000);

  it("two concurrent runs apply every migration exactly once", async () => {
    const [a, b] = await Promise.all([
      migrate(db, { schema }),
      migrate(db, { schema }),
    ]);

    expect([...a.applied, ...b.applied].sort()).toEqual(ids);
    for (const r of [a, b]) {
      expect(r.skipped).toEqual(ids.filter((id) => !r.applied.includes(id)));
    }
    const sizes = [a.applied.length, b.applied.length].sort((x, y) => x - y);
    expect(sizes).toEqual([0, ids.length]);
  }, 30_000);

  it("releases the lock when it returns", async () => {
    expect(await migrate(db, { schema })).toEqual({
      applied: [],
      skipped: ids,
    });

    const other = new pg.Client({ connectionString: url });
    await other.connect();
    try {
      const { rows } = await other.query<{ got: boolean }>(
        "SELECT pg_try_advisory_lock(hashtext($1)) AS got",
        [`nagi:migrate:${schema}`],
      );
      expect(rows[0]?.got).toBe(true);
    } finally {
      await other.end();
    }
  }, 30_000);
});
