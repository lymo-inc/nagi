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

  // Staging 2026-10-13: the deploy's bootstrap died on a 57014 statement
  // timeout inside pg_advisory_lock('nagi:migrate:nagi'), held by a
  // connection a cancelled deploy left behind. The wait had no bound and
  // named no holder.
  it("gives up waiting for the migrate lock and names who holds it", async () => {
    const holder = new pg.Client({
      connectionString: url,
      application_name: "stale-deploy",
    });
    await holder.connect();
    try {
      await holder.query("SELECT pg_advisory_lock(hashtext($1))", [
        `nagi:migrate:${schema}`,
      ]);
      const t0 = Date.now();
      await expect(migrate(db, { schema, lockWaitMs: 300 })).rejects.toThrow(
        /stale-deploy/,
      );
      expect(Date.now() - t0).toBeLessThan(3_000);
    } finally {
      await holder.end();
    }
  }, 30_000);

  // A DDL that queues behind a long transaction takes every later query on
  // that table down with it (they queue behind its ACCESS EXCLUSIVE request).
  // Boot must fail fast instead.
  it("a migration that cannot get its table lock fails fast instead of queueing", async () => {
    await migrate(db, { schema });
    await sql
      .raw(`DELETE FROM ${schema}.schema_migrations WHERE id = '0011_fact_seq'`)
      .execute(db);
    const reader = new pg.Client({ connectionString: url });
    await reader.connect();
    try {
      await reader.query("BEGIN");
      await reader.query(`LOCK TABLE ${schema}.fact IN ACCESS SHARE MODE`);
      const t0 = Date.now();
      await expect(migrate(db, { schema, lockTimeoutMs: 200 })).rejects.toThrow(
        /lock timeout/,
      );
      expect(Date.now() - t0).toBeLessThan(3_000);
    } finally {
      await reader.query("ROLLBACK");
      await reader.end();
    }
    // Nothing half-applied: the retry after the reader is gone succeeds.
    expect((await migrate(db, { schema })).applied).toEqual(["0011_fact_seq"]);
  }, 30_000);
});
