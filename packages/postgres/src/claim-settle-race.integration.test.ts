import type { RunId } from "@nagi-js/core";
import { Kysely, PostgresDialect, sql } from "kysely";
import pg from "pg";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { migrate } from "./migrations";
import { postgresStore } from "./store";
import { uuidv7 } from "./uuidv7";

const url = process.env["NAGI_POSTGRES_TEST_URL"];
const d = url ? describe : describe.skip;

d("@nagi-js/postgres — claimStep races a settle", () => {
  let db: Kysely<unknown>;
  let pool: pg.Pool;
  let schema: string;

  beforeAll(async () => {
    pool = new pg.Pool({ connectionString: url });
    db = new Kysely<unknown>({ dialect: new PostgresDialect({ pool }) });
    schema = `nagi_claimrace_${uuidv7().replace(/-/g, "").slice(0, 14)}`;
    await migrate(db, { schema });
  }, 30_000);

  afterAll(async () => {
    if (!db) return;
    await sql.raw(`DROP SCHEMA IF EXISTS ${schema} CASCADE`).execute(db);
    await db.destroy();
  }, 30_000);

  // The claim's INSERT blocks on the lease row the open settle has deleted and
  // goes through once it commits; the settle must still win.
  it("refuses a claim that waited on the settle's transaction", async () => {
    const store = postgresStore({ db, schema });
    const runId = `run-${uuidv7()}` as RunId;
    await store.tryStartRun(runId, {
      kind: "flow.started",
      runId,
      flowId: "claim-race",
      input: {},
      at: new Date(),
    });
    await store.appendFact(runId, {
      kind: "step.started",
      runId,
      stepId: "s",
      attempt: 1,
      stepKind: "task",
      at: new Date(),
    });
    expect(await store.claimStep(runId, "s", 1)).not.toBeNull();

    const settle = await pool.connect();
    try {
      await settle.query("BEGIN");
      await settle.query(
        `UPDATE ${schema}.step_run SET status = 'completed'
          WHERE run_id = $1 AND step_id = 's'`,
        [runId],
      );
      await settle.query(
        `DELETE FROM ${schema}.lease WHERE run_id = $1 AND step_id = 's'`,
        [runId],
      );

      let settledClaim: string | null | undefined;
      const claim = store.claimStep(runId, "s", 1).then((token) => {
        settledClaim = token;
      });
      await new Promise((resolve) => setTimeout(resolve, 200));
      expect(settledClaim).toBeUndefined();

      await settle.query("COMMIT");
      await claim;
      expect(settledClaim).toBeNull();
    } finally {
      settle.release();
    }

    const leases = await sql<{ n: number }>`
      SELECT count(*)::int AS n FROM ${sql.raw(`${schema}.lease`)}
       WHERE run_id = ${runId}
    `.execute(db);
    expect(leases.rows[0]?.n).toBe(0);
  }, 30_000);
});
