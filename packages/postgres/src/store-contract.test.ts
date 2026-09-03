import {
  type StoreContractHarness,
  storeContract,
} from "@nagi-js/core/testing";
import { Kysely, PostgresDialect, sql } from "kysely";
import pg from "pg";
import { afterAll, beforeAll, describe, it } from "vitest";
import { migrate } from "./migrations";
import { postgresStore } from "./store";
import { uuidv7 } from "./uuidv7";

const url = process.env["NAGI_POSTGRES_TEST_URL"];
const d = url ? describe : describe.skip;

d("Store contract — postgresStore", () => {
  let db: Kysely<unknown>;
  let pool: pg.Pool;
  let schema: string;

  beforeAll(async () => {
    pool = new pg.Pool({ connectionString: url });
    db = new Kysely<unknown>({ dialect: new PostgresDialect({ pool }) });
    schema = `nagi_contract_${uuidv7().replace(/-/g, "").slice(0, 16)}`;
    await migrate(db, { schema });
  }, 30_000);

  afterAll(async () => {
    if (!db) return;
    await sql.raw(`DROP SCHEMA IF EXISTS ${schema} CASCADE`).execute(db);
    await db.destroy();
  }, 30_000);

  const harness: StoreContractHarness = {
    async makeStore({ leaseMs }) {
      const tables = [
        "fact",
        "step_run",
        "lease",
        "timer",
        "dedupe",
        "workflow_run",
        "flow_snapshot",
        "flow_ref",
        "global_fact",
      ];
      await sql
        .raw(`TRUNCATE ${tables.map((t) => `${schema}.${t}`).join(", ")}`)
        .execute(db);
      return postgresStore({ db, schema, leaseMs });
    },
  };

  for (const c of storeContract) {
    it(c.name, () => c.run(harness), 15_000);
  }
});
