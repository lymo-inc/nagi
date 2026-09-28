import type { Tx } from "@nagi-js/core";
import {
  type StoreContractHarness,
  storeContract,
} from "@nagi-js/core/testing";
import { Kysely, PostgresDialect, sql } from "kysely";
import pg from "pg";
import { afterAll, beforeAll, describe, it } from "vitest";
import { migrate } from "./migrations";
import { postgresStore } from "./store";
import type { StreamListener } from "./stream";
import { uuidv7 } from "./uuidv7";

const url = process.env["NAGI_POSTGRES_TEST_URL"];
const d = url ? describe : describe.skip;

// One LISTEN connection for the whole suite: each makeStore re-points it at the
// newest store, so a case never hears a previous store's hub.
function sharedListener(connectionString: string) {
  const client = new pg.Client({ connectionString });
  const handlers = new Map<string, (payload: string) => void>();
  const listened = new Set<string>();
  client.on("notification", (msg) => {
    if (msg.payload !== undefined) handlers.get(msg.channel)?.(msg.payload);
  });
  const listener: StreamListener = {
    async listen(channel, onNotify) {
      handlers.set(channel, onNotify);
      if (!listened.has(channel)) {
        listened.add(channel);
        await client.query(`LISTEN "${channel}"`);
      }
      return () => {
        if (handlers.get(channel) === onNotify) handlers.delete(channel);
      };
    },
  };
  return { client, listener };
}

d("Store contract — postgresStore", () => {
  let db: Kysely<unknown>;
  let pool: pg.Pool;
  let schema: string;
  let shared: ReturnType<typeof sharedListener>;

  beforeAll(async () => {
    pool = new pg.Pool({ connectionString: url });
    db = new Kysely<unknown>({ dialect: new PostgresDialect({ pool }) });
    schema = `nagi_contract_${uuidv7().replace(/-/g, "").slice(0, 16)}`;
    await migrate(db, { schema });
    shared = sharedListener(url as string);
    await shared.client.connect();
  }, 30_000);

  afterAll(async () => {
    await shared?.client.end().catch(() => undefined);
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
      const store = postgresStore({
        db,
        schema,
        leaseMs,
        listener: shared.listener,
      });
      await store.ready();
      return store;
    },
    withTx: (_store, body) =>
      db.transaction().execute((trx) => body(trx as unknown as Tx)),
  };

  for (const c of storeContract) {
    it(c.name, () => c.run(harness), 15_000);
  }
});
