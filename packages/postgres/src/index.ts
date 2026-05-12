// @nagi-js/postgres — Postgres Store adapter (Kysely-shaped).
//
// Wiring:
//   import { postgresStore, postgresTrigger, migrate } from "@nagi-js/postgres";
//   import { Kysely, PostgresDialect } from "kysely";
//   import pg from "pg";
//
//   const pool = new pg.Pool({ connectionString: process.env.DATABASE_URL });
//   const db   = new Kysely<MyDB>({ dialect: new PostgresDialect({ pool }) });
//   await migrate(db);
//
//   const wf = nagi({
//     store:   postgresStore({ db, notifyChannel: "nagi" }),
//     queue:   pgmqQueue({ db, queueName: "nagi-default" }),
//     trigger: postgresTrigger({ listen: dedicatedPgClient, channel: "nagi" }),
//     flows:   [...],
//   });
//
// Typing `ctx.tx`: users opt into a typed transaction handle via the
// `Register` augmentation pattern (drizzle-style):
//
//   declare module "@nagi-js/core" {
//     interface Register {
//       tx: import("kysely").Kysely<MyDB>;
//     }
//   }

export type { MigrateOpts, Migration } from "./migrations";
export { migrate, migrations } from "./migrations";
export type { PostgresStoreOpts } from "./store";
export { postgresStore } from "./store";
export type {
  ListenClient,
  NotificationMessage,
  PostgresTriggerOpts,
} from "./trigger";
export { postgresTrigger } from "./trigger";
export { uuidv7 } from "./uuidv7";
