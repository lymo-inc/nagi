import { flow, InMemoryClock, InMemoryQueue, nagi } from "@nagi-js/core";
import { passthroughSchema } from "@nagi-js/core/testing";
import { Kysely, PostgresDialect, sql } from "kysely";
import pg from "pg";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { migrate } from "./migrations";
import { postgresStore } from "./store";
import { uuidv7 } from "./uuidv7";

const url = process.env["NAGI_POSTGRES_TEST_URL"];
const d = url ? describe : describe.skip;

// Prod pg_stat_statements, rc.25 era (2026-11-12 → 11-24): the full fact-log
// read `SELECT kind, at, payload FROM nagi.fact WHERE run_id = $1 ORDER BY seq`
// ran 742,557 times (18.2M rows, every step output included) for 11,360
// started steps — 65 per step. The cancel watcher folded the whole log every
// 250 ms for every running step just to ask "was I canceled?".
d("@nagi-js/postgres — cancel watcher probe", () => {
  let db: Kysely<unknown>;
  let pool: pg.Pool;
  let schema: string;
  const logLoads: string[] = [];

  beforeAll(async () => {
    pool = new pg.Pool({ connectionString: url });
    db = new Kysely<unknown>({
      dialect: new PostgresDialect({ pool }),
      log: (e) => {
        if (e.level === "query" && /SELECT kind, at, payload/.test(e.query.sql))
          logLoads.push(e.query.sql);
      },
    });
    schema = `nagi_probe_${uuidv7().replace(/-/g, "").slice(0, 16)}`;
    await migrate(db, { schema });
  }, 30_000);

  afterAll(async () => {
    if (!db) return;
    await sql.raw(`DROP SCHEMA IF EXISTS ${schema} CASCADE`).execute(db);
    await db.destroy();
  }, 30_000);

  it("a running step does not re-read its run's fact log while it waits", async () => {
    let during = 0;
    const f = flow({
      id: "probe-slow",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => ({
        slow: b.activity({
          run: async () => {
            const before = logLoads.length;
            await new Promise((r) => setTimeout(r, 1_000));
            during = logLoads.length - before;
            return 1;
          },
        }),
      }),
    });
    const wf = await nagi({
      flows: [f],
      store: postgresStore({ db, schema }),
      queue: new InMemoryQueue(),
      clock: new InMemoryClock(),
    });
    const runId = await wf.start(f, {});
    await wf.worker({ timerSweepIntervalMs: 0, reaperIntervalMs: 0 }).runOnce();
    const { rows } = await sql<{ status: string }>`
      SELECT status FROM ${sql.raw(`${schema}.workflow_run`)} WHERE run_id = ${runId}
    `.execute(db);
    expect(rows[0]?.status).toBe("completed");
    // Was 4 (one per 250 ms tick).
    expect(during).toBe(0);
  }, 30_000);

  it("a cancel still reaches the running step within a tick", async () => {
    let aborted = false;
    const f = flow({
      id: "probe-cancel",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => ({
        slow: b.activity({
          run: async ({ ctx }) => {
            await new Promise<void>((r) => {
              const t = setTimeout(r, 5_000);
              ctx.signal.addEventListener("abort", () => {
                aborted = true;
                clearTimeout(t);
                r();
              });
            });
            return 1;
          },
        }),
      }),
    });
    const wf = await nagi({
      flows: [f],
      store: postgresStore({ db, schema }),
      queue: new InMemoryQueue(),
      clock: new InMemoryClock(),
    });
    const runId = await wf.start(f, {});
    const done = wf
      .worker({ timerSweepIntervalMs: 0, reaperIntervalMs: 0 })
      .runOnce();
    await new Promise((r) => setTimeout(r, 200));
    const t0 = Date.now();
    await wf.cancel(runId);
    await done;
    expect(aborted).toBe(true);
    expect(Date.now() - t0).toBeLessThan(1_500);
  }, 30_000);
});
