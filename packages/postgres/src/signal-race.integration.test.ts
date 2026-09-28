import { type RunId, stepStateOf } from "@nagi-js/core";
import { Kysely, PostgresDialect, sql } from "kysely";
import pg from "pg";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { migrate } from "./migrations";
import { postgresStore } from "./store";
import { uuidv7 } from "./uuidv7";

const url = process.env["NAGI_POSTGRES_TEST_URL"];
const d = url ? describe : describe.skip;

const ITERATIONS = 32;
// The sweep reads due timers in a round trip before it takes the per-run lock,
// so fired at the same instant delivery nearly always wins. Staggering the
// signal across a few ms scans the window where both contend for the lock.
// Which side wins is machine-dependent, so only the invariants are asserted.
const STAGGER_MS = [0, 1, 2, 3, 5, 8];

d("@nagi-js/postgres — settleSignal races sweepSignalTimeouts", () => {
  let db: Kysely<unknown>;
  let pool: pg.Pool;
  let schema: string;

  beforeAll(async () => {
    pool = new pg.Pool({ connectionString: url });
    db = new Kysely<unknown>({ dialect: new PostgresDialect({ pool }) });
    schema = `nagi_sigrace_${uuidv7().replace(/-/g, "").slice(0, 14)}`;
    await migrate(db, { schema });
  }, 30_000);

  afterAll(async () => {
    if (!db) return;
    await sql.raw(`DROP SCHEMA IF EXISTS ${schema} CASCADE`).execute(db);
    await db.destroy();
  }, 30_000);

  it("exactly one of delivery or timeout resolves an awaiting step whose timer is due", async () => {
    const store = postgresStore({ db, schema });

    for (let i = 0; i < ITERATIONS; i++) {
      const runId = `run-${uuidv7()}` as RunId;
      await store.tryStartRun(runId, {
        kind: "flow.started",
        runId,
        flowId: "signal-race",
        input: {},
        at: new Date(),
      });
      await store.appendFact(runId, {
        kind: "step.started",
        runId,
        stepId: "gate",
        attempt: 1,
        stepKind: "signal",
        at: new Date(),
      });
      expect(await store.claimStep(runId, "gate", 1)).not.toBeNull();
      const fireAt = new Date(Date.now() - 1000);
      await store.upsertTimer(runId, "gate", fireAt);

      const stagger = STAGGER_MS[i % STAGGER_MS.length] ?? 0;
      const [swept, res] = await Promise.all([
        store.sweepSignalTimeouts({ now: new Date() }),
        // setTimeout(0) still waits ~1ms, which would collapse the 0 slot into 1.
        (stagger === 0
          ? Promise.resolve()
          : new Promise((r) => setTimeout(r, stagger))
        ).then(() =>
          store.settleSignal({
            runId,
            stepId: "gate",
            at: new Date(),
            incoming: { payload: { i } },
          }),
        ),
      ]);

      const state = await store.loadRunState(runId);
      const count = (kind: string) =>
        state.facts.filter((f) => f.kind === kind).length;
      const resolution = {
        received: count("signal.received"),
        completed: count("step.completed"),
        failed: count("step.failed"),
      };
      expect([
        { received: 1, completed: 1, failed: 0 },
        { received: 0, completed: 0, failed: 1 },
      ]).toContainEqual(resolution);

      const step = stepStateOf(state, "gate");
      const view = (await store.describe(runId))?.steps.find(
        (s) => s.stepId === "gate",
      );
      if (resolution.received === 1) {
        expect(res).toEqual({ tag: "delivered", attempt: 1, payload: { i } });
        expect(swept).toEqual([]);
        expect(step.tag).toBe("completed");
        expect(view?.status).toBe("completed");
      } else {
        expect(res).toEqual({ tag: "noop" });
        expect(swept).toEqual([{ runId, stepId: "gate", attempt: 1, fireAt }]);
        expect(step.tag === "failed" && step.error.name).toBe(
          "NagiSignalTimeoutError",
        );
        expect(view?.status).toBe("failed");
        expect(view?.error).toMatchObject({ name: "NagiSignalTimeoutError" });
      }

      const timers = await sql<{ n: number }>`
        SELECT count(*)::int AS n FROM ${sql.raw(`${schema}.timer`)}
         WHERE run_id = ${runId}
      `.execute(db);
      expect(timers.rows[0]?.n).toBe(0);
      expect(await store.claimStep(runId, "gate", 1)).not.toBeNull();
    }
  }, 60_000);
});
