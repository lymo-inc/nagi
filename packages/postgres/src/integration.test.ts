/**
 * End-to-end conformance against a real Postgres.
 *
 * Skipped unless `NAGI_POSTGRES_TEST_URL` is set, e.g.:
 *
 *   NAGI_POSTGRES_TEST_URL=postgres://postgres:postgres@localhost:5432/nagi_test \
 *     pnpm --filter @nagi-js/postgres test
 *
 * Each test uses a unique schema (`nagi_test_<uuid7>`) so concurrent runs and
 * stale data never interfere. Schema is dropped at the end.
 */
import {
  flow,
  InMemoryClock,
  InMemoryQueue,
  nagi,
  type RunId,
  type Wf,
} from "@nagi-js/core";
import { Kysely, PostgresDialect, sql } from "kysely";
import pg from "pg";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { migrate } from "./migrations";
import { postgresStore } from "./store";
import { uuidv7 } from "./uuidv7";

// biome-ignore lint/complexity/useLiteralKeys: index-signature access requires bracket notation under TS strict
const url = process.env["NAGI_POSTGRES_TEST_URL"];
const d = url ? describe : describe.skip;

d("@nagi-js/postgres — end-to-end conformance", () => {
  let db: Kysely<unknown>;
  let pool: pg.Pool;
  let schema: string;

  beforeAll(async () => {
    pool = new pg.Pool({ connectionString: url });
    db = new Kysely<unknown>({ dialect: new PostgresDialect({ pool }) });
    schema = `nagi_test_${uuidv7().replace(/-/g, "").slice(0, 16)}`;
    await migrate(db, { schema });
  }, 30_000);

  afterAll(async () => {
    if (!db) return;
    await sql.raw(`DROP SCHEMA IF EXISTS ${schema} CASCADE`).execute(db);
    // db.destroy() ends the underlying pg.Pool — do not call pool.end() again.
    await db.destroy();
  }, 30_000);

  async function makeNagi(
    ...flows: Parameters<typeof nagi>[0]["flows"]
  ): Promise<Wf> {
    return nagi({
      store: postgresStore({ db, schema }),
      queue: new InMemoryQueue(),
      clock: new InMemoryClock(),
      flows,
    });
  }

  async function runToEnd(wf: Wf, runId: RunId, timeoutMs = 10_000) {
    const ac = new AbortController();
    const worker = wf.worker({ pollIntervalMs: 5, signal: ac.signal });
    const done = worker.run();
    try {
      const start = Date.now();
      while (Date.now() - start < timeoutMs) {
        const r = await loadStatus(db, schema, runId);
        if (r === "completed" || r === "failed" || r === "canceled") return;
        await new Promise((res) => setTimeout(res, 10));
      }
      throw new Error("runToEnd: timeout");
    } finally {
      ac.abort();
      await done;
    }
  }

  it("runs a single-task flow end-to-end through the worker", async () => {
    const f = flow({
      id: "pg-single-task",
      input: passthroughSchema<{ x: number }>(),
      build: (b) => ({
        only: b.task({
          run: async ({ input }) => ({ doubled: input.x * 2 }),
        }),
      }),
      output(s) {
        return s.only;
      },
    });

    const wf = await makeNagi(f);
    const runId = await wf.start(f, { x: 21 });
    await runToEnd(wf, runId);

    const output = await loadOutput(db, schema, runId);
    expect(output).toEqual({ doubled: 42 });
  }, 15_000);

  it("memoizes step output across replay()", async () => {
    let invocations = 0;
    const f = flow({
      id: "pg-memoize",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => ({
        once: b.task({
          run: async () => {
            invocations++;
            return { invocations };
          },
        }),
      }),
    });

    const wf = await makeNagi(f);
    const runId = await wf.start(f, {});
    await runToEnd(wf, runId);

    // replay() on a completed run is a no-op; should NOT re-invoke the handler.
    await wf.replay(runId, { mode: "continue" });
    expect(invocations).toBe(1);
  }, 15_000);

  it("recordOnce / getOnce is durable and idempotent", async () => {
    const store = postgresStore({ db, schema });
    const runId = `run-${uuidv7()}` as RunId;

    expect(await store.getOnce(runId, "step", "scope")).toBeNull();
    await store.recordOnce(runId, "step", "scope", { value: 1 });
    expect(await store.getOnce(runId, "step", "scope")).toEqual({ value: 1 });
    // Second write must not overwrite — Option A locked: first writer wins.
    await store.recordOnce(runId, "step", "scope", { value: 2 });
    expect(await store.getOnce(runId, "step", "scope")).toEqual({ value: 1 });
  });

  it("claimStep returns null on a live lease", async () => {
    const store = postgresStore({ db, schema, leaseMs: 30_000 });
    const runId = `run-${uuidv7()}` as RunId;

    expect(await store.claimStep(runId, "step", 1)).not.toBeNull();
    expect(await store.claimStep(runId, "step", 1)).toBeNull();
  });

  it("claimStep re-acquires after lease expiry", async () => {
    const store = postgresStore({ db, schema, leaseMs: 50 });
    const runId = `run-${uuidv7()}` as RunId;

    expect(await store.claimStep(runId, "step", 1)).not.toBeNull();
    await new Promise((r) => setTimeout(r, 80));
    expect(await store.claimStep(runId, "step", 1)).not.toBeNull();
  });

  it("concurrent start() with the same runId produces one run and one dispatch", async () => {
    let invocations = 0;
    const f = flow({
      id: "pg-idempotent-start",
      input: passthroughSchema<{ x: number }>(),
      build: (b) => ({
        only: b.task({
          run: async ({ input }) => {
            invocations++;
            return { doubled: input.x * 2 };
          },
        }),
      }),
      output(s) {
        return s.only;
      },
    });

    const wf = await makeNagi(f);
    const supplied = `run-${uuidv7()}` as RunId;

    // Fire two concurrent start() calls. The Postgres ON CONFLICT DO NOTHING
    // on workflow_run.run_id must serialize them so exactly one writes the
    // flow.started fact and dispatches the work.
    const [a, b] = await Promise.all([
      wf.start(f, { x: 5 }, { runId: supplied }),
      wf.start(f, { x: 5 }, { runId: supplied }),
    ]);
    expect(a).toBe(supplied);
    expect(b).toBe(supplied);

    await runToEnd(wf, supplied);

    // Workflow row exists exactly once.
    const rows = await sql<{
      count: string;
    }>`SELECT COUNT(*)::text AS count FROM ${sql.raw(`${schema}.workflow_run`)} WHERE run_id = ${supplied}`.execute(
      db,
    );
    expect(rows.rows[0]?.count).toBe("1");

    // Exactly one flow.started fact.
    const facts = await sql<{
      count: string;
    }>`SELECT COUNT(*)::text AS count FROM ${sql.raw(`${schema}.fact`)} WHERE run_id = ${supplied} AND kind = 'flow.started'`.execute(
      db,
    );
    expect(facts.rows[0]?.count).toBe("1");

    // The task ran exactly once (no double-dispatch).
    expect(invocations).toBe(1);

    const output = await loadOutput(db, schema, supplied);
    expect(output).toEqual({ doubled: 10 });
  }, 15_000);

  it("cancel-in-progress: second start with the same concurrency key cancels the first", async () => {
    const f = flow({
      id: "pg-conc-basic",
      input: passthroughSchema<{ videoId: string }>(),
      concurrency: {
        keyFn: (input) => input.videoId,
        mode: "cancel-in-progress",
      },
      build: (b) => ({
        analyze: b.task({
          run: async ({ input }) => {
            // Hold the step briefly so the second start can land first.
            await new Promise((r) => setTimeout(r, 50));
            return { v: input.videoId };
          },
        }),
      }),
    });

    const wf = await makeNagi(f);
    const first = await wf.start(f, { videoId: "v1" });
    const second = await wf.start(f, { videoId: "v1" });
    expect(first).not.toBe(second);

    // First run is canceled (in workflow_run.status), and the fact log has
    // a flow.canceled entry pointing at the second run.
    await runToEnd(wf, first);
    const firstStatus = await loadStatus(db, schema, first);
    expect(firstStatus).toBe("canceled");

    const canceledRow = await sql<{
      canceled_by_run_id: string | null;
    }>`SELECT canceled_by_run_id FROM ${sql.raw(`${schema}.workflow_run`)} WHERE run_id = ${first}`.execute(
      db,
    );
    expect(canceledRow.rows[0]?.canceled_by_run_id).toBe(second);

    const cancelFact = await sql<{
      payload: { canceledByRunId: string; concurrencyKey: string };
    }>`SELECT payload FROM ${sql.raw(`${schema}.fact`)} WHERE run_id = ${first} AND kind = 'flow.canceled'`.execute(
      db,
    );
    expect(cancelFact.rows[0]?.payload.canceledByRunId).toBe(second);
    expect(cancelFact.rows[0]?.payload.concurrencyKey).toBe("v1");

    // Second run completes successfully.
    await runToEnd(wf, second);
    const secondStatus = await loadStatus(db, schema, second);
    expect(secondStatus).toBe("completed");
  }, 20_000);

  it("partial unique index rejects direct insert of a second active row for the same key", async () => {
    const runIdA = `run-${uuidv7()}` as RunId;
    const runIdB = `run-${uuidv7()}` as RunId;
    const flowId = "pg-conc-uidx";
    const key = "k1";

    // Insert run A as 'running' with concurrency_key = 'k1'. OK.
    await sql`
      INSERT INTO ${sql.raw(`${schema}.workflow_run`)}
        (run_id, flow_id, status, input, started_at, concurrency_key)
      VALUES (${runIdA}, ${flowId}, 'running', '{}'::jsonb, now(), ${key})
    `.execute(db);

    // Try to insert run B as 'running' with same flow_id + key → should fail
    // due to partial unique index.
    await expect(
      sql`
        INSERT INTO ${sql.raw(`${schema}.workflow_run`)}
          (run_id, flow_id, status, input, started_at, concurrency_key)
        VALUES (${runIdB}, ${flowId}, 'running', '{}'::jsonb, now(), ${key})
      `.execute(db),
    ).rejects.toThrow(/workflow_run_concurrency_active_uidx/);

    // After A transitions to canceled, the index slot is free.
    await sql`
      UPDATE ${sql.raw(`${schema}.workflow_run`)} SET status = 'canceled' WHERE run_id = ${runIdA}
    `.execute(db);
    await sql`
      INSERT INTO ${sql.raw(`${schema}.workflow_run`)}
        (run_id, flow_id, status, input, started_at, concurrency_key)
      VALUES (${runIdB}, ${flowId}, 'running', '{}'::jsonb, now(), ${key})
    `.execute(db);

    // Cleanup so subsequent tests aren't affected.
    await sql`DELETE FROM ${sql.raw(`${schema}.workflow_run`)} WHERE run_id IN (${runIdA}, ${runIdB})`.execute(
      db,
    );
  }, 15_000);

  it("concurrent starts with the same key produce exactly one active run", async () => {
    const f = flow({
      id: "pg-conc-race",
      input: passthroughSchema<{ key: string }>(),
      concurrency: {
        keyFn: (input) => input.key,
        mode: "cancel-in-progress",
      },
      build: (b) => ({
        only: b.task({ run: async ({ input }) => ({ k: input.key }) }),
      }),
    });

    const wf = await makeNagi(f);
    const key = "race-key";

    // Fire 8 concurrent start() calls. The advisory lock + cancel-then-insert
    // path must serialize them so exactly one ends up running and no two
    // active rows for the same key coexist (which would violate the partial
    // unique index).
    const runIds = await Promise.all(
      Array.from({ length: 8 }, () => wf.start(f, { key })),
    );
    expect(new Set(runIds).size).toBe(8);

    // Exactly one row should be in an active status.
    const activeCount = await sql<{
      count: string;
    }>`SELECT COUNT(*)::text AS count FROM ${sql.raw(`${schema}.workflow_run`)}
       WHERE flow_id = ${f.id} AND concurrency_key = ${key} AND status IN ('pending', 'running')
    `.execute(db);
    expect(activeCount.rows[0]?.count).toBe("1");

    // All other runs should be in 'canceled' status.
    const canceledCount = await sql<{
      count: string;
    }>`SELECT COUNT(*)::text AS count FROM ${sql.raw(`${schema}.workflow_run`)}
       WHERE flow_id = ${f.id} AND concurrency_key = ${key} AND status = 'canceled'
    `.execute(db);
    expect(canceledCount.rows[0]?.count).toBe("7");
  }, 30_000);

  it("start() with a previously-used runId is an idempotent no-op", async () => {
    const f = flow({
      id: "pg-idempotent-replay",
      input: passthroughSchema<{ x: number }>(),
      build: (b) => ({
        only: b.task({ run: async ({ input }) => ({ v: input.x }) }),
      }),
    });

    const wf = await makeNagi(f);
    const supplied = `run-${uuidv7()}` as RunId;

    const first = await wf.start(f, { x: 1 }, { runId: supplied });
    await runToEnd(wf, supplied);

    // Second start with same runId after completion: must return same id
    // without re-appending or clobbering the original input.
    const second = await wf.start(f, { x: 999 }, { runId: supplied });
    expect(second).toBe(first);

    const facts = await sql<{
      count: string;
    }>`SELECT COUNT(*)::text AS count FROM ${sql.raw(`${schema}.fact`)} WHERE run_id = ${supplied} AND kind = 'flow.started'`.execute(
      db,
    );
    expect(facts.rows[0]?.count).toBe("1");

    // Original input preserved.
    const row = await sql<{
      input: { x: number };
    }>`SELECT input FROM ${sql.raw(`${schema}.workflow_run`)} WHERE run_id = ${supplied}`.execute(
      db,
    );
    expect(row.rows[0]?.input).toEqual({ x: 1 });
  }, 15_000);
});

function passthroughSchema<T>() {
  return {
    "~standard": {
      version: 1 as const,
      vendor: "nagi-test",
      validate: (value: unknown) => ({ value: value as T }),
    },
  };
}

async function loadStatus(
  db: Kysely<unknown>,
  schema: string,
  runId: RunId,
): Promise<string> {
  const r = await sql<{
    status: string;
  }>`SELECT status FROM ${sql.raw(`${schema}.workflow_run`)} WHERE run_id = ${runId}`.execute(
    db,
  );
  return r.rows[0]?.status ?? "missing";
}

async function loadOutput(
  db: Kysely<unknown>,
  schema: string,
  runId: RunId,
): Promise<unknown> {
  const r = await sql<{
    output: unknown;
  }>`SELECT output FROM ${sql.raw(`${schema}.workflow_run`)} WHERE run_id = ${runId}`.execute(
    db,
  );
  return r.rows[0]?.output ?? null;
}
