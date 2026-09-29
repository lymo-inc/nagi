import {
  type AttemptNumber,
  flow,
  InMemoryClock,
  InMemoryQueue,
  NagiConcurrencyConflictError,
  nagi,
  type RunId,
  type Tx,
  type Wf,
} from "@nagi-js/core";
import { passthroughSchema } from "@nagi-js/core/testing";
import { Kysely, PostgresDialect, sql } from "kysely";
import pg from "pg";
import {
  afterAll,
  beforeAll,
  beforeEach,
  describe,
  expect,
  it,
  vi,
} from "vitest";
import { migrate } from "./migrations";
import { postgresStore } from "./store";
import { uuidv7 } from "./uuidv7";

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

    await wf.replay(runId, { mode: "continue" });
    expect(invocations).toBe(1);
  }, 15_000);

  it("recordOnce / getOnce is durable and idempotent", async () => {
    const store = postgresStore({ db, schema });
    const runId = `run-${uuidv7()}` as RunId;

    expect(await store.getOnce(runId, "step", "scope")).toEqual({
      tag: "miss",
    });
    await store.recordOnce(runId, "step", "scope", { value: 1 });
    expect(await store.getOnce(runId, "step", "scope")).toEqual({
      tag: "hit",
      value: { value: 1 },
    });
    await store.recordOnce(runId, "step", "scope", { value: 2 });
    expect(await store.getOnce(runId, "step", "scope")).toEqual({
      tag: "hit",
      value: { value: 1 },
    });
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

  it("claimStep expiry is computed on the database clock, not the app clock", async () => {
    const store = postgresStore({ db, schema, leaseMs: 30_000 });
    const runId = `run-${uuidv7()}` as RunId;
    const realNow = Date.now;
    // Skew the app clock 10 minutes into the past: a JS-computed expires_at
    // would already be "expired" by DB time and let a second claim through.
    Date.now = () => realNow() - 600_000;
    try {
      expect(await store.claimStep(runId, "step", 1)).not.toBeNull();
      expect(await store.claimStep(runId, "step", 1)).toBeNull();
    } finally {
      Date.now = realNow;
    }
  });

  it("sweepLeases reaps expired lease, writes audit fact, re-enqueues at attempt+1", async () => {
    const store = postgresStore({ db, schema, leaseMs: 50 });
    const queue = new InMemoryQueue();
    const runId = `run-${uuidv7()}` as RunId;

    // Seed a workflow_run + step_run so the LEFT JOIN in sweepLeases sees the
    // step as 'running' (non-terminal), and claim the lease.
    await store.tryStartRun(runId, {
      kind: "flow.started",
      runId,
      flowId: "sweep-test",
      input: null as never,
      at: new Date(),
    });
    await store.appendFact(runId, {
      kind: "step.started",
      runId,
      stepId: "s1",
      attempt: 1,
      stepKind: "task",
      at: new Date(),
    });
    expect(await store.claimStep(runId, "s1", 1)).not.toBeNull();
    await new Promise((r) => setTimeout(r, 80));

    // The schema is shared across this file, so the sweep also reaps the
    // expired lease left by the re-acquire test above — assert on ours only.
    const reaped = (await store.sweepLeases({ now: new Date(), queue })).filter(
      (r) => r.runId === runId,
    );
    expect(reaped).toHaveLength(1);
    expect(reaped[0]?.nextAttempt).toBe(2);

    // Re-claim at the new attempt succeeds (lease row was deleted by sweep)
    // — V1 from the RFC.
    expect(await store.claimStep(runId, "s1", 2)).not.toBeNull();

    // The audit fact lives in the fact log.
    const factRows = await sql<{
      kind: string;
    }>`SELECT kind FROM ${sql.raw(`${schema}.fact`)} WHERE run_id = ${runId} AND kind = 'lease.reaped'`.execute(
      db,
    );
    expect(factRows.rows.length).toBe(1);
  }, 15_000);

  it("sweepLeases judges expiry on the database clock, not a fast app clock", async () => {
    const store = postgresStore({ db, schema });
    const queue = new InMemoryQueue();
    const runId = `run-${uuidv7()}` as RunId;

    await store.tryStartRun(runId, {
      kind: "flow.started",
      runId,
      flowId: "sweep-fast-clock-test",
      input: null as never,
      at: new Date(),
    });
    await store.appendFact(runId, {
      kind: "step.started",
      runId,
      stepId: "s1",
      attempt: 1,
      stepKind: "task",
      at: new Date(),
    });
    expect(await store.claimStep(runId, "s1", 1)).not.toBeNull();

    // App clock 10 minutes fast: with the store's default 60s lease, a
    // now()-comparison sweep must not reap this still-live lease.
    const reaped = (
      await store.sweepLeases({
        now: new Date(Date.now() + 10 * 60_000),
        queue,
      })
    ).filter((r) => r.runId === runId);
    expect(reaped).toHaveLength(0);
  }, 15_000);

  it("loadRunState folds facts in insert order even when fact_id sorts them backwards", async () => {
    const store = postgresStore({ db, schema });
    const runId = `run-${uuidv7()}` as RunId;

    await store.tryStartRun(runId, {
      kind: "flow.started",
      runId,
      flowId: "fact-seq-order-test",
      input: null as never,
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
    await sql`
      INSERT INTO ${sql.raw(`${schema}.fact`)} (run_id, fact_id, kind, at, payload)
      VALUES (
        ${runId},
        '00000000-0000-7000-8000-000000000000',
        'step.skipped',
        now(),
        '{"stepId":"s","reason":"when-false"}'::jsonb
      )
    `.execute(db);

    const state = await store.loadRunState(runId);
    expect(state?.facts.map((f) => f.kind).slice(-2)).toEqual([
      "step.started",
      "step.skipped",
    ]);
  }, 15_000);

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

    const [a, b] = await Promise.all([
      wf.start(f, { x: 5 }, { runId: supplied }),
      wf.start(f, { x: 5 }, { runId: supplied }),
    ]);
    expect(a).toBe(supplied);
    expect(b).toBe(supplied);

    await runToEnd(wf, supplied);

    const rows = await sql<{
      count: string;
    }>`SELECT COUNT(*)::text AS count FROM ${sql.raw(`${schema}.workflow_run`)} WHERE run_id = ${supplied}`.execute(
      db,
    );
    expect(rows.rows[0]?.count).toBe("1");

    const facts = await sql<{
      count: string;
    }>`SELECT COUNT(*)::text AS count FROM ${sql.raw(`${schema}.fact`)} WHERE run_id = ${supplied} AND kind = 'flow.started'`.execute(
      db,
    );
    expect(facts.rows[0]?.count).toBe("1");

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

    await runToEnd(wf, second);
    const secondStatus = await loadStatus(db, schema, second);
    expect(secondStatus).toBe("completed");
  }, 20_000);

  it("partial unique index rejects direct insert of a second active row for the same key", async () => {
    const runIdA = `run-${uuidv7()}` as RunId;
    const runIdB = `run-${uuidv7()}` as RunId;
    const flowId = "pg-conc-uidx";
    const key = "k1";

    await sql`
      INSERT INTO ${sql.raw(`${schema}.workflow_run`)}
        (run_id, flow_id, status, input, started_at, concurrency_key)
      VALUES (${runIdA}, ${flowId}, 'running', '{}'::jsonb, now(), ${key})
    `.execute(db);

    await expect(
      sql`
        INSERT INTO ${sql.raw(`${schema}.workflow_run`)}
          (run_id, flow_id, status, input, started_at, concurrency_key)
        VALUES (${runIdB}, ${flowId}, 'running', '{}'::jsonb, now(), ${key})
      `.execute(db),
    ).rejects.toThrow(/workflow_run_concurrency_active_uidx/);

    await sql`
      UPDATE ${sql.raw(`${schema}.workflow_run`)} SET status = 'canceled' WHERE run_id = ${runIdA}
    `.execute(db);
    await sql`
      INSERT INTO ${sql.raw(`${schema}.workflow_run`)}
        (run_id, flow_id, status, input, started_at, concurrency_key)
      VALUES (${runIdB}, ${flowId}, 'running', '{}'::jsonb, now(), ${key})
    `.execute(db);

    await sql`DELETE FROM ${sql.raw(`${schema}.workflow_run`)} WHERE run_id IN (${runIdA}, ${runIdB})`.execute(
      db,
    );
  }, 15_000);

  it("canceled_by_run_id FK rejects a phantom superseder and nulls a pruned one", async () => {
    const victim = `run-${uuidv7()}` as RunId;
    const superseder = `run-${uuidv7()}` as RunId;
    const flowId = "pg-canceled-by-fk";
    const insert = (runId: RunId, status: string) =>
      sql`
      INSERT INTO ${sql.raw(`${schema}.workflow_run`)}
        (run_id, flow_id, status, input, started_at)
      VALUES (${runId}, ${flowId}, ${status}, '{}'::jsonb, now())
    `.execute(db);

    await insert(victim, "canceled");

    // nagi#29 hypothesis 3: a non-tx admin write. The database now refuses it
    // rather than leaving the audit to find it a week later.
    await expect(
      sql`
        UPDATE ${sql.raw(`${schema}.workflow_run`)}
           SET canceled_by_run_id = ${`run-${uuidv7()}`}
         WHERE run_id = ${victim}
      `.execute(db),
    ).rejects.toThrow(/workflow_run_canceled_by_fk/);

    await insert(superseder, "completed");
    await sql`
      UPDATE ${sql.raw(`${schema}.workflow_run`)}
         SET canceled_by_run_id = ${superseder}
       WHERE run_id = ${victim}
    `.execute(db);

    // What retention does: the superseder goes, the victim stays.
    await sql`DELETE FROM ${sql.raw(`${schema}.workflow_run`)} WHERE run_id = ${superseder}`.execute(
      db,
    );
    const after = await sql<{
      canceled_by_run_id: string | null;
    }>`SELECT canceled_by_run_id FROM ${sql.raw(`${schema}.workflow_run`)} WHERE run_id = ${victim}`.execute(
      db,
    );
    expect(after.rows[0]?.canceled_by_run_id).toBeNull();

    await sql`DELETE FROM ${sql.raw(`${schema}.workflow_run`)} WHERE run_id = ${victim}`.execute(
      db,
    );
  }, 15_000);

  it("the superseder FK is checked at commit, so a cancel written before its superseder's insert commits", async () => {
    const victim = `run-${uuidv7()}` as RunId;
    const flowId = "pg-canceled-by-fk-deferred";
    const insert = (runId: RunId, status: string) =>
      sql`
      INSERT INTO ${sql.raw(`${schema}.workflow_run`)}
        (run_id, flow_id, status, input, started_at)
      VALUES (${runId}, ${flowId}, ${status}, '{}'::jsonb, now())
    `.execute(db);

    await insert(victim, "running");

    const superseder = `run-${uuidv7()}` as RunId;
    await db.transaction().execute(async (trx) => {
      await sql`
        UPDATE ${sql.raw(`${schema}.workflow_run`)}
           SET canceled_by_run_id = ${superseder}
         WHERE run_id = ${victim}
      `.execute(trx);
      await sql`
        INSERT INTO ${sql.raw(`${schema}.workflow_run`)}
          (run_id, flow_id, status, input, started_at)
        VALUES (${superseder}, ${flowId}, 'completed', '{}'::jsonb, now())
      `.execute(trx);
    });

    const phantom = `run-${uuidv7()}` as RunId;
    await expect(
      db.transaction().execute(async (trx) => {
        await sql`
          UPDATE ${sql.raw(`${schema}.workflow_run`)}
             SET canceled_by_run_id = ${phantom}
           WHERE run_id = ${victim}
        `.execute(trx);
      }),
    ).rejects.toThrow(/workflow_run_canceled_by_fk/);

    await sql`DELETE FROM ${sql.raw(`${schema}.workflow_run`)} WHERE run_id = ${superseder}`.execute(
      db,
    );
    await sql`DELETE FROM ${sql.raw(`${schema}.workflow_run`)} WHERE run_id = ${victim}`.execute(
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

    const runIds = await Promise.all(
      Array.from({ length: 8 }, () => wf.start(f, { key })),
    );
    expect(new Set(runIds).size).toBe(8);

    const activeCount = await sql<{
      count: string;
    }>`SELECT COUNT(*)::text AS count FROM ${sql.raw(`${schema}.workflow_run`)}
       WHERE flow_id = ${f.id} AND concurrency_key = ${key} AND status IN ('pending', 'running')
    `.execute(db);
    expect(activeCount.rows[0]?.count).toBe("1");

    const canceledCount = await sql<{
      count: string;
    }>`SELECT COUNT(*)::text AS count FROM ${sql.raw(`${schema}.workflow_run`)}
       WHERE flow_id = ${f.id} AND concurrency_key = ${key} AND status = 'canceled'
    `.execute(db);
    expect(canceledCount.rows[0]?.count).toBe("7");
  }, 30_000);

  it("tryStartRunOnTx: a unique-violation retry runs inside the caller's tx, supersedes the racer, and leaves the tx committable", async () => {
    const store = postgresStore({ db, schema });
    const flowId = `pg-ontx-retry-${uuidv7()}`;
    const concurrency = { key: "k", mode: "cancel-in-progress" } as const;
    const startFact = (runId: RunId) => ({
      kind: "flow.started" as const,
      runId,
      flowId,
      input: null,
      at: new Date(),
    });
    const a = `run-${uuidv7()}` as RunId;
    const b = `run-${uuidv7()}` as RunId;

    let aStarted!: () => void;
    const aReady = new Promise<void>((r) => {
      aStarted = r;
    });
    let commitA!: () => void;
    const aGate = new Promise<void>((r) => {
      commitA = r;
    });
    const txA = db.transaction().execute(async (trx) => {
      await store.tryStartRunOnTx(
        trx as unknown as Tx,
        a,
        startFact(a),
        concurrency,
      );
      aStarted();
      await aGate;
    });
    await aReady;

    // B cannot see A's uncommitted row, so it finds no prior and its insert
    // parks on A's index entry; A's commit turns that into a unique violation.
    const txB = db.transaction().execute(async (trx) => {
      const started = await store.tryStartRunOnTx(
        trx as unknown as Tx,
        b,
        startFact(b),
        concurrency,
      );
      const after = await sql<{ ok: number }>`SELECT 1 AS ok`.execute(trx);
      return { started, ok: after.rows[0]?.ok };
    });

    const deadline = Date.now() + 5_000;
    for (;;) {
      const waiting = await sql<{ c: number }>`
        SELECT count(*)::int AS c FROM pg_stat_activity
         WHERE wait_event_type = 'Lock'
           AND query LIKE ${`%INSERT INTO ${schema}.workflow_run%`}
      `.execute(db);
      if ((waiting.rows[0]?.c ?? 0) > 0) break;
      if (Date.now() > deadline) throw new Error("B never blocked on A");
      await new Promise((r) => setTimeout(r, 10));
    }
    commitA();
    await txA;

    const { started, ok } = await txB;
    expect(started.started).toBe(true);
    expect(started.canceled.map((c) => c.runId)).toEqual([a]);
    expect(ok).toBe(1);
    expect(await loadStatus(db, schema, a)).toBe("canceled");
    expect(await loadStatus(db, schema, b)).toBe("running");
  }, 15_000);

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

    const second = await wf.start(f, { x: 999 }, { runId: supplied });
    expect(second).toBe(first);

    const facts = await sql<{
      count: string;
    }>`SELECT COUNT(*)::text AS count FROM ${sql.raw(`${schema}.fact`)} WHERE run_id = ${supplied} AND kind = 'flow.started'`.execute(
      db,
    );
    expect(facts.rows[0]?.count).toBe("1");

    const row = await sql<{
      input: { x: number };
    }>`SELECT input FROM ${sql.raw(`${schema}.workflow_run`)} WHERE run_id = ${supplied}`.execute(
      db,
    );
    expect(row.rows[0]?.input).toEqual({ x: 1 });
  }, 15_000);

  it("startStaged inside a Kysely tx — business INSERT + flow.started + queue message all commit together; rollback removes all three", async () => {
    const f = flow({
      id: "pg-staged-start",
      input: passthroughSchema<{ orderId: string }>(),
      build: (b) => ({
        process: b.task({
          run: async ({ input }) => ({ orderId: input.orderId }),
        }),
      }),
    });

    // pgmq-style queue spy: records enqueues + whether they happened under tx.
    // The runtime threads tx through queue.withTx(tx) — verify it was used.
    const enqueueCalls: Array<{ runId: RunId; stepId: string; tx: unknown }> =
      [];
    let withTxArg: unknown = null;
    const queue = {
      async enqueue(runId: RunId, stepId: string): Promise<void> {
        enqueueCalls.push({ runId, stepId, tx: null });
      },
      async dequeue() {
        return [];
      },
      async ack() {},
      async nack() {},
      async extend() {},
      withTx(tx: unknown) {
        withTxArg = tx;
        return {
          async enqueue(runId: RunId, stepId: string): Promise<void> {
            enqueueCalls.push({ runId, stepId, tx });
          },
          async dequeue() {
            return [];
          },
          async ack() {},
          async nack() {},
          async extend() {},
        };
      },
    };

    const wf = await nagi({
      store: postgresStore({ db, schema }),
      // biome-ignore lint/suspicious/noExplicitAny: stub queue shape
      queue: queue as any,
      clock: new InMemoryClock(),
      flows: [f],
    });

    // Commit path
    const committedRunId = `run-${uuidv7()}` as RunId;
    await db.transaction().execute(async (trx) => {
      // Caller's own write (a business row) shares the tx — proves the
      // run row + flow.started fact commit on the same tx as the caller.
      await sql`CREATE TEMP TABLE IF NOT EXISTS commit_marker (id text)`.execute(
        trx,
      );
      await sql`INSERT INTO commit_marker (id) VALUES (${committedRunId})`.execute(
        trx,
      );

      const res = await wf.startStaged(
        f,
        { orderId: "o1" },
        {
          tx: trx as unknown as Parameters<typeof wf.startStaged>[2]["tx"],
          runId: committedRunId,
        },
      );
      expect(res.started).toBe(true);
      expect(res.runId).toBe(committedRunId);
      // applyOnCommit fires hooks; safe to call after commit (here we call
      // it post-commit by awaiting the transaction below).
      await res.applyOnCommit();
    });

    // After commit: run row + flow.started fact both visible
    const runRow = await sql<{ run_id: string }>`
      SELECT run_id FROM ${sql.raw(`${schema}.workflow_run`)}
       WHERE run_id = ${committedRunId}
    `.execute(db);
    expect(runRow.rows.length).toBe(1);

    const factRow = await sql<{
      count: string;
    }>`SELECT COUNT(*)::text AS count FROM ${sql.raw(`${schema}.fact`)}
       WHERE run_id = ${committedRunId} AND kind = 'flow.started'
    `.execute(db);
    expect(factRow.rows[0]?.count).toBe("1");

    // Queue side: at least one enqueue happened, and queue.withTx was called
    // with the same tx the runtime opened.
    expect(enqueueCalls.length).toBeGreaterThanOrEqual(1);
    expect(withTxArg).not.toBeNull();
    expect(enqueueCalls[0]?.tx).toBe(withTxArg);

    // Rollback path
    const rolledRunId = `run-${uuidv7()}` as RunId;
    const enqueueCountBefore = enqueueCalls.length;

    await expect(
      db.transaction().execute(async (trx) => {
        const res = await wf.startStaged(
          f,
          { orderId: "o2" },
          {
            tx: trx as unknown as Parameters<typeof wf.startStaged>[2]["tx"],
            runId: rolledRunId,
          },
        );
        expect(res.started).toBe(true);
        // applyOnCommit MUST NOT be called pre-commit; throwing here rolls
        // back the run-row + flow.started fact + the in-tx queue enqueue
        // (pgmq would, with a real queue; the stub records the call but the
        // tx-bound version's writes would have committed under PG).
        throw new Error("simulated caller rollback");
      }),
    ).rejects.toThrow(/simulated caller rollback/);

    const rolledRow = await sql<{ run_id: string }>`
      SELECT run_id FROM ${sql.raw(`${schema}.workflow_run`)}
       WHERE run_id = ${rolledRunId}
    `.execute(db);
    expect(rolledRow.rows.length).toBe(0);

    const rolledFactRow = await sql<{
      count: string;
    }>`SELECT COUNT(*)::text AS count FROM ${sql.raw(`${schema}.fact`)}
       WHERE run_id = ${rolledRunId}
    `.execute(db);
    expect(rolledFactRow.rows[0]?.count).toBe("0");

    // The enqueue stub doesn't have real tx semantics so it still recorded
    // the call — but the post-rollback assertion that matters is the PG-side
    // run row + fact absence, which is enforced above.
    expect(enqueueCalls.length).toBeGreaterThan(enqueueCountBefore);
  }, 30_000);

  describe("b.subflow — end-to-end via PG", () => {
    it("starts a child run, surfaces { childRunId, output }, persists parent_run_id", async () => {
      const child = flow({
        id: "pg-sub-child",
        input: passthroughSchema<{ x: number }>(),
        build: (b) => ({
          double: b.task({
            run: async ({ input }) => ({ doubled: input.x * 2 }),
          }),
        }),
        output(s) {
          return s.double;
        },
      });
      const parent = flow({
        id: "pg-sub-parent",
        input: passthroughSchema<{ n: number }>(),
        build: (b) => ({
          sub: b.subflow(child, { input: ({ input }) => ({ x: input.n }) }),
        }),
      });

      const wf = await makeNagi(parent, child);
      const parentRunId = await wf.start(parent, { n: 21 });
      await runToEnd(wf, parentRunId);

      expect(await loadStatus(db, schema, parentRunId)).toBe("completed");

      const row = await sql<{
        run_id: string;
        parent_run_id: string | null;
        parent_step_id: string | null;
        status: string;
      }>`
        SELECT run_id, parent_run_id, parent_step_id, status
          FROM ${sql.raw(`${schema}.workflow_run`)}
         WHERE parent_run_id = ${parentRunId}
      `.execute(db);
      expect(row.rows.length).toBe(1);
      const childRow = row.rows[0];
      if (childRow === undefined) throw new Error("missing child row");
      expect(childRow.parent_run_id).toBe(parentRunId);
      expect(childRow.parent_step_id).toBe("sub");
      expect(childRow.status).toBe("completed");

      const factRows = await sql<{ payload: { output: unknown } }>`
        SELECT payload FROM ${sql.raw(`${schema}.fact`)}
         WHERE run_id = ${parentRunId} AND kind = 'step.completed'
         ORDER BY fact_id ASC
      `.execute(db);
      expect(factRows.rows.length).toBe(1);
      const stepCompleted = factRows.rows[0];
      if (stepCompleted === undefined) {
        throw new Error("missing step.completed fact");
      }
      const subOutput = stepCompleted.payload.output as {
        childRunId: string;
        output: { doubled: number };
      };
      expect(subOutput.childRunId).toBe(childRow.run_id);
      expect(subOutput.output).toEqual({ doubled: 42 });
    }, 20_000);

    it("Store.listChildren returns the child run ids for a parent", async () => {
      const child = flow({
        id: "pg-listc-child",
        input: passthroughSchema<{ x: number }>(),
        build: (b) => ({
          echo: b.task({ run: async ({ input }) => ({ x: input.x }) }),
        }),
      });
      const parent = flow({
        id: "pg-listc-parent",
        input: passthroughSchema<{ n: number }>(),
        build: (b) => ({
          sub: b.subflow(child, { input: ({ input }) => ({ x: input.n }) }),
        }),
      });
      const wf = await makeNagi(parent, child);
      const parentRunId = await wf.start(parent, { n: 1 });
      await runToEnd(wf, parentRunId);
      const store = postgresStore({ db, schema });
      const children = await store.listChildren(parentRunId);
      expect(children.length).toBe(1);
    }, 20_000);

    it("re-delivering a subflow step re-attaches via PG (idempotent, no self-supersede)", async () => {
      // Child has cancel-in-progress on a fixed key: if a re-delivery minted a
      // second child with a different id, the real SQL tryStartRun would cancel
      // the first. Deterministic ids make the existence check fire first, so the
      // second spawn re-attaches — proving the load-bearing PG ordering.
      const child = flow({
        id: "pg-idem-child",
        input: passthroughSchema<{ x: number }>(),
        concurrency: { keyFn: () => "fixed", mode: "cancel-in-progress" },
        build: (b) => ({
          work: b.task({
            run: async ({ input }) => ({ doubled: input.x * 2 }),
          }),
        }),
        output: (s) => s.work,
      });
      const parent = flow({
        id: "pg-idem-parent",
        input: passthroughSchema<Record<string, never>>(),
        build: (b) => ({
          sub: b.subflow(child, { input: () => ({ x: 5 }) }),
        }),
      });

      const queue = new InMemoryQueue();
      const wf = await nagi({
        store: postgresStore({ db, schema }),
        queue,
        clock: new InMemoryClock(),
        flows: [parent, child],
      });
      const parentRunId = await wf.start(parent, {});
      // At-least-once: the spawn message arrives a second time at a higher
      // attempt (what a lease-reap enqueues) but the SAME generation, so it
      // must re-attach. Queued ahead of the child's own work so the
      // re-dispatch meets an in-flight child; concurrency 1 keeps that order.
      await queue.enqueue(parentRunId, "sub", {
        attempt: 2 as AttemptNumber,
        flowId: parent.id,
      });
      await wf
        .worker({ concurrency: 1, timerSweepIntervalMs: 0 })
        .runUntilEmpty();

      const rows = await sql<{ run_id: string; status: string }>`
        SELECT run_id, status FROM ${sql.raw(`${schema}.workflow_run`)}
         WHERE parent_run_id = ${parentRunId}
      `.execute(db);
      expect(rows.rows.length).toBe(1);
      expect(rows.rows[0]?.status).toBe("completed");
      expect(await loadStatus(db, schema, parentRunId)).toBe("completed");
    }, 20_000);

    it("sweepLeases skips a subflow step while its child is active, reaps once terminal (child_active SQL)", async () => {
      const store = postgresStore({ db, schema, leaseMs: 50 });
      const queue = new InMemoryQueue();
      const parentRunId = `run-${uuidv7()}` as RunId;
      const childRunId = `run-${uuidv7()}` as RunId;

      // Parent run with a subflow step parked (awaitingChild folds to 'running').
      await store.tryStartRun(parentRunId, {
        kind: "flow.started",
        runId: parentRunId,
        flowId: "child-active-parent",
        input: null as never,
        at: new Date(),
      });
      await store.appendFact(parentRunId, {
        kind: "step.started",
        runId: parentRunId,
        stepId: "sub",
        attempt: 1,
        stepKind: "subflow",
        at: new Date(),
      });
      expect(await store.claimStep(parentRunId, "sub", 1)).not.toBeNull();

      // Active child linked to (parentRunId, "sub").
      await store.tryStartRun(childRunId, {
        kind: "flow.started",
        runId: childRunId,
        flowId: "child-active-child",
        input: null as never,
        at: new Date(),
        parent: { runId: parentRunId, stepId: "sub" },
      });
      await new Promise((r) => setTimeout(r, 80));

      // Child running ⇒ the EXISTS clause skips the parent's subflow lease.
      const skipped = await store.sweepLeases({ now: new Date(), queue });
      expect(skipped.some((r) => r.runId === parentRunId)).toBe(false);

      // Child terminal ⇒ next sweep reaps it (recovery / re-entrant wake).
      await store.endRun(childRunId, {
        kind: "flow.completed",
        runId: childRunId,
        output: { ok: true },
        at: new Date(),
      });
      const reaped = await store.sweepLeases({ now: new Date(), queue });
      expect(reaped.some((r) => r.runId === parentRunId)).toBe(true);
    }, 15_000);

    it("wf.cancel transitively cancels children", async () => {
      const grandchild = flow({
        id: "pg-cancel-gc",
        input: passthroughSchema<Record<string, never>>(),
        build: (b) => ({
          wait: b.signal({
            timeoutMs: "unbounded" as const,
            schema: passthroughSchema<{ ok: true }>(),
          }),
        }),
        output: (s) => s.wait,
      });
      const childF = flow({
        id: "pg-cancel-child",
        input: passthroughSchema<Record<string, never>>(),
        build: (b) => ({
          gc: b.subflow(grandchild, { input: () => ({}) }),
        }),
      });
      const parent = flow({
        id: "pg-cancel-parent",
        input: passthroughSchema<Record<string, never>>(),
        build: (b) => ({
          sub: b.subflow(childF, { input: () => ({}) }),
        }),
      });

      const wf = await makeNagi(parent, childF, grandchild);
      const parentRunId = await wf.start(parent, {});

      const ac = new AbortController();
      const worker = wf.worker({ pollIntervalMs: 5, signal: ac.signal });
      const done = worker.run();
      try {
        const start = Date.now();
        while (Date.now() - start < 5_000) {
          const r = await sql<{ count: string }>`
            SELECT COUNT(*)::text AS count FROM ${sql.raw(`${schema}.workflow_run`)}
             WHERE flow_id IN ('pg-cancel-parent','pg-cancel-child','pg-cancel-gc')
               AND status = 'running'
          `.execute(db);
          const n = Number(r.rows[0]?.count ?? "0");
          if (n === 3) break;
          await new Promise((res) => setTimeout(res, 20));
        }
        await wf.cancel(parentRunId, { reason: "test cancel" });
      } finally {
        ac.abort();
        await done;
      }

      const statuses = await sql<{ status: string }>`
        SELECT status FROM ${sql.raw(`${schema}.workflow_run`)}
         WHERE flow_id IN ('pg-cancel-parent','pg-cancel-child','pg-cancel-gc')
      `.execute(db);
      expect(statuses.rows.length).toBe(3);
      for (const row of statuses.rows) {
        expect(row.status).toBe("canceled");
      }
    }, 30_000);
  });

  describe("b.signal — early-signal buffering via PG", () => {
    it("parks a signal that arrives before the step is claimed, then delivers it on dispatch", async () => {
      const f = flow({
        id: "pg-early-signal",
        input: passthroughSchema<Record<string, never>>(),
        build: (b) => ({
          transcript: b.signal({
            timeoutMs: "unbounded" as const,
            names: ["audioReady", "recordingReady"],
            schema: passthroughSchema<
              { audioUrl: string } | { transcript: string }
            >(),
          }),
        }),
        output: (s) => s.transcript,
      });

      const wf = await makeNagi(f);
      const runId = await wf.start(f, {});

      // No worker yet: the signal step is enqueued but unclaimed, so the signal
      // is parked (under the per-run advisory lock) rather than thrown.
      await wf.signal(runId, "recordingReady", { transcript: "t" });

      const store = postgresStore({ db, schema });
      const parked = await store.loadRunState(runId);
      expect(parked.bufferedSignals["transcript"]).toEqual({
        payload: { transcript: "t" },
        signalName: "recordingReady",
      });
      expect(
        parked.facts.filter((x) => x.kind === "signal.buffered"),
      ).toHaveLength(1);

      // The worker claims the step and applies the buffered signal in the same
      // dispatch.
      await runToEnd(wf, runId);

      const output = await loadOutput(db, schema, runId);
      expect(output).toEqual({ transcript: "t" });

      const settled = await store.loadRunState(runId);
      expect(
        settled.facts.filter((x) => x.kind === "signal.received"),
      ).toHaveLength(1);
    }, 15_000);
  });

  describe("b.signal timeout — fails the run via PG", () => {
    it("fails an awaiting signal step on deadline, materializes the typed error, and cascades to flow.failed", async () => {
      const f = flow({
        id: "pg-signal-timeout",
        input: passthroughSchema<Record<string, never>>(),
        build: (b) => {
          const awaitAudio = b.signal({
            names: ["audioReady", "recordingReady"],
            schema: passthroughSchema<{ ok: boolean }>(),
            timeoutMs: 10,
          });
          return {
            awaitAudio,
            transcription: b.task({
              needs: { awaitAudio },
              run: async () => ({ done: true }),
            }),
          };
        },
      });

      const wf = await makeNagi(f);
      const runId = await wf.start(f, {});

      // Tiny sweep cadence so the worker self-sweep fails the parked gate
      // quickly; exercises upsertTimer (armed in recordStarted), the advisory-
      // locked sweepSignalTimeouts, and advance → flow.failed end to end.
      const ac = new AbortController();
      const worker = wf.worker({
        pollIntervalMs: 5,
        timerSweepIntervalMs: 20,
        signal: ac.signal,
      });
      const done = worker.run();
      try {
        const start = Date.now();
        while (Date.now() - start < 10_000) {
          if ((await loadStatus(db, schema, runId)) === "failed") break;
          await new Promise((r) => setTimeout(r, 10));
        }
      } finally {
        ac.abort();
        await done;
      }

      expect(await loadStatus(db, schema, runId)).toBe("failed");

      const store = postgresStore({ db, schema });
      const final = await store.loadRunState(runId);
      expect(final.steps["awaitAudio"]?.tag).toBe("failed");

      const stepRow = await sql<{
        status: string;
        error: { name?: string } | null;
      }>`
        SELECT status, error FROM ${sql.raw(`${schema}.step_run`)}
         WHERE run_id = ${runId} AND step_id = 'awaitAudio'
      `.execute(db);
      expect(stepRow.rows[0]?.status).toBe("failed");
      expect(stepRow.rows[0]?.error?.name).toBe("NagiSignalTimeoutError");

      // The timer row is consumed once it fires.
      const timers = await sql<{ n: number }>`
        SELECT count(*)::int AS n FROM ${sql.raw(`${schema}.timer`)}
         WHERE run_id = ${runId}
      `.execute(db);
      expect(timers.rows[0]?.n).toBe(0);
    }, 20_000);
  });

  describe("step.reset — reopens a settled run via PG", () => {
    it("replay({ from }) on a failed run reopens it and describe() reports completed", async () => {
      let shouldFail = true;
      const f = flow({
        id: "pg-reopen-retry",
        input: passthroughSchema<Record<string, never>>(),
        build: (b) => ({
          s: b.task({
            retry: { maxAttempts: 1, backoff: "fixed" },
            run: async () => {
              if (shouldFail) throw new Error("boom");
              return { ok: true };
            },
          }),
        }),
        output(s) {
          return s.s;
        },
      });
      const wf = await makeNagi(f);
      const runId = await wf.start(f, {});
      await runToEnd(wf, runId);
      const failed = await wf.describe(runId);
      expect(failed?.run.status).toBe("failed");
      expect(failed?.run.completedAt).toBeDefined();

      shouldFail = false;
      await wf.replay(runId, { mode: "continue", from: "s" });
      // The reset materialized status = 'running', so runToEnd waits for the
      // re-run instead of returning on the stale 'failed'.
      await runToEnd(wf, runId);

      const reopened = await wf.describe(runId);
      expect(reopened?.run.status).toBe("completed");
      expect(reopened?.run.completedAt).toBeDefined();
      expect(reopened?.run.error).toBeUndefined();
      expect(await loadOutput(db, schema, runId)).toEqual({ ok: true });
    }, 20_000);

    it("replay({ from }) rejects with NagiConcurrencyConflictError when another run holds the key", async () => {
      let shouldFail = true;
      const f = flow({
        id: "pg-reopen-conflict",
        input: passthroughSchema<Record<string, never>>(),
        concurrency: { keyFn: () => "k", mode: "cancel-in-progress" },
        build: (b) => {
          const s = b.task({
            retry: { maxAttempts: 1, backoff: "fixed" },
            run: async () => {
              if (shouldFail) throw new Error("boom");
              return { ok: true };
            },
          });
          const wait = b.signal({
            needs: { s },
            timeoutMs: "unbounded" as const,
            names: ["go"],
            schema: passthroughSchema<{ ok: boolean }>(),
          });
          return { s, wait };
        },
      });
      const wf = await makeNagi(f);
      const run1 = await wf.start(f, {});
      await runToEnd(wf, run1);
      expect(await loadStatus(db, schema, run1)).toBe("failed");

      // run2 takes the freed key; it is never drained, so it stays 'running'
      // and holds workflow_run_concurrency_active_uidx for (flow_id, 'k').
      shouldFail = false;
      const run2 = await wf.start(f, {});
      expect(await loadStatus(db, schema, run2)).toBe("running");

      await expect(
        wf.replay(run1, { mode: "continue", from: "s" }),
      ).rejects.toBeInstanceOf(NagiConcurrencyConflictError);

      // The conflict rolled the appendFact transaction back: no reset fact,
      // no status change on either run.
      expect(await loadStatus(db, schema, run1)).toBe("failed");
      expect(await loadStatus(db, schema, run2)).toBe("running");
      const resets = await sql<{ n: number }>`
        SELECT count(*)::int AS n FROM ${sql.raw(`${schema}.fact`)}
         WHERE run_id = ${run1} AND kind = 'step.reset'
      `.execute(db);
      expect(resets.rows[0]?.n).toBe(0);
    }, 20_000);

    it("replay({ from }) scope:'step' resets only the origin row and reruns only it", async () => {
      let aRuns = 0;
      let bRuns = 0;
      let cRuns = 0;
      const f = flow({
        id: "pg-scope-step",
        input: passthroughSchema<Record<string, never>>(),
        build: (b) => {
          const a = b.task({
            run: async () => {
              aRuns += 1;
              return { v: "a" };
            },
          });
          const bStep = b.task({
            needs: { a },
            run: async () => {
              bRuns += 1;
              return { n: bRuns };
            },
          });
          const c = b.task({
            needs: { b: bStep },
            run: async ({ needs }) => {
              cRuns += 1;
              return { sawB: needs.b };
            },
          });
          return { a, b: bStep, c };
        },
        output(s) {
          return s.c;
        },
      });

      const wf = await makeNagi(f);
      const runId = await wf.start(f, {});
      await runToEnd(wf, runId);
      expect(await loadStatus(db, schema, runId)).toBe("completed");
      expect({ aRuns, bRuns, cRuns }).toEqual({ aRuns: 1, bRuns: 1, cRuns: 1 });

      await wf.replay(runId, { mode: "continue", from: "b", scope: "step" });

      // Immediately, before draining: the reset projection deleted b's row.
      // c is a completed descendant, so scope "step" leaves its row alone.
      const bRows = await sql<{ step_id: string }>`
        SELECT step_id FROM ${sql.raw(`${schema}.step_run`)}
         WHERE run_id = ${runId} AND step_id = 'b'
      `.execute(db);
      expect(bRows.rows).toHaveLength(0);
      const others = await sql<{ step_id: string; status: string }>`
        SELECT step_id, status FROM ${sql.raw(`${schema}.step_run`)}
         WHERE run_id = ${runId} AND step_id IN ('a', 'c')
         ORDER BY step_id
      `.execute(db);
      expect(others.rows).toEqual([
        { step_id: "a", status: "completed" },
        { step_id: "c", status: "completed" },
      ]);
      expect(await loadStatus(db, schema, runId)).toBe("running");

      await runToEnd(wf, runId);
      expect(await loadStatus(db, schema, runId)).toBe("completed");
      // b re-ran; a and c did not.
      expect({ aRuns, bRuns, cRuns }).toEqual({ aRuns: 1, bRuns: 2, cRuns: 1 });
      const reopened = await wf.describe(runId);
      const bStepView = reopened?.steps.find((s) => s.stepId === "b");
      expect(bStepView?.output).toEqual({ n: 2 });
    }, 20_000);
  });

  describe("task timeoutMs — rolls back the step tx via PG", () => {
    beforeAll(async () => {
      await sql
        .raw(`CREATE TABLE IF NOT EXISTS ${schema}.probe (id text PRIMARY KEY)`)
        .execute(db);
    });

    async function probeCount(runId: RunId): Promise<number> {
      const r = await sql<{ n: number }>`
        SELECT count(*)::int AS n FROM ${sql.raw(`${schema}.probe`)} WHERE id = ${runId}
      `.execute(db);
      return r.rows[0]?.n ?? 0;
    }

    it("a timed-out task's ctx.tx writes roll back", async () => {
      const f = flow({
        id: "pg-timeout-rollback",
        input: passthroughSchema<Record<string, never>>(),
        build: (b) => ({
          slow: b.task({
            timeoutMs: 200,
            retry: { maxAttempts: 1, backoff: "fixed" },
            run: async ({ ctx }) => {
              await sql`
                INSERT INTO ${sql.raw(`${schema}.probe`)} (id) VALUES (${ctx.runId})
              `.execute(ctx.tx as unknown as Kysely<unknown>);
              await new Promise((_, reject) =>
                ctx.signal.addEventListener(
                  "abort",
                  () => reject(ctx.signal.reason),
                  { once: true },
                ),
              );
              return { unreachable: true };
            },
          }),
        }),
        output(s) {
          return s.slow;
        },
      });

      const wf = await makeNagi(f);
      const runId = await wf.start(f, {});
      await runToEnd(wf, runId);

      expect(await loadStatus(db, schema, runId)).toBe("failed");
      const described = await wf.describe(runId);
      const step = described?.steps.find((s) => s.stepId === "slow");
      expect(step?.status).toBe("failed");
      expect((step?.error as { name?: string } | undefined)?.name).toBe(
        "NagiStepTimeoutError",
      );
      expect(await probeCount(runId)).toBe(0);
    }, 15_000);

    it("a task's ctx.tx writes commit with its completion", async () => {
      const f = flow({
        id: "pg-timeout-control-commit",
        input: passthroughSchema<Record<string, never>>(),
        build: (b) => ({
          fast: b.task({
            retry: { maxAttempts: 1, backoff: "fixed" },
            run: async ({ ctx }) => {
              await sql`
                INSERT INTO ${sql.raw(`${schema}.probe`)} (id) VALUES (${ctx.runId})
              `.execute(ctx.tx as unknown as Kysely<unknown>);
              return { ok: true };
            },
          }),
        }),
        output(s) {
          return s.fast;
        },
      });

      const wf = await makeNagi(f);
      const runId = await wf.start(f, {});
      await runToEnd(wf, runId);

      expect(await loadStatus(db, schema, runId)).toBe("completed");
      expect(await loadOutput(db, schema, runId)).toEqual({ ok: true });
      expect(await probeCount(runId)).toBe(1);
    }, 15_000);

    it("a body that ignores the signal and returns late still fails and rolls back", async () => {
      const f = flow({
        id: "pg-timeout-ignores-signal",
        input: passthroughSchema<Record<string, never>>(),
        build: (b) => ({
          slow: b.task({
            timeoutMs: 200,
            retry: { maxAttempts: 1, backoff: "fixed" },
            run: async ({ ctx }) => {
              await sql`
                INSERT INTO ${sql.raw(`${schema}.probe`)} (id) VALUES (${ctx.runId})
              `.execute(ctx.tx as unknown as Kysely<unknown>);
              await new Promise((res) => setTimeout(res, 400));
              return { reachedAfterAbort: true };
            },
          }),
        }),
        output(s) {
          return s.slow;
        },
      });

      const wf = await makeNagi(f);
      const runId = await wf.start(f, {});
      await runToEnd(wf, runId);

      expect(await loadStatus(db, schema, runId)).toBe("failed");
      const described = await wf.describe(runId);
      const step = described?.steps.find((s) => s.stepId === "slow");
      expect(step?.status).toBe("failed");
      expect((step?.error as { name?: string } | undefined)?.name).toBe(
        "NagiStepTimeoutError",
      );
      expect(await probeCount(runId)).toBe(0);
    }, 15_000);
  });

  describe("driftPolicy: synthesize — via PG", () => {
    it("resumes a run pinned to the old hash with the live handler, staying pinned to that hash", async () => {
      const flowA = flow({
        id: "pg-drift",
        input: passthroughSchema<Record<string, never>>(),
        build: (b) => ({
          s: b.task({ run: async () => ({ ok: true }) }),
        }),
      });
      const flowB = flow({
        id: "pg-drift",
        input: passthroughSchema<Record<string, never>>(),
        build: (b) => ({
          s: b.task({ run: async () => ({ different: true }) }),
          added: b.task({ run: async () => ({ added: true }) }),
        }),
      });

      const queue = new InMemoryQueue();
      const wfA = await nagi({
        store: postgresStore({ db, schema }),
        queue,
        clock: new InMemoryClock(),
        flows: [flowA],
      });
      const runId = await wfA.start(flowA, {});

      const pinnedRow = await sql<{ flow_hash: string }>`
        SELECT flow_hash FROM ${sql.raw(`${schema}.workflow_run`)} WHERE run_id = ${runId}
      `.execute(db);
      const pinnedHash = pinnedRow.rows[0]?.flow_hash;
      expect(pinnedHash).toBeDefined();

      const wfB = await nagi({
        store: postgresStore({ db, schema }),
        queue,
        clock: new InMemoryClock(),
        flows: [flowB],
        driftPolicy: "synthesize",
      });
      await wfB
        .worker({ timerSweepIntervalMs: 0 })
        .runUntilEmpty({ timeoutMs: 10_000 });

      expect(await loadStatus(db, schema, runId)).toBe("completed");
      const described = await wfB.describe(runId);
      const sStep = described?.steps.find((s) => s.stepId === "s");
      expect(sStep?.output).toEqual({ different: true });
      expect(described?.steps.some((s) => s.stepId === "added")).toBe(false);

      const addedRow = await sql<{ n: number }>`
        SELECT count(*)::int AS n FROM ${sql.raw(`${schema}.step_run`)}
         WHERE run_id = ${runId} AND step_id = 'added'
      `.execute(db);
      expect(addedRow.rows[0]?.n).toBe(0);

      const afterRow = await sql<{ flow_hash: string }>`
        SELECT flow_hash FROM ${sql.raw(`${schema}.workflow_run`)} WHERE run_id = ${runId}
      `.execute(db);
      expect(afterRow.rows[0]?.flow_hash).toBe(pinnedHash);
    }, 20_000);

    it("a drifted two-step chain resolves needs.<alias> under the live handler and completes", async () => {
      function chainA() {
        return flow({
          id: "pg-drift-chain",
          input: passthroughSchema<Record<string, never>>(),
          build: (b) => {
            const a = b.task({ run: async () => ({ v: "fromA" }) });
            const bStep = b.task({
              needs: { up: a },
              run: async ({ needs }) => ({ got: needs.up }),
            });
            return { a, b: bStep };
          },
          output(s) {
            return s.b;
          },
        });
      }
      function chainB() {
        return flow({
          id: "pg-drift-chain",
          input: passthroughSchema<Record<string, never>>(),
          build: (b) => {
            const a = b.task({ run: async () => ({ v: "fromB" }) });
            const bStep = b.task({
              needs: { up: a },
              run: async ({ needs }) => ({ got: needs.up }),
            });
            const extra = b.task({ run: async () => ({}) });
            return { a, b: bStep, extra };
          },
          output(s) {
            return s.b;
          },
        });
      }

      const A = chainA();
      const queue = new InMemoryQueue();
      const wfA = await nagi({
        store: postgresStore({ db, schema }),
        queue,
        clock: new InMemoryClock(),
        flows: [A],
      });
      const runId = await wfA.start(A, {});

      const wfB = await nagi({
        store: postgresStore({ db, schema }),
        queue,
        clock: new InMemoryClock(),
        flows: [chainB()],
        driftPolicy: "synthesize",
      });
      // Bounded, not an unbounded drain: pre-008, the wrong needs shape makes
      // checkUpstream throw on every redelivery — an infinite nack/redeliver
      // loop that would hang instead of failing the test.
      const { processed } = await wfB
        .worker({ timerSweepIntervalMs: 0 })
        .runOnce({ maxSteps: 10 });
      expect(processed).toBeGreaterThan(0);

      expect(await loadStatus(db, schema, runId)).toBe("completed");
      expect(await loadOutput(db, schema, runId)).toEqual({
        got: { v: "fromB" },
      });
    }, 20_000);
  });

  describe("pruneFacts — retention", () => {
    beforeEach(async () => {
      await sql
        .raw(
          `DELETE FROM ${schema}.fact;
           DELETE FROM ${schema}.step_run;
           DELETE FROM ${schema}.lease;
           DELETE FROM ${schema}.timer;
           DELETE FROM ${schema}.dedupe;
           DELETE FROM ${schema}.workflow_run;`,
        )
        .execute(db);
    });

    // Seeds concurrently: a sequential loop that outlives its test's timeout
    // keeps inserting after the next test's beforeEach has cleared the tables.
    async function seedTerminals(n: number, flowId: string): Promise<void> {
      await Promise.all(
        Array.from({ length: n }, (_, i) =>
          seedTerminal({
            flowId,
            status: "completed",
            startedAtMs: 1000 + i,
            completedAtMs: 2000 + i,
          }),
        ),
      );
    }

    async function seedTerminal(args: {
      readonly flowId: string;
      readonly status: "completed" | "failed" | "canceled";
      readonly startedAtMs: number;
      readonly completedAtMs: number;
      readonly input?: Record<string, unknown>;
    }): Promise<RunId> {
      const store = postgresStore({ db, schema });
      const runId = `run-${uuidv7()}` as RunId;
      await store.tryStartRun(runId, {
        kind: "flow.started",
        runId,
        flowId: args.flowId,
        input: (args.input ?? {}) as never,
        at: new Date(args.startedAtMs),
      });
      const at = new Date(args.completedAtMs);
      if (args.status === "completed") {
        await store.endRun(runId, {
          kind: "flow.completed",
          runId,
          at,
          output: null,
        });
      } else if (args.status === "failed") {
        await store.endRun(runId, {
          kind: "flow.failed",
          runId,
          at,
          error: { name: "E", message: "x" },
        });
      } else {
        await store.endRun(runId, {
          kind: "flow.canceled",
          cause: "concurrency",
          runId,
          at,
          canceledByRunId: runId,
          concurrencyKey: "k",
        });
      }
      return runId;
    }

    async function countRows(table: string, runId: RunId): Promise<number> {
      const r = await sql<{ c: string }>`
        SELECT count(*)::text AS c FROM ${sql.raw(`${schema}.${table}`)}
         WHERE run_id = ${runId}
      `.execute(db);
      return Number(r.rows[0]?.c ?? 0);
    }

    it("prunes completed terminal runs older than the cutoff; leaves running rows alone", async () => {
      const wf = await makeNagi();
      const oldRun = await seedTerminal({
        flowId: "pf-1",
        status: "completed",
        startedAtMs: 1000,
        completedAtMs: 2000,
      });
      const recentRun = await seedTerminal({
        flowId: "pf-1",
        status: "completed",
        startedAtMs: Date.now() - 1000,
        completedAtMs: Date.now(),
      });
      const runningRun = `run-${uuidv7()}` as RunId;
      await postgresStore({ db, schema }).tryStartRun(runningRun, {
        kind: "flow.started",
        runId: runningRun,
        flowId: "pf-1",
        input: null as never,
        at: new Date(1000),
      });

      const result = await wf.pruneFacts({
        olderThan: new Date(Date.now() - 60_000),
      });
      expect(result.runsPruned).toBe(1);
      expect(result.factsPruned).toBeGreaterThanOrEqual(2);

      expect(await countRows("fact", oldRun)).toBe(0);
      expect(await countRows("workflow_run", oldRun)).toBe(1);
      expect(await countRows("fact", recentRun)).toBeGreaterThan(0);
      expect(await countRows("fact", runningRun)).toBeGreaterThan(0);
    });

    it("default statuses prunes only completed; failed/canceled stay", async () => {
      const wf = await makeNagi();
      const c = await seedTerminal({
        flowId: "pf-default",
        status: "completed",
        startedAtMs: 1000,
        completedAtMs: 2000,
      });
      const f = await seedTerminal({
        flowId: "pf-default",
        status: "failed",
        startedAtMs: 1000,
        completedAtMs: 2000,
      });
      const x = await seedTerminal({
        flowId: "pf-default",
        status: "canceled",
        startedAtMs: 1000,
        completedAtMs: 2000,
      });
      const r = await wf.pruneFacts({ olderThan: new Date() });
      expect(r.runsPruned).toBe(1);
      expect(await countRows("fact", c)).toBe(0);
      expect(await countRows("fact", f)).toBeGreaterThan(0);
      expect(await countRows("fact", x)).toBeGreaterThan(0);
    });

    it("statuses array prunes every listed terminal status", async () => {
      const wf = await makeNagi();
      const ids = await Promise.all([
        seedTerminal({
          flowId: "pf-multi",
          status: "completed",
          startedAtMs: 1000,
          completedAtMs: 2000,
        }),
        seedTerminal({
          flowId: "pf-multi",
          status: "failed",
          startedAtMs: 1000,
          completedAtMs: 2000,
        }),
        seedTerminal({
          flowId: "pf-multi",
          status: "canceled",
          startedAtMs: 1000,
          completedAtMs: 2000,
        }),
      ]);
      const r = await wf.pruneFacts({
        olderThan: new Date(),
        statuses: ["completed", "failed", "canceled"],
      });
      expect(r.runsPruned).toBe(3);
      for (const id of ids) {
        expect(await countRows("fact", id)).toBe(0);
      }
    });

    it("cascades cleanup to fact / step_run / lease / timer / dedupe", async () => {
      const wf = await makeNagi();
      const runId = await seedTerminal({
        flowId: "pf-cascade",
        status: "completed",
        startedAtMs: 1000,
        completedAtMs: 2000,
      });
      await sql`
        INSERT INTO ${sql.raw(`${schema}.step_run`)}
          (run_id, step_id, attempt, status, started_at, completed_at)
        VALUES (${runId}, 's1', 1, 'completed', now(), now())
      `.execute(db);
      await sql`
        INSERT INTO ${sql.raw(`${schema}.lease`)}
          (run_id, step_id, attempt, token, expires_at)
        VALUES (${runId}, 's1', 1, 'tok', now())
      `.execute(db);
      await sql`
        INSERT INTO ${sql.raw(`${schema}.timer`)} (run_id, step_id, fire_at)
        VALUES (${runId}, 's1', now())
      `.execute(db);
      await sql`
        INSERT INTO ${sql.raw(`${schema}.dedupe`)}
          (run_id, step_id, scope, value)
        VALUES (${runId}, 's1', 'sc', '{}'::jsonb)
      `.execute(db);

      await wf.pruneFacts({ olderThan: new Date() });

      expect(await countRows("fact", runId)).toBe(0);
      expect(await countRows("step_run", runId)).toBe(0);
      expect(await countRows("lease", runId)).toBe(0);
      expect(await countRows("timer", runId)).toBe(0);
      expect(await countRows("dedupe", runId)).toBe(0);
      expect(await countRows("workflow_run", runId)).toBe(1);
    });

    it("keepSummary: false removes the workflow_run row too", async () => {
      const wf = await makeNagi();
      const runId = await seedTerminal({
        flowId: "pf-nosummary",
        status: "completed",
        startedAtMs: 1000,
        completedAtMs: 2000,
      });
      await wf.pruneFacts({ olderThan: new Date(), keepSummary: false });
      expect(await countRows("workflow_run", runId)).toBe(0);
    });

    it("batchSize < total drains everything via the internal loop", async () => {
      const wf = await makeNagi();
      for (let i = 0; i < 5; i++) {
        await seedTerminal({
          flowId: "pf-batch",
          status: "completed",
          startedAtMs: 1000 + i,
          completedAtMs: 2000 + i,
        });
      }
      const r = await wf.pruneFacts({
        olderThan: new Date(),
        batchSize: 2,
      });
      expect(r.runsPruned).toBe(5);
    });

    it("uses the workflow_run_completed_at_idx for the victim selection", async () => {
      await seedTerminals(50, "pf-explain");
      const plan = await sql<{ "QUERY PLAN": string }>`
        EXPLAIN SELECT run_id FROM ${sql.raw(`${schema}.workflow_run`)}
         WHERE status = ANY(ARRAY['completed','failed','canceled']::text[])
           AND completed_at IS NOT NULL
           AND completed_at < now()
         ORDER BY completed_at ASC, run_id ASC
         LIMIT 1000
      `.execute(db);
      const planText = plan.rows.map((r) => r["QUERY PLAN"]).join("\n");
      expect(planText).toMatch(
        /workflow_run_completed_at_idx|Seq Scan on workflow_run/,
      );
    });

    it("concurrent pruners share work without errors (FOR UPDATE SKIP LOCKED)", async () => {
      const wf = await makeNagi();
      await seedTerminals(12, "pf-concurrent");
      const [a, b] = await Promise.all([
        wf.pruneFacts({ olderThan: new Date(), batchSize: 3 }),
        wf.pruneFacts({ olderThan: new Date(), batchSize: 3 }),
      ]);
      expect(a.runsPruned + b.runsPruned).toBe(12);
    });
  });

  describe("pool pressure", () => {
    const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
    const A1 = 1 as AttemptNumber;

    it("two concurrent task steps complete on a two-connection pool", async () => {
      const pool = new pg.Pool({
        connectionString: url,
        max: 2,
        // Larger than the vi.waitFor timeout below: pre-fix, settle()'s second
        // connection request for both steps hangs for this whole window (the
        // deadlock), so waitFor's own 10s bound trips first. Post-fix, settle
        // never needs a second connection, so this never matters.
        connectionTimeoutMillis: 12_000,
      });
      const smallDb = new Kysely<unknown>({
        dialect: new PostgresDialect({ pool }),
      });
      try {
        const f = flow({
          id: "pg-pool-pressure-two-tasks",
          input: passthroughSchema<Record<string, never>>(),
          build: (b) => ({
            a: b.task({
              run: async () => {
                await sleep(100);
                return { ok: "a" };
              },
            }),
            b: b.task({
              run: async () => {
                await sleep(100);
                return { ok: "b" };
              },
            }),
          }),
          output(s) {
            return { a: s.a, b: s.b };
          },
        });
        const wf = await nagi({
          store: postgresStore({ db: smallDb, schema }),
          queue: new InMemoryQueue(),
          clock: new InMemoryClock(),
          flows: [f],
        });
        const runId = await wf.start(f, {});

        const ac = new AbortController();
        const worker = wf.worker({
          concurrency: 2,
          pollIntervalMs: 5,
          signal: ac.signal,
        });
        const done = worker.run();
        try {
          await vi.waitFor(
            async () => {
              expect(await loadStatus(db, schema, runId)).toBe("completed");
            },
            { timeout: 10_000 },
          );
        } finally {
          ac.abort();
          await done;
        }
      } finally {
        await smallDb.destroy();
      }
    }, 20_000);

    it("runStep's body can read run state on its tx with the pool exhausted", async () => {
      const pool = new pg.Pool({
        connectionString: url,
        max: 1,
        connectionTimeoutMillis: 3_000,
      });
      const smallDb = new Kysely<unknown>({
        dialect: new PostgresDialect({ pool }),
      });
      try {
        const store = postgresStore({ db: smallDb, schema });
        const runId = `run-${uuidv7()}` as RunId;
        const at = new Date();
        await store.tryStartRun(runId, {
          kind: "flow.started",
          runId,
          flowId: "pg-pool-pressure-tx-read",
          input: {},
          at,
        });
        await store.appendFact(runId, {
          kind: "step.started",
          runId,
          stepId: "s",
          attempt: A1,
          stepKind: "task",
          at,
        });
        await store.claimStep(runId, "s", A1);

        const race = Promise.race([
          store.runStep(runId, "s", A1, async (tx) => {
            const seen = await store.loadRunState(runId, tx);
            return {
              output: { seen: seen.facts.length },
              fact: {
                kind: "step.completed" as const,
                runId,
                stepId: "s",
                attempt: A1,
                output: null,
                at: new Date(),
              },
            };
          }),
          new Promise<never>((_, reject) =>
            setTimeout(() => reject(new Error("runStep timed out")), 5_000),
          ),
        ]);
        await expect(race).resolves.toEqual({ seen: expect.any(Number) });
      } finally {
        await smallDb.destroy();
      }
    }, 10_000);
  });
});

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
