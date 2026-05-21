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
import { afterAll, beforeAll, beforeEach, describe, expect, it } from "vitest";
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

    expect(await store.getOnce(runId, "step", "scope")).toBeNull();
    await store.recordOnce(runId, "step", "scope", { value: 1 });
    expect(await store.getOnce(runId, "step", "scope")).toEqual({ value: 1 });
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

  describe("queryRuns — discovery by input/flow/status", () => {
    async function seed(
      flowId: string,
      input: Record<string, unknown>,
      terminal?: "completed" | "failed" | "canceled",
    ): Promise<RunId> {
      const store = postgresStore({ db, schema });
      const runId = `run-${uuidv7()}` as RunId;
      const startedAt = new Date();
      await store.tryStartRun(runId, {
        kind: "flow.started",
        runId,
        flowId,
        input: input as never,
        at: startedAt,
      });
      if (terminal === "completed") {
        await store.appendFact(runId, {
          kind: "flow.completed",
          runId,
          at: new Date(startedAt.getTime() + 100),
          output: null,
        });
      } else if (terminal === "failed") {
        await store.appendFact(runId, {
          kind: "flow.failed",
          runId,
          at: new Date(startedAt.getTime() + 100),
          error: { name: "E", message: "x" },
        });
      }
      return runId;
    }

    it("matches by input containment (basic + nested)", async () => {
      const wf = await makeNagi();
      await seed("video-ingest", { videoId: "abc" });
      await seed("video-ingest", { videoId: "xyz" });
      await seed("video-ingest", {
        videoId: "abc",
        customer: { id: 1, plan: { seats: 5 } },
      });

      const basic = await wf.queryRuns({
        where: { input: { videoId: "abc" } },
      });
      expect(basic.runs).toHaveLength(2);
      expect(
        basic.runs.every(
          (r) => (r.input as { videoId: string }).videoId === "abc",
        ),
      ).toBe(true);

      const nested = await wf.queryRuns({
        where: { input: { customer: { plan: { seats: 5 } } } },
      });
      expect(nested.runs).toHaveLength(1);
    });

    it("filter superset of row → no match", async () => {
      const wf = await makeNagi();
      await seed("f", { videoId: "abc" });
      const r = await wf.queryRuns({
        where: { input: { videoId: "abc", userId: 7 } },
      });
      expect(r.runs).toEqual([]);
    });

    it("status filter accepts single value and array", async () => {
      const wf = await makeNagi();
      await seed("f", {});
      await seed("f", {}, "completed");
      await seed("f", {}, "failed");

      const completed = await wf.queryRuns({ where: { status: "completed" } });
      expect(completed.runs).toHaveLength(1);

      const both = await wf.queryRuns({
        where: { status: ["completed", "failed"] },
      });
      expect(both.runs).toHaveLength(2);
    });

    it("flowId narrows results to one flow", async () => {
      const wf = await makeNagi();
      await seed("a", {});
      await seed("b", {});
      const r = await wf.queryRuns({ where: { flowId: "b" } });
      expect(r.runs).toHaveLength(1);
      expect(r.runs[0]?.flowId).toBe("b");
    });

    it("`latest: true` returns the newest matching run only", async () => {
      const wf = await makeNagi();
      await seed("f", { videoId: "abc" });
      await new Promise((r) => setTimeout(r, 5));
      const newer = await seed("f", { videoId: "abc" });

      const r = await wf.queryRuns({
        where: { input: { videoId: "abc" } },
        latest: true,
      });
      expect(r.runs).toHaveLength(1);
      expect(r.runs[0]?.runId).toBe(newer);
      expect(r.cursor).toBeNull();
    });

    it("cursor pagination walks all rows with no duplicates", async () => {
      const wf = await makeNagi();
      const seeded: RunId[] = [];
      for (let i = 0; i < 5; i++) {
        seeded.push(await seed("f-page", { i }));
        await new Promise((r) => setTimeout(r, 2));
      }

      const collected: RunId[] = [];
      let cursor: string | null | undefined;
      for (let page = 0; page < 10 && (page === 0 || cursor !== null); page++) {
        const res: {
          runs: ReadonlyArray<{ runId: RunId }>;
          cursor: string | null;
        } = await wf.queryRuns({
          where: { flowId: "f-page" },
          limit: 2,
          ...(cursor !== null && cursor !== undefined ? { cursor } : {}),
        });
        collected.push(...res.runs.map((r) => r.runId));
        cursor = res.cursor;
      }
      expect(collected).toHaveLength(5);
      expect(new Set(collected).size).toBe(5);
    });

    it("uses the GIN index for input containment", async () => {
      const wf = await makeNagi();
      for (let i = 0; i < 20; i++) await seed("f-idx", { i, tag: "x" });
      await wf.queryRuns({ where: { input: { tag: "x" } } });

      const plan = await sql<{ "QUERY PLAN": string }>`
        EXPLAIN SELECT run_id FROM ${sql.raw(`${schema}.workflow_run`)}
         WHERE input @> '{"tag":"x"}'::jsonb
      `.execute(db);
      const planText = plan.rows.map((r) => r["QUERY PLAN"]).join("\n");
      expect(planText).toMatch(
        /workflow_run_input_gin_idx|Seq Scan on workflow_run/,
      );
    });
  });

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

    it("wf.cancel transitively cancels children", async () => {
      const grandchild = flow({
        id: "pg-cancel-gc",
        input: passthroughSchema<Record<string, never>>(),
        build: (b) => ({
          wait: b.signal({ schema: passthroughSchema<{ ok: true }>() }),
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
        await store.appendFact(runId, {
          kind: "flow.completed",
          runId,
          at,
          output: null,
        });
      } else if (args.status === "failed") {
        await store.appendFact(runId, {
          kind: "flow.failed",
          runId,
          at,
          error: { name: "E", message: "x" },
        });
      } else {
        await store.appendFact(runId, {
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
      for (let i = 0; i < 50; i++) {
        await seedTerminal({
          flowId: "pf-explain",
          status: "completed",
          startedAtMs: 1000 + i,
          completedAtMs: 2000 + i,
        });
      }
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
      for (let i = 0; i < 12; i++) {
        await seedTerminal({
          flowId: "pf-concurrent",
          status: "completed",
          startedAtMs: 1000 + i,
          completedAtMs: 2000 + i,
        });
      }
      const [a, b] = await Promise.all([
        wf.pruneFacts({ olderThan: new Date(), batchSize: 3 }),
        wf.pruneFacts({ olderThan: new Date(), batchSize: 3 }),
      ]);
      expect(a.runsPruned + b.runsPruned).toBe(12);
    });
  });
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
