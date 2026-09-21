import {
  flow,
  InMemoryClock,
  InMemoryQueue,
  nagi,
  type RunEventEnvelope,
  type RunId,
  type Wf,
} from "@nagi-js/core";
import { passthroughSchema } from "@nagi-js/core/testing";
import { Kysely, PostgresDialect, sql } from "kysely";
import pg from "pg";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { migrate } from "./migrations";
import { postgresStore } from "./store";
import type { StreamListener } from "./stream";
import { uuidv7 } from "./uuidv7";

const url = process.env["NAGI_POSTGRES_TEST_URL"];
const d = url ? describe : describe.skip;

function pgListener(connectionString: string): {
  listener: StreamListener;
  close: () => Promise<void>;
} {
  const clients: pg.Client[] = [];
  return {
    listener: {
      async listen(channel, onNotify) {
        const client = new pg.Client({ connectionString });
        clients.push(client);
        await client.connect();
        client.on("notification", (msg) => {
          if (msg.channel === channel && msg.payload) onNotify(msg.payload);
        });
        await client.query(`LISTEN "${channel}"`);
        return async () => {
          await client.end();
        };
      },
    },
    close: async () => {
      await Promise.all(clients.map((c) => c.end().catch(() => undefined)));
    },
  };
}

d("@nagi-js/postgres — run events over LISTEN/NOTIFY", () => {
  let db: Kysely<unknown>;
  let pool: pg.Pool;
  let schema: string;
  let listening: ReturnType<typeof pgListener>;
  let wf: Wf;
  const seen: RunEventEnvelope[] = [];

  beforeAll(async () => {
    pool = new pg.Pool({ connectionString: url });
    db = new Kysely<unknown>({ dialect: new PostgresDialect({ pool }) });
    schema = `nagi_ev_${uuidv7().replace(/-/g, "").slice(0, 14)}`;
    await migrate(db, { schema });
    listening = pgListener(url as string);

    const store = postgresStore({ db, schema, listener: listening.listener });
    wf = await nagi({
      store,
      queue: new InMemoryQueue(),
      clock: new InMemoryClock(),
      flows: [okFlow, failFlow, superFlow],
    });
    wf.watchRuns((e) => seen.push(e));
    // NOTIFY does not queue for a connection that is not yet listening, so an
    // event published before this resolves is gone, not late.
    await store.ready();
  }, 30_000);

  afterAll(async () => {
    await listening?.close();
    if (!db) return;
    await sql.raw(`DROP SCHEMA IF EXISTS ${schema} CASCADE`).execute(db);
    await db.destroy();
  }, 30_000);

  const okFlow = flow({
    id: "ev-ok",
    input: passthroughSchema<Record<string, never>>(),
    build: (b) => ({ only: b.task({ run: async () => ({ ok: true }) }) }),
    output: (s) => s.only,
  });

  const failFlow = flow({
    id: "ev-fail",
    input: passthroughSchema<Record<string, never>>(),
    build: (b) => ({
      boom: b.task({
        retry: { maxAttempts: 1, backoff: "fixed" },
        run: async () => {
          throw new Error("nope");
        },
      }),
    }),
  });

  const superFlow = flow({
    id: "ev-super",
    input: passthroughSchema<{ k: string }>(),
    concurrency: {
      keyFn: (i: { k: string }) => i.k,
      mode: "cancel-in-progress",
    },
    build: (b) => ({ only: b.task({ run: async () => ({}) }) }),
  });

  async function withWorker<T>(body: () => Promise<T>): Promise<T> {
    const ac = new AbortController();
    const done = wf.worker({ pollIntervalMs: 5, signal: ac.signal }).run();
    try {
      return await body();
    } finally {
      ac.abort();
      await done;
    }
  }

  async function settle(runId: RunId, want: string): Promise<void> {
    const start = Date.now();
    while (Date.now() - start < 15_000) {
      const r = await sql<{ status: string }>`
        SELECT status FROM ${sql.raw(`${schema}.workflow_run`)}
        WHERE run_id = ${runId}
      `.execute(db);
      if (r.rows[0]?.status === want) return;
      await new Promise((res) => setTimeout(res, 10));
    }
    throw new Error(`run ${runId} never reached ${want}`);
  }

  async function eventsFor(runId: RunId): Promise<RunEventEnvelope[]> {
    // NOTIFY delivery is asynchronous to the commit that queued it.
    await new Promise((r) => setTimeout(r, 300));
    return seen.filter((e) => e.runId === runId);
  }

  it("delivers a full successful lifecycle across the connection", async () => {
    const runId = await wf.start(okFlow, {});
    await withWorker(() => settle(runId, "completed"));

    const types = (await eventsFor(runId)).map((e) => e.type);
    expect(types).toContain("flow.started");
    expect(types).toContain("step.started");
    expect(types).toContain("step.completed");
    expect(types).toContain("flow.completed");
  }, 30_000);

  it("carries step output and error payloads intact", async () => {
    const okId = await wf.start(okFlow, {});
    await withWorker(() => settle(okId, "completed"));
    const done = (await eventsFor(okId)).find(
      (e) => e.type === "step.completed",
    );
    expect(done).toMatchObject({ stepId: "only", output: { ok: true } });

    const failId = await wf.start(failFlow, {});
    await withWorker(() => settle(failId, "failed"));
    const failed = (await eventsFor(failId)).find(
      (e) => e.type === "step.failed",
    );
    expect(failed).toMatchObject({ stepId: "boom", attempt: 1 });
    expect((await eventsFor(failId)).map((e) => e.type)).toContain(
      "flow.failed",
    );
  }, 30_000);

  it("observes concurrency supersession, minted inside the store's tx", async () => {
    // The event core cannot see: flow.canceled(cause: concurrency) is written
    // by the adapter, in the same transaction as the row updates.
    const first = await wf.start(superFlow, { k: "dupe" });
    const second = await wf.start(superFlow, { k: "dupe" });

    const canceled = (await eventsFor(first)).find(
      (e) => e.type === "flow.canceled",
    );
    expect(canceled).toMatchObject({
      type: "flow.canceled",
      cause: "concurrency",
      canceledByRunId: second,
    });
  }, 30_000);

  it("watchRun ends on its own once the run is terminal", async () => {
    const runId = await wf.start(okFlow, {});
    const perRun: RunEventEnvelope[] = [];
    wf.watchRun(runId, (e) => perRun.push(e));
    await withWorker(() => settle(runId, "completed"));
    await new Promise((r) => setTimeout(r, 300));
    const afterTerminal = perRun.length;
    expect(perRun.map((e) => e.type)).toContain("flow.completed");

    // Reopening must not reach the auto-disposed watcher.
    await wf.operator().retry(runId, "only", { actor: "ops", scope: "step" });
    await new Promise((r) => setTimeout(r, 300));
    expect(perRun).toHaveLength(afterTerminal);
  }, 30_000);

  it("throws when the store has no listener configured", async () => {
    const bare = await nagi({
      store: postgresStore({ db, schema }),
      queue: new InMemoryQueue(),
      clock: new InMemoryClock(),
      flows: [okFlow],
    });
    expect(() => bare.watchRuns(() => {})).toThrow(/listener/);
  }, 30_000);
});
