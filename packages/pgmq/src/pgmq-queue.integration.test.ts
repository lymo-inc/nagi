import type {
  AttemptNumber,
  Queue,
  QueueMessage,
  RunId,
  StepId,
} from "@nagi-js/core";
import { Kysely, PostgresDialect, sql } from "kysely";
import pg from "pg";
import { afterAll, afterEach, beforeAll, describe, expect, it } from "vitest";
import { type PgmqQueue, type PgmqQueueOpts, pgmqQueue } from "./pgmq-queue";

const url = process.env["NAGI_PGMQ_TEST_URL"];
const d = url ? describe : describe.skip;

const runA = "run-a" as RunId;
const runB = "run-b" as RunId;
const s1 = "s1" as StepId;
const s2 = "s2" as StepId;

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

d("@nagi-js/pgmq — against the real pgmq extension", () => {
  let db: Kysely<unknown>;
  const created: string[] = [];

  beforeAll(() => {
    const pool = new pg.Pool({ connectionString: url });
    db = new Kysely<unknown>({ dialect: new PostgresDialect({ pool }) });
  });

  afterEach(async () => {
    for (const name of created.splice(0)) {
      await sql`SELECT pgmq.drop_queue(${name})`.execute(db);
    }
  });

  afterAll(async () => {
    await db?.destroy();
  });

  // pgmq queues are database-global, so every test gets its own.
  async function makeQueue(
    opts: Omit<PgmqQueueOpts, "db" | "queueName"> = {},
  ): Promise<{ q: PgmqQueue; name: string }> {
    const name = `nagi_it_${crypto.randomUUID().replace(/-/g, "").slice(0, 16)}`;
    const q = pgmqQueue({ db, queueName: name, ...opts });
    await q.ensureSchema();
    created.push(name);
    return { q, name };
  }

  async function serverNow(): Promise<number> {
    const { rows } = await sql<{
      now: Date;
    }>`SELECT clock_timestamp() AS now`.execute(db);
    return rows[0]!.now.getTime();
  }

  async function dequeueOne(q: Queue): Promise<QueueMessage> {
    const messages = await q.dequeue({ count: 10 });
    expect(messages).toHaveLength(1);
    return messages[0] as QueueMessage;
  }

  async function dequeueWithin(q: Queue, ms: number): Promise<QueueMessage> {
    const deadline = Date.now() + ms;
    for (;;) {
      const [m] = await q.dequeue({ count: 1 });
      if (m) return m;
      if (Date.now() > deadline) throw new Error(`no message within ${ms}ms`);
      await sleep(50);
    }
  }

  async function archivedCount(name: string): Promise<number> {
    const { rows } = await sql<{
      n: number;
    }>`SELECT count(*)::int AS n FROM ${sql.table(`pgmq.a_${name}`)}`.execute(
      db,
    );
    return rows[0]?.n ?? -1;
  }

  function inspect(q: Queue, runId: RunId) {
    return q.inspect!(runId);
  }

  it("ensureSchema is idempotent", async () => {
    const { q, name } = await makeQueue();
    await q.ensureSchema();

    const { rows } = await sql<{
      n: number;
    }>`SELECT count(*)::int AS n FROM pgmq.meta WHERE queue_name = ${name}`.execute(
      db,
    );
    expect(rows[0]?.n).toBe(1);
    await q.enqueue(runA, s1);
    expect((await dequeueOne(q)).stepId).toBe(s1);
  });

  it("enqueue → dequeue round-trips the envelope with a msg_id receipt", async () => {
    const { q } = await makeQueue();
    await q.enqueue(runA, s1, {
      attempt: 3 as AttemptNumber,
      flowId: "flow-x",
    });
    await q.enqueue(runB, s2);

    const messages = [...(await q.dequeue({ count: 10 }))].sort((a, b) =>
      a.stepId.localeCompare(b.stepId),
    );

    expect(messages).toEqual([
      {
        receipt: expect.stringMatching(/^\d+$/),
        runId: runA,
        stepId: s1,
        attempt: 3,
        readCount: 1,
        payload: null,
        flowId: "flow-x",
      },
      {
        receipt: expect.stringMatching(/^\d+$/),
        runId: runB,
        stepId: s2,
        attempt: 1,
        readCount: 1,
        payload: null,
      },
    ]);
    expect(messages[0]?.receipt).not.toBe(messages[1]?.receipt);
  });

  it("dequeue returns at most `count` messages", async () => {
    const { q } = await makeQueue();
    await q.enqueue(runA, s1);
    await q.enqueue(runA, s2);
    await q.enqueue(runB, s1);

    expect(await q.dequeue({ count: 2 })).toHaveLength(2);
    expect(await q.dequeue({ count: 2 })).toHaveLength(1);
    expect(await q.dequeue({ count: 2 })).toEqual([]);
  });

  it("archives a malformed envelope and returns only the valid ones", async () => {
    const { q, name } = await makeQueue();
    await sql`SELECT pgmq.send(${name}, ${JSON.stringify({ stepId: "x" })}::jsonb)`.execute(
      db,
    );
    await q.enqueue(runA, s1);

    const messages = await q.dequeue({ count: 10 });

    expect(messages).toHaveLength(1);
    expect(messages[0]?.stepId).toBe(s1);
    expect(await q.dequeue({ count: 10 })).toEqual([]);
    expect(await archivedCount(name)).toBe(1);
  });

  it("a dequeued message stays hidden for the visibility timeout, then is redelivered", async () => {
    const { q } = await makeQueue({ visibilityTimeoutMs: 2_000 });
    await q.enqueue(runA, s1);

    const startedAt = Date.now();
    const first = await dequeueOne(q);
    expect(await q.dequeue({ count: 10 })).toEqual([]);

    const again = await dequeueWithin(q, 10_000);
    expect(Date.now() - startedAt).toBeGreaterThanOrEqual(1_500);
    expect(again.receipt).toBe(first.receipt);
    expect(again.attempt).toBe(1);
    expect(again.readCount).toBe(2);
  }, 20_000);

  it("ack deletes the message", async () => {
    const { q, name } = await makeQueue();
    await q.enqueue(runA, s1);
    const m = await dequeueOne(q);

    await q.ack(m.receipt);

    expect(await inspect(q, runA)).toEqual([]);
    expect(await archivedCount(name)).toBe(0);
  });

  it("ack with archiveOnAck moves the message to the archive table", async () => {
    const { q, name } = await makeQueue({ archiveOnAck: true });
    await q.enqueue(runA, s1);
    const m = await dequeueOne(q);

    await q.ack(m.receipt);

    expect(await inspect(q, runA)).toEqual([]);
    expect(await archivedCount(name)).toBe(1);
  });

  it("nack makes a leased message visible again immediately", async () => {
    const { q } = await makeQueue();
    await q.enqueue(runA, s1);
    const m = await dequeueOne(q);
    expect(await q.dequeue({ count: 10 })).toEqual([]);

    await q.nack(m.receipt);

    const again = await dequeueOne(q);
    expect(again.receipt).toBe(m.receipt);
    expect(again.readCount).toBe(2);
  });

  it("nack with delayMs keeps the message hidden for that long", async () => {
    const { q } = await makeQueue({ visibilityTimeoutMs: 1_000 });
    await q.enqueue(runA, s1);
    const m = await dequeueOne(q);

    const before = await serverNow();
    await q.nack(m.receipt, { delayMs: 30_000 });

    const [entry] = await inspect(q, runA);
    expect(entry?.visibleAt.getTime()).toBeGreaterThanOrEqual(before + 29_000);
    await sleep(2_000);
    expect(await q.dequeue({ count: 10 })).toEqual([]);
  }, 15_000);

  it("extend pushes visibility past the original lease", async () => {
    const { q } = await makeQueue({ visibilityTimeoutMs: 1_000 });
    await q.enqueue(runA, s1);
    const m = await dequeueOne(q);

    const before = await serverNow();
    await q.extend(m.receipt, 60_000);

    const [entry] = await inspect(q, runA);
    expect(entry?.visibleAt.getTime()).toBeGreaterThanOrEqual(before + 59_000);
    await sleep(2_000);
    expect(await q.dequeue({ count: 10 })).toEqual([]);
  }, 15_000);

  it("inspect lists only the given run's queued messages, leased and delayed included", async () => {
    const { q } = await makeQueue();
    await q.enqueue(runA, s1);
    await q.enqueue(runA, s2, { attempt: 2 as AttemptNumber, delayMs: 30_000 });
    await q.enqueue(runB, s1);
    const leased = await q.dequeue({ count: 10 });
    expect(leased.map((m) => m.runId).sort()).toEqual([runA, runB]);

    const now = await serverNow();
    const entries = [...(await inspect(q, runA))].sort((a, b) =>
      a.stepId.localeCompare(b.stepId),
    );

    expect(entries).toEqual([
      { stepId: s1, attempt: 1, readCount: 1, visibleAt: expect.any(Date) },
      { stepId: s2, attempt: 2, readCount: 0, visibleAt: expect.any(Date) },
    ]);
    for (const e of entries) expect(e.visibleAt.getTime()).toBeGreaterThan(now);
    expect((await inspect(q, runB)).map((e) => e.stepId)).toEqual([s1]);
    expect(await inspect(q, "run-none" as RunId)).toEqual([]);
  });

  it("withTx enqueue commits with the caller's transaction", async () => {
    const { q } = await makeQueue();

    await db.transaction().execute(async (trx) => {
      await q.withTx(trx).enqueue(runA, s1);
      expect(await inspect(q.withTx(trx), runA)).toHaveLength(1);
      // The plain queue reads through another pool connection.
      expect(await inspect(q, runA)).toEqual([]);
    });

    const m = await dequeueOne(q);
    expect(m.runId).toBe(runA);
  });

  it("withTx writes roll back with the caller's transaction", async () => {
    const { q } = await makeQueue();
    await q.enqueue(runB, s1);
    const leased = await dequeueOne(q);

    await expect(
      db.transaction().execute(async (trx) => {
        const txq = q.withTx(trx);
        await txq.enqueue(runA, s1);
        await txq.ack(leased.receipt);
        throw new Error("rollback");
      }),
    ).rejects.toThrow("rollback");

    expect(await inspect(q, runA)).toEqual([]);
    expect(await inspect(q, runB)).toHaveLength(1);
  });
});
