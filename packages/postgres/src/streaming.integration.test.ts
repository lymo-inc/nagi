import {
  flow,
  InMemoryClock,
  InMemoryQueue,
  nagi,
  type RunId,
  type StepId,
  type StreamEvent,
  type Wf,
} from "@nagi-js/core";
import { passthroughSchema } from "@nagi-js/core/testing";
import { Kysely, PostgresDialect, sql } from "kysely";
import pg from "pg";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { migrate } from "./migrations";
import { postgresStore } from "./store";
import { MAX_CHUNK_BYTES, type StreamListener } from "./stream";
import { uuidv7 } from "./uuidv7";

const url = process.env["NAGI_POSTGRES_TEST_URL"];
const d = url ? describe : describe.skip;

// The listener a real consumer writes: nagi never opens this connection itself,
// because Kysely hides the driver and this package does not depend on `pg`.
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
        // Quoted so it matches pg_notify's literal, case-sensitive channel.
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

async function collect<C>(
  iter: AsyncIterable<StreamEvent<C>>,
): Promise<StreamEvent<C>[]> {
  const out: StreamEvent<C>[] = [];
  for await (const e of iter) out.push(e);
  return out;
}

d("@nagi-js/postgres — streaming over LISTEN/NOTIFY", () => {
  let db: Kysely<unknown>;
  let pool: pg.Pool;
  let schema: string;
  let listening: ReturnType<typeof pgListener>;

  beforeAll(async () => {
    pool = new pg.Pool({ connectionString: url });
    db = new Kysely<unknown>({ dialect: new PostgresDialect({ pool }) });
    schema = `nagi_stream_${uuidv7().replace(/-/g, "").slice(0, 14)}`;
    await migrate(db, { schema });
    listening = pgListener(url as string);
  }, 30_000);

  afterAll(async () => {
    await listening?.close();
    if (!db) return;
    await sql.raw(`DROP SCHEMA IF EXISTS ${schema} CASCADE`).execute(db);
    await db.destroy();
  }, 30_000);

  async function makeWf(
    flows: Parameters<typeof nagi>[0]["flows"],
  ): Promise<Wf> {
    const store = postgresStore({ db, schema, listener: listening.listener });
    // Every test here starts a worker that emits immediately. NOTIFY does not
    // queue for a connection that is not yet listening, so without this barrier
    // the chunks raced against LISTEN are lost outright, not delivered late.
    await store.ready();
    return nagi({
      store,
      queue: new InMemoryQueue(),
      clock: new InMemoryClock(),
      flows,
    });
  }

  // worker.run() settles only when its abort signal fires, so every test drives
  // it through this: start, await the condition, abort, await the loop.
  async function withWorker<T>(wf: Wf, body: () => Promise<T>): Promise<T> {
    const ac = new AbortController();
    const done = wf.worker({ pollIntervalMs: 5, signal: ac.signal }).run();
    try {
      return await body();
    } finally {
      ac.abort();
      await done;
    }
  }

  function tokenFlow(id: string, tokens: readonly string[]) {
    return flow({
      id,
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => ({
        gen: b.streamingTask({
          retry: { maxAttempts: 1, backoff: "fixed" },
          run: async ({ ctx }) => {
            for (const t of tokens) await ctx.emit(t);
            return { full: tokens.join("") };
          },
        }),
      }),
      output: (s) => s.gen,
    });
  }

  it("carries chunks from the worker to a subscriber and closes on completion", async () => {
    const f = tokenFlow("pg-stream-ok", ["He", "llo", "!"]);
    const wf = await makeWf([f]);
    const runId = await wf.start(f, {});

    const sub = collect<string>(wf.subscribe<string>(runId, "gen" as StepId));
    const events = await withWorker(wf, async () => {
      await waitForStatus(db, schema, runId, "completed");
      return sub;
    });
    expect(events.filter((e) => e.kind === "chunk")).toEqual([
      { kind: "chunk", chunk: "He" },
      { kind: "chunk", chunk: "llo" },
      { kind: "chunk", chunk: "!" },
    ]);
  }, 30_000);

  it("preserves chunk order across many chunks", async () => {
    // `db` is a pool: un-awaited pg_notify calls can take different connections
    // and arrive reversed. Three chunks reordered only intermittently; this many
    // makes the regression deterministic.
    const tokens = Array.from({ length: 40 }, (_, i) => `t${i}`);
    const f = tokenFlow("pg-stream-order", tokens);
    const wf = await makeWf([f]);
    const runId = await wf.start(f, {});

    const sub = collect<string>(wf.subscribe<string>(runId, "gen" as StepId));
    const events = await withWorker(wf, async () => {
      await waitForStatus(db, schema, runId, "completed");
      return sub;
    });

    const chunks = events
      .filter((e): e is { kind: "chunk"; chunk: string } => e.kind === "chunk")
      .map((e) => e.chunk);
    expect(chunks).toEqual(tokens);
  }, 30_000);

  it("closes with an error event when the step fails terminally", async () => {
    const f = flow({
      id: "pg-stream-fail",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => ({
        gen: b.streamingTask({
          retry: { maxAttempts: 1, backoff: "fixed" },
          run: async ({ ctx }) => {
            await ctx.emit("partial");
            throw new Error("upstream died");
          },
        }),
      }),
    });
    const wf = await makeWf([f]);
    const runId = await wf.start(f, {});

    const sub = collect<string>(wf.subscribe<string>(runId, "gen" as StepId));
    const events = await withWorker(wf, async () => {
      await waitForStatus(db, schema, runId, "failed");
      return sub;
    });
    expect(events[0]).toEqual({ kind: "chunk", chunk: "partial" });
    expect(events[events.length - 1]?.kind).toBe("error");
  }, 30_000);

  it("returns an already-closed stream for a settled step instead of hanging", async () => {
    const f = tokenFlow("pg-stream-settled", ["x"]);
    const wf = await makeWf([f]);
    const runId = await wf.start(f, {});
    await withWorker(wf, () => waitForStatus(db, schema, runId, "completed"));
    // Subscribing AFTER the step settled must terminate immediately.
    const events = await collect<string>(
      wf.subscribe<string>(runId, "gen" as StepId),
    );
    expect(events).toEqual([]);
  }, 30_000);

  it("rejects a chunk larger than the NOTIFY payload limit", async () => {
    const big = "x".repeat(MAX_CHUNK_BYTES + 100);
    const f = flow({
      id: "pg-stream-toobig",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => ({
        gen: b.streamingTask({
          retry: { maxAttempts: 1, backoff: "fixed" },
          run: async ({ ctx }) => {
            await ctx.emit(big);
            return {};
          },
        }),
      }),
    });
    const wf = await makeWf([f]);
    const runId = await wf.start(f, {});
    await withWorker(wf, () => waitForStatus(db, schema, runId, "failed"));
    const status = await loadStatus(db, schema, runId);
    expect(status).toBe("failed");
  }, 30_000);

  it("refuses to register a streaming flow when no listener is configured", async () => {
    const f = tokenFlow("pg-stream-nolistener", ["a"]);
    await expect(
      nagi({
        store: postgresStore({ db, schema }),
        queue: new InMemoryQueue(),
        clock: new InMemoryClock(),
        flows: [f],
      }),
    ).rejects.toThrow(/listener/);
  }, 30_000);

  it("ready() is total — it resolves on a store with no listener", async () => {
    // Nothing to wait for is not the same as nothing to call: a consumer
    // awaiting readiness must not have to branch on whether it wired a
    // listener.
    await expect(
      postgresStore({ db, schema }).ready(),
    ).resolves.toBeUndefined();
  }, 30_000);

  it("ready() resolves once, and every chunk after it reaches a subscriber", async () => {
    const tokens = Array.from({ length: 40 }, (_, i) => `t${i}`);
    const f = tokenFlow("pg-stream-ready", tokens);
    const store = postgresStore({ db, schema, listener: listening.listener });
    await store.ready();
    // Idempotent: the second await is the same settled promise, not a second
    // LISTEN.
    await store.ready();

    const wf = await nagi({
      store,
      queue: new InMemoryQueue(),
      clock: new InMemoryClock(),
      flows: [f],
    });
    const runId = await wf.start(f, {});
    const sub = collect<string>(wf.subscribe<string>(runId, "gen" as StepId));
    const events = await withWorker(wf, async () => {
      await waitForStatus(db, schema, runId, "completed");
      return sub;
    });

    // The head of the stream is what a lost LISTEN race truncates.
    expect(
      events.filter((e) => e.kind === "chunk").map((e) => e.chunk),
    ).toEqual(tokens);
  }, 30_000);
});

async function loadStatus(
  db: Kysely<unknown>,
  schema: string,
  runId: RunId,
): Promise<string | undefined> {
  const r = await sql<{ status: string }>`
    SELECT status FROM ${sql.raw(`${schema}.workflow_run`)}
    WHERE run_id = ${runId}
  `.execute(db);
  return r.rows[0]?.status;
}

async function waitForStatus(
  db: Kysely<unknown>,
  schema: string,
  runId: RunId,
  want: string,
  timeoutMs = 15_000,
): Promise<void> {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    if ((await loadStatus(db, schema, runId)) === want) return;
    await new Promise((r) => setTimeout(r, 10));
  }
  throw new Error(`run ${runId} never reached ${want}`);
}
