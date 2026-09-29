import { flow, InMemoryClock, InMemoryQueue, nagi } from "@nagi-js/core";
import { passthroughSchema } from "@nagi-js/core/testing";
import { Kysely, PostgresDialect, sql } from "kysely";
import pg from "pg";
import { describe, expect, it } from "vitest";
import { migrate } from "./migrations";
import { postgresStore } from "./store";
import type { StreamListener } from "./stream";
import { uuidv7 } from "./uuidv7";

const url = process.env["NAGI_POSTGRES_TEST_URL"];
const on = url !== undefined && process.env["NAGI_BENCH"] === "1";
const d = on ? describe : describe.skip;

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

const chain = flow({
  id: "bench-chain",
  input: passthroughSchema<Record<string, never>>(),
  build: (b) => {
    const s0 = b.task({ run: async () => ({ i: 0 }) });
    const s1 = b.task({ needs: { s0 }, run: async () => ({ i: 1 }) });
    const s2 = b.task({ needs: { s1 }, run: async () => ({ i: 2 }) });
    const s3 = b.task({ needs: { s2 }, run: async () => ({ i: 3 }) });
    const s4 = b.task({ needs: { s3 }, run: async () => ({ i: 4 }) });
    const s5 = b.task({ needs: { s4 }, run: async () => ({ i: 5 }) });
    const s6 = b.task({ needs: { s5 }, run: async () => ({ i: 6 }) });
    const s7 = b.task({ needs: { s6 }, run: async () => ({ i: 7 }) });
    const s8 = b.task({ needs: { s7 }, run: async () => ({ i: 8 }) });
    const s9 = b.task({ needs: { s8 }, run: async () => ({ i: 9 }) });
    return { s0, s1, s2, s3, s4, s5, s6, s7, s8, s9 };
  },
  output: (s) => s.s9,
});

const chunk = "x".repeat(200);
const streamFlow = flow({
  id: "bench-stream",
  input: passthroughSchema<Record<string, never>>(),
  build: (b) => ({
    gen: b.streamingTask({
      retry: { maxAttempts: 1, backoff: "fixed" },
      run: async ({ ctx }) => {
        for (let i = 0; i < 100; i++) await ctx.emit(chunk);
        return { n: 100 };
      },
    }),
  }),
  output: (s) => s.gen,
});

interface TrialResult {
  ms: number;
  runsPerSec: number;
  events: number;
  stream: number;
}

async function trial(opts: {
  flowKind: "chain" | "stream";
  listener: boolean;
  runs: number;
  concurrency: number;
}): Promise<TrialResult> {
  const connectionString = url as string;
  const pool = new pg.Pool({
    connectionString,
    max: opts.concurrency + 4,
  });
  const db = new Kysely<unknown>({ dialect: new PostgresDialect({ pool }) });
  const schema = `nagi_bn_${uuidv7().replace(/-/g, "").slice(0, 14)}`;
  await migrate(db, { schema });

  const counter = new pg.Client({ connectionString });
  await counter.connect();
  const counts = { events: 0, stream: 0 };
  counter.on("notification", (msg) => {
    if (msg.channel === `${schema}_events`) counts.events++;
    else if (msg.channel === `${schema}_stream`) counts.stream++;
  });
  await counter.query(`LISTEN "${schema}_events"`);
  await counter.query(`LISTEN "${schema}_stream"`);

  const listening = pgListener(connectionString);
  const store = postgresStore({
    db,
    schema,
    ...(opts.listener ? { listener: listening.listener } : {}),
  });
  try {
    const flows = opts.flowKind === "chain" ? [chain] : [streamFlow];
    const wf = await nagi({
      store,
      queue: new InMemoryQueue(),
      clock: new InMemoryClock(),
      flows,
    });
    if (opts.listener) await store.ready();

    counts.events = 0;
    counts.stream = 0;
    for (let i = 0; i < opts.runs; i++) {
      await wf.start(opts.flowKind === "chain" ? chain : streamFlow, {});
    }
    counts.events = 0;
    counts.stream = 0;

    const t0 = performance.now();
    await wf
      .worker({ concurrency: opts.concurrency, timerSweepIntervalMs: 0 })
      .runUntilEmpty();
    const t1 = performance.now();

    const bad = await sql<{ n: string }>`
      SELECT count(*) AS n FROM ${sql.raw(`${schema}.workflow_run`)}
      WHERE status <> 'completed'
    `.execute(db);
    expect(Number(bad.rows[0]?.n)).toBe(0);

    await new Promise((r) => setTimeout(r, 200));
    const ms = t1 - t0;
    return {
      ms,
      runsPerSec: opts.runs / (ms / 1000),
      events: counts.events,
      stream: counts.stream,
    };
  } finally {
    await store.close();
    await listening.close();
    await counter.end().catch(() => undefined);
    await sql.raw(`DROP SCHEMA IF EXISTS ${schema} CASCADE`).execute(db);
    await db.destroy();
  }
}

function median(xs: number[]): number {
  const s = [...xs].sort((a, b) => a - b);
  return s[Math.floor(s.length / 2)] as number;
}

d("@nagi-js/postgres — LISTEN/NOTIFY cost", () => {
  it("non-streaming matrix", async () => {
    const rows: Record<string, string | number>[] = [];
    const raw: string[] = [];
    for (const concurrency of [4, 16, 32]) {
      let baseline = 0;
      for (const listener of [false, true]) {
        await trial({ flowKind: "chain", listener, runs: 20, concurrency });
        const trials: TrialResult[] = [];
        for (let i = 0; i < 3; i++) {
          trials.push(
            await trial({
              flowKind: "chain",
              listener,
              runs: 200,
              concurrency,
            }),
          );
        }
        const rates = trials.map((t) => t.runsPerSec);
        const spread =
          (Math.max(...rates) - Math.min(...rates)) / median(rates);
        raw.push(
          `c=${concurrency} listener=${listener} runs/s=[${rates.map((r) => r.toFixed(1)).join(", ")}] spread=${(spread * 100).toFixed(0)}%`,
        );
        const med = median(rates);
        const events = median(trials.map((t) => t.events)) / 200;
        const stream = median(trials.map((t) => t.stream)) / 200;
        if (!listener) {
          expect(trials.every((t) => t.events === 0 && t.stream === 0)).toBe(
            true,
          );
          baseline = med;
        }
        rows.push({
          concurrency,
          listener: String(listener),
          "runs/s (median)": Number(med.toFixed(1)),
          "NOTIFY events/run": Number(events.toFixed(2)),
          "NOTIFY stream/run": Number(stream.toFixed(2)),
          "slowdown vs no listener": listener
            ? `${(((baseline - med) / baseline) * 100).toFixed(1)}%`
            : "-",
        });
      }
    }
    console.table(rows);
    console.log(raw.join("\n"));
  }, 600_000);

  it("streaming cell", async () => {
    const t0 = performance.now();
    const r = await trial({
      flowKind: "stream",
      listener: true,
      runs: 50,
      concurrency: 8,
    });
    const wall = performance.now() - t0;
    console.table([
      {
        "runs/s": Number(r.runsPerSec.toFixed(1)),
        "stream NOTIFY/run": Number((r.stream / 50).toFixed(2)),
        "events NOTIFY/run": Number((r.events / 50).toFixed(2)),
        "drain ms": Number(r.ms.toFixed(0)),
        "total wall ms": Number(wall.toFixed(0)),
      },
    ]);
  }, 600_000);
});
