import type { AttemptNumber, RunId, StepId } from "@nagi-js/core";
import { CamelCasePlugin } from "kysely";
import { describe, expect, it } from "vitest";
import { pgmqQueue } from "./pgmq-queue";
import { createCapturingDb } from "./test-helpers";

const runId = "run-1" as RunId;
const stepId = "stepA" as StepId;

describe("pgmqQueue.enqueue", () => {
  it("calls pgmq.send with default queue, attempt=1, zero delay", async () => {
    const fake = createCapturingDb();
    const q = pgmqQueue({ db: fake.db });

    await q.enqueue(runId, stepId);

    expect(fake.queries).toHaveLength(1);
    const query = fake.queries[0];
    expect(query?.sql).toContain("pgmq.send");
    expect(query?.parameters).toEqual([
      "nagi",
      JSON.stringify({ runId: "run-1", stepId: "stepA", attempt: 1 }),
      0,
    ]);
  });

  it("uses provided queue name, attempt, and ceil(delayMs / 1000)", async () => {
    const fake = createCapturingDb();
    const q = pgmqQueue({ db: fake.db, queueName: "custom" });

    await q.enqueue(runId, stepId, {
      attempt: 3 as AttemptNumber,
      delayMs: 2500,
    });

    const params = fake.queries[0]?.parameters ?? [];
    expect(params[0]).toBe("custom");
    expect(params[1]).toBe(
      JSON.stringify({ runId: "run-1", stepId: "stepA", attempt: 3 }),
    );
    expect(params[2]).toBe(3);
  });
});

describe("pgmqQueue.dequeue", () => {
  it("issues pgmq.read with the visibility timeout and count", async () => {
    const fake = createCapturingDb();
    fake.enqueueRows([]);
    const q = pgmqQueue({ db: fake.db, visibilityTimeoutMs: 45_000 });

    await q.dequeue({ count: 5 });

    const query = fake.queries[0];
    expect(query?.sql).toContain("pgmq.read");
    expect(query?.parameters).toEqual(["nagi", 45, 5]);
  });

  it("projects rows to QueueMessage with receipt = String(msg_id)", async () => {
    const fake = createCapturingDb();
    fake.enqueueRows([
      { msg_id: "42", message: { runId: "r1", stepId: "s1", attempt: 1 } },
      { msg_id: 99, message: { runId: "r2", stepId: "s2", attempt: 2 } },
    ]);
    const q = pgmqQueue({ db: fake.db });

    const messages = await q.dequeue({ count: 10 });

    expect(messages).toEqual([
      { receipt: "42", runId: "r1", stepId: "s1", attempt: 1, payload: null },
      { receipt: "99", runId: "r2", stepId: "s2", attempt: 2, payload: null },
    ]);
  });

  it("throws on a malformed envelope", async () => {
    const fake = createCapturingDb();
    fake.enqueueRows([{ msg_id: "1", message: { stepId: "x", attempt: 1 } }]);
    const q = pgmqQueue({ db: fake.db });

    await expect(q.dequeue({ count: 1 })).rejects.toThrow(
      /malformed message envelope/,
    );
  });

  it("floors visibility timeout at 1 second", async () => {
    const fake = createCapturingDb();
    fake.enqueueRows([]);
    const q = pgmqQueue({ db: fake.db, visibilityTimeoutMs: 0 });

    await q.dequeue({ count: 1 });

    expect(fake.queries[0]?.parameters[1]).toBe(1);
  });
});

describe("pgmqQueue.ack", () => {
  it("uses pgmq.delete by default", async () => {
    const fake = createCapturingDb();
    const q = pgmqQueue({ db: fake.db });

    await q.ack("42");

    const query = fake.queries[0];
    expect(query?.sql).toContain("pgmq.delete");
    expect(query?.parameters).toEqual(["nagi", "42"]);
  });

  it("uses pgmq.archive when archiveOnAck is true", async () => {
    const fake = createCapturingDb();
    const q = pgmqQueue({ db: fake.db, archiveOnAck: true });

    await q.ack("42");

    expect(fake.queries[0]?.sql).toContain("pgmq.archive");
  });

  it("rejects malformed receipts before issuing SQL", async () => {
    const fake = createCapturingDb();
    const q = pgmqQueue({ db: fake.db });

    await expect(q.ack("not-a-bigint")).rejects.toThrow(/malformed receipt/);
    expect(fake.queries).toHaveLength(0);
  });
});

describe("pgmqQueue.nack", () => {
  it("uses pgmq.set_vt with 0 seconds by default", async () => {
    const fake = createCapturingDb();
    const q = pgmqQueue({ db: fake.db });

    await q.nack("100");

    const query = fake.queries[0];
    expect(query?.sql).toContain("pgmq.set_vt");
    expect(query?.parameters).toEqual(["nagi", "100", 0]);
  });

  it("converts delayMs to ceil seconds; does NOT mutate attempt", async () => {
    const fake = createCapturingDb();
    const q = pgmqQueue({ db: fake.db });

    await q.nack("100", { delayMs: 1500 });

    expect(fake.queries[0]?.parameters[2]).toBe(2);
  });
});

describe("pgmqQueue.extend", () => {
  it("calls pgmq.set_vt with ceil(leaseMs / 1000), min 1", async () => {
    const fake = createCapturingDb();
    const q = pgmqQueue({ db: fake.db });

    await q.extend("100", 30_000);

    expect(fake.queries[0]?.parameters).toEqual(["nagi", "100", 30]);
  });

  it("clamps tiny lease values to 1 second minimum", async () => {
    const fake = createCapturingDb();
    const q = pgmqQueue({ db: fake.db });

    await q.extend("100", 100);

    expect(fake.queries[0]?.parameters[2]).toBe(1);
  });
});

describe("pgmqQueue.withTx", () => {
  it("routes enqueue SQL to the tx executor, not the construction-time db", async () => {
    const baseDb = createCapturingDb();
    const txDb = createCapturingDb();
    const q = pgmqQueue({ db: baseDb.db });

    await q.withTx(txDb.db as unknown as never).enqueue(runId, stepId);

    expect(baseDb.queries).toHaveLength(0);
    expect(txDb.queries).toHaveLength(1);
    const query = txDb.queries[0];
    expect(query?.sql).toContain("pgmq.send");
    expect(query?.parameters[0]).toBe("nagi");
  });

  it("preserves the configured queueName and archiveOnAck on the tx-bound queue", async () => {
    const baseDb = createCapturingDb();
    const txDb = createCapturingDb();
    const q = pgmqQueue({
      db: baseDb.db,
      queueName: "audit",
      archiveOnAck: true,
    });

    await q.withTx(txDb.db as unknown as never).ack("123");

    expect(txDb.queries[0]?.sql).toContain("pgmq.archive");
    expect(txDb.queries[0]?.parameters[0]).toBe("audit");
  });

  it("the construction-time queue and the tx queue do not share state", async () => {
    const baseDb = createCapturingDb();
    const txDb = createCapturingDb();
    const q = pgmqQueue({ db: baseDb.db });

    await q.enqueue(runId, stepId);
    await q.withTx(txDb.db as unknown as never).enqueue(runId, stepId);

    expect(baseDb.queries).toHaveLength(1);
    expect(txDb.queries).toHaveLength(1);
  });
});

describe("pgmqQueue.ensureSchema", () => {
  it("installs the extension and creates a regular queue by default", async () => {
    const fake = createCapturingDb();
    const q = pgmqQueue({ db: fake.db });

    await q.ensureSchema();

    expect(fake.queries).toHaveLength(2);
    expect(fake.queries[0]?.sql).toMatch(/CREATE EXTENSION/i);
    expect(fake.queries[0]?.sql).toContain("pgmq");
    expect(fake.queries[1]?.sql).toContain("pgmq.create");
    expect(fake.queries[1]?.sql).not.toContain("create_partitioned");
  });

  it("creates a partitioned queue when partitioned: true", async () => {
    const fake = createCapturingDb();
    const q = pgmqQueue({ db: fake.db, partitioned: true });

    await q.ensureSchema();

    expect(fake.queries[1]?.sql).toContain("pgmq.create_partitioned");
  });
});

// Regression suite for N1 (RFC 0014). The pgmq adapter's internal SQL reads
// snake_case columns (`msg_id`); a caller-installed Kysely CamelCasePlugin
// rewrites the result to camelCase (`msgId`), so receipts came back as
// `String(undefined)` ("undefined") and every ack threw. The fix: strip
// plugins from the adapter's internal executor only.
describe("pgmqQueue under Kysely CamelCasePlugin (N1 regression)", () => {
  it("baseline: receive on a plain Kysely instance returns a stringified msg_id receipt", async () => {
    const fake = createCapturingDb();
    fake.enqueueRows([
      { msg_id: "42", message: { runId: "r1", stepId: "s1", attempt: 1 } },
    ]);
    const q = pgmqQueue({ db: fake.db });

    const messages = await q.dequeue({ count: 1 });
    expect(messages[0]?.receipt).toBe("42");
    expect(() => BigInt(messages[0]?.receipt ?? "")).not.toThrow();
  });

  it("receive under CamelCasePlugin still surfaces a valid receipt (msg_id is read via plugin-free executor)", async () => {
    const fake = createCapturingDb();
    fake.enqueueRows([
      { msg_id: "42", message: { runId: "r1", stepId: "s1", attempt: 1 } },
    ]);
    const dbWithPlugin = fake.db.withPlugin(new CamelCasePlugin());
    const q = pgmqQueue({ db: dbWithPlugin });

    const messages = await q.dequeue({ count: 1 });
    expect(messages[0]?.receipt).toBe("42");
    expect(BigInt(messages[0]?.receipt ?? "")).toBe(42n);
  });

  it("ack under CamelCasePlugin succeeds without a malformed-receipt throw", async () => {
    const fake = createCapturingDb();
    const dbWithPlugin = fake.db.withPlugin(new CamelCasePlugin());
    const q = pgmqQueue({ db: dbWithPlugin });

    await expect(q.ack("42")).resolves.toBeUndefined();
    expect(fake.queries[0]?.sql).toContain("pgmq.delete");
    expect(fake.queries[0]?.parameters).toEqual(["nagi", "42"]);
  });

  it("nack under CamelCasePlugin reschedules via set_vt", async () => {
    const fake = createCapturingDb();
    const dbWithPlugin = fake.db.withPlugin(new CamelCasePlugin());
    const q = pgmqQueue({ db: dbWithPlugin });

    await q.nack("100", { delayMs: 2000 });
    expect(fake.queries[0]?.sql).toContain("pgmq.set_vt");
    expect(fake.queries[0]?.parameters).toEqual(["nagi", "100", 2]);
  });

  it("extend under CamelCasePlugin pushes the lease forward via set_vt", async () => {
    const fake = createCapturingDb();
    const dbWithPlugin = fake.db.withPlugin(new CamelCasePlugin());
    const q = pgmqQueue({ db: dbWithPlugin });

    await q.extend("100", 30_000);
    expect(fake.queries[0]?.sql).toContain("pgmq.set_vt");
    expect(fake.queries[0]?.parameters).toEqual(["nagi", "100", 30]);
  });

  it('parseReceipt rejects "undefined" with a typed error that names the CamelCase failure mode', async () => {
    const fake = createCapturingDb();
    const q = pgmqQueue({ db: fake.db });

    await expect(q.ack("undefined")).rejects.toThrow(/CamelCasePlugin/);
  });
});
