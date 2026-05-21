import type {
  AttemptNumber,
  Millis,
  Queue,
  QueueDequeueOpts,
  QueueEnqueueOpts,
  QueueMessage,
  RunId,
  StepId,
  Tx,
} from "@nagi-js/core";
import { type Kysely, sql } from "kysely";

const DEFAULT_QUEUE_NAME = "nagi";
const DEFAULT_VISIBILITY_TIMEOUT_MS: Millis = 30_000;

export interface PgmqQueueOpts<DB = unknown> {
  readonly db: Kysely<DB>;
  readonly queueName?: string;
  readonly visibilityTimeoutMs?: Millis;
  readonly partitioned?: boolean;
  readonly archiveOnAck?: boolean;
}

export interface PgmqQueue extends Queue {
  ensureSchema(): Promise<void>;
  withTx(tx: Tx): Queue;
}

interface MessageEnvelope {
  readonly runId: string;
  readonly stepId: string;
  readonly attempt: number;
}

interface QueueConfig {
  readonly queueName: string;
  readonly vtSeconds: number;
  readonly archiveOnAck: boolean;
}

export function pgmqQueue<DB = unknown>(opts: PgmqQueueOpts<DB>): PgmqQueue {
  // Single internal erasure: callers keep their concrete Kysely<DB> (no cast at
  // the callsite); the queue body is schema-agnostic, so widen once here.
  const db = opts.db as unknown as Kysely<unknown>;
  const queueName = opts.queueName ?? DEFAULT_QUEUE_NAME;
  const vtSeconds = Math.max(
    1,
    Math.ceil(
      (opts.visibilityTimeoutMs ?? DEFAULT_VISIBILITY_TIMEOUT_MS) / 1000,
    ),
  );
  const partitioned = opts.partitioned ?? false;
  const archiveOnAck = opts.archiveOnAck ?? false;
  const config: QueueConfig = { queueName, vtSeconds, archiveOnAck };

  return {
    ...buildQueue(db, config),

    async ensureSchema(): Promise<void> {
      await sql`CREATE EXTENSION IF NOT EXISTS pgmq`.execute(db);
      if (partitioned) {
        await sql`SELECT pgmq.create_partitioned(${queueName})`.execute(db);
      } else {
        await sql`SELECT pgmq.create(${queueName})`.execute(db);
      }
    },

    withTx(tx: Tx): Queue {
      return buildQueue(tx as unknown as Kysely<unknown>, config);
    },
  };
}

function buildQueue(executor: Kysely<unknown>, config: QueueConfig): Queue {
  const { queueName, vtSeconds, archiveOnAck } = config;

  return {
    async enqueue(
      runId: RunId,
      stepId: StepId,
      options?: QueueEnqueueOpts,
    ): Promise<void> {
      const envelope: MessageEnvelope = {
        runId,
        stepId,
        attempt: options?.attempt ?? 1,
      };
      const delaySeconds = Math.max(
        0,
        Math.ceil((options?.delayMs ?? 0) / 1000),
      );
      await sql`SELECT pgmq.send(${queueName}, ${JSON.stringify(envelope)}::jsonb, ${delaySeconds}::int)`.execute(
        executor,
      );
    },

    async dequeue({
      count,
    }: QueueDequeueOpts): Promise<readonly QueueMessage[]> {
      const { rows } = await sql<{
        msg_id: string | number | bigint;
        message: unknown;
      }>`SELECT msg_id, message FROM pgmq.read(${queueName}, ${vtSeconds}::int, ${count}::int)`.execute(
        executor,
      );
      return rows.map((row) => projectMessage(row.msg_id, row.message));
    },

    async ack(receipt: string): Promise<void> {
      const msgId = parseReceipt(receipt);
      if (archiveOnAck) {
        await sql`SELECT pgmq.archive(${queueName}, ${msgId}::bigint)`.execute(
          executor,
        );
      } else {
        await sql`SELECT pgmq.delete(${queueName}, ${msgId}::bigint)`.execute(
          executor,
        );
      }
    },

    async nack(
      receipt: string,
      options?: { readonly delayMs?: Millis },
    ): Promise<void> {
      const msgId = parseReceipt(receipt);
      const delaySeconds = Math.max(
        0,
        Math.ceil((options?.delayMs ?? 0) / 1000),
      );
      await sql`SELECT pgmq.set_vt(${queueName}, ${msgId}::bigint, ${delaySeconds}::int)`.execute(
        executor,
      );
    },

    async extend(receipt: string, leaseMs: Millis): Promise<void> {
      const msgId = parseReceipt(receipt);
      const vt = Math.max(1, Math.ceil(leaseMs / 1000));
      await sql`SELECT pgmq.set_vt(${queueName}, ${msgId}::bigint, ${vt}::int)`.execute(
        executor,
      );
    },
  };
}

function projectMessage(
  rawMsgId: string | number | bigint,
  raw: unknown,
): QueueMessage {
  if (
    raw === null ||
    typeof raw !== "object" ||
    typeof (raw as { runId?: unknown }).runId !== "string" ||
    typeof (raw as { stepId?: unknown }).stepId !== "string" ||
    typeof (raw as { attempt?: unknown }).attempt !== "number"
  ) {
    throw new Error(
      `pgmq: malformed message envelope ${JSON.stringify(raw)} — expected { runId, stepId, attempt }`,
    );
  }
  const envelope = raw as MessageEnvelope;
  return {
    receipt: String(rawMsgId),
    runId: envelope.runId as RunId,
    stepId: envelope.stepId as StepId,
    attempt: envelope.attempt as AttemptNumber,
    payload: null,
  };
}

function parseReceipt(receipt: string): string {
  try {
    BigInt(receipt);
  } catch {
    throw new Error(
      `pgmq: malformed receipt ${JSON.stringify(receipt)} — expected stringified bigint msg_id`,
    );
  }
  return receipt;
}
