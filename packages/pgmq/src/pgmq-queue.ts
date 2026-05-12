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

export interface PgmqQueueOpts {
  /** Kysely instance. The adapter does NOT own the connection lifecycle. */
  readonly db: Kysely<unknown>;
  /** PGMQ queue name. Default: "nagi". */
  readonly queueName?: string;
  /** Visibility timeout applied on every dequeue, in ms. Default: 30_000. */
  readonly visibilityTimeoutMs?: Millis;
  /**
   * Use `pgmq.create_partitioned()` instead of `pgmq.create()` when
   * `ensureSchema()` runs. Default: false.
   */
  readonly partitioned?: boolean;
  /**
   * Use `pgmq.archive()` instead of `pgmq.delete()` on ack — retains messages
   * in the archive table for audit. Default: false.
   */
  readonly archiveOnAck?: boolean;
}

export interface PgmqQueue extends Queue {
  /**
   * Idempotent setup: installs the pgmq extension and creates the queue.
   * Requires database privileges to `CREATE EXTENSION`. Suitable for dev/test;
   * production should run these statements out-of-band.
   */
  ensureSchema(): Promise<void>;
  /**
   * Returns a `Queue` whose operations execute on the supplied transaction.
   * Pass `ctx.tx` to enqueue messages atomically with the handler's domain
   * writes — the pgmq message commits with `step.completed` or rolls back
   * with the handler. The user must have wired `@nagi-js/postgres` and
   * augmented `Register.tx` so that `Tx` is a Kysely transaction at compile
   * time; at runtime, `tx` must be the same Kysely handle the postgres store
   * handed to `runStep`.
   */
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

export function pgmqQueue(opts: PgmqQueueOpts): PgmqQueue {
  const db = opts.db;
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
      // `Tx` is the user-augmented transaction type from `@nagi-js/core`.
      // When `@nagi-js/postgres` is wired and `Register.tx` is augmented to
      // a Kysely transaction, the cast is structurally sound at runtime.
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
      // set_vt expects seconds offset from now. 0 = immediately re-visible.
      // Attempt counters live in the dispatcher — nack must not mutate them.
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
