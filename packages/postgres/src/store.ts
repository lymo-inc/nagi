import type {
  AttemptNumber,
  ClaimToken,
  ConcurrencyMode,
  Fact,
  FlowCanceledByConcurrencyFact,
  FlowStartedFact,
  GlobalFact,
  Json,
  Millis,
  PrunableStatus,
  PruneOpts,
  PruneResult,
  QueryRunsOpts,
  QueryRunsResult,
  Queue,
  ReapedLease,
  RowDelta,
  RunDescription,
  RunId,
  RunState,
  RunStatus,
  RunSummary,
  RunView,
  SerializedError,
  SettleSignalResult,
  StepCanceledFact,
  StepCompletedFact,
  StepFailedFact,
  StepId,
  StepRunStatus,
  StepView,
  Store,
  StreamEvent,
  StreamTransport,
  TimedOutSignal,
  Tx,
} from "@nagi-js/core";
import {
  clampQueryLimit,
  DEFAULT_SWEEP_LIMIT,
  decideExpiredLeaseAction,
  decideSignal,
  decideTimeout,
  decodeRunCursor,
  encodeRunCursor,
  factEffects,
  InMemoryStreamHub,
  isTerminalRun,
  NagiConcurrencyConflictError,
  projectRunState,
  rowDeltaOf,
  stepStateOf,
  stepStatusOf,
} from "@nagi-js/core";
import type { Kysely } from "kysely";
import { sql } from "kysely";
import {
  applyFrame,
  decodeFrame,
  encodeFrame,
  MAX_CHUNK_BYTES,
  type StreamFrame,
  type StreamListener,
} from "./stream";
import { uuidv7 } from "./uuidv7";

const DEFAULT_LEASE_MS: Millis = 60_000;
const SCHEMA_RE = /^[A-Za-z_][A-Za-z0-9_]*$/;

export interface PostgresStoreOpts<DB = unknown> {
  readonly db: Kysely<DB>;
  readonly schema?: string;
  readonly leaseMs?: Millis;
  readonly notifyChannel?: string;
  // The LISTEN connection carrying `b.streamingTask` chunks. Without it the
  // store exposes no stream transport and nagi() refuses to register a
  // streaming flow. See StreamListener for why it is injected.
  readonly listener?: StreamListener;
}

// `listen()` is async and a constructor cannot await it, so a freshly built
// store is not yet receiving NOTIFY. Callers that start work and subscribe in
// the same breath MUST await ready() first.
export interface PostgresStoreHandle extends Store {
  ready(): Promise<void>;
}

export function postgresStore<DB = unknown>(
  opts: PostgresStoreOpts<DB>,
): PostgresStoreHandle {
  return new PostgresStore(opts);
}

class PostgresStore<DB = unknown> implements Store {
  private readonly db: Kysely<DB>;
  private readonly schema: string;
  private readonly leaseMs: Millis;
  private readonly notifyChannel: string | undefined;
  private readonly streamChannel: string;
  // Until LISTEN is live NOTIFY delivers nothing here — it never queues for a
  // connection that is not yet listening — so chunks published before then are
  // gone, not delayed. Total rather than optional: with no listener there is
  // simply nothing to wait for.
  private readonly listening: Promise<void>;
  // Left UNASSIGNED when no listener is supplied: under
  // exactOptionalPropertyTypes an absent property and one set to undefined are
  // different things, and Store declares `stream?`.
  readonly stream?: StreamTransport;

  constructor(opts: PostgresStoreOpts<DB>) {
    if (!SCHEMA_RE.test(opts.schema ?? "nagi")) {
      throw new Error(
        `@nagi-js/postgres: invalid schema name "${opts.schema}". Must match /^[A-Za-z_][A-Za-z0-9_]*$/.`,
      );
    }
    this.db = opts.db;
    this.schema = opts.schema ?? "nagi";
    this.leaseMs = opts.leaseMs ?? DEFAULT_LEASE_MS;
    this.notifyChannel = opts.notifyChannel;
    // SCHEMA_RE already bounded this to an identifier, so the channel name is
    // safe and stays inside PostgreSQL's 63-byte limit.
    this.streamChannel = `${this.schema}_stream`;
    let listening: Promise<void> = Promise.resolve();
    if (opts.listener !== undefined) {
      const hub = new InMemoryStreamHub();
      const streamListening = opts.listener.listen(
        this.streamChannel,
        (payload) => {
          const frame = decodeFrame(payload);
          if (frame !== null) applyFrame(hub, frame);
        },
      );
      this.stream = this.makeStreamTransport(hub);

      listening = streamListening.then(() => undefined);
    }
    this.listening = listening;
    // ready() reports a failed LISTEN by rejecting, but nothing forces a caller
    // to ask. This keeps an unobserved failure from surfacing as an unhandled
    // rejection while leaving this.listening itself rejected for those who do.
    void this.listening.catch(() => undefined);
  }

  // Resolves once this process is actually receiving NOTIFY on the stream
  // channel.
  ready(): Promise<void> {
    return this.listening;
  }

  private makeStreamTransport(hub: InMemoryStreamHub): StreamTransport {
    return {
      subscribeStream: (
        runId: RunId,
        stepId: StepId,
        opts?: { readonly replayBuffered?: boolean },
      ): AsyncIterable<StreamEvent<Json>> => {
        // Register with the hub SYNCHRONOUSLY, before any await. An earlier cut
        // resolved the durable state first and subscribed after, which dropped
        // every chunk published during that round-trip — the worker emits as
        // soon as it claims the step.
        const live = hub.subscribeStream(runId, stepId, opts);

        // The step may already be settled, in which case no close frame is ever
        // coming (this process was not listening when it fired) and the iterator
        // would hang. Resolve that off the critical path and close the channel
        // if so; closeOk is a no-op on an already-closed one.
        void (async () => {
          try {
            await this.listening;
            const state = await this.loadRunState(runId);
            const status = stepStatusOf(stepStateOf(state, stepId));
            if (
              isTerminalRun(state) ||
              status === "completed" ||
              status === "failed" ||
              status === "canceled" ||
              status === "skipped"
            ) {
              hub.closeOk(runId, stepId);
            }
          } catch {
            /* a failed liveness probe must not tear down the subscription */
          }
        })();

        return live;
      },

      publishChunk: (runId: RunId, stepId: StepId, chunk: Json): void => {
        const frame = encodeFrame({
          k: "chunk",
          r: runId,
          s: stepId,
          c: chunk,
        });
        const size = byteLength(frame);
        if (size > MAX_CHUNK_BYTES) {
          throw new Error(
            `@nagi-js/postgres: streaming chunk for step "${stepId}" is ${size} bytes, over the ${MAX_CHUNK_BYTES}-byte NOTIFY limit. Emit smaller chunks, or carry the payload elsewhere and emit a reference.`,
          );
        }
        // Deliberately NOT on a transaction: chunks must reach subscribers
        // while the step is still running, and PostgreSQL holds a NOTIFY issued
        // inside a transaction until that transaction commits.
        this.notifyStream(`${runId}::${stepId}`, frame);
      },
    };
  }

  // Chunk order is the whole point of a token stream, and `db` is a POOL: two
  // un-awaited pg_notify calls can take different connections and arrive
  // reversed. So publishes for one step are chained — each waits for the
  // previous — while different steps stay independent. The chain is still
  // fire-and-forget to the caller, per the StreamTransport contract.
  private readonly publishTails = new Map<string, Promise<void>>();

  // Waits for in-flight chunk publishes: one step's chain, or every chain of a
  // run when the whole run is closing.
  private async drainPublishes(
    runId: RunId,
    stepId: StepId | undefined,
  ): Promise<void> {
    if (stepId !== undefined) {
      await this.publishTails.get(`${runId}::${stepId}`);
      return;
    }
    const prefix = `${runId}::`;
    await Promise.all(
      [...this.publishTails.entries()]
        .filter(([k]) => k.startsWith(prefix))
        .map(([, tail]) => tail),
    );
  }

  private notifyStream(key: string, frame: string): void {
    const prev = this.publishTails.get(key) ?? Promise.resolve();
    const next: Promise<void> = prev
      .then(async () => {
        await sql`SELECT pg_notify(${this.streamChannel}, ${frame})`.execute(
          this.db,
        );
      })
      .catch(() => {
        /* chunks are ephemeral; a lost one never corrupts the run */
      })
      .finally(() => {
        // Self-cleaning: drop the entry once this step's chain has drained, so
        // a long-lived process does not accumulate one per step ever streamed.
        if (this.publishTails.get(key) === next) this.publishTails.delete(key);
      });
    this.publishTails.set(key, next);
  }

  private t(table: string): string {
    return `${this.schema}.${table}`;
  }

  async appendFact(runId: RunId, fact: Fact): Promise<void> {
    await this.db.transaction().execute(async (trx) => {
      await this.persistFact(trx, runId, fact);
      await this.applyFactEffects(trx, runId, fact);
    });
    await this.maybeNotify(runId);
  }

  async tryStartRun(
    runId: RunId,
    fact: FlowStartedFact,
    concurrency?: {
      readonly key: string;
      readonly mode: ConcurrencyMode;
    },
  ): Promise<{
    readonly started: boolean;
    readonly canceled: ReadonlyArray<{
      readonly runId: RunId;
      readonly fact: FlowCanceledByConcurrencyFact;
    }>;
  }> {
    if (concurrency === undefined) {
      const started = await this.db.transaction().execute(async (trx) => {
        const insert = await sql<{ run_id: string }>`
          INSERT INTO ${sql.raw(this.t("workflow_run"))}
            (run_id, flow_id, status, input, started_at, flow_hash, code_version, parent_run_id, parent_step_id)
          VALUES
            (${runId}, ${fact.flowId}, 'running', ${jsonb(fact.input)}, ${fact.at}, ${fact.flowHash ?? null}, ${fact.codeVersion ?? null}, ${fact.parent?.runId ?? null}, ${fact.parent?.stepId ?? null})
          ON CONFLICT (run_id) DO NOTHING
          RETURNING run_id
        `.execute(trx);

        if (insert.rows.length === 0) {
          return false;
        }
        await this.insertFact(trx, runId, fact);
        return true;
      });

      if (started) {
        await this.maybeNotify(runId);
      }
      return { started, canceled: [] };
    }

    const result = await this.db.transaction().execute(async (trx) => {
      const lockText = `nagi:concurrency:${fact.flowId}:${concurrency.key}`;
      await sql`SELECT pg_advisory_xact_lock(hashtext(${lockText}))`.execute(
        trx,
      );

      const existing = await sql<{ run_id: string }>`
        SELECT run_id FROM ${sql.raw(this.t("workflow_run"))}
         WHERE run_id = ${runId}
         LIMIT 1
      `.execute(trx);
      if (existing.rows.length > 0) {
        return {
          started: false,
          canceled: [] as ReadonlyArray<{
            runId: RunId;
            fact: FlowCanceledByConcurrencyFact;
          }>,
        };
      }

      const others = await sql<{ run_id: string }>`
        SELECT run_id FROM ${sql.raw(this.t("workflow_run"))}
         WHERE flow_id = ${fact.flowId}
           AND concurrency_key = ${concurrency.key}
           AND status IN ('pending', 'running')
        FOR UPDATE
      `.execute(trx);

      const canceled: Array<{
        runId: RunId;
        fact: FlowCanceledByConcurrencyFact;
      }> = [];
      for (const row of others.rows) {
        const priorRunId = row.run_id as RunId;
        const cancelFact: FlowCanceledByConcurrencyFact = {
          kind: "flow.canceled",
          cause: "concurrency",
          runId: priorRunId,
          at: fact.at,
          canceledByRunId: runId,
          concurrencyKey: concurrency.key,
        };
        await this.persistFact(trx, priorRunId, cancelFact);
        canceled.push({ runId: priorRunId, fact: cancelFact });
      }

      await sql`
        INSERT INTO ${sql.raw(this.t("workflow_run"))}
          (run_id, flow_id, status, input, started_at, flow_hash, code_version, concurrency_key, parent_run_id, parent_step_id)
        VALUES
          (${runId}, ${fact.flowId}, 'running', ${jsonb(fact.input)}, ${fact.at}, ${fact.flowHash ?? null}, ${fact.codeVersion ?? null}, ${concurrency.key}, ${fact.parent?.runId ?? null}, ${fact.parent?.stepId ?? null})
      `.execute(trx);
      await this.insertFact(trx, runId, fact);
      await this.linkSuperseder(
        trx,
        runId,
        canceled.map((c) => c.runId),
      );

      return { started: true, canceled };
    });

    if (result.started) {
      await this.maybeNotify(runId);
      for (const c of result.canceled) {
        await this.maybeNotify(c.runId);
      }
    }
    return result;
  }

  async tryStartRunOnTx(
    tx: Tx,
    runId: RunId,
    fact: FlowStartedFact,
    concurrency?: {
      readonly key: string;
      readonly mode: ConcurrencyMode;
    },
  ): Promise<{
    readonly started: boolean;
    readonly canceled: ReadonlyArray<{
      readonly runId: RunId;
      readonly fact: FlowCanceledByConcurrencyFact;
    }>;
  }> {
    const trx = tx as unknown as Kysely<DB>;

    if (concurrency === undefined) {
      const insert = await sql<{ run_id: string }>`
        INSERT INTO ${sql.raw(this.t("workflow_run"))}
          (run_id, flow_id, status, input, started_at, flow_hash, code_version, parent_run_id, parent_step_id)
        VALUES
          (${runId}, ${fact.flowId}, 'running', ${jsonb(fact.input)}, ${fact.at}, ${fact.flowHash ?? null}, ${fact.codeVersion ?? null}, ${fact.parent?.runId ?? null}, ${fact.parent?.stepId ?? null})
        ON CONFLICT (run_id) DO NOTHING
        RETURNING run_id
      `.execute(trx);

      if (insert.rows.length === 0) {
        return { started: false, canceled: [] };
      }
      await this.insertFact(trx, runId, fact);
      return { started: true, canceled: [] };
    }

    // D6=A: no advisory lock under shared tx — the partial unique index on
    // (flow_id, concurrency_key) WHERE status IN ('pending','running') is the
    // load-bearing invariant. We retry the SELECT-prior + INSERT-new pass once
    // on a unique-violation race; on a second violation we surface
    // NagiConcurrencyConflictError so the caller can decide how to recover.
    let attemptedRetry = false;
    for (;;) {
      const existing = await sql<{ run_id: string }>`
        SELECT run_id FROM ${sql.raw(this.t("workflow_run"))}
         WHERE run_id = ${runId}
         LIMIT 1
      `.execute(trx);
      if (existing.rows.length > 0) {
        return { started: false, canceled: [] };
      }

      const others = await sql<{ run_id: string }>`
        SELECT run_id FROM ${sql.raw(this.t("workflow_run"))}
         WHERE flow_id = ${fact.flowId}
           AND concurrency_key = ${concurrency.key}
           AND status IN ('pending', 'running')
        FOR UPDATE
      `.execute(trx);

      const canceled: Array<{
        runId: RunId;
        fact: FlowCanceledByConcurrencyFact;
      }> = [];
      for (const row of others.rows) {
        const priorRunId = row.run_id as RunId;
        const cancelFact: FlowCanceledByConcurrencyFact = {
          kind: "flow.canceled",
          cause: "concurrency",
          runId: priorRunId,
          at: fact.at,
          canceledByRunId: runId,
          concurrencyKey: concurrency.key,
        };
        await this.persistFact(trx, priorRunId, cancelFact);
        canceled.push({ runId: priorRunId, fact: cancelFact });
      }

      try {
        await sql`
          INSERT INTO ${sql.raw(this.t("workflow_run"))}
            (run_id, flow_id, status, input, started_at, flow_hash, code_version, concurrency_key, parent_run_id, parent_step_id)
          VALUES
            (${runId}, ${fact.flowId}, 'running', ${jsonb(fact.input)}, ${fact.at}, ${fact.flowHash ?? null}, ${fact.codeVersion ?? null}, ${concurrency.key}, ${fact.parent?.runId ?? null}, ${fact.parent?.stepId ?? null})
        `.execute(trx);
      } catch (err) {
        if (isUniqueViolation(err) && !attemptedRetry) {
          attemptedRetry = true;
          continue;
        }
        if (isUniqueViolation(err)) {
          throw new NagiConcurrencyConflictError({
            runId,
            flowId: fact.flowId,
            concurrencyKey: concurrency.key,
          });
        }
        throw err;
      }
      await this.insertFact(trx, runId, fact);
      await this.linkSuperseder(
        trx,
        runId,
        canceled.map((c) => c.runId),
      );
      return { started: true, canceled };
    }
  }

  async loadRunState(runId: RunId): Promise<RunState> {
    return this.loadRunStateWith(this.db, runId);
  }

  private async loadRunStateWith(
    executor: Kysely<DB>,
    runId: RunId,
  ): Promise<RunState> {
    const rows = await sql<{
      kind: string;
      at: Date;
      payload: unknown;
    }>`
      SELECT kind, at, payload
        FROM ${sql.raw(this.t("fact"))}
       WHERE run_id = ${runId}
       ORDER BY fact_id ASC
    `.execute(executor);

    const facts: Fact[] = rows.rows.map((r) =>
      reviveFact(r.kind, r.at, r.payload),
    );
    return projectRunState(runId, facts);
  }

  async settleSignal(args: {
    readonly runId: RunId;
    readonly stepId: StepId;
    readonly at: Date;
    readonly incoming?: {
      readonly payload: Json;
      readonly signalName?: string;
    };
  }): Promise<SettleSignalResult> {
    const { runId, stepId, at, incoming } = args;
    const result = await this.db
      .transaction()
      .execute(async (trx): Promise<SettleSignalResult> => {
        // Serialize signal reconciliation per run so a wf.signal() call and the
        // worker claiming the step can't interleave and drop an early signal.
        const lockText = `nagi:signal:${runId}`;
        await sql`SELECT pg_advisory_xact_lock(hashtext(${lockText}))`.execute(
          trx,
        );

        const runState = await this.loadRunStateWith(trx, runId);
        const decision = decideSignal({ runState, stepId, at, incoming });
        switch (decision.kind) {
          case "noop":
            return decision.result;
          case "buffer":
            if (decision.fact !== null)
              await this.insertFact(trx, runId, decision.fact);
            return decision.result;
          case "deliver":
            await this.persistFact(trx, runId, decision.received);
            await this.persistFact(trx, runId, decision.completed);
            await this.applyFactEffects(trx, runId, decision.completed);
            return decision.result;
        }
      });
    await this.maybeNotify(runId);
    return result;
  }

  async claimStep(
    runId: RunId,
    stepId: StepId,
    attempt: AttemptNumber,
  ): Promise<ClaimToken | null> {
    const token = `lease-${crypto.randomUUID()}`;

    // Expiry on the database clock, like extendLease and the ON CONFLICT guard
    // below — an app clock a few seconds fast must not grant a second claim.
    const result = await sql<{ token: string }>`
      INSERT INTO ${sql.raw(this.t("lease"))} (run_id, step_id, attempt, token, expires_at)
      VALUES (${runId}, ${stepId}, ${attempt}, ${token}, now() + (${this.leaseMs}::int * interval '1 ms'))
      ON CONFLICT (run_id, step_id, attempt) DO UPDATE
        SET token = EXCLUDED.token, expires_at = EXCLUDED.expires_at
        WHERE ${sql.raw(this.t("lease"))}.expires_at < now()
      RETURNING token
    `.execute(this.db);

    const row = result.rows[0];
    return row ? (row.token as ClaimToken) : null;
  }

  async extendLease(
    runId: RunId,
    stepId: StepId,
    attempt: AttemptNumber,
    leaseMs: Millis,
  ): Promise<void> {
    // Idempotent: 0-row update (settled/reaped/stale) is fine — the next
    // heartbeat tick re-checks. Computing expires_at from now() inside the SQL
    // keeps the value source-of-truth in the database, matching claimStep.
    await sql`
      UPDATE ${sql.raw(this.t("lease"))}
         SET expires_at = now() + (${leaseMs}::int * interval '1 ms')
       WHERE run_id = ${runId} AND step_id = ${stepId} AND attempt = ${attempt}
    `.execute(this.db);
  }

  async sweepLeases(args: {
    readonly now: Date;
    readonly queue: Queue;
    readonly limit?: number;
  }): Promise<readonly ReapedLease[]> {
    const { now, queue, limit = DEFAULT_SWEEP_LIMIT } = args;
    return this.db.transaction().execute(async (trx) => {
      // FOR UPDATE OF l SKIP LOCKED so concurrent reapers split the batch
      // without retry; the LEFT JOIN surfaces the step status that
      // decideExpiredLeaseAction needs to skip terminal steps cleanly.
      // child_active: a subflow step parked on a still-running child must NOT be
      // re-dispatched (it would re-park, or self-supersede the live child). The
      // EXISTS mirrors the concurrency-active set (status IN pending/running).
      const rows = await sql<{
        run_id: string;
        step_id: string;
        attempt: number;
        expires_at: Date;
        status: StepRunStatus | null;
        child_active: boolean;
        flow_id: string | null;
      }>`
        SELECT l.run_id, l.step_id, l.attempt, l.expires_at, s.status,
          EXISTS (
            SELECT 1 FROM ${sql.raw(this.t("workflow_run"))} c
             WHERE c.parent_run_id = l.run_id
               AND c.parent_step_id = l.step_id
               AND c.status IN ('pending', 'running')
          ) AS child_active,
          r.flow_id
          FROM ${sql.raw(this.t("lease"))} l
          LEFT JOIN ${sql.raw(this.t("step_run"))} s
            ON s.run_id = l.run_id
           AND s.step_id = l.step_id
           AND s.attempt = l.attempt
          LEFT JOIN ${sql.raw(this.t("workflow_run"))} r
            ON r.run_id = l.run_id
         WHERE l.expires_at < ${now}
         FOR UPDATE OF l SKIP LOCKED
         LIMIT ${limit}
      `.execute(trx);

      // Bind to the same tx so the delete + fact insert + queue enqueue all
      // commit atomically; a tx rollback (e.g. on enqueue error) leaves the
      // lease intact for the next sweep, never a half-reaped state.
      const txQueue = queue.withTx?.(trx as unknown as Tx) ?? queue;
      const reaped: ReapedLease[] = [];

      for (const row of rows.rows) {
        const runId = row.run_id as RunId;
        const stepId = row.step_id as StepId;
        const attempt = row.attempt as AttemptNumber;
        const expiresAt =
          row.expires_at instanceof Date
            ? row.expires_at
            : new Date(row.expires_at);
        const stepStatus: StepRunStatus = row.status ?? "pending";

        const decision = decideExpiredLeaseAction({
          lease: { runId, stepId, attempt, expiresAt },
          stepStatus,
          childActive: row.child_active,
          now,
        });
        if (decision.tag === "skip") continue;

        await sql`
          DELETE FROM ${sql.raw(this.t("lease"))}
           WHERE run_id = ${runId} AND step_id = ${stepId} AND attempt = ${attempt}
        `.execute(trx);

        const fact: Fact = {
          kind: "lease.reaped",
          runId,
          stepId,
          attempt,
          at: now,
          reapedAt: now,
          reason: "expired",
        };
        await this.insertFact(trx, runId, fact);

        await txQueue.enqueue(runId, stepId, {
          attempt: decision.nextAttempt,
          delayMs: decision.backoffMs,
          ...(row.flow_id !== null ? { flowId: row.flow_id } : {}),
        });

        reaped.push({
          runId,
          stepId,
          attempt,
          nextAttempt: decision.nextAttempt,
        });
      }

      return reaped;
    });
  }

  async upsertTimer(runId: RunId, stepId: StepId, fireAt: Date): Promise<void> {
    // DO NOTHING, not DO UPDATE: the deadline anchors to when the step FIRST
    // started awaiting. A lease-reap re-dispatch (which re-runs recordStarted at
    // attempt+1 on a still-parked signal) must NOT push the deadline out, or a
    // reaper running every leaseMs would reset it forever and the timeout would
    // never fire. A genuine restart deletes the row first (step.reset), so the
    // next arm is fresh.
    await sql`
      INSERT INTO ${sql.raw(this.t("timer"))} (run_id, step_id, fire_at)
      VALUES (${runId}, ${stepId}, ${fireAt})
      ON CONFLICT (run_id, step_id) DO NOTHING
    `.execute(this.db);
  }

  async sweepSignalTimeouts(args: {
    readonly now: Date;
    readonly limit?: number;
  }): Promise<readonly TimedOutSignal[]> {
    const { now, limit = DEFAULT_SWEEP_LIMIT } = args;

    // Read due timers WITHOUT a row lock, then resolve each in its OWN
    // transaction under one per-run advisory lock — exactly settleSignal's
    // shape. Holding only a single advisory xact-lock at a time avoids two
    // hazards a batch-tx would create: an ABBA deadlock with settleSignal
    // (which takes the advisory lock, then deletes the timer row), and a
    // deadlock between two sweepers that lock overlapping runs in different
    // order. Concurrent sweepers stay correct without SKIP LOCKED: the advisory
    // lock serializes them, and the loser folds a non-awaiting step → noop.
    const rows = await sql<{
      run_id: string;
      step_id: string;
      fire_at: Date;
    }>`
      SELECT run_id, step_id, fire_at
        FROM ${sql.raw(this.t("timer"))}
       WHERE fire_at < ${now}
       LIMIT ${limit}
    `.execute(this.db);

    const timedOut: TimedOutSignal[] = [];
    for (const row of rows.rows) {
      const runId = row.run_id as RunId;
      const stepId = row.step_id as StepId;
      const fireAt =
        row.fire_at instanceof Date ? row.fire_at : new Date(row.fire_at);

      const failed = await this.db.transaction().execute(async (trx) => {
        // The SAME per-run lock settleSignal takes: a delivery and a timeout
        // can never both resolve the step. The lock-loser folds a non-awaiting
        // step → noop. Held until commit, so the fail + delete land atomically.
        const lockText = `nagi:signal:${runId}`;
        await sql`SELECT pg_advisory_xact_lock(hashtext(${lockText}))`.execute(
          trx,
        );

        const runState = await this.loadRunStateWith(trx, runId);
        const decision = decideTimeout({ runState, stepId, fireAt, at: now });

        // Fired once: drop the row whether or not it still had an awaiting step.
        await this.deleteTimer(trx, runId, stepId);
        if (decision.kind === "noop") return false;

        await this.persistFact(trx, runId, decision.fact);
        await this.applyFactEffects(trx, runId, decision.fact);
        return decision.attempt;
      });

      if (failed !== false) {
        timedOut.push({ runId, stepId, attempt: failed, fireAt });
      }
    }
    return timedOut;
  }

  async settleStep(
    runId: RunId,
    _stepId: StepId,
    fact: StepCompletedFact | StepFailedFact,
  ): Promise<void> {
    await this.db.transaction().execute(async (trx) => {
      await this.persistFact(trx, runId, fact);
      await this.applyFactEffects(trx, runId, fact);
    });
    await this.maybeNotify(runId);
  }

  async recordOnce(
    runId: RunId,
    stepId: StepId,
    scope: string,
    value: Json,
  ): Promise<void> {
    await sql`
      INSERT INTO ${sql.raw(this.t("dedupe"))} (run_id, step_id, scope, value)
      VALUES (${runId}, ${stepId}, ${scope}, ${jsonb(value)})
      ON CONFLICT (run_id, step_id, scope) DO NOTHING
    `.execute(this.db);
  }

  async getOnce(
    runId: RunId,
    stepId: StepId,
    scope: string,
  ): Promise<Json | null> {
    const r = await sql<{ value: Json | null }>`
      SELECT value
        FROM ${sql.raw(this.t("dedupe"))}
       WHERE run_id = ${runId} AND step_id = ${stepId} AND scope = ${scope}
    `.execute(this.db);
    return r.rows[0]?.value ?? null;
  }

  async runStep<T extends Json>(
    runId: RunId,
    _stepId: StepId,
    _attempt: AttemptNumber,
    body: (tx: Tx) => Promise<{
      readonly output: T;
      readonly fact: StepCompletedFact | StepFailedFact | StepCanceledFact;
    }>,
  ): Promise<T> {
    const output = await this.db.transaction().execute(async (trx) => {
      const result = await body(trx as unknown as Tx);
      await this.persistFact(trx, runId, result.fact);
      await this.applyFactEffects(trx, runId, result.fact);
      return result.output;
    });

    await this.maybeNotify(runId);
    return output;
  }

  private async insertFact(
    trx: Kysely<DB>,
    runId: RunId,
    fact: Fact,
  ): Promise<void> {
    await sql`
      INSERT INTO ${sql.raw(this.t("fact"))} (run_id, fact_id, kind, at, payload)
      VALUES (${runId}, ${uuidv7()}, ${fact.kind}, ${fact.at}, ${jsonb(serializeFactPayload(fact))})
    `.execute(trx);
  }

  // Victims are canceled BEFORE the superseder row exists: the partial unique
  // index on (flow_id, concurrency_key) only frees the slot once they leave
  // 'pending'/'running'. Their reference is therefore resolved here, after the
  // insert, rather than by the cancel write that cannot yet name it.
  private async linkSuperseder(
    trx: Kysely<DB>,
    runId: RunId,
    priors: ReadonlyArray<RunId>,
  ): Promise<void> {
    if (priors.length === 0) return;
    await sql`
      UPDATE ${sql.raw(this.t("workflow_run"))}
         SET canceled_by_run_id = ${runId}
       WHERE run_id = ANY(${priors}::text[])
    `.execute(trx);
  }

  private async persistFact(
    trx: Kysely<DB>,
    runId: RunId,
    fact: Fact,
  ): Promise<void> {
    await this.insertFact(trx, runId, fact);
    const delta = rowDeltaOf(fact);
    if (delta !== null) await this.applyRowDelta(trx, runId, delta);
    await this.notifyStreamClose(trx, runId, fact);
  }

  // Stream close/retry events ride the SAME transaction as the fact that caused
  // them. That is the opposite of publishChunk on purpose: PostgreSQL holds a
  // NOTIFY until commit, so a subscriber is told the step is finished only once
  // the fact is durable, and a rolled-back attempt never announces anything.
  private async notifyStreamClose(
    trx: Kysely<DB>,
    runId: RunId,
    fact: Fact,
  ): Promise<void> {
    if (this.stream === undefined) return;
    const frame = closeFrameOf(runId, fact);
    if (frame === null) return;
    // Chunks go out on their own connections and commit immediately; this frame
    // rides `trx` and lands at commit. Without draining the step's publish chain
    // first, a close can overtake chunks still in flight and truncate the
    // stream — the channel closes and the stragglers are discarded.
    await this.drainPublishes(runId, frame.k === "run" ? undefined : frame.s);
    await sql`SELECT pg_notify(${this.streamChannel}, ${encodeFrame(frame)})`.execute(
      trx,
    );
  }

  private applyRowDelta(
    trx: Kysely<DB>,
    runId: RunId,
    delta: RowDelta,
  ): Promise<void> {
    switch (delta.row) {
      case "run":
        return this.applyRunDelta(trx, runId, delta);
      case "step":
        return this.applyStepDelta(trx, runId, delta);
      case "once":
        return this.insertOnce(trx, runId, delta);
    }
  }

  private async applyRunDelta(
    trx: Kysely<DB>,
    runId: RunId,
    delta: Extract<RowDelta, { row: "run" }>,
  ): Promise<void> {
    switch (delta.status) {
      case "running":
        await sql`
          INSERT INTO ${sql.raw(this.t("workflow_run"))}
            (run_id, flow_id, status, input, started_at, flow_hash, code_version, parent_run_id, parent_step_id)
          VALUES
            (${runId}, ${delta.flowId}, 'running', ${jsonb(delta.input)}, ${delta.startedAt}, ${delta.flowHash}, ${delta.codeVersion}, ${delta.parent?.runId ?? null}, ${delta.parent?.stepId ?? null})
          ON CONFLICT (run_id) DO NOTHING
        `.execute(trx);
        return;
      case "completed":
        await sql`
          UPDATE ${sql.raw(this.t("workflow_run"))}
             SET status = 'completed', output = ${jsonb(delta.output)}, completed_at = ${delta.completedAt}
           WHERE run_id = ${runId}
        `.execute(trx);
        return;
      case "failed":
        await sql`
          UPDATE ${sql.raw(this.t("workflow_run"))}
             SET status = 'failed', error = ${jsonb(delta.error as unknown as Json)}, completed_at = ${delta.completedAt}
           WHERE run_id = ${runId}
        `.execute(trx);
        return;
      case "canceled":
        // NagiCanceledError is public: a handler can name any run as its
        // canceler, and classifyFailure turns that into this delta. The FACT
        // keeps the claim verbatim; the column takes it only when it resolves,
        // so the reference stays joinable (nagi#29).
        await sql`
          UPDATE ${sql.raw(this.t("workflow_run"))}
             SET status = 'canceled',
                 canceled_by_run_id = (
                   SELECT s.run_id FROM ${sql.raw(this.t("workflow_run"))} s
                    WHERE s.run_id = ${delta.canceledByRunId}
                 ),
                 completed_at = ${delta.completedAt}
           WHERE run_id = ${runId}
        `.execute(trx);
        return;
    }
  }

  private async applyStepDelta(
    trx: Kysely<DB>,
    runId: RunId,
    delta: Extract<RowDelta, { row: "step" }>,
  ): Promise<void> {
    switch (delta.status) {
      case "running":
        await sql`
          INSERT INTO ${sql.raw(this.t("step_run"))} (run_id, step_id, attempt, status, started_at)
          VALUES (${runId}, ${delta.stepId}, ${delta.attempt}, 'running', ${delta.startedAt})
          ON CONFLICT (run_id, step_id, attempt) DO UPDATE
            SET status = 'running', started_at = EXCLUDED.started_at
        `.execute(trx);
        return;
      case "completed":
        await this.upsertStepCompleted(
          trx,
          runId,
          delta.stepId,
          delta.attempt,
          delta.output,
        );
        return;
      case "failed":
        await this.upsertStepFailed(
          trx,
          runId,
          delta.stepId,
          delta.attempt,
          delta.error,
        );
        return;
      case "canceled":
        await this.upsertStepCanceled(
          trx,
          runId,
          delta.stepId,
          delta.attempt,
          delta.error ?? undefined,
        );
        return;
      case "skipped":
        await sql`
          INSERT INTO ${sql.raw(this.t("step_run"))} (run_id, step_id, attempt, status)
          VALUES (${runId}, ${delta.stepId}, 0, 'skipped')
          ON CONFLICT (run_id, step_id, attempt) DO UPDATE SET status = 'skipped'
        `.execute(trx);
        return;
      case "reset": {
        // Reopen a completed/failed run (replay, operator.retry). Read the
        // identity first: after a unique violation the tx is aborted and no
        // further SELECT would succeed.
        const row = await sql<{
          flow_id: string;
          concurrency_key: string | null;
        }>`
          SELECT flow_id, concurrency_key FROM ${sql.raw(this.t("workflow_run"))}
           WHERE run_id = ${runId}
        `.execute(trx);
        try {
          await sql`
            UPDATE ${sql.raw(this.t("workflow_run"))}
               SET status = 'running', output = NULL, error = NULL, completed_at = NULL
             WHERE run_id = ${runId} AND status IN ('completed', 'failed')
          `.execute(trx);
        } catch (err) {
          // workflow_run_concurrency_active_uidx: another active run holds this key.
          if (!isUniqueViolation(err)) throw err;
          throw new NagiConcurrencyConflictError({
            runId,
            flowId: row.rows[0]?.flow_id ?? "",
            concurrencyKey: row.rows[0]?.concurrency_key ?? "",
          });
        }
        await sql`
          DELETE FROM ${sql.raw(this.t("step_run"))}
           WHERE run_id = ${runId} AND step_id = ${delta.stepId}
        `.execute(trx);
        return;
      }
    }
  }

  private async insertOnce(
    trx: Kysely<DB>,
    runId: RunId,
    delta: Extract<RowDelta, { row: "once" }>,
  ): Promise<void> {
    await sql`
      INSERT INTO ${sql.raw(this.t("dedupe"))} (run_id, step_id, scope, value)
      VALUES (${runId}, ${delta.stepId}, ${delta.scope}, ${jsonb(delta.value)})
      ON CONFLICT (run_id, step_id, scope) DO NOTHING
    `.execute(trx);
  }

  private async upsertStepCompleted(
    trx: Kysely<DB>,
    runId: RunId,
    stepId: StepId,
    attempt: AttemptNumber,
    output: Json,
  ): Promise<void> {
    await sql`
      INSERT INTO ${sql.raw(this.t("step_run"))}
        (run_id, step_id, attempt, status, output, started_at, completed_at)
      VALUES
        (${runId}, ${stepId}, ${attempt}, 'completed', ${jsonb(output)}, now(), now())
      ON CONFLICT (run_id, step_id, attempt) DO UPDATE
        SET status = 'completed', output = EXCLUDED.output, completed_at = now()
    `.execute(trx);
  }

  private async upsertStepFailed(
    trx: Kysely<DB>,
    runId: RunId,
    stepId: StepId,
    attempt: AttemptNumber,
    error: SerializedError,
  ): Promise<void> {
    await sql`
      INSERT INTO ${sql.raw(this.t("step_run"))}
        (run_id, step_id, attempt, status, error, started_at, completed_at)
      VALUES
        (${runId}, ${stepId}, ${attempt}, 'failed', ${jsonb(error as unknown as Json)}, now(), now())
      ON CONFLICT (run_id, step_id, attempt) DO UPDATE
        SET status = 'failed', error = EXCLUDED.error, completed_at = now()
    `.execute(trx);
  }

  private async upsertStepCanceled(
    trx: Kysely<DB>,
    runId: RunId,
    stepId: StepId,
    attempt: AttemptNumber,
    error: SerializedError | undefined,
  ): Promise<void> {
    const errorJson =
      error === undefined ? null : jsonb(error as unknown as Json);
    await sql`
      INSERT INTO ${sql.raw(this.t("step_run"))}
        (run_id, step_id, attempt, status, error, started_at, completed_at)
      VALUES
        (${runId}, ${stepId}, ${attempt}, 'canceled', ${errorJson}, now(), now())
      ON CONFLICT (run_id, step_id, attempt) DO UPDATE
        SET status = 'canceled', error = EXCLUDED.error, completed_at = now()
    `.execute(trx);
  }

  private async deleteLease(
    trx: Kysely<DB>,
    runId: RunId,
    stepId: StepId,
  ): Promise<void> {
    await sql`
      DELETE FROM ${sql.raw(this.t("lease"))}
       WHERE run_id = ${runId} AND step_id = ${stepId}
    `.execute(trx);
  }

  private async deleteTimer(
    trx: Kysely<DB>,
    runId: RunId,
    stepId: StepId,
  ): Promise<void> {
    await sql`
      DELETE FROM ${sql.raw(this.t("timer"))}
       WHERE run_id = ${runId} AND step_id = ${stepId}
    `.execute(trx);
  }

  // The adapter's whole share of the release table: lease / timer rows. The
  // concurrency slot needs no write — the partial unique index keys on status,
  // which the materializer already set.
  private async applyFactEffects(
    trx: Kysely<DB>,
    runId: RunId,
    fact: Fact,
  ): Promise<void> {
    const effects = factEffects(fact);
    if (effects.tag !== "release-step") return;
    await this.deleteLease(trx, runId, effects.stepId);
    if (effects.timer) await this.deleteTimer(trx, runId, effects.stepId);
  }

  private async maybeNotify(runId: RunId): Promise<void> {
    if (!this.notifyChannel) return;
    await sql`SELECT pg_notify(${this.notifyChannel}, ${runId})`.execute(
      this.db,
    );
  }

  async upsertSnapshot(args: {
    readonly flowHash: string;
    readonly flowId: string;
    readonly dag: Json;
  }): Promise<void> {
    await sql`
      INSERT INTO ${sql.raw(this.t("flow_snapshot"))} (flow_hash, flow_id, dag)
      VALUES (${args.flowHash}, ${args.flowId}, ${jsonb(args.dag)})
      ON CONFLICT (flow_hash) DO NOTHING
    `.execute(this.db);
  }

  async getRef(flowId: string): Promise<string | null> {
    const r = await sql<{ flow_hash: string }>`
      SELECT flow_hash
        FROM ${sql.raw(this.t("flow_ref"))}
       WHERE flow_id = ${flowId}
    `.execute(this.db);
    return r.rows[0]?.flow_hash ?? null;
  }

  async setRef(flowId: string, flowHash: string): Promise<void> {
    await sql`
      INSERT INTO ${sql.raw(this.t("flow_ref"))} (flow_id, flow_hash, updated_at)
      VALUES (${flowId}, ${flowHash}, now())
      ON CONFLICT (flow_id) DO UPDATE
        SET flow_hash = EXCLUDED.flow_hash, updated_at = now()
    `.execute(this.db);
  }

  async loadSnapshot(
    flowHash: string,
  ): Promise<{ readonly flowId: string; readonly dag: Json } | null> {
    const r = await sql<{ flow_id: string; dag: Json }>`
      SELECT flow_id, dag
        FROM ${sql.raw(this.t("flow_snapshot"))}
       WHERE flow_hash = ${flowHash}
    `.execute(this.db);
    const row = r.rows[0];
    return row ? { flowId: row.flow_id, dag: row.dag } : null;
  }

  async appendGlobalFact(fact: GlobalFact): Promise<void> {
    await sql`
      INSERT INTO ${sql.raw(this.t("global_fact"))} (fact_id, kind, at, payload)
      VALUES (${uuidv7()}, ${fact.kind}, ${fact.at}, ${jsonb(serializeGlobalFactPayload(fact))})
    `.execute(this.db);
  }

  async queryRuns(opts: QueryRunsOpts): Promise<QueryRunsResult> {
    const where = opts.where ?? {};
    const flowId = where.flowId;
    const statuses = where.status ? Array.from(where.status) : undefined;
    // Typed null: an untyped `$n IS NULL` cannot be planned.
    const inputJson =
      where.input === undefined ? null : JSON.stringify(where.input);

    const isLatest = opts.latest === true;
    const limit = isLatest ? 1 : clampQueryLimit(opts.limit);
    const cursor =
      !isLatest && opts.cursor !== undefined
        ? decodeRunCursor(opts.cursor)
        : null;

    const fetchLimit = isLatest ? 1 : limit + 1;

    const rows = await sql<{
      run_id: string;
      flow_id: string;
      status: RunStatus;
      input: Json;
      started_at: Date;
      completed_at: Date | null;
    }>`
      SELECT run_id, flow_id, status, input, started_at, completed_at
        FROM ${sql.raw(this.t("workflow_run"))}
       WHERE (${flowId ?? null}::text IS NULL OR flow_id = ${flowId ?? null})
         AND (${statuses === undefined ? null : statuses}::text[] IS NULL
              OR status = ANY(${statuses === undefined ? null : statuses}::text[]))
         AND (${inputJson}::jsonb IS NULL OR input @> ${inputJson}::jsonb)
         AND (${cursor === null ? null : cursor.startedAt}::timestamptz IS NULL
              OR (started_at, run_id) <
                 (${cursor === null ? null : cursor.startedAt}::timestamptz,
                  ${cursor === null ? null : cursor.runId}::text))
       ORDER BY started_at DESC, run_id DESC
       LIMIT ${fetchLimit}
    `.execute(this.db);

    const summaries: RunSummary[] = rows.rows.map((r) => ({
      runId: r.run_id as RunId,
      flowId: r.flow_id,
      status: r.status,
      startedAt:
        r.started_at instanceof Date ? r.started_at : new Date(r.started_at),
      completedAt:
        r.completed_at === null
          ? null
          : r.completed_at instanceof Date
            ? r.completed_at
            : new Date(r.completed_at),
      input: r.input,
    }));

    if (isLatest) {
      return { runs: summaries.slice(0, 1), cursor: null };
    }

    const hasMore = summaries.length > limit;
    const page = hasMore ? summaries.slice(0, limit) : summaries;
    const last = page[page.length - 1];
    const nextCursor =
      hasMore && last !== undefined
        ? encodeRunCursor({ startedAt: last.startedAt, runId: last.runId })
        : null;
    return { runs: page, cursor: nextCursor };
  }

  async describe(runId: RunId): Promise<RunDescription> {
    // Single tx for read consistency across the three SELECTs.
    return this.db.transaction().execute(async (trx) => {
      const runRows = await sql<{
        run_id: string;
        flow_id: string;
        flow_hash: string | null;
        status: RunStatus;
        input: Json;
        output: Json | null;
        error: Json | null;
        started_at: Date;
        completed_at: Date | null;
        concurrency_key: string | null;
        canceled_by_run_id: string | null;
        parent_run_id: string | null;
        parent_step_id: string | null;
      }>`
        SELECT run_id, flow_id, flow_hash, status, input, output, error,
               started_at, completed_at, concurrency_key, canceled_by_run_id,
               parent_run_id, parent_step_id
          FROM ${sql.raw(this.t("workflow_run"))}
         WHERE run_id = ${runId}
         LIMIT 1
      `.execute(trx);

      const r = runRows.rows[0];
      if (r === undefined) return null;

      const stepRows = await sql<{
        step_id: string;
        attempt: number;
        status: StepRunStatus;
        output: Json | null;
        error: Json | null;
        started_at: Date | null;
        completed_at: Date | null;
        lease_expires_at: Date | null;
      }>`
        SELECT s.step_id, s.attempt, s.status, s.output, s.error,
               s.started_at, s.completed_at, l.expires_at AS lease_expires_at
          FROM ${sql.raw(this.t("step_run"))} s
          LEFT JOIN ${sql.raw(this.t("lease"))} l
            ON l.run_id = s.run_id
           AND l.step_id = s.step_id
           AND l.attempt = s.attempt
           AND l.expires_at > now()
         WHERE s.run_id = ${runId}
         ORDER BY s.started_at ASC NULLS LAST, s.step_id ASC
      `.execute(trx);

      const childRows = await sql<{ run_id: string }>`
        SELECT run_id
          FROM ${sql.raw(this.t("workflow_run"))}
         WHERE parent_run_id = ${runId}
         ORDER BY started_at ASC
      `.execute(trx);

      const startedAt =
        r.started_at instanceof Date ? r.started_at : new Date(r.started_at);
      const completedAt =
        r.completed_at === null
          ? undefined
          : r.completed_at instanceof Date
            ? r.completed_at
            : new Date(r.completed_at);
      const parent =
        r.parent_run_id !== null && r.parent_step_id !== null
          ? {
              runId: r.parent_run_id as RunId,
              stepId: r.parent_step_id,
            }
          : undefined;
      const run: RunView = {
        runId: r.run_id as RunId,
        flowId: r.flow_id,
        flowHash: r.flow_hash ?? "",
        status: r.status,
        startedAt,
        input: r.input,
        children: childRows.rows.map((c) => c.run_id as RunId),
        ...(completedAt !== undefined ? { completedAt } : {}),
        ...(r.output !== null ? { output: r.output } : {}),
        ...(r.error !== null ? { error: r.error } : {}),
        ...(r.canceled_by_run_id !== null
          ? { canceledByRunId: r.canceled_by_run_id as RunId }
          : {}),
        ...(r.concurrency_key !== null
          ? { concurrencyKey: r.concurrency_key }
          : {}),
        ...(parent !== undefined ? { parent } : {}),
      };

      const steps: StepView[] = stepRows.rows.map((sr) => {
        const sStartedAt =
          sr.started_at === null
            ? undefined
            : sr.started_at instanceof Date
              ? sr.started_at
              : new Date(sr.started_at);
        const sCompletedAt =
          sr.completed_at === null
            ? undefined
            : sr.completed_at instanceof Date
              ? sr.completed_at
              : new Date(sr.completed_at);
        const sLeaseAt =
          sr.lease_expires_at === null
            ? undefined
            : sr.lease_expires_at instanceof Date
              ? sr.lease_expires_at
              : new Date(sr.lease_expires_at);
        return {
          stepId: sr.step_id,
          attempt: sr.attempt as AttemptNumber,
          status: sr.status,
          ...(sStartedAt !== undefined ? { startedAt: sStartedAt } : {}),
          ...(sCompletedAt !== undefined ? { completedAt: sCompletedAt } : {}),
          ...(sr.output !== null ? { output: sr.output } : {}),
          ...(sr.error !== null ? { error: sr.error } : {}),
          ...(sLeaseAt !== undefined ? { lease: { expiresAt: sLeaseAt } } : {}),
        };
      });

      return { run, steps };
    });
  }

  async listChildren(parentRunId: RunId): Promise<ReadonlyArray<RunId>> {
    const rows = await sql<{ run_id: string }>`
      SELECT run_id
        FROM ${sql.raw(this.t("workflow_run"))}
       WHERE parent_run_id = ${parentRunId}
    `.execute(this.db);
    return rows.rows.map((r) => r.run_id as RunId);
  }

  async pruneFacts(opts: Required<PruneOpts>): Promise<PruneResult> {
    const statuses: PrunableStatus[] = Array.from(opts.statuses);
    let runsPruned = 0;
    let factsPruned = 0;

    for (;;) {
      const batch = await this.db.transaction().execute(async (trx) => {
        const victimRows = await sql<{ run_id: string }>`
          SELECT w.run_id
            FROM ${sql.raw(this.t("workflow_run"))} w
           WHERE w.status = ANY(${statuses}::text[])
             AND w.completed_at IS NOT NULL
             AND w.completed_at < ${opts.olderThan}
             AND EXISTS (
                   SELECT 1 FROM ${sql.raw(this.t("fact"))} f
                    WHERE f.run_id = w.run_id
                 )
           ORDER BY w.completed_at ASC, w.run_id ASC
           LIMIT ${opts.batchSize}
           FOR UPDATE SKIP LOCKED
        `.execute(trx);

        const victims = victimRows.rows.map((r) => r.run_id);
        if (victims.length === 0) {
          return { candidates: 0, runs: 0, facts: 0 };
        }

        const factDel = await sql<{ run_id: string }>`
          DELETE FROM ${sql.raw(this.t("fact"))}
           WHERE run_id = ANY(${victims}::text[])
           RETURNING run_id
        `.execute(trx);
        await sql`
          DELETE FROM ${sql.raw(this.t("step_run"))}
           WHERE run_id = ANY(${victims}::text[])
        `.execute(trx);
        await sql`
          DELETE FROM ${sql.raw(this.t("lease"))}
           WHERE run_id = ANY(${victims}::text[])
        `.execute(trx);
        await sql`
          DELETE FROM ${sql.raw(this.t("timer"))}
           WHERE run_id = ANY(${victims}::text[])
        `.execute(trx);
        await sql`
          DELETE FROM ${sql.raw(this.t("dedupe"))}
           WHERE run_id = ANY(${victims}::text[])
        `.execute(trx);

        if (!opts.keepSummary) {
          await sql`
            DELETE FROM ${sql.raw(this.t("workflow_run"))}
             WHERE run_id = ANY(${victims}::text[])
          `.execute(trx);
        }

        // Under READ COMMITTED a concurrent pruner can have emptied a victim's
        // facts after our snapshot (the workflow_run row is untouched when
        // keepSummary is on, so the EXISTS qual is never re-checked). Count only
        // runs whose facts this call actually removed.
        return {
          candidates: victims.length,
          runs: new Set(factDel.rows.map((r) => r.run_id)).size,
          facts: factDel.rows.length,
        };
      });

      if (batch.candidates === 0) break;
      runsPruned += batch.runs;
      factsPruned += batch.facts;
    }

    return { runsPruned, factsPruned };
  }
}

function serializeFactPayload(fact: Fact): Json {
  const { kind: _kind, at: _at, runId: _runId, ...rest } = fact;
  return rest as unknown as Json;
}

function serializeGlobalFactPayload(fact: GlobalFact): Json {
  const { kind: _kind, at: _at, ...rest } = fact;
  return rest as unknown as Json;
}

function reviveFact(kind: string, at: Date, payload: unknown): Fact {
  const body = (payload ?? {}) as Record<string, unknown>;
  return {
    kind: kind as Fact["kind"],
    at: at instanceof Date ? at : new Date(at as string),
    ...body,
  } as Fact;
}

function jsonb(value: Json) {
  return sql`${JSON.stringify(value)}::jsonb`;
}

// pg driver attaches the PostgreSQL SQLSTATE code on the error's `code` field.
// 23505 = unique_violation. Kysely/pg may wrap or re-throw, so we walk a couple
// of common shapes (direct .code, .cause.code) before giving up.
function isUniqueViolation(err: unknown): boolean {
  if (err === null || typeof err !== "object") return false;
  const code = (err as { code?: unknown }).code;
  if (typeof code === "string" && code === "23505") return true;
  const cause = (err as { cause?: unknown }).cause;
  if (cause !== null && typeof cause === "object") {
    const c = (cause as { code?: unknown }).code;
    if (typeof c === "string" && c === "23505") return true;
  }
  return false;
}

// Mirrors InMemoryStore.appendFact's hub wiring, so both adapters close streams
// on exactly the same facts. Returns null for facts that do not affect a stream.
function closeFrameOf(runId: RunId, fact: Fact): StreamFrame | null {
  switch (fact.kind) {
    case "step.completed":
      return { k: "ok", r: runId, s: fact.stepId };
    case "step.failed":
      // Only written on terminal failure (retries use step.retried).
      return { k: "err", r: runId, s: fact.stepId, e: fact.error };
    case "step.retried":
      // The retry event carries the NEXT attempt number.
      return { k: "retry", r: runId, s: fact.stepId, a: fact.attempt + 1 };
    case "flow.completed":
    case "flow.failed":
    case "flow.canceled":
      // Close every still-open channel so a subscriber to a skipped or
      // never-emitting step never hangs.
      return { k: "run", r: runId };
    default:
      return null;
  }
}

function byteLength(s: string): number {
  return new TextEncoder().encode(s).length;
}
