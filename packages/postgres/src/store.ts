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
  RunId,
  RunState,
  RunStatus,
  RunSummary,
  SerializedError,
  StepCanceledFact,
  StepCompletedFact,
  StepFailedFact,
  StepId,
  Store,
  Tx,
} from "@nagi-js/core";
import { projectRunState } from "@nagi-js/core";
import type { Kysely } from "kysely";
import { sql } from "kysely";
import { uuidv7 } from "./uuidv7";

const DEFAULT_LEASE_MS: Millis = 60_000;
const SCHEMA_RE = /^[A-Za-z_][A-Za-z0-9_]*$/;

export interface PostgresStoreOpts<DB = unknown> {
  readonly db: Kysely<DB>;
  readonly schema?: string;
  readonly leaseMs?: Millis;
  readonly notifyChannel?: string;
}

export function postgresStore<DB = unknown>(
  opts: PostgresStoreOpts<DB>,
): Store {
  return new PostgresStore(opts);
}

class PostgresStore<DB = unknown> implements Store {
  private readonly db: Kysely<DB>;
  private readonly schema: string;
  private readonly leaseMs: Millis;
  private readonly notifyChannel: string | undefined;

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
  }

  private t(table: string): string {
    return `${this.schema}.${table}`;
  }

  async appendFact(runId: RunId, fact: Fact): Promise<void> {
    await this.db.transaction().execute(async (trx) => {
      await this.insertFact(trx, runId, fact);
      await this.applyFactToMaterialized(trx, runId, fact);
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
        await sql`
          UPDATE ${sql.raw(this.t("workflow_run"))}
             SET status = 'canceled',
                 canceled_by_run_id = ${runId},
                 completed_at = ${fact.at}
           WHERE run_id = ${priorRunId}
        `.execute(trx);
        await this.insertFact(trx, priorRunId, cancelFact);
        canceled.push({ runId: priorRunId, fact: cancelFact });
      }

      await sql`
        INSERT INTO ${sql.raw(this.t("workflow_run"))}
          (run_id, flow_id, status, input, started_at, flow_hash, code_version, concurrency_key, parent_run_id, parent_step_id)
        VALUES
          (${runId}, ${fact.flowId}, 'running', ${jsonb(fact.input)}, ${fact.at}, ${fact.flowHash ?? null}, ${fact.codeVersion ?? null}, ${concurrency.key}, ${fact.parent?.runId ?? null}, ${fact.parent?.stepId ?? null})
      `.execute(trx);
      await this.insertFact(trx, runId, fact);

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

  async loadRunState(runId: RunId): Promise<RunState> {
    const rows = await sql<{
      kind: string;
      at: Date;
      payload: unknown;
    }>`
      SELECT kind, at, payload
        FROM ${sql.raw(this.t("fact"))}
       WHERE run_id = ${runId}
       ORDER BY fact_id ASC
    `.execute(this.db);

    const facts: Fact[] = rows.rows.map((r) =>
      reviveFact(r.kind, r.at, r.payload),
    );
    return projectRunState(runId, facts);
  }

  async claimStep(
    runId: RunId,
    stepId: StepId,
    attempt: AttemptNumber,
  ): Promise<ClaimToken | null> {
    const token = `lease-${crypto.randomUUID()}`;
    const expiresAt = new Date(Date.now() + this.leaseMs);

    const result = await sql<{ token: string }>`
      INSERT INTO ${sql.raw(this.t("lease"))} (run_id, step_id, attempt, token, expires_at)
      VALUES (${runId}, ${stepId}, ${attempt}, ${token}, ${expiresAt})
      ON CONFLICT (run_id, step_id, attempt) DO UPDATE
        SET token = EXCLUDED.token, expires_at = EXCLUDED.expires_at
        WHERE ${sql.raw(this.t("lease"))}.expires_at < now()
      RETURNING token
    `.execute(this.db);

    const row = result.rows[0];
    return row ? (row.token as ClaimToken) : null;
  }

  async completeStep(
    runId: RunId,
    stepId: StepId,
    output: Json,
    fact: Fact,
  ): Promise<void> {
    await this.db.transaction().execute(async (trx) => {
      await this.upsertStepCompleted(
        trx,
        runId,
        stepId,
        fact.kind === "step.completed" ? fact.attempt : 1,
        output,
      );
      await this.insertFact(trx, runId, fact);
      await this.applyFactToMaterialized(trx, runId, fact);
      await this.deleteLease(trx, runId, stepId);
    });
    await this.maybeNotify(runId);
  }

  async failStep(
    runId: RunId,
    stepId: StepId,
    error: SerializedError,
    fact: Fact,
  ): Promise<void> {
    await this.db.transaction().execute(async (trx) => {
      await this.upsertStepFailed(
        trx,
        runId,
        stepId,
        fact.kind === "step.failed" ? fact.attempt : 1,
        error,
      );
      await this.insertFact(trx, runId, fact);
      await this.applyFactToMaterialized(trx, runId, fact);
      await this.deleteLease(trx, runId, stepId);
    });
    await this.maybeNotify(runId);
  }

  async getStepOutput(runId: RunId, stepId: StepId): Promise<Json | null> {
    const r = await sql<{ output: Json | null }>`
      SELECT output
        FROM ${sql.raw(this.t("step_run"))}
       WHERE run_id = ${runId} AND step_id = ${stepId} AND status = 'completed'
       ORDER BY attempt DESC
       LIMIT 1
    `.execute(this.db);
    return r.rows[0]?.output ?? null;
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
    stepId: StepId,
    attempt: AttemptNumber,
    body: (tx: Tx) => Promise<{
      readonly output: T;
      readonly fact: StepCompletedFact | StepFailedFact | StepCanceledFact;
    }>,
  ): Promise<T> {
    const output = await this.db.transaction().execute(async (trx) => {
      const result = await body(trx as unknown as Tx);
      if (result.fact.kind === "step.completed") {
        await this.upsertStepCompleted(
          trx,
          runId,
          stepId,
          attempt,
          result.fact.output,
        );
      } else if (result.fact.kind === "step.failed") {
        await this.upsertStepFailed(
          trx,
          runId,
          stepId,
          attempt,
          result.fact.error,
        );
      } else {
        await this.upsertStepCanceled(
          trx,
          runId,
          stepId,
          attempt,
          result.fact.error,
        );
      }
      await this.insertFact(trx, runId, result.fact);
      await this.deleteLease(trx, runId, stepId);
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

  private async applyFactToMaterialized(
    trx: Kysely<DB>,
    runId: RunId,
    fact: Fact,
  ): Promise<void> {
    switch (fact.kind) {
      case "flow.started":
        await sql`
          INSERT INTO ${sql.raw(this.t("workflow_run"))}
            (run_id, flow_id, status, input, started_at, flow_hash, code_version)
          VALUES
            (${runId}, ${fact.flowId}, 'running', ${jsonb(fact.input)}, ${fact.at}, ${fact.flowHash ?? null}, ${fact.codeVersion ?? null})
          ON CONFLICT (run_id) DO NOTHING
        `.execute(trx);
        return;
      case "flow.completed":
        await sql`
          UPDATE ${sql.raw(this.t("workflow_run"))}
             SET status = 'completed', output = ${jsonb(fact.output)}, completed_at = ${fact.at}
           WHERE run_id = ${runId}
        `.execute(trx);
        return;
      case "flow.failed":
        await sql`
          UPDATE ${sql.raw(this.t("workflow_run"))}
             SET status = 'failed', error = ${jsonb(fact.error as unknown as Json)}, completed_at = ${fact.at}
           WHERE run_id = ${runId}
        `.execute(trx);
        return;
      case "flow.canceled": {
        const canceledByRunId =
          fact.cause === "concurrency" ? fact.canceledByRunId : null;
        await sql`
          UPDATE ${sql.raw(this.t("workflow_run"))}
             SET status = 'canceled',
                 canceled_by_run_id = ${canceledByRunId},
                 completed_at = ${fact.at}
           WHERE run_id = ${runId}
        `.execute(trx);
        return;
      }
      case "step.started":
        await sql`
          INSERT INTO ${sql.raw(this.t("step_run"))} (run_id, step_id, attempt, status, started_at)
          VALUES (${runId}, ${fact.stepId}, ${fact.attempt}, 'running', ${fact.at})
          ON CONFLICT (run_id, step_id, attempt) DO UPDATE
            SET status = 'running', started_at = EXCLUDED.started_at
        `.execute(trx);
        return;
      case "step.skipped":
        await sql`
          INSERT INTO ${sql.raw(this.t("step_run"))} (run_id, step_id, attempt, status)
          VALUES (${runId}, ${fact.stepId}, 0, 'skipped')
          ON CONFLICT (run_id, step_id, attempt) DO UPDATE SET status = 'skipped'
        `.execute(trx);
        return;
      case "step.reset":
        await sql`
          DELETE FROM ${sql.raw(this.t("step_run"))}
           WHERE run_id = ${runId} AND step_id = ${fact.stepId}
        `.execute(trx);
        await this.deleteLease(trx, runId, fact.stepId);
        return;
      case "once.recorded":
        await sql`
          INSERT INTO ${sql.raw(this.t("dedupe"))} (run_id, step_id, scope, value)
          VALUES (${runId}, ${fact.stepId}, ${fact.scope}, ${jsonb(fact.value)})
          ON CONFLICT (run_id, step_id, scope) DO NOTHING
        `.execute(trx);
        return;
      case "step.canceled":
        await this.upsertStepCanceled(
          trx,
          runId,
          fact.stepId,
          fact.attempt,
          fact.error,
        );
        await this.deleteLease(trx, runId, fact.stepId);
        return;
      case "step.completed":
      case "step.failed":
      case "step.retried":
      case "step.abort-requested":
      case "signal.sent":
      case "signal.received":
      case "match.arm-selected":
        return;
    }
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
    const statuses =
      where.status === undefined
        ? undefined
        : Array.isArray(where.status)
          ? Array.from(where.status)
          : [where.status as RunStatus];
    const inputFilter = where.input;

    const isLatest = opts.latest === true;
    const limit = isLatest ? 1 : clampLimit(opts.limit);
    const cursor =
      !isLatest && opts.cursor !== undefined ? decodeCursor(opts.cursor) : null;

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
         AND (${inputFilter === undefined ? null : jsonb(inputFilter as unknown as Json)} IS NULL
              OR input @> ${inputFilter === undefined ? null : jsonb(inputFilter as unknown as Json)})
         AND (${cursor === null ? null : new Date(cursor.t)}::timestamptz IS NULL
              OR (started_at, run_id) <
                 (${cursor === null ? null : new Date(cursor.t)}::timestamptz,
                  ${cursor === null ? null : cursor.r}::text))
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
        ? encodeCursor({ t: last.startedAt.getTime(), r: last.runId })
        : null;
    return { runs: page, cursor: nextCursor };
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
          return { runs: 0, facts: 0 };
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

        return { runs: victims.length, facts: factDel.rows.length };
      });

      if (batch.runs === 0) break;
      runsPruned += batch.runs;
      factsPruned += batch.facts;
    }

    return { runsPruned, factsPruned };
  }
}

const DEFAULT_LIMIT = 50;
const MAX_LIMIT = 500;

function clampLimit(limit: number | undefined): number {
  if (limit === undefined) return DEFAULT_LIMIT;
  if (!Number.isInteger(limit) || limit < 1) return DEFAULT_LIMIT;
  return Math.min(limit, MAX_LIMIT);
}

interface DecodedCursor {
  readonly t: number;
  readonly r: string;
}

function encodeCursor(c: DecodedCursor): string {
  const bytes = new TextEncoder().encode(JSON.stringify(c));
  let binary = "";
  for (let i = 0; i < bytes.length; i++) {
    binary += String.fromCharCode(bytes[i] as number);
  }
  return btoa(binary)
    .replace(/\+/g, "-")
    .replace(/\//g, "_")
    .replace(/=+$/, "");
}

function decodeCursor(s: string): DecodedCursor {
  try {
    const b64 = s.replace(/-/g, "+").replace(/_/g, "/");
    const binary = atob(b64);
    const bytes = new Uint8Array(binary.length);
    for (let i = 0; i < binary.length; i++) bytes[i] = binary.charCodeAt(i);
    const parsed: unknown = JSON.parse(new TextDecoder().decode(bytes));
    if (
      typeof parsed === "object" &&
      parsed !== null &&
      typeof (parsed as DecodedCursor).t === "number" &&
      typeof (parsed as DecodedCursor).r === "string"
    ) {
      return parsed as DecodedCursor;
    }
    throw new Error("malformed cursor body");
  } catch (err) {
    throw new Error(
      `queryRuns: invalid cursor — ${err instanceof Error ? err.message : String(err)}`,
    );
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
