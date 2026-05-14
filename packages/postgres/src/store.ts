import type {
  AttemptNumber,
  ClaimToken,
  Fact,
  FlowStartedFact,
  GlobalFact,
  Json,
  Millis,
  RunId,
  RunState,
  SerializedError,
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
  /** User-supplied Kysely instance. Driver (`pg`, `postgres.js`, Neon) is the user's choice. */
  readonly db: Kysely<DB>;
  /** Schema name; default `nagi`. Must match `/^[A-Za-z_][A-Za-z0-9_]*$/`. */
  readonly schema?: string;
  /** Lease duration in milliseconds; default 60_000. */
  readonly leaseMs?: Millis;
  /**
   * If provided, the store emits `pg_notify(<channel>, runId)` after writing
   * each fact. Pair with `postgresTrigger({ listen, channel })` to wake
   * schedulers without polling.
   */
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

  /** Schema-qualified table name for raw SQL interpolation. */
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
  ): Promise<{ readonly started: boolean }> {
    // Race-safe atomic insert. We lean on the `workflow_run.run_id` PRIMARY
    // KEY: `ON CONFLICT DO NOTHING` plus `RETURNING run_id` lets us detect in
    // one round-trip whether THIS call is the writer that created the run.
    // The fact insert and notify happen only on the winning branch — losers
    // get a clean idempotent no-op with no `flow.started` double-emit.
    const started = await this.db.transaction().execute(async (trx) => {
      const insert = await sql<{ run_id: string }>`
        INSERT INTO ${sql.raw(this.t("workflow_run"))}
          (run_id, flow_id, status, input, started_at, flow_hash, code_version)
        VALUES
          (${runId}, ${fact.flowId}, 'running', ${jsonb(fact.input)}, ${fact.at}, ${fact.flowHash ?? null}, ${fact.codeVersion ?? null})
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
    return { started };
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
      readonly fact: StepCompletedFact | StepFailedFact;
    }>,
  ): Promise<T> {
    // The handler runs inside this Kysely tx; its ctx.tx points to the same
    // tx, so user-written domain rows commit atomically with the fact write
    // and step_run upsert below. If body() throws, the tx is rolled back —
    // dispatcher's outer handleStepError will record `step.failed` separately.
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
      } else {
        await this.upsertStepFailed(
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
        // `tryStartRun` is the canonical entry point for `flow.started`. This
        // branch only fires if a caller threads the fact through `appendFact`
        // directly (e.g. tests). Use DO NOTHING to preserve idempotent
        // semantics — never clobber an existing run row.
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
      case "once.recorded":
        await sql`
          INSERT INTO ${sql.raw(this.t("dedupe"))} (run_id, step_id, scope, value)
          VALUES (${runId}, ${fact.stepId}, ${fact.scope}, ${jsonb(fact.value)})
          ON CONFLICT (run_id, step_id, scope) DO NOTHING
        `.execute(trx);
        return;
      // step.completed / step.failed are materialized by runStep / completeStep / failStep
      // before insertFact, so applyFactToMaterialized is a no-op for them.
      case "step.completed":
      case "step.failed":
      case "step.retried":
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
    // pg_notify takes (channel text, payload text). Bind both as parameters.
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
}

/**
 * Build the JSON payload column for a fact. Stores the full discriminated
 * union body (minus `kind`/`at` which are already separate columns).
 */
function serializeFactPayload(fact: Fact): Json {
  // Strip the redundantly-stored top-level fields. `runId` is the row key.
  const { kind: _kind, at: _at, runId: _runId, ...rest } = fact;
  return rest as unknown as Json;
}

function serializeGlobalFactPayload(fact: GlobalFact): Json {
  const { kind: _kind, at: _at, ...rest } = fact;
  return rest as unknown as Json;
}

/**
 * Re-hydrate a Fact from a fact-table row. The DB carries `kind` + `at` in
 * their own columns, and the rest of the discriminated-union body in `payload`.
 */
function reviveFact(kind: string, at: Date, payload: unknown): Fact {
  const body = (payload ?? {}) as Record<string, unknown>;
  return {
    kind: kind as Fact["kind"],
    at: at instanceof Date ? at : new Date(at as string),
    ...body,
  } as Fact;
}

/**
 * Bind a `Json` value as a jsonb-typed parameter. `pg` will send strings as
 * TEXT and objects as JSON, but jsonb columns require an explicit cast for
 * the parameter to be typed correctly across all primitive cases. The
 * returned Kysely `sql` fragment expands to `$N::jsonb` when interpolated.
 */
function jsonb(value: Json) {
  return sql`${JSON.stringify(value)}::jsonb`;
}
