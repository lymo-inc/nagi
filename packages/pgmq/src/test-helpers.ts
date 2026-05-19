import {
  type CompiledQuery,
  type DatabaseConnection,
  type Dialect,
  type Driver,
  Kysely,
  PostgresAdapter,
  PostgresIntrospector,
  PostgresQueryCompiler,
  type QueryResult,
} from "kysely";

export interface CapturedQuery {
  readonly sql: string;
  readonly parameters: readonly unknown[];
}

export interface CapturingDb {
  readonly db: Kysely<unknown>;
  readonly queries: readonly CapturedQuery[];
  enqueueRows(rows: readonly unknown[]): void;
  reset(): void;
}

class CapturingConnection implements DatabaseConnection {
  constructor(
    private readonly captured: CapturedQuery[],
    private readonly responses: unknown[][],
  ) {}

  async executeQuery<R>(query: CompiledQuery): Promise<QueryResult<R>> {
    this.captured.push({ sql: query.sql, parameters: query.parameters });
    const rows = (this.responses.shift() ?? []) as R[];
    return { rows };
  }

  streamQuery<R>(): AsyncIterableIterator<QueryResult<R>> {
    throw new Error("streamQuery not supported in CapturingDriver");
  }
}

class CapturingDriver implements Driver {
  constructor(
    private readonly captured: CapturedQuery[],
    private readonly responses: unknown[][],
  ) {}

  async init(): Promise<void> {}
  async acquireConnection(): Promise<DatabaseConnection> {
    return new CapturingConnection(this.captured, this.responses);
  }
  async beginTransaction(): Promise<void> {}
  async commitTransaction(): Promise<void> {}
  async rollbackTransaction(): Promise<void> {}
  async releaseConnection(): Promise<void> {}
  async destroy(): Promise<void> {}
}

export function createCapturingDb(): CapturingDb {
  const captured: CapturedQuery[] = [];
  const responses: unknown[][] = [];

  const dialect: Dialect = {
    createAdapter: () => new PostgresAdapter(),
    createDriver: () => new CapturingDriver(captured, responses),
    createIntrospector: (db) => new PostgresIntrospector(db),
    createQueryCompiler: () => new PostgresQueryCompiler(),
  };

  return {
    db: new Kysely({ dialect }),
    get queries() {
      return captured;
    },
    enqueueRows(rows) {
      responses.push([...rows]);
    },
    reset() {
      captured.length = 0;
      responses.length = 0;
    },
  };
}
