import type { Queue, Tx } from "@nagi-js/core";
import type { Kysely } from "kysely";
import { describe, expectTypeOf, it } from "vitest";
import { type PgmqQueue, type PgmqQueueOpts, pgmqQueue } from "./pgmq-queue";

declare const db: Kysely<unknown>;

interface SampleDb {
  readonly users: { readonly id: string; readonly email: string };
}
declare const typedDb: Kysely<SampleDb>;

describe("pgmqQueue type surface", () => {
  it("returns a PgmqQueue, which is structurally assignable to Queue", () => {
    expectTypeOf(pgmqQueue({ db })).toMatchTypeOf<Queue>();
    expectTypeOf<PgmqQueue>().toMatchTypeOf<Queue>();
  });

  it("PgmqQueue exposes ensureSchema in addition to the Queue methods", () => {
    expectTypeOf<PgmqQueue["ensureSchema"]>().toEqualTypeOf<
      () => Promise<void>
    >();
  });

  it("PgmqQueue.withTx returns a Queue bound to a tx", () => {
    expectTypeOf<PgmqQueue["withTx"]>().parameters.toEqualTypeOf<[Tx]>();
    expectTypeOf<ReturnType<PgmqQueue["withTx"]>>().toMatchTypeOf<Queue>();
  });

  it("PgmqQueueOpts.db is required; the rest are optional", () => {
    expectTypeOf<PgmqQueueOpts["db"]>().toEqualTypeOf<Kysely<unknown>>();
    expectTypeOf<PgmqQueueOpts["queueName"]>().toEqualTypeOf<
      string | undefined
    >();
    expectTypeOf<PgmqQueueOpts["visibilityTimeoutMs"]>().toEqualTypeOf<
      number | undefined
    >();
    expectTypeOf<PgmqQueueOpts["partitioned"]>().toEqualTypeOf<
      boolean | undefined
    >();
    expectTypeOf<PgmqQueueOpts["archiveOnAck"]>().toEqualTypeOf<
      boolean | undefined
    >();
  });

  it("pgmqQueue<DB> accepts a typed Kysely<DB> with no cast and infers DB", () => {
    expectTypeOf(pgmqQueue({ db: typedDb })).toEqualTypeOf<PgmqQueue>();
    expectTypeOf(pgmqQueue({ db: typedDb })).toMatchTypeOf<Queue>();
    expectTypeOf<PgmqQueueOpts<SampleDb>["db"]>().toEqualTypeOf<
      Kysely<SampleDb>
    >();
    expectTypeOf<PgmqQueueOpts["db"]>().toEqualTypeOf<Kysely<unknown>>();
  });
});
