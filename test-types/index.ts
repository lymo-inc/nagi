// Resolution smoke test (replaces attw): tsc fails here if a package's
// exports -> types entry can't be resolved by a consumer under NodeNext.
import type * as core from "@nagi-js/core";
import type * as otel from "@nagi-js/otel";
import type * as pgmq from "@nagi-js/pgmq";
import type * as postgres from "@nagi-js/postgres";

export type ResolvedExports = {
  core: typeof core;
  otel: typeof otel;
  pgmq: typeof pgmq;
  postgres: typeof postgres;
};
