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

// Deliberate public removals (#42): tsc fails here if one comes back.
// Type-only removals (Trigger, PostgresTriggerOpts, ListenClient,
// NotificationMessage) cannot be asserted absent; see the changeset.
type Removed<T, K extends string> = K extends keyof T
  ? ["still exported", K]
  : true;
export type RemovedSurface = [
  Removed<typeof core, "InMemoryTrigger">,
  Removed<core.Clock, "schedule">,
  Removed<core.InMemoryClock, "dispose">,
  Removed<core.NagiConfig, "trigger">,
  Removed<core.NagiConfig, "streamTransport">,
  Removed<typeof postgres, "postgresTrigger">,
];
export const removedSurfaceOk: RemovedSurface extends true[] ? true : never =
  true;
