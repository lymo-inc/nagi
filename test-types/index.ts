// Resolution smoke test (replaces attw): tsc fails here if a package's
// exports -> types entry can't be resolved by a consumer under NodeNext.
import type * as core from "@nagi-js/core";
import type * as coreTesting from "@nagi-js/core/testing";
import type * as pgmq from "@nagi-js/pgmq";
import type * as postgres from "@nagi-js/postgres";

export type ResolvedExports = {
  core: typeof core;
  coreTesting: typeof coreTesting;
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
  Removed<postgres.PostgresStoreOpts, "notifyChannel">,
  Removed<core.WorkerRunUntilEmptyOpts, "deadline">,
  Removed<typeof core, "isStreamOver">,
];
export const removedSurfaceOk: RemovedSurface extends true[] ? true : never =
  true;

// Reference-only run events (issue: >8KB output/error aborted a NOTIFY-backed
// fact write): tsc fails here if `output`/`error` come back on a RunEvent or
// StreamEvent member.
type NoPayload<T> = T extends { output: unknown } | { error: unknown }
  ? ["carries payload", T]
  : true;
export type RunEventsAreReferences =
  | NoPayload<core.RunEvent>
  | NoPayload<core.StreamEvent>;
export const runEventsAreReferencesOk: RunEventsAreReferences extends true
  ? true
  : never = true;
