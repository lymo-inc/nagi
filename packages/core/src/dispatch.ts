import { makeHooks } from "./exec/hooks";
import { makeMessage } from "./exec/message";
import { makeProgression } from "./exec/progression";
import type { EmitLog } from "./internal";
import type {
  Clock,
  Flow,
  FlowHooks,
  Json,
  Millis,
  ParentRef,
  Queue,
  QueueMessage,
  RetryPolicy,
  RunId,
  SerializedError,
  Store,
  StreamTransport,
} from "./types";

export interface HeartbeatConfig {
  readonly intervalMs: Millis;
  readonly leaseMs: Millis;
}

export interface DispatchDeps {
  readonly flowFor: (runId: RunId) => Promise<Flow>;
  readonly lookupFlow: (flowId: string) => Flow | undefined;
  readonly startChildRun: (args: {
    readonly child: Flow;
    readonly childInput: unknown;
    readonly parent: ParentRef;
  }) => Promise<RunId>;
  readonly store: Store;
  readonly streamTransport?: StreamTransport;
  readonly queue: Queue;
  readonly clock: Clock;
  readonly hooks?: FlowHooks;
  readonly emitLog: EmitLog;
  readonly defaultRetry?: RetryPolicy;
  readonly fireHooks?: boolean;
  readonly cancelPollIntervalMs?: Millis;
  readonly heartbeat: HeartbeatConfig;
}

export type SubflowChildOutcome =
  | { readonly kind: "completed"; readonly output: Json }
  | { readonly kind: "failed"; readonly error: SerializedError }
  | { readonly kind: "canceled"; readonly error: SerializedError };

export interface Dispatcher {
  dispatchMessage(message: QueueMessage): Promise<void>;
  advance(runId: RunId): Promise<void>;
  propagateToParent(
    childRunId: RunId,
    outcome: SubflowChildOutcome,
  ): Promise<void>;
}

export function makeDispatcher(deps: DispatchDeps): Dispatcher {
  const hooks = makeHooks(deps);
  const progression = makeProgression(deps, hooks);
  const message = makeMessage(deps, hooks, progression);
  return {
    dispatchMessage: message.dispatchMessage,
    advance: progression.advance,
    propagateToParent: progression.propagateToParent,
  };
}
