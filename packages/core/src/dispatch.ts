import { NagiSignalTimeoutError, serializeError } from "./errors";
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
    readonly generation: number;
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
  // Fail any signal step whose timeout has elapsed, then advance the run so the
  // failure propagates to flow.failed. Returns the number of runs failed. The
  // store does the atomic fail+delete under its per-run lock; advance runs here,
  // outside that lock, exactly as wf.signal() advances after settleSignal.
  sweepTimers(now: Date): Promise<number>;
}

export function makeDispatcher(deps: DispatchDeps): Dispatcher {
  const hooks = makeHooks(deps);
  const progression = makeProgression(deps, hooks);
  const message = makeMessage(deps, hooks, progression);

  async function sweepTimers(now: Date): Promise<number> {
    const timedOut = await deps.store.sweepSignalTimeouts({ now });
    for (const { runId, stepId, attempt, fireAt } of timedOut) {
      // Fire onStepError before finalizing the flow, the same ordering every
      // other failure path uses (handleStepError, markStepSettled) — so a
      // consumer's per-step error handling sees signal timeouts too.
      if (deps.hooks?.onStepError) {
        const flow = await deps.flowFor(runId);
        await hooks.fireHook(
          deps.hooks.onStepError,
          {
            runId,
            flowId: flow.id,
            stepId,
            attempt,
            kind: "signal",
            error: serializeError(
              new NagiSignalTimeoutError({ runId, stepId, fireAt }),
            ),
            at: now,
          },
          "onStepError",
        );
      }
      await progression.advance(runId);
    }
    return timedOut.length;
  }

  return {
    dispatchMessage: message.dispatchMessage,
    advance: progression.advance,
    propagateToParent: progression.propagateToParent,
    sweepTimers,
  };
}
