import { type DispatchDeps, type Dispatcher, makeDispatcher } from "./dispatch";
import {
  NagiRuntimeError,
  NagiSnapshotDriftError,
  validationError,
} from "./errors";
import { Facts } from "./facts";
import type { FlowRegistry } from "./flows";
import { descendantsOf } from "./scheduler";
import type { Flow, Queue, ReplayOpts, RunId } from "./types";

export interface ReplayDeps extends DispatchDeps {
  readonly registry: FlowRegistry;
}

export function makeReplay(deps: ReplayDeps): {
  replay(runId: RunId, opts?: ReplayOpts): Promise<void>;
} {
  const { registry, store, queue, clock } = deps;

  async function replay(
    runId: RunId,
    opts: ReplayOpts = { mode: "continue" },
  ): Promise<void> {
    const runState = await store.loadRunState(runId);
    const resolution = registry.resolve(runState);
    const liveFlow =
      resolution.kind === "current" ? resolution.flow : resolution.live;
    if (liveFlow === undefined) {
      throw new NagiRuntimeError(
        `Run ${runId} references flow "${runState.flowId}" which is not registered with nagi().`,
      );
    }
    if (runState.phase.tag === "canceled") {
      throw new NagiRuntimeError(
        `Run ${runId} was canceled (superseded by a newer run with the same concurrency key). ` +
          `Replay is not supported for canceled runs — start a new run instead.`,
      );
    }
    if (opts.mode === "inspect") return;

    const fireHooks = opts.fireHooks !== false;
    const baseDeps: DispatchDeps = fireHooks
      ? deps
      : { ...deps, fireHooks: false };

    let replayDeps = baseDeps;
    let effectiveFlow: Flow = liveFlow;
    if (resolution.kind !== "current") {
      if (!opts.allowDrift) {
        throw new NagiSnapshotDriftError({
          runId,
          expected: resolution.error.pinnedHash,
          actual: registry.hashOf(liveFlow.id),
        });
      }
      const synthesized = await registry.synthesize(resolution);
      replayDeps = {
        ...baseDeps,
        resolveFlow: async () => ({ kind: "current", flow: synthesized }),
      };
      effectiveFlow = synthesized;
    }

    if (opts.from !== undefined) {
      if (runState.phase.tag === "running") {
        throw new NagiRuntimeError(
          `Run ${runId} is still running — replay({ from }) would race in-flight workers. ` +
            `Wait for the run to settle (completed / failed) before resetting from a step.`,
        );
      }
      if (!(opts.from in effectiveFlow.steps)) {
        throw validationError(
          `replay({ from }): step "${opts.from}" is not a step in flow "${effectiveFlow.id}".`,
          ["from"],
        );
      }
      const cascade = descendantsOf(effectiveFlow, opts.from);
      const at = clock.now();
      for (const stepId of cascade) {
        const fact =
          stepId === opts.from
            ? Facts.stepReset({ runId, stepId, at })
            : Facts.stepReset({ runId, stepId, at, cascadedFrom: opts.from });
        await store.appendFact(runId, fact);
      }
    }

    const replayDispatcher = makeDispatcher(replayDeps);
    await replayDispatcher.advance(runId);
    if (!fireHooks) await drainInline(replayDispatcher, queue);
  }

  return { replay };
}

const MAX_REPLAY_DISPATCHES = 4096;
async function drainInline(
  dispatcher: Dispatcher,
  queue: Queue,
): Promise<void> {
  for (let i = 0; i < MAX_REPLAY_DISPATCHES; i++) {
    const messages = await queue.dequeue({ count: 1 });
    if (messages.length === 0) return;
    for (const msg of messages) {
      const result = await dispatcher.dispatchMessage(msg);
      if (result.kind === "snapshot-gone") throw result.error;
    }
  }
}
