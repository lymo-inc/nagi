import {
  type CanonicalDag,
  canonicalize,
  sha256Canonical,
} from "./canonicalize";
import { NagiFlowSnapshotGoneError, NagiRuntimeError } from "./errors";
import { Facts } from "./facts";
import {
  asStepMapWithDefs,
  attachDef,
  compact,
  getDef,
  type StepDef,
  setDef,
} from "./internal";
import { isTerminalRun } from "./state";
import type { Clock, Flow, Json, RunState, Store } from "./types";

// The one answer to "which Flow runs this run". A run pinned to a flowHash
// this process did not register is `gone`; the arm says what the holder may
// do about it. `live` is the flow registered under the same id on this code,
// when there is one — replay borrows its handlers to synthesize against the
// pinned snapshot; undefined ⇔ error.currentHash === null.
export type FlowResolution =
  | { readonly kind: "current"; readonly flow: Flow }
  // Run still pending/running: the worker's snapshot-gone policy decides
  // (retry for a frozen-version worker, or terminal fail).
  | {
      readonly kind: "gone-live";
      readonly error: NagiFlowSnapshotGoneError;
      readonly live: Flow | undefined;
    }
  // Run already completed/failed/canceled: nothing can advance it, so its
  // message is acked and dropped without consulting any policy.
  | {
      readonly kind: "gone-terminal";
      readonly error: NagiFlowSnapshotGoneError;
      readonly live: Flow | undefined;
    };

export type FlowGone = Exclude<FlowResolution, { kind: "current" }>;

export interface FlowRegistry {
  get(flowId: string): Flow | undefined;
  has(flowId: string): boolean;
  require(flowId: string): Flow;
  hashOf(flowId: string): string;
  isStreaming(stepId: string): boolean;
  resolve(state: RunState): FlowResolution;
  // Drift-allowed replay: the pinned snapshot's DAG with the live flow's
  // handlers attached. Throws NagiRuntimeError when there is no live flow to
  // borrow handlers from or the snapshot is no longer in the store.
  synthesize(gone: FlowGone): Promise<Flow>;
}

export function requireCurrent(resolution: FlowResolution): Flow {
  if (resolution.kind === "current") return resolution.flow;
  throw resolution.error;
}

export async function registerFlows(deps: {
  readonly flows: ReadonlyArray<Flow>;
  readonly store: Store;
  readonly clock: Clock;
}): Promise<FlowRegistry> {
  const { flows, store, clock } = deps;
  const byId = new Map<string, Flow>();
  const hashById = new Map<string, string>();
  const streamingStepIds = new Set<string>();

  for (const flow of flows) {
    if (byId.has(flow.id)) {
      throw new NagiRuntimeError(
        `Duplicate flow id "${flow.id}" passed to nagi()`,
      );
    }
    byId.set(flow.id, flow);
    for (const [stepId, step] of Object.entries(
      asStepMapWithDefs(flow.steps),
    )) {
      if (getDef(step).kind === "streaming") streamingStepIds.add(stepId);
    }
  }

  for (const flow of flows) {
    const dag = await canonicalize(flow);
    const flowHash = await sha256Canonical(dag);
    hashById.set(flow.id, flowHash);
    await store.upsertSnapshot({
      flowHash,
      flowId: flow.id,
      dag: dag as unknown as Json,
    });

    const previousHash = await store.getRef(flow.id);
    if (previousHash !== flowHash) {
      await store.setRef(flow.id, flowHash);
      await store.appendGlobalFact(
        Facts.flowRefUpdated({
          flowId: flow.id,
          from: previousHash,
          to: flowHash,
          at: clock.now(),
        }),
      );
    }
  }

  function require(flowId: string): Flow {
    const flow = byId.get(flowId);
    if (!flow) {
      throw new NagiRuntimeError(
        `Flow "${flowId}" not registered with nagi(). Pass it to flows[].`,
      );
    }
    return flow;
  }

  function hashOf(flowId: string): string {
    const hash = hashById.get(flowId);
    if (hash === undefined) {
      throw new NagiRuntimeError(
        `Flow "${flowId}" not registered with nagi(). Pass it to flows[].`,
      );
    }
    return hash;
  }

  // pinnedHash undefined ⇒ legacy run with no flow_hash recorded (pre-pinning);
  // resolve by flowId alone. Otherwise a mismatch (or missing flow) is `gone`
  // so a forward-incompatible deploy fails loud rather than silently running
  // the run against the new code.
  function resolve(state: RunState): FlowResolution {
    const { runId, flowId, flowHash: pinnedHash } = state;
    const live = byId.get(flowId);
    if (pinnedHash === undefined) {
      if (live === undefined) {
        throw new NagiRuntimeError(
          `Run ${runId} references flow "${flowId}" which is not registered with nagi().`,
        );
      }
      return { kind: "current", flow: live };
    }
    const currentHash = live === undefined ? null : hashOf(flowId);
    if (live !== undefined && currentHash === pinnedHash) {
      return { kind: "current", flow: live };
    }
    const error = new NagiFlowSnapshotGoneError({
      runId,
      flowId,
      pinnedHash,
      currentHash,
    });
    return isTerminalRun(state)
      ? { kind: "gone-terminal", error, live }
      : { kind: "gone-live", error, live };
  }

  async function synthesize(gone: FlowGone): Promise<Flow> {
    const { runId, flowId, pinnedHash } = gone.error;
    if (gone.live === undefined) {
      throw new NagiRuntimeError(
        `Run ${runId} references flow "${flowId}" which is not registered with nagi().`,
      );
    }
    const snapshot = await store.loadSnapshot(pinnedHash);
    if (snapshot === null) {
      throw new NagiRuntimeError(
        `Run ${runId} pinned to flow hash ${pinnedHash.slice(0, 12)}… but ` +
          `no snapshot with that hash was found. Cannot replay with allowDrift.`,
      );
    }
    return synthesizeReplayFlow(
      snapshot.dag as unknown as CanonicalDag,
      gone.live,
    );
  }

  return {
    get: (flowId) => byId.get(flowId),
    has: (flowId) => byId.has(flowId),
    require,
    hashOf,
    isStreaming: (stepId) => streamingStepIds.has(stepId),
    resolve,
    synthesize,
  };
}

function synthesizeReplayFlow(dag: CanonicalDag, liveFlow: Flow): Flow {
  const liveSteps = asStepMapWithDefs(liveFlow.steps);
  const synthesized: Record<string, ReturnType<typeof attachDef>> = {};

  for (const canonStep of dag.steps) {
    const liveStep = liveSteps[canonStep.id];
    if (liveStep === undefined) {
      throw new NagiRuntimeError(
        `Drift-allowed replay: step "${canonStep.id}" exists in snapshot but ` +
          `is missing from the live flow "${liveFlow.id}". Cannot synthesize a handler.`,
      );
    }
    synthesized[canonStep.id] = attachDef(
      { kind: canonStep.kind, id: canonStep.id },
      getDef(liveStep),
    );
  }

  for (const canonStep of dag.steps) {
    const shell = synthesized[canonStep.id];
    if (shell === undefined) continue;

    const synthesizedNeeds: Record<string, unknown> = {};
    for (const upstreamId of canonStep.needs) {
      const upstreamShell = synthesized[upstreamId];
      if (upstreamShell === undefined) {
        throw new NagiRuntimeError(
          `Drift-allowed replay: step "${canonStep.id}" needs upstream ` +
            `"${upstreamId}" which the snapshot does not declare.`,
        );
      }
      synthesizedNeeds[upstreamId] = upstreamShell;
    }

    setDef(shell, { ...getDef(shell), needs: synthesizedNeeds } as StepDef);
  }

  return {
    id: liveFlow.id,
    input: liveFlow.input,
    steps: synthesized,
    ...compact({ output: liveFlow.output }),
  };
}
