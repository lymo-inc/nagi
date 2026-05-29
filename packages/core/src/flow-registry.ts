import { NagiFlowSnapshotGoneError, NagiRuntimeError } from "./errors";
import { asStepMapWithDefs, getDef } from "./internal";
import type { Flow, RunId } from "./types";

export interface FlowRegistry {
  readonly all: ReadonlyArray<Flow>;
  get(flowId: string): Flow | undefined;
  has(flowId: string): boolean;
  require(flowId: string): Flow;
  // pinnedHash undefined ⇒ legacy run with no flow_hash recorded (pre-pinning);
  // skip the hash check and resolve by flowId alone. pinnedHash present ⇒
  // dispatch-time guard: a mismatch (or missing flow) throws
  // NagiFlowSnapshotGoneError so a forward-incompatible deploy fails loud
  // rather than silently running the run against the new code.
  requireForRun(
    flowId: string,
    runId: RunId,
    pinnedHash?: string,
    currentHashOf?: (flowId: string) => string | undefined,
  ): Flow;
  isStreaming(stepId: string): boolean;
}

export function makeFlowRegistry(flows: ReadonlyArray<Flow>): FlowRegistry {
  const byId = new Map<string, Flow>();
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

  return {
    all: flows,
    get: (flowId) => byId.get(flowId),
    has: (flowId) => byId.has(flowId),
    require(flowId) {
      const flow = byId.get(flowId);
      if (!flow) {
        throw new NagiRuntimeError(
          `Flow "${flowId}" not registered with nagi(). Pass it to flows[].`,
        );
      }
      return flow;
    },
    requireForRun(flowId, runId, pinnedHash, currentHashOf) {
      const flow = byId.get(flowId);
      if (pinnedHash !== undefined) {
        const currentHash =
          flow === undefined ? null : (currentHashOf?.(flowId) ?? null);
        if (flow === undefined || currentHash !== pinnedHash) {
          throw new NagiFlowSnapshotGoneError({
            runId,
            flowId,
            pinnedHash,
            currentHash,
          });
        }
        return flow;
      }
      if (!flow) {
        throw new NagiRuntimeError(
          `Run ${runId} references flow "${flowId}" which is not registered with nagi().`,
        );
      }
      return flow;
    },
    isStreaming: (stepId) => streamingStepIds.has(stepId),
  };
}
