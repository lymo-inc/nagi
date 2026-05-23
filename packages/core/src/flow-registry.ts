import { NagiRuntimeError } from "./errors";
import { asStepMapWithDefs, getDef } from "./internal";
import type { Flow, RunId } from "./types";

export interface FlowRegistry {
  readonly all: ReadonlyArray<Flow>;
  get(flowId: string): Flow | undefined;
  has(flowId: string): boolean;
  require(flowId: string): Flow;
  requireForRun(flowId: string, runId: RunId): Flow;
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
    requireForRun(flowId, runId) {
      const flow = byId.get(flowId);
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
