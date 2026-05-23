import type { CanonicalDag } from "./canonicalize";
import { NagiRuntimeError } from "./errors";
import {
  asStepMapWithDefs,
  attachDef,
  compact,
  getDef,
  type StepDef,
  setDef,
} from "./internal";
import type { Flow } from "./types";

export function synthesizeReplayFlow(dag: CanonicalDag, liveFlow: Flow): Flow {
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
