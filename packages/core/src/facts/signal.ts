import { compact } from "../internal";
import type { Json, RunId, StepId } from "../types";
import type { FactBase, KindTable } from "./base";

export interface SignalReceivedFact extends FactBase {
  readonly kind: "signal.received";
  readonly stepId: StepId;
  readonly payload: Json;
  readonly signalName?: string;
}

// A signal that arrived before its target step entered awaitingSignal (the
// start/await race). Parked in the fact log and applied the moment the worker
// claims the step. See Store.settleSignal.
export interface SignalBufferedFact extends FactBase {
  readonly kind: "signal.buffered";
  readonly stepId: StepId;
  readonly payload: Json;
  readonly signalName?: string;
}

export type SignalFact = SignalReceivedFact | SignalBufferedFact;

export const signalFacts = {
  signalReceived(a: {
    readonly runId: RunId;
    readonly stepId: StepId;
    readonly payload: Json;
    readonly at: Date;
    readonly signalName?: string;
  }): SignalReceivedFact {
    return {
      kind: "signal.received",
      runId: a.runId,
      stepId: a.stepId,
      payload: a.payload,
      at: a.at,
      ...compact({ signalName: a.signalName }),
    };
  },

  signalBuffered(a: {
    readonly runId: RunId;
    readonly stepId: StepId;
    readonly payload: Json;
    readonly at: Date;
    readonly signalName?: string;
  }): SignalBufferedFact {
    return {
      kind: "signal.buffered",
      runId: a.runId,
      stepId: a.stepId,
      payload: a.payload,
      at: a.at,
      ...compact({ signalName: a.signalName }),
    };
  },
} as const;

export const signalKinds = {
  // Audit-only: delivery settles the step through its own step.completed.
  "signal.received": { fold: () => {}, rows: null },
  "signal.buffered": {
    // First buffered signal per step wins, mirroring the happy path where the
    // first delivered signal completes the step and later ones no-op.
    fold: (draft, fact) => {
      if (!(fact.stepId in draft.bufferedSignals)) {
        draft.bufferedSignals[fact.stepId] = {
          payload: fact.payload,
          ...compact({ signalName: fact.signalName }),
        };
      }
    },
    rows: null,
  },
} satisfies KindTable<SignalFact>;
