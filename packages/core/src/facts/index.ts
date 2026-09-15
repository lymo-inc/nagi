import { compact } from "../internal";
import type { RunState } from "../state";
import type { RunId } from "../types";
import type { KindTable, RowDelta, RunDraft } from "./base";
import { type FlowFact, flowFacts, flowKinds } from "./flow";
import { type LeaseFact, leaseFacts, leaseKinds } from "./lease";
import { type SignalFact, signalFacts, signalKinds } from "./signal";
import { type StepFact, stepFacts, stepKinds } from "./step";

export type { BufferedSignal, RowDelta } from "./base";
export type * from "./flow";
export type * from "./lease";
export type * from "./signal";
export type * from "./step";

export type Fact = FlowFact | StepFact | SignalFact | LeaseFact;
export type FactKind = Fact["kind"];

export const Facts = {
  ...flowFacts,
  ...stepFacts,
  ...signalFacts,
  ...leaseFacts,
} as const;

const KINDS = {
  ...flowKinds,
  ...stepKinds,
  ...signalKinds,
  ...leaseKinds,
} satisfies KindTable<Fact>;

interface Arm {
  readonly fold: (draft: RunDraft, fact: Fact) => void;
  readonly rows: ((fact: Fact) => RowDelta) | null;
}

// Looked up by string, not FactKind: a persisted log may carry a kind this
// build no longer declares, and it must fold and materialize as a no-op rather
// than throw. The cast also erases the per-kind parameter types, which TS
// cannot correlate through an indexed lookup.
function armOf(kind: string): Arm | undefined {
  return (KINDS as unknown as Readonly<Record<string, Arm | undefined>>)[kind];
}

export function foldRun(runId: RunId, facts: readonly Fact[]): RunState {
  const draft: RunDraft = {
    flowId: "",
    input: null,
    phase: { tag: "pending" },
    parent: undefined,
    flowHash: undefined,
    codeVersion: undefined,
    steps: {},
    selectedArms: {},
    bufferedSignals: {},
  };
  for (const fact of facts) armOf(fact.kind)?.fold(draft, fact);
  const { parent, flowHash, codeVersion, ...projected } = draft;
  return {
    runId,
    ...projected,
    facts,
    ...compact({ parent, flowHash, codeVersion }),
  };
}

export function rowDeltaOf(fact: Fact): RowDelta | null {
  return armOf(fact.kind)?.rows?.(fact) ?? null;
}
