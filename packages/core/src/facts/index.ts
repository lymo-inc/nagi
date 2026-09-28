import { compact } from "../internal";
import type { RunState } from "../state";
import type { RunEvent, RunId } from "../types";
import type {
  KindTable,
  Release,
  RowDelta,
  RunDraft,
  StreamEffect,
} from "./base";
import { type FlowFact, flowFacts, flowKinds } from "./flow";
import { type LeaseFact, leaseFacts, leaseKinds } from "./lease";
import { type SignalFact, signalFacts, signalKinds } from "./signal";
import { type StepFact, stepFacts, stepKinds } from "./step";

export type {
  BufferedSignal,
  Release,
  RowDelta,
  StreamEffect,
} from "./base";
export type * from "./flow";
export { isRunEnd, runCancelCause } from "./flow";
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
  readonly release: ((fact: Fact) => Release) | null;
  readonly stream: ((fact: Fact) => StreamEffect) | null;
  readonly event: ((fact: Fact) => RunEvent) | null;
}

// Looked up by string, not FactKind: a persisted log may carry a kind this
// build no longer declares, and it must fold and write as a no-op rather than
// throw. The cast also erases the per-kind parameter types, which TS
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
    resetCounts: {},
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

// Everything writing `fact` implies. Store adapters apply all of it inside the
// fact's own transaction, at their single fact-write path, so no call site
// chooses which consequences a write gets.
export interface FactConsequences {
  readonly rows: RowDelta | null;
  readonly release: Release | null;
  readonly stream: StreamEffect | null;
  readonly event: RunEvent | null;
}

export function factConsequences(fact: Fact): FactConsequences {
  const arm = armOf(fact.kind);
  return {
    rows: arm?.rows?.(fact) ?? null,
    release: arm?.release?.(fact) ?? null,
    stream: arm?.stream?.(fact) ?? null,
    event: arm?.event?.(fact) ?? null,
  };
}
