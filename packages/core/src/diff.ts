import {
  type CanonicalDag,
  type CanonicalStep,
  stableStringify,
} from "./canonicalize";
import type { StepId } from "./types";

// Every hashed CanonicalStep field except `needs` (reported as edges) must be
// listed, so a snapshot-hash change always surfaces in the diff.
const CHANGED_FIELD = {
  kind: "kind",
  whenHash: "when",
  retry: "retry",
  timeoutMs: "timeoutMs",
  signalSchema: "signalSchema",
  signalNames: "signalNames",
  matchArms: "matchArms",
  childFlowId: "childFlowId",
  subflowInputHash: "subflowInput",
} as const satisfies Record<
  Exclude<keyof CanonicalStep, "id" | "needs">,
  string
>;

export type SnapshotChangedField =
  (typeof CHANGED_FIELD)[keyof typeof CHANGED_FIELD];

// Every hashed CanonicalDag field except `steps` (diffed per step) must be listed.
const CHANGED_FLOW_FIELD = {
  flowId: "flowId",
  inputSchema: "inputSchema",
} as const satisfies Record<Exclude<keyof CanonicalDag, "steps">, string>;

export type SnapshotChangedFlowField =
  (typeof CHANGED_FLOW_FIELD)[keyof typeof CHANGED_FLOW_FIELD];

export interface SnapshotChangedEdge {
  readonly from: StepId;
  readonly to: StepId;
  readonly before: "needed" | "absent";
  readonly after: "needed" | "absent";
}

export interface SnapshotChangedPredicate {
  readonly stepId: StepId;
  readonly field: SnapshotChangedField;
}

export interface SnapshotDiff {
  readonly changedFlowFields: readonly SnapshotChangedFlowField[];
  readonly addedSteps: readonly StepId[];
  readonly removedSteps: readonly StepId[];
  readonly changedEdges: readonly SnapshotChangedEdge[];
  readonly changedPredicates: readonly SnapshotChangedPredicate[];
}

export function diffSnapshots(
  before: CanonicalDag,
  after: CanonicalDag,
): SnapshotDiff {
  const beforeStepsById = new Map(before.steps.map((s) => [s.id, s]));
  const afterStepsById = new Map(after.steps.map((s) => [s.id, s]));

  const addedSteps: StepId[] = [];
  const removedSteps: StepId[] = [];
  for (const id of afterStepsById.keys()) {
    if (!beforeStepsById.has(id)) addedSteps.push(id);
  }
  for (const id of beforeStepsById.keys()) {
    if (!afterStepsById.has(id)) removedSteps.push(id);
  }
  addedSteps.sort();
  removedSteps.sort();

  const changedEdges: SnapshotChangedEdge[] = [];
  const changedPredicates: SnapshotChangedPredicate[] = [];

  for (const id of afterStepsById.keys()) {
    const beforeStep = beforeStepsById.get(id);
    const afterStep = afterStepsById.get(id);
    if (beforeStep === undefined || afterStep === undefined) continue;

    diffEdges(id, beforeStep, afterStep, changedEdges);
    diffFields(beforeStep, afterStep, changedPredicates);
  }

  changedEdges.sort((a, b) =>
    a.to !== b.to ? cmp(a.to, b.to) : cmp(a.from, b.from),
  );
  changedPredicates.sort((a, b) =>
    a.stepId !== b.stepId ? cmp(a.stepId, b.stepId) : cmp(a.field, b.field),
  );

  const changedFlowFields: SnapshotChangedFlowField[] = [];
  for (const key of Object.keys(CHANGED_FLOW_FIELD) as Array<
    keyof typeof CHANGED_FLOW_FIELD
  >) {
    if (stableStringify(before[key]) !== stableStringify(after[key])) {
      changedFlowFields.push(CHANGED_FLOW_FIELD[key]);
    }
  }
  changedFlowFields.sort(cmp);

  return {
    changedFlowFields,
    addedSteps,
    removedSteps,
    changedEdges,
    changedPredicates,
  };
}

function diffEdges(
  stepId: StepId,
  before: CanonicalStep,
  after: CanonicalStep,
  out: SnapshotChangedEdge[],
): void {
  const beforeSet = new Set(before.needs);
  const afterSet = new Set(after.needs);
  for (const upstream of afterSet) {
    if (!beforeSet.has(upstream)) {
      out.push({
        from: upstream,
        to: stepId,
        before: "absent",
        after: "needed",
      });
    }
  }
  for (const upstream of beforeSet) {
    if (!afterSet.has(upstream)) {
      out.push({
        from: upstream,
        to: stepId,
        before: "needed",
        after: "absent",
      });
    }
  }
}

function diffFields(
  before: CanonicalStep,
  after: CanonicalStep,
  out: SnapshotChangedPredicate[],
): void {
  for (const key of Object.keys(CHANGED_FIELD) as Array<
    keyof typeof CHANGED_FIELD
  >) {
    // Same serializer as the snapshot hash, so "changed" means exactly "hash-relevant change".
    if (stableStringify(before[key]) !== stableStringify(after[key])) {
      out.push({ stepId: after.id, field: CHANGED_FIELD[key] });
    }
  }
}

function cmp(a: string, b: string): number {
  return a < b ? -1 : a > b ? 1 : 0;
}
