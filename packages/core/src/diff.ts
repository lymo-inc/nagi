import type { CanonicalDag, CanonicalStep } from "./canonicalize";
import type { StepId } from "./types";

export type SnapshotChangedField =
  | "kind"
  | "when"
  | "retry"
  | "timeoutMs"
  | "signalSchema"
  | "matchMode"
  | "matchOn"
  | "matchArms";

export interface SnapshotChangedEdge {
  readonly from: StepId;
  readonly to: StepId;
  readonly before: "needed" | "absent";
  readonly after: "needed" | "absent";
}

export interface SnapshotChangedField_ {
  readonly stepId: StepId;
  readonly field: SnapshotChangedField;
}

export interface SnapshotDiff {
  readonly addedSteps: readonly StepId[];
  readonly removedSteps: readonly StepId[];
  readonly changedEdges: readonly SnapshotChangedEdge[];
  readonly changedPredicates: readonly SnapshotChangedField_[];
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
  const changedPredicates: SnapshotChangedField_[] = [];

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

  return { addedSteps, removedSteps, changedEdges, changedPredicates };
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
  out: SnapshotChangedField_[],
): void {
  const id = after.id;
  if (before.kind !== after.kind) {
    out.push({ stepId: id, field: "kind" });
  }
  if (before.whenHash !== after.whenHash) {
    out.push({ stepId: id, field: "when" });
  }
  if (!retryEq(before.retry, after.retry)) {
    out.push({ stepId: id, field: "retry" });
  }
  if (before.timeoutMs !== after.timeoutMs) {
    out.push({ stepId: id, field: "timeoutMs" });
  }
  if (!schemaEq(before.signalSchema, after.signalSchema)) {
    out.push({ stepId: id, field: "signalSchema" });
  }
  if (before.matchMode !== after.matchMode) {
    out.push({ stepId: id, field: "matchMode" });
  }
  if (before.matchOnHash !== after.matchOnHash) {
    out.push({ stepId: id, field: "matchOn" });
  }
  if (!armsEq(before.matchArms, after.matchArms)) {
    out.push({ stepId: id, field: "matchArms" });
  }
}

function retryEq(
  a: CanonicalStep["retry"],
  b: CanonicalStep["retry"],
): boolean {
  if (a === b) return true;
  if (a === undefined || b === undefined) return false;
  return (
    a.maxAttempts === b.maxAttempts &&
    a.backoff === b.backoff &&
    a.initialDelayMs === b.initialDelayMs &&
    a.maxDelayMs === b.maxDelayMs
  );
}

function schemaEq(
  a: CanonicalStep["signalSchema"],
  b: CanonicalStep["signalSchema"],
): boolean {
  if (a === b) return true;
  if (a === undefined || b === undefined) return false;
  return (
    a.vendor === b.vendor &&
    a.version === b.version &&
    a.validateHash === b.validateHash
  );
}

function armsEq(
  a: CanonicalStep["matchArms"],
  b: CanonicalStep["matchArms"],
): boolean {
  if (a === b) return true;
  if (a === undefined || b === undefined) return false;
  if (a.length !== b.length) return false;
  for (let i = 0; i < a.length; i++) {
    const x = a[i];
    const y = b[i];
    if (x === undefined || y === undefined) return false;
    if (x.id !== y.id) return false;
    if (x.otherwise !== y.otherwise) return false;
    if (x.whenHash !== y.whenHash) return false;
    if (x.stepIds.length !== y.stepIds.length) return false;
    for (let j = 0; j < x.stepIds.length; j++) {
      if (x.stepIds[j] !== y.stepIds[j]) return false;
    }
  }
  return true;
}

function cmp(a: string, b: string): number {
  return a < b ? -1 : a > b ? 1 : 0;
}
