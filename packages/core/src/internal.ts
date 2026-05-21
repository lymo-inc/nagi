import type {
  Json,
  Millis,
  NeedsMap,
  RetryPolicy,
  RunState,
  StandardSchemaV1,
  Step,
  StepCompleteEvent,
  StepCtx,
  StepErrorEvent,
  StepMap,
  StepRetryEvent,
  StepStartEvent,
} from "./types";

export interface ParentMatchRef {
  readonly matchId: string;
  readonly armId: string;
}

export interface TaskDef {
  readonly kind: "task";
  readonly needs: NeedsMap;
  readonly retry?: RetryPolicy;
  readonly timeoutMs?: Millis;
  readonly when?: (args: {
    input: unknown;
    needs: Record<string, unknown>;
  }) => boolean;
  readonly run: (args: {
    input: unknown;
    needs: Record<string, unknown>;
    ctx: StepCtx<unknown>;
  }) => Promise<Json>;
  readonly parentMatch?: ParentMatchRef;

  readonly onStart?: (event: StepStartEvent) => void | Promise<void>;
  readonly onComplete?: (event: StepCompleteEvent) => void | Promise<void>;
  readonly onError?: (event: StepErrorEvent) => void | Promise<void>;
  readonly onRetry?: (event: StepRetryEvent) => void | Promise<void>;
}

export interface SignalDef {
  readonly kind: "signal";
  readonly needs: NeedsMap;
  readonly schema: StandardSchemaV1;
  readonly names?: readonly [string, ...string[]];
  readonly timeoutMs?: Millis;
  readonly when?: (args: {
    input: unknown;
    needs: Record<string, unknown>;
  }) => boolean;
  readonly parentMatch?: ParentMatchRef;
}

export interface MatchArmDef {
  readonly id: string;
  readonly when?: (args: {
    input: unknown;
    needs: Record<string, unknown>;
  }) => boolean;
  readonly otherwise?: true;
  readonly stepIds: readonly string[];
}

export interface MatchDef {
  readonly kind: "match";
  readonly needs: NeedsMap;
  readonly arms: readonly MatchArmDef[];
  readonly parentMatch?: ParentMatchRef;
}

/**
 * Builder-only, pre-walk shape of a match. `match()` produces this with each
 * arm's un-walked nested step map; `walkAndRewrite` collapses it into the
 * finalized {@link MatchDef} (arms gain `stepIds`, lose `nested`). The runtime
 * never sees this type.
 */
export interface PendingMatchArm {
  readonly id: string;
  readonly when?: (args: {
    input: unknown;
    needs: Record<string, unknown>;
  }) => boolean;
  readonly otherwise?: true;
  readonly nested: StepMap;
}

export interface PendingMatchDef {
  readonly kind: "match";
  readonly needs: NeedsMap;
  readonly arms: readonly PendingMatchArm[];
  readonly parentMatch?: ParentMatchRef;
}

export interface SubflowDef {
  readonly kind: "subflow";
  readonly needs: NeedsMap;
  readonly childFlowId: string;
  readonly buildInput: (args: {
    input: unknown;
    needs: Record<string, unknown>;
  }) => unknown;
  readonly timeoutMs?: Millis;
  readonly when?: (args: {
    input: unknown;
    needs: Record<string, unknown>;
  }) => boolean;
  readonly parentMatch?: ParentMatchRef;
}

export type StepDef = TaskDef | SignalDef | MatchDef | SubflowDef;

const DEF = "__def" as const;

export type StepWithDef<Output = unknown> = Step<Output> & {
  readonly [DEF]: StepDef;
};

export function attachDef<Output>(
  meta: { readonly kind: StepDef["kind"]; readonly id: string },
  def: StepDef,
): StepWithDef<Output> {
  return { kind: meta.kind, id: meta.id, [DEF]: def };
}

export function attachDefMut(step: Step<unknown>, def: StepDef): void {
  (step as unknown as { [DEF]: StepDef })[DEF] = def;
}

export function getDef(step: Step<unknown>): StepDef {
  const def = (step as Partial<StepWithDef>)[DEF];
  if (def === undefined) {
    throw new Error(
      `Step "${step.id}" has no internal definition. Construct steps via the builder.`,
    );
  }
  return def;
}

export function isStepKind(def: StepDef, kind: StepDef["kind"]): boolean {
  return def.kind === kind;
}

export function needsKeys(def: StepDef): readonly string[] {
  return Object.keys(def.needs);
}

export function needsStepIds(def: StepDef): readonly string[] {
  return Object.values(def.needs).map((upstream) => upstream.id);
}

export function resolveNeeds(
  def: StepDef,
  loadOutput: (stepId: string) => Json | null,
): Record<string, unknown> {
  const result: Record<string, unknown> = {};
  for (const [localKey, upstream] of Object.entries(def.needs)) {
    result[localKey] = loadOutput(upstream.id);
  }
  return result;
}

export type StepMapWithDefs = Readonly<Record<string, StepWithDef<unknown>>>;

export function asStepMapWithDefs(steps: StepMap): StepMapWithDefs {
  return steps as StepMapWithDefs;
}

export function findArm(def: MatchDef, armId: string): MatchArmDef | undefined {
  return def.arms.find((a) => a.id === armId);
}

export function readSelectedArm(
  matchId: string,
  runState: RunState,
): string | null {
  let selected: string | null = null;
  for (const fact of runState.facts) {
    if (fact.kind === "match.arm-selected" && fact.stepId === matchId) {
      selected = fact.arm;
    } else if (fact.kind === "step.reset" && fact.stepId === matchId) {
      selected = null;
    }
  }
  return selected;
}

export function selectArm(
  def: MatchDef,
  args: { readonly input: unknown; readonly needs: Record<string, unknown> },
): string {
  for (const arm of def.arms) {
    if (arm.otherwise) return arm.id;
    if (arm.when?.(args)) return arm.id;
  }
  throw new Error(
    `match: no arm matched and no { otherwise: true } fallback was provided`,
  );
}
