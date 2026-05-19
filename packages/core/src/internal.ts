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
  readonly _nested?: StepMap;
}

interface MatchDefBase {
  readonly kind: "match";
  readonly needs: NeedsMap;
  readonly parentMatch?: ParentMatchRef;
}

export interface DiscriminatorMatchDef extends MatchDefBase {
  readonly mode: "discriminator";
  readonly on: (args: {
    input: unknown;
    needs: Record<string, unknown>;
  }) => string;
  readonly arms: Readonly<Record<string, MatchArmDef>>;
}

export interface GuardMatchDef extends MatchDefBase {
  readonly mode: "guard";
  readonly arms: readonly MatchArmDef[];
}

export type MatchDef = DiscriminatorMatchDef | GuardMatchDef;

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
  const out: string[] = [];
  for (const upstream of Object.values(def.needs)) {
    if (
      upstream &&
      typeof upstream === "object" &&
      "id" in upstream &&
      typeof upstream.id === "string"
    ) {
      out.push(upstream.id);
    }
  }
  return out;
}

export function resolveNeeds(
  def: StepDef,
  loadOutput: (stepId: string) => Json | null,
): Record<string, unknown> {
  const result: Record<string, unknown> = {};
  for (const [localKey, upstream] of Object.entries(def.needs)) {
    if (
      upstream &&
      typeof upstream === "object" &&
      "id" in upstream &&
      typeof upstream.id === "string"
    ) {
      result[localKey] = loadOutput(upstream.id);
    }
  }
  return result;
}

export type StepMapWithDefs = Readonly<Record<string, StepWithDef<unknown>>>;

export function asStepMapWithDefs(steps: StepMap): StepMapWithDefs {
  return steps as StepMapWithDefs;
}

export function matchArms(def: MatchDef): readonly MatchArmDef[] {
  return def.mode === "discriminator" ? Object.values(def.arms) : def.arms;
}

export function findArm(def: MatchDef, armId: string): MatchArmDef | undefined {
  if (def.mode === "discriminator") return def.arms[armId];
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
  if (def.mode === "discriminator") {
    const key = def.on(args);
    if (def.arms[key] === undefined) {
      const available = Object.keys(def.arms).join(", ");
      throw new Error(
        `match: discriminator returned "${key}" which has no arm (available: ${available || "<none>"})`,
      );
    }
    return key;
  }

  for (const arm of def.arms) {
    if (arm.otherwise) return arm.id;
    if (arm.when?.(args)) return arm.id;
  }
  throw new Error(
    `match: no guard arm matched and no { otherwise: true } fallback was provided`,
  );
}
