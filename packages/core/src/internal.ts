import type { Resolved } from "./state";
import type {
  Json,
  LogEntry,
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
  StreamingStepCtx,
} from "./types";

export type EmitLog = (entry: LogEntry) => void;

export function makeEmit(onLog?: (entry: LogEntry) => void): EmitLog {
  if (!onLog) return () => {};
  return (entry) => {
    try {
      onLog(entry);
    } catch {
      /* D5: swallow; logging never fails a step */
    }
  };
}

export type Compacted<T> = { [K in keyof T]?: Exclude<T[K], undefined> };

export function compact<T extends object>(obj: T): Compacted<T> {
  const out: Record<string, unknown> = {};
  for (const [key, value] of Object.entries(obj)) {
    if (value !== undefined) out[key] = value;
  }
  return out as Compacted<T>;
}

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

export interface StreamingTaskDef {
  readonly kind: "streaming";
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
    ctx: StreamingStepCtx<unknown>;
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

export type StepDef =
  | TaskDef
  | StreamingTaskDef
  | SignalDef
  | MatchDef
  | SubflowDef;

export type HandlerDef = TaskDef | StreamingTaskDef;

export const DEF = Symbol("nagi.def");

export type StepWithDef<Output = unknown> = Step<Output> & {
  readonly [DEF]: StepDef | PendingMatchDef;
};

export function attachDef<Output>(
  meta: { readonly kind: StepDef["kind"]; readonly id: string },
  def: StepDef | PendingMatchDef,
): StepWithDef<Output> {
  return { kind: meta.kind, id: meta.id, [DEF]: def };
}

export function setDef(step: StepWithDef, def: StepDef): void {
  (step as { [DEF]: StepDef })[DEF] = def;
}

// Runtime/projection-phase read: walkAndRewrite has finalized every match into a
// MatchDef, so narrowing the widened slot back to StepDef here is sound.
export function getDef(step: StepWithDef): StepDef {
  return step[DEF] as StepDef;
}

// Builder-phase read: a match may still carry its pre-walk PendingMatchDef.
export function peekDef(
  step: Step<unknown>,
): StepDef | PendingMatchDef | undefined {
  return (step as Partial<StepWithDef>)[DEF];
}

export function isStepKind(def: StepDef, kind: StepDef["kind"]): boolean {
  return def.kind === kind;
}

export function handlerDef(def: StepDef): HandlerDef | undefined {
  return def.kind === "task" || def.kind === "streaming" ? def : undefined;
}

export function needsKeys(def: StepDef): readonly string[] {
  return Object.keys(def.needs);
}

export function needsStepIds(def: StepDef): readonly string[] {
  return Object.values(def.needs).map((upstream) => upstream.id);
}

export function resolveNeeds(
  def: StepDef,
  loadResolved: (stepId: string) => Resolved,
): Record<string, unknown> {
  const result: Record<string, unknown> = {};
  for (const [localKey, upstream] of Object.entries(def.needs)) {
    result[localKey] = loadResolved(upstream.id);
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
