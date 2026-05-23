import { type Resolved, unwrap } from "./state";
import type {
  Json,
  LogEntry,
  Millis,
  NeedRef,
  NeedsMap,
  Optional,
  RetryPolicy,
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

export interface NeedRefDef {
  readonly step: Step;
  readonly optional: boolean;
}
export type NeedsDefMap = Readonly<Record<string, NeedRefDef>>;

function isOptional(ref: NeedRef): ref is Optional<Step> {
  return "__optional" in ref;
}

// Normalizes optionality once, at the builder boundary, so internals work
// against a single {step, optional} shape.
export function normalizeNeeds(needs: NeedsMap | undefined): NeedsDefMap {
  const out: Record<string, NeedRefDef> = {};
  if (needs === undefined) return out;
  for (const [key, ref] of Object.entries(needs)) {
    out[key] = isOptional(ref)
      ? { step: ref.__optional, optional: true }
      : { step: ref, optional: false };
  }
  return out;
}

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

export type GuardArgs = {
  readonly input: unknown;
  readonly needs: Record<string, unknown>;
};
export type Guard = (args: GuardArgs) => boolean;

export type ArmGuard =
  | { readonly kind: "when"; readonly when: Guard }
  | { readonly kind: "otherwise" };

export interface TaskDef {
  readonly kind: "task";
  readonly needs: NeedsDefMap;
  readonly retry?: RetryPolicy;
  readonly timeoutMs?: Millis;
  readonly when?: Guard;
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
  readonly needs: NeedsDefMap;
  readonly retry?: RetryPolicy;
  readonly timeoutMs?: Millis;
  readonly when?: Guard;
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
  readonly needs: NeedsDefMap;
  readonly schema: StandardSchemaV1;
  readonly names?: readonly [string, ...string[]];
  readonly timeoutMs?: Millis;
  readonly when?: Guard;
  readonly parentMatch?: ParentMatchRef;
}

export interface MatchArmDef {
  readonly id: string;
  readonly guard: ArmGuard;
  readonly stepIds: readonly string[];
}

export interface MatchDef {
  readonly kind: "match";
  readonly needs: NeedsDefMap;
  readonly arms: readonly MatchArmDef[];
  readonly parentMatch?: ParentMatchRef;
}

export interface PendingMatchArm {
  readonly id: string;
  readonly guard: ArmGuard;
  readonly nested: StepMap;
}

export interface PendingMatchDef {
  readonly kind: "match";
  readonly needs: NeedsDefMap;
  readonly arms: readonly PendingMatchArm[];
  readonly parentMatch?: ParentMatchRef;
}

export interface SubflowDef {
  readonly kind: "subflow";
  readonly needs: NeedsDefMap;
  readonly childFlowId: string;
  readonly buildInput: (args: GuardArgs) => unknown;
  readonly timeoutMs?: Millis;
  readonly when?: Guard;
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

export function handlerDef(def: StepDef): HandlerDef | undefined {
  return def.kind === "task" || def.kind === "streaming" ? def : undefined;
}

export function needsStepIds(def: StepDef): readonly string[] {
  return Object.values(def.needs).map((ref) => ref.step.id);
}

export function resolveNeeds(
  def: StepDef,
  loadResolved: (stepId: string) => Resolved,
): Record<string, unknown> {
  const result: Record<string, unknown> = {};
  for (const [localKey, ref] of Object.entries(def.needs)) {
    const resolved = loadResolved(ref.step.id);
    // A required need is gated on completion, so it always carries a value;
    // only optional needs surface the Resolved union to the handler.
    result[localKey] = ref.optional ? resolved : unwrap(resolved);
  }
  return result;
}

export type StepMapWithDefs = Readonly<Record<string, StepWithDef<unknown>>>;

export function asStepMapWithDefs(steps: StepMap): StepMapWithDefs {
  return steps as StepMapWithDefs;
}

export function selectArm(def: MatchDef, args: GuardArgs): string {
  for (const arm of def.arms) {
    if (arm.guard.kind === "otherwise") return arm.id;
    if (arm.guard.when(args)) return arm.id;
  }
  throw new Error(
    `match: no arm matched and no { otherwise: true } fallback was provided`,
  );
}
