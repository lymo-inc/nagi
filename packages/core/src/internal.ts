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

/**
 * Internal emit choke point. Built once from {@link NagiConfig.onLog} and
 * threaded everywhere a diagnostic is produced. Always a callable (a no-op when
 * `onLog` is absent), so call sites never branch on its presence. Kept out of
 * the public API surface (not re-exported from `index.ts`).
 */
export type EmitLog = (entry: LogEntry) => void;

/**
 * Wrap the host's `onLog` into the {@link EmitLog} choke point. When `onLog` is
 * absent the result is a no-op and no {@link LogEntry} is constructed upstream
 * (D3: silent by default). A throwing sink is swallowed so logging can never
 * fail a workflow step (D5).
 */
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

/**
 * Drop keys whose value is `undefined`, turning a `{ x: T | undefined }` bag
 * into `{ x?: T }`. The single home for the `exactOptionalPropertyTypes`
 * bridge, replacing per-field `...(x !== undefined ? { x } : {})` spreads. Pass
 * only optional fields; keep required fields as explicit literal properties.
 */
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

export type StepDef =
  | TaskDef
  | StreamingTaskDef
  | SignalDef
  | MatchDef
  | SubflowDef;

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

/**
 * Read a step's def in the runtime/projection phase, where {@link walkAndRewrite}
 * has already finalized every match into a {@link MatchDef}. The stored slot is
 * widened to admit the builder-only {@link PendingMatchDef}; narrowing it back to
 * {@link StepDef} here is the single phase boundary, so runtime callers never
 * have to consider the pending shape.
 */
export function getDef(step: StepWithDef): StepDef {
  return step[DEF] as StepDef;
}

/**
 * Read a step's def during the builder phase, where a match still carries its
 * pre-walk {@link PendingMatchDef}. Runtime code should use {@link getDef}.
 */
export function peekDef(
  step: Step<unknown>,
): StepDef | PendingMatchDef | undefined {
  return (step as Partial<StepWithDef>)[DEF];
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
