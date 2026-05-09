import type {
  Json,
  Millis,
  NeedsMap,
  RetryPolicy,
  RunState,
  StandardSchemaV1,
  Step,
  StepCtx,
  StepMap,
} from "./types";

/**
 * A nested step's annotation — set on any step that lives inside a match arm.
 * The scheduler uses this to gate the step:
 *   - blocked     until the parent match selects an arm
 *   - skipped     when the parent's selected arm differs from this step's arm
 *   - eligible    when the parent's selected arm matches
 */
export interface ParentMatchRef {
  readonly matchId: string;
  readonly armId: string;
}

export interface TaskDef {
  readonly kind: "task";
  readonly needs: NeedsMap;
  readonly retry?: RetryPolicy;
  readonly timeout?: Millis;
  readonly when?: (args: { input: unknown; needs: Record<string, unknown> }) => boolean;
  readonly run: (args: {
    input: unknown;
    needs: Record<string, unknown>;
    ctx: StepCtx<unknown>;
  }) => Promise<Json>;
  readonly parentMatch?: ParentMatchRef;
}

export interface SignalDef {
  readonly kind: "signal";
  readonly needs: NeedsMap;
  readonly schema: StandardSchemaV1;
  readonly timeout?: Millis;
  readonly when?: (args: { input: unknown; needs: Record<string, unknown> }) => boolean;
  readonly parentMatch?: ParentMatchRef;
}

/**
 * One arm of a match. For discriminator matches `id` is the case key
 * (e.g. "hot"); for guard matches it's `arm0` / `arm1` / ... / `otherwise`.
 *
 * `stepIds` is populated by `flow()` once nested steps have been assigned
 * namespaced IDs (`<matchKey>.<armId>.<stepKey>`). During builder execution
 * `stepIds` is empty and the unflattened map lives on `_nested`, which
 * `flow()` consumes and discards.
 */
export interface MatchArmDef {
  readonly id: string;
  readonly when?: (args: { input: unknown; needs: Record<string, unknown> }) => boolean;
  readonly otherwise?: true;
  readonly stepIds: readonly string[];
  /** @internal Transient. Populated by builder, consumed by flow(). */
  readonly _nested?: StepMap;
}

interface MatchDefBase {
  readonly kind: "match";
  readonly needs: NeedsMap;
  readonly parentMatch?: ParentMatchRef;
}

export interface DiscriminatorMatchDef extends MatchDefBase {
  readonly mode: "discriminator";
  readonly on: (args: { input: unknown; needs: Record<string, unknown> }) => string;
  readonly arms: Readonly<Record<string, MatchArmDef>>;
}

export interface GuardMatchDef extends MatchDefBase {
  readonly mode: "guard";
  readonly arms: readonly MatchArmDef[];
}

export type MatchDef = DiscriminatorMatchDef | GuardMatchDef;

export type StepDef = TaskDef | SignalDef | MatchDef;

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

export function getDef(step: Step<unknown>): StepDef {
  const def = (step as Partial<StepWithDef>)[DEF];
  if (def === undefined) {
    throw new Error(
      `Step "${step.id}" has no internal definition. Construct steps via the builder.`,
    );
  }
  return def;
}

/** A step is "complete-blocking" if downstream can't run until it finishes. */
export function isStepKind(def: StepDef, kind: StepDef["kind"]): boolean {
  return def.kind === kind;
}

/** Names of the upstream steps this step depends on. */
export function needsKeys(def: StepDef): readonly string[] {
  return Object.keys(def.needs);
}

/** Extract the upstream step IDs (after `flow()` has assigned ids from keys). */
export function needsStepIds(def: StepDef): readonly string[] {
  const out: string[] = [];
  for (const upstream of Object.values(def.needs)) {
    if (upstream && typeof upstream === "object" && "id" in upstream && typeof upstream.id === "string") {
      out.push(upstream.id);
    }
  }
  return out;
}

/** Build the `needs` record passed to a handler — `{ localKey: upstreamOutput }`. */
export function resolveNeeds(
  def: StepDef,
  loadOutput: (stepId: string) => Json | null,
): Record<string, unknown> {
  const result: Record<string, unknown> = {};
  for (const [localKey, upstream] of Object.entries(def.needs)) {
    if (upstream && typeof upstream === "object" && "id" in upstream && typeof upstream.id === "string") {
      result[localKey] = loadOutput(upstream.id);
    }
  }
  return result;
}

export type StepMapWithDefs = Readonly<Record<string, StepWithDef<unknown>>>;

/** Cast a public StepMap to the internal one. Builder produces StepWithDef instances. */
export function asStepMapWithDefs(steps: StepMap): StepMapWithDefs {
  return steps as StepMapWithDefs;
}

/** All arms of a match, regardless of mode, as an ordered list. */
export function matchArms(def: MatchDef): readonly MatchArmDef[] {
  return def.mode === "discriminator" ? Object.values(def.arms) : def.arms;
}

/** Look up an arm by id. Returns undefined if no arm has that id. */
export function findArm(def: MatchDef, armId: string): MatchArmDef | undefined {
  if (def.mode === "discriminator") return def.arms[armId];
  return def.arms.find((a) => a.id === armId);
}

/**
 * Read the chosen arm of a match from the fact log. Returns `null` if the
 * match has not yet selected an arm in this run (i.e. no `match.arm-selected`
 * fact for `matchId`).
 *
 * The fact log is the source of truth for the selection — it survives crash
 * and replay, and the match's own `running` state in the steps projection
 * doesn't carry the chosen-arm bit.
 */
export function readSelectedArm(matchId: string, runState: RunState): string | null {
  for (const fact of runState.facts) {
    if (fact.kind === "match.arm-selected" && fact.stepId === matchId) {
      return fact.arm;
    }
  }
  return null;
}

/**
 * Pick the arm to run for a given match invocation.
 *
 * Discriminator: invoke `on(args)`, look up the case key in `def.arms`. Throw
 *   if the returned key has no arm — the type system guards exhaustiveness,
 *   but the discriminant might be a runtime-narrowed string that escapes the
 *   declared union.
 *
 * Guard: walk `def.arms` top-to-bottom. The first arm whose `when()` returns
 *   true wins; an arm with `otherwise: true` matches unconditionally. Throw
 *   if no arm matches and no `otherwise` is present (per the v0 fallthrough
 *   policy: unreachable fallthrough is a programmer error → match fails
 *   terminally).
 *
 * The thrown Error's message becomes the `step.failed.error.message` for the
 * match, so phrase it for an operator reading a fact log.
 *
 * TODO(user): implement. ~10 lines. The two branches are dispatched by
 * `def.mode`. `args` carries the same `{ input, needs }` shape you'd hand to
 * a `task.run` — `on` and `when` callbacks expect exactly that.
 */
export function selectArm(
  def: MatchDef,
  args: { readonly input: unknown; readonly needs: Record<string, unknown> },
): string {
  throw new Error("selectArm: not yet implemented");
}
