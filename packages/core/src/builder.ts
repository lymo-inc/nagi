import {
  attachDef,
  attachDefMut,
  type DiscriminatorMatchDef,
  type GuardMatchDef,
  type MatchArmDef,
  type MatchDef,
  type ParentMatchRef,
  type SignalDef,
  type StepDef,
  type TaskDef,
} from "./internal";
import type {
  AsStepMap,
  Builder,
  Flow,
  FlowCompleteEvent,
  FlowConcurrency,
  FlowConfig,
  InferSchemaOutput,
  Json,
  MatchArm,
  MatchDiscriminatorConfig,
  MatchGuardConfig,
  NeedsMap,
  SignalConfig,
  StandardSchemaV1,
  Step,
  StepCompleteEvent,
  StepEntryConfig,
  StepMap,
  TaskConfig,
} from "./types";

/**
 * Marker stamped on every Builder instance so `flow()` (and match-case
 * extraction) can distinguish a chain return from a plain `StepMap`.
 */
const BUILDER_BRAND = Symbol.for("nagi.builder");

interface BuilderInternal {
  readonly [BUILDER_BRAND]: true;
  readonly __steps: Map<string, Step<unknown>>;
}

function isBuilder(value: unknown): value is BuilderInternal {
  return (
    typeof value === "object" &&
    value !== null &&
    (value as { [BUILDER_BRAND]?: unknown })[BUILDER_BRAND] === true
  );
}

/** Resolve a build-callback result (Builder | StepMap) to a plain StepMap. */
function resolveBuildResult(value: unknown): StepMap {
  if (isBuilder(value)) {
    return Object.fromEntries(value.__steps) as StepMap;
  }
  return value as StepMap;
}

function isDiscriminator(
  config: object,
): config is MatchDiscriminatorConfig<
  unknown,
  NeedsMap,
  string,
  Record<string, StepMap>
> {
  return "cases" in config && "on" in config;
}

function makeBuilder<Input>(): Builder<Input> {
  // Chain accumulator. Each `.step()` / `.include()` call writes here so the
  // build callback can return the builder directly and `flow()` can read the
  // assembled StepMap.
  const chainSteps = new Map<string, Step<unknown>>();

  function task<N extends NeedsMap, O>(
    config: TaskConfig<Input, N, O>,
  ): Step<O> {
    const def: TaskDef = {
      kind: "task",
      needs: (config.needs ?? {}) as NeedsMap,
      ...(config.retry !== undefined ? { retry: config.retry } : {}),
      ...(config.timeoutMs !== undefined
        ? { timeoutMs: config.timeoutMs }
        : {}),
      ...(config.when !== undefined
        ? {
            when: config.when as (args: {
              input: unknown;
              needs: Record<string, unknown>;
            }) => boolean,
          }
        : {}),
      run: config.run as TaskDef["run"],
      ...(config.onStart !== undefined ? { onStart: config.onStart } : {}),
      ...(config.onComplete !== undefined
        ? {
            onComplete: config.onComplete as (
              event: StepCompleteEvent,
            ) => void | Promise<void>,
          }
        : {}),
      ...(config.onError !== undefined ? { onError: config.onError } : {}),
      ...(config.onRetry !== undefined ? { onRetry: config.onRetry } : {}),
    };
    return attachDef<O>({ kind: "task", id: "" }, def);
  }

  function signal<N extends NeedsMap, S extends StandardSchemaV1>(
    config: SignalConfig<Input, N, S>,
  ): Step<InferSchemaOutput<S>> {
    // `name` xor `names` is a type-level constraint (see SignalConfig union);
    // normalize to a single internal `names` field. Leave it undefined when
    // the caller supplied neither — the lookup path resolves via step id for
    // that case, preserving byte-identical hashes for pre-RFC-0004 flows.
    const namedAs = "names" in config ? config.names : undefined;
    const namedAsSingle = "name" in config ? config.name : undefined;
    const explicit: readonly [string, ...string[]] | undefined =
      namedAs !== undefined
        ? namedAs
        : namedAsSingle !== undefined
          ? [namedAsSingle]
          : undefined;

    const def: SignalDef = {
      kind: "signal",
      needs: (config.needs ?? {}) as NeedsMap,
      schema: config.schema,
      ...(explicit !== undefined ? { names: explicit } : {}),
      ...(config.timeoutMs !== undefined
        ? { timeoutMs: config.timeoutMs }
        : {}),
      ...(config.when !== undefined
        ? {
            when: config.when as (args: {
              input: unknown;
              needs: Record<string, unknown>;
            }) => boolean,
          }
        : {}),
    };
    return attachDef<InferSchemaOutput<S>>({ kind: "signal", id: "" }, def);
  }

  function match(config: object): Step<unknown> {
    const needs =
      "needs" in config && config.needs
        ? (config.needs as NeedsMap)
        : ({} as NeedsMap);

    if (isDiscriminator(config)) {
      const arms: Record<string, MatchArmDef> = {};
      for (const [caseKey, build] of Object.entries(config.cases)) {
        const nested = resolveBuildResult(
          (build as (b: Builder<Input>) => unknown)(makeBuilder<Input>()),
        );
        arms[caseKey] = { id: caseKey, stepIds: [], _nested: nested };
      }
      const def: DiscriminatorMatchDef = {
        kind: "match",
        mode: "discriminator",
        needs,
        on: config.on as DiscriminatorMatchDef["on"],
        arms,
      };
      return attachDef<unknown>({ kind: "match", id: "" }, def);
    }

    const guardConfig = config as MatchGuardConfig<Input, NeedsMap, StepMap>;
    const arms: MatchArmDef[] = [];
    for (let i = 0; i < guardConfig.arms.length; i++) {
      const arm = guardConfig.arms[i] as MatchArm<Input, NeedsMap, StepMap>;
      const nested = resolveBuildResult(arm.build(makeBuilder<Input>()));
      const armId = arm.otherwise ? "otherwise" : `arm${i}`;
      arms.push({
        id: armId,
        ...(arm.when !== undefined
          ? {
              when: arm.when as (args: {
                input: unknown;
                needs: Record<string, unknown>;
              }) => boolean,
            }
          : {}),
        ...(arm.otherwise ? { otherwise: true as const } : {}),
        stepIds: [],
        _nested: nested,
      });
    }
    const def: GuardMatchDef = {
      kind: "match",
      mode: "guard",
      needs,
      arms,
    };
    return attachDef<unknown>({ kind: "match", id: "" }, def);
  }

  /**
   * Chainable task entry. Pre-allocates a `Step` shell so sibling `needs`
   * keys can hold a stable object reference, then immediately stamps the
   * built `TaskDef` onto the shell. The shell is registered in the chain
   * accumulator so subsequent `.step()` calls can resolve sibling refs.
   */
  function step(
    key: string,
    config: StepEntryConfig<
      Input,
      Record<string, unknown>,
      ReadonlyArray<string>,
      unknown
    >,
  ): Builder<Input, Record<string, unknown>> {
    if (chainSteps.has(key)) {
      throw new Error(
        `b.step: duplicate key "${key}" — each chain entry must have a unique key.`,
      );
    }
    const shell: Step<unknown> = { kind: "task", id: "" };
    const needs: Record<string, Step<unknown>> = {};
    if (config.needs) {
      for (const sibKey of config.needs) {
        const sibling = chainSteps.get(sibKey);
        if (sibling === undefined) {
          throw new Error(
            `b.step("${key}"): unknown sibling "${sibKey}". ` +
              `Available keys so far: ${[...chainSteps.keys()].join(", ") || "<none>"}.`,
          );
        }
        needs[sibKey] = sibling;
      }
    }
    const def: TaskDef = {
      kind: "task",
      needs,
      ...(config.retry !== undefined ? { retry: config.retry } : {}),
      ...(config.timeoutMs !== undefined
        ? { timeoutMs: config.timeoutMs }
        : {}),
      ...(config.when !== undefined
        ? {
            when: config.when as (args: {
              input: unknown;
              needs: Record<string, unknown>;
            }) => boolean,
          }
        : {}),
      run: config.run as TaskDef["run"],
      ...(config.onStart !== undefined ? { onStart: config.onStart } : {}),
      ...(config.onComplete !== undefined
        ? {
            onComplete: config.onComplete as (
              event: StepCompleteEvent,
            ) => void | Promise<void>,
          }
        : {}),
      ...(config.onError !== undefined ? { onError: config.onError } : {}),
      ...(config.onRetry !== undefined ? { onRetry: config.onRetry } : {}),
    };
    attachDefMut(shell, def);
    chainSteps.set(key, shell);
    return builder as Builder<Input, Record<string, unknown>>;
  }

  /**
   * Bring a pre-built `Step` (from `b.task` / `b.signal` / `b.match`) into
   * the chain under a key. The step's def is already attached; we only
   * register the identity in the accumulator.
   */
  function include(
    key: string,
    s: Step<unknown>,
  ): Builder<Input, Record<string, unknown>> {
    if (chainSteps.has(key)) {
      throw new Error(
        `b.include: duplicate key "${key}" — each chain entry must have a unique key.`,
      );
    }
    chainSteps.set(key, s);
    return builder as Builder<Input, Record<string, unknown>>;
  }

  const builder = {
    task,
    signal,
    match: match as Builder<Input>["match"],
    step: step as Builder<Input>["step"],
    include: include as Builder<Input>["include"],
    [BUILDER_BRAND]: true as const,
    __steps: chainSteps,
  } as Builder<Input> & BuilderInternal;

  return builder;
}

/**
 * Construct a flow.
 *
 * 1. Run `build(b)` to collect the user's StepMap. Each call to `b.task` /
 *    `b.signal` / `b.match` produced a Step with `id: ""` and a captured def
 *    (which may reference upstream Step values from earlier in the closure).
 *    Match defs additionally hold their arms' nested StepMaps on `_nested`.
 * 2. Walk the returned map *recursively*, descending into match arms. Each
 *    step gets an id derived from its position: top-level keys verbatim,
 *    nested arm steps namespaced as `<matchKey>.<armId>.<stepKey>`.
 * 3. Rewrite every def's `needs` so each upstream Step value carries its
 *    assigned id. Nested-arm step defs additionally get a `parentMatch`
 *    annotation so the scheduler can gate them on arm selection.
 * 4. Promote `MatchArmDef._nested` → `MatchArmDef.stepIds` (the namespaced
 *    IDs of the arm's nested steps), then drop `_nested`.
 */
export function flow<
  const Id extends string,
  InputSchema extends StandardSchemaV1,
  R,
  Output = unknown,
>(
  config: FlowConfig<Id, InputSchema, R, Output>,
): Flow<Id, InputSchema, AsStepMap<R>, Output> {
  const builder = makeBuilder<InferSchemaOutput<InputSchema>>();
  const built = resolveBuildResult(config.build(builder));

  const idByIdentity = new Map<Step<unknown>, string>();
  collectIds(built, "", idByIdentity);

  const finalSteps: Record<string, Step<unknown>> = {};
  walkAndRewrite({
    flowId: config.id,
    map: built,
    prefix: "",
    parentMatch: undefined,
    idByIdentity,
    out: finalSteps,
  });

  assertSignalNameUniqueness(config.id, finalSteps);

  return {
    id: config.id,
    input: config.input,
    steps: finalSteps as AsStepMap<R>,
    ...(config.output !== undefined ? { output: config.output } : {}),
    ...(config.onStart !== undefined ? { onStart: config.onStart } : {}),
    ...(config.onComplete !== undefined
      ? {
          onComplete: config.onComplete as (
            event: FlowCompleteEvent,
          ) => void | Promise<void>,
        }
      : {}),
    ...(config.onError !== undefined ? { onError: config.onError } : {}),
    ...(config.concurrency !== undefined
      ? { concurrency: config.concurrency as FlowConcurrency<Json> }
      : {}),
  };
}

/** Recursively assign namespaced IDs and record them by Step identity. */
function collectIds(
  map: StepMap,
  prefix: string,
  idByIdentity: Map<Step<unknown>, string>,
): void {
  for (const [key, step] of Object.entries(map)) {
    const id = prefix ? `${prefix}.${key}` : key;
    idByIdentity.set(step, id);
    const def = (step as { __def?: StepDef }).__def;
    if (def?.kind === "match") {
      const arms: readonly MatchArmDef[] =
        def.mode === "discriminator" ? Object.values(def.arms) : def.arms;
      for (const arm of arms) {
        if (arm._nested)
          collectIds(arm._nested, `${id}.${arm.id}`, idByIdentity);
      }
    }
  }
}

interface WalkArgs {
  readonly flowId: string;
  readonly map: StepMap;
  readonly prefix: string;
  readonly parentMatch: ParentMatchRef | undefined;
  readonly idByIdentity: Map<Step<unknown>, string>;
  readonly out: Record<string, Step<unknown>>;
}

/**
 * Recursively rewrite defs and emit them into `out` under their assigned
 * namespaced IDs. For matches, each arm's nested steps are emitted into the
 * same flat `out` map (so the scheduler sees the full graph), then the arm
 * is finalized with the namespaced ID list.
 */
function walkAndRewrite(args: WalkArgs): void {
  const { flowId, map, prefix, parentMatch, idByIdentity, out } = args;

  for (const [key, step] of Object.entries(map)) {
    const id = prefix ? `${prefix}.${key}` : key;
    const def = (step as { __def?: StepDef }).__def;
    if (def === undefined) {
      throw new Error(
        `Flow "${flowId}": step "${id}" has no internal def. ` +
          `Did you return a value not produced by the builder?`,
      );
    }
    // Steps fresh from `b.task` / `b.signal` / `b.match` carry id "". A
    // non-empty id means this step was already processed by a different
    // `flow()` call — sharing a step across flows is unsupported.
    if (step.id !== "") {
      throw new Error(
        `Flow "${flowId}": step "${id}" was produced by a different flow() ` +
          `call (its id is already "${step.id}"). Each flow's build must use ` +
          `only the builder passed to it; steps cannot be shared between flows.`,
      );
    }

    const rewrittenNeeds: Record<string, Step<unknown>> = {};
    for (const [localKey, upstream] of Object.entries(def.needs)) {
      const upstreamId = idByIdentity.get(upstream);
      if (upstreamId === undefined) {
        const fromOtherFlow = upstream.id !== "";
        throw new Error(
          fromOtherFlow
            ? `Flow "${flowId}": step "${id}" needs an upstream step from a ` +
                `different flow() call (upstream id "${upstream.id}"). Steps ` +
                `cannot be shared between flows.`
            : `Flow "${flowId}": step "${id}" references an upstream step ` +
                `that was not returned from build(). Add it to the returned object.`,
        );
      }
      rewrittenNeeds[localKey] = { ...upstream, id: upstreamId };
    }

    if (def.kind === "match") {
      const armList: readonly MatchArmDef[] =
        def.mode === "discriminator" ? Object.values(def.arms) : def.arms;
      const finalizedArms: MatchArmDef[] = [];

      for (const arm of armList) {
        const armPrefix = `${id}.${arm.id}`;
        const nestedStepIds: string[] = [];
        if (arm._nested) {
          for (const nestedKey of Object.keys(arm._nested)) {
            nestedStepIds.push(`${armPrefix}.${nestedKey}`);
          }
          walkAndRewrite({
            flowId,
            map: arm._nested,
            prefix: armPrefix,
            parentMatch: { matchId: id, armId: arm.id },
            idByIdentity,
            out,
          });
        }
        finalizedArms.push({
          id: arm.id,
          ...(arm.when !== undefined ? { when: arm.when } : {}),
          ...(arm.otherwise ? { otherwise: true as const } : {}),
          stepIds: nestedStepIds,
        });
      }

      const finalizedDef: MatchDef =
        def.mode === "discriminator"
          ? {
              kind: "match",
              mode: "discriminator",
              needs: rewrittenNeeds,
              on: def.on,
              arms: Object.fromEntries(finalizedArms.map((a) => [a.id, a])),
              ...(parentMatch ? { parentMatch } : {}),
            }
          : {
              kind: "match",
              mode: "guard",
              needs: rewrittenNeeds,
              arms: finalizedArms,
              ...(parentMatch ? { parentMatch } : {}),
            };

      out[id] = attachDef({ kind: "match", id }, finalizedDef);
      continue;
    }

    const baseRewritten = { ...def, needs: rewrittenNeeds } as StepDef;
    const finalizedDef = parentMatch
      ? ({ ...baseRewritten, parentMatch } as StepDef)
      : baseRewritten;
    out[id] = attachDef({ kind: finalizedDef.kind, id }, finalizedDef);
  }
}

/**
 * Signal names share a namespace with step ids: every step's id is implicitly
 * an accepted signal name, and explicit `name` / `names` on a signal step
 * adds aliases to that same namespace. Two declarations of the same name —
 * step-id-vs-alias OR alias-vs-alias — make `wf.signal(runId, name, ...)`
 * ambiguous. We catch that at construction so a bad flow can't boot.
 */
function assertSignalNameUniqueness(
  flowId: string,
  finalSteps: Record<string, Step<unknown>>,
): void {
  // Map<name, "<sourceKind>:<sourceStepId>"> — first writer wins, second hit throws.
  const owners = new Map<string, string>();

  for (const stepId of Object.keys(finalSteps)) {
    owners.set(stepId, `step id "${stepId}"`);
  }

  for (const [stepId, step] of Object.entries(finalSteps)) {
    const def = (step as { __def?: StepDef }).__def;
    if (def === undefined || def.kind !== "signal") continue;
    if (def.names === undefined) continue;

    for (const alias of def.names) {
      // An alias listing the step's own id is a no-op (deduped), not a clash.
      if (alias === stepId) continue;

      const prior = owners.get(alias);
      const here = `alias of step "${stepId}"`;
      if (prior !== undefined && prior !== here) {
        throw new Error(
          `Flow "${flowId}": signal name "${alias}" is declared as both ` +
            `${prior} and ${here}. ` +
            `Pick one — signal names share a namespace with step ids.`,
        );
      }
      owners.set(alias, here);
    }
  }
}
