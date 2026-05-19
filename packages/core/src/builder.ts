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
  type SubflowDef,
  type TaskDef,
} from "./internal";
import type {
  AsStepMap,
  Builder,
  Flow,
  FlowCompleteEvent,
  FlowConcurrency,
  FlowConfig,
  FlowOutput,
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
  SubflowConfig,
  SubflowStepOutput,
  TaskConfig,
} from "./types";

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
    const def: SignalDef = {
      kind: "signal",
      needs: (config.needs ?? {}) as NeedsMap,
      schema: config.schema,
      ...(config.names !== undefined ? { names: config.names } : {}),
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

  function subflow<N extends NeedsMap, Child extends Flow>(
    child: Child,
    config: SubflowConfig<Input, N, Child>,
  ): Step<SubflowStepOutput<FlowOutput<Child>>> {
    const def: SubflowDef = {
      kind: "subflow",
      needs: (config.needs ?? {}) as NeedsMap,
      childFlowId: child.id,
      buildInput: config.input as SubflowDef["buildInput"],
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
    return attachDef<SubflowStepOutput<FlowOutput<Child>>>(
      { kind: "subflow", id: "" },
      def,
    );
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
    subflow,
    match: match as Builder<Input>["match"],
    step: step as Builder<Input>["step"],
    include: include as Builder<Input>["include"],
    [BUILDER_BRAND]: true as const,
    __steps: chainSteps,
  } as Builder<Input> & BuilderInternal;

  return builder;
}

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

function assertSignalNameUniqueness(
  flowId: string,
  finalSteps: Record<string, Step<unknown>>,
): void {
  const owners = new Map<string, string>();

  for (const stepId of Object.keys(finalSteps)) {
    owners.set(stepId, `step id "${stepId}"`);
  }

  for (const [stepId, step] of Object.entries(finalSteps)) {
    const def = (step as { __def?: StepDef }).__def;
    if (def === undefined || def.kind !== "signal") continue;
    if (def.names === undefined) continue;

    for (const alias of def.names) {
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
