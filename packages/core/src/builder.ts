import {
  type ActivityDef,
  type ArmGuard,
  attachDef,
  compact,
  type Guard,
  type MatchArmDef,
  type MatchDef,
  type NeedRefDef,
  normalizeNeeds,
  type ParentMatchRef,
  type PendingMatchArm,
  type PendingMatchDef,
  peekDef,
  type SignalDef,
  type StepDef,
  type StreamingTaskDef,
  type SubflowDef,
  type TaskDef,
} from "./internal";
import type {
  ActivityConfig,
  Builder,
  Flow,
  FlowCompleteEvent,
  FlowConcurrency,
  FlowConfig,
  FlowOutput,
  InferSchemaOutput,
  Json,
  MatchArm,
  MatchGuardConfig,
  NeedsMap,
  Optional,
  ResolvedConcurrency,
  SignalConfig,
  StandardSchemaV1,
  Step,
  StepMap,
  StreamingTaskConfig,
  SubflowConfig,
  SubflowStepOutput,
  TaskConfig,
} from "./types";

function makeBuilder<Input>(): Builder<Input> {
  function task<N extends NeedsMap, O>(
    config: TaskConfig<Input, N, O>,
  ): Step<O> {
    const def: TaskDef = {
      kind: "task",
      needs: normalizeNeeds(config.needs),
      run: config.run as TaskDef["run"],
      ...compact({
        retry: config.retry,
        when: config.when as TaskDef["when"],
        onStart: config.onStart,
        onComplete: config.onComplete as TaskDef["onComplete"],
        onError: config.onError,
        onRetry: config.onRetry,
      }),
    };
    return attachDef<O>({ kind: "task", id: "" }, def);
  }

  function activity<N extends NeedsMap, O>(
    config: ActivityConfig<Input, N, O>,
  ): Step<O> {
    const def: ActivityDef = {
      kind: "activity",
      needs: normalizeNeeds(config.needs),
      run: config.run as ActivityDef["run"],
      ...compact({
        retry: config.retry,
        when: config.when as ActivityDef["when"],
        onStart: config.onStart,
        onComplete: config.onComplete as ActivityDef["onComplete"],
        onError: config.onError,
        onRetry: config.onRetry,
      }),
    };
    return attachDef<O>({ kind: "activity", id: "" }, def);
  }

  function streamingTask<N extends NeedsMap, O, C = Json>(
    config: StreamingTaskConfig<Input, N, O, C>,
  ): Step<O> {
    const def: StreamingTaskDef = {
      kind: "streaming",
      needs: normalizeNeeds(config.needs),
      run: config.run as StreamingTaskDef["run"],
      ...compact({
        retry: config.retry,
        when: config.when as StreamingTaskDef["when"],
        onStart: config.onStart,
        onComplete: config.onComplete as StreamingTaskDef["onComplete"],
        onError: config.onError,
        onRetry: config.onRetry,
      }),
    };
    return attachDef<O>({ kind: "streaming", id: "" }, def);
  }

  function signal<N extends NeedsMap, S extends StandardSchemaV1>(
    config: SignalConfig<Input, N, S>,
  ): Step<InferSchemaOutput<S>> {
    const def: SignalDef = {
      kind: "signal",
      needs: normalizeNeeds(config.needs),
      schema: config.schema,
      timeoutMs: config.timeoutMs,
      ...compact({
        names: config.names,
        when: config.when as SignalDef["when"],
      }),
    };
    return attachDef<InferSchemaOutput<S>>({ kind: "signal", id: "" }, def);
  }

  function subflow<N extends NeedsMap, Child extends Flow>(
    child: Child,
    config: SubflowConfig<Input, N, Child>,
  ): Step<SubflowStepOutput<FlowOutput<Child>>> {
    const def: SubflowDef = {
      kind: "subflow",
      needs: normalizeNeeds(config.needs),
      childFlowId: child.id,
      buildInput: config.input as SubflowDef["buildInput"],
      ...compact({
        when: config.when as SubflowDef["when"],
      }),
    };
    return attachDef<SubflowStepOutput<FlowOutput<Child>>>(
      { kind: "subflow", id: "" },
      def,
    );
  }

  function match(
    config: MatchGuardConfig<Input, NeedsMap, StepMap>,
  ): Step<unknown> {
    const needs = normalizeNeeds(config.needs);
    const arms: PendingMatchArm[] = [];
    for (let i = 0; i < config.arms.length; i++) {
      const arm = config.arms[i] as MatchArm<Input, NeedsMap, StepMap>;
      const nested = arm.build(makeBuilder<Input>()) as StepMap;
      const guard: ArmGuard = arm.otherwise
        ? { kind: "otherwise" }
        : { kind: "when", when: arm.when as Guard };
      const armId = guard.kind === "otherwise" ? "otherwise" : `arm${i}`;
      arms.push({ id: armId, guard, nested });
    }
    const def: PendingMatchDef = { kind: "match", needs, arms };
    return attachDef<unknown>({ kind: "match", id: "" }, def);
  }

  return {
    task,
    activity,
    streamingTask,
    signal,
    subflow,
    match: match as Builder<Input>["match"],
  };
}

export function flow<
  const Id extends string,
  InputSchema extends StandardSchemaV1,
  R extends StepMap,
  Output = unknown,
>(
  config: FlowConfig<Id, InputSchema, R, Output>,
): Flow<Id, InputSchema, R, Output> {
  const builder = makeBuilder<InferSchemaOutput<InputSchema>>();
  const built = config.build(builder);

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
    steps: finalSteps as R,
    ...compact({
      output: config.output,
      onStart: config.onStart,
      onComplete: config.onComplete as
        | ((event: FlowCompleteEvent) => void | Promise<void>)
        | undefined,
      onError: config.onError,
    }),
    ...(config.concurrency !== undefined
      ? { concurrency: normalizeConcurrency(config.concurrency) }
      : {}),
  };
}

export function optional<S extends Step>(step: S): Optional<S> {
  return { __optional: step };
}

function normalizeConcurrency<Input>(
  c: FlowConcurrency<Input>,
): ResolvedConcurrency {
  if (typeof c === "object") {
    return {
      keyFn: c.keyFn as (input: Json) => string,
      mode: c.mode ?? "cancel-in-progress",
    };
  }
  const key = c;
  return {
    keyFn: (input) =>
      (input as Record<string, unknown>)[key as string] as string,
    mode: "cancel-in-progress",
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
    const def = peekDef(step);
    if (def?.kind === "match") {
      const pending = def as PendingMatchDef;
      for (const arm of pending.arms) {
        collectIds(arm.nested, `${id}.${arm.id}`, idByIdentity);
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
    const def = peekDef(step);
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

    const rewrittenNeeds: Record<string, NeedRefDef> = {};
    for (const [localKey, ref] of Object.entries(def.needs)) {
      const upstream = ref.step;
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
      rewrittenNeeds[localKey] = {
        step: { ...upstream, id: upstreamId },
        optional: ref.optional,
      };
    }

    if (def.kind === "match") {
      const pending = def as PendingMatchDef;
      const finalizedArms: MatchArmDef[] = [];

      for (const arm of pending.arms) {
        const armPrefix = `${id}.${arm.id}`;
        const nestedStepIds: string[] = [];
        for (const nestedKey of Object.keys(arm.nested)) {
          nestedStepIds.push(`${armPrefix}.${nestedKey}`);
        }
        walkAndRewrite({
          flowId,
          map: arm.nested,
          prefix: armPrefix,
          parentMatch: { matchId: id, armId: arm.id },
          idByIdentity,
          out,
        });
        finalizedArms.push({
          id: arm.id,
          guard: arm.guard,
          stepIds: nestedStepIds,
        });
      }

      const finalizedDef: MatchDef = {
        kind: "match",
        needs: rewrittenNeeds,
        arms: finalizedArms,
        ...compact({ parentMatch }),
      };

      out[id] = attachDef({ kind: "match", id }, finalizedDef);
      continue;
    }

    const finalizedDef = {
      ...def,
      needs: rewrittenNeeds,
      ...compact({ parentMatch }),
    } as StepDef;
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
    const def = peekDef(step);
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
