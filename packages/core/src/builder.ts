import { validationError } from "./errors";
import {
  attachDef,
  compact,
  type HandlerDef,
  type HandlerKind,
  type NeedRefDef,
  normalizeNeeds,
  peekDef,
  type SignalDef,
  type StepDef,
  type SubflowDef,
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

const MAX_TIMER_MS = 2_147_483_647;

// Node's setTimeout treats any delay above 2^31-1ms, below 1, or NaN as 1ms —
// an unrepresentable deadline would fire on every attempt instead of the one
// the caller configured. Reject at build time rather than at every runtime
// attempt.
function assertHandlerTimeout(ms: number | undefined): void {
  if (ms === undefined) return;
  if (!Number.isInteger(ms) || ms < 1 || ms > MAX_TIMER_MS) {
    throw validationError(
      `timeoutMs must be an integer between 1 and ${MAX_TIMER_MS} (got ${ms}). Omit it for no deadline.`,
      ["timeoutMs"],
    );
  }
}

function assertSignalTimeout(ms: number | "unbounded"): void {
  if (ms === "unbounded") return;
  if (!Number.isFinite(ms) || ms < 1) {
    throw validationError(
      `timeoutMs must be "unbounded" or a finite number >= 1 (got ${ms}).`,
      ["timeoutMs"],
    );
  }
}

function makeBuilder<Input>(): Builder<Input> {
  function handler<N extends NeedsMap, O>(
    kind: HandlerKind,
    config:
      | TaskConfig<Input, N, O>
      | ActivityConfig<Input, N, O>
      | StreamingTaskConfig<Input, N, O, unknown>,
  ): Step<O> {
    assertHandlerTimeout(config.timeoutMs);
    const def: HandlerDef = {
      kind,
      needs: normalizeNeeds(config.needs),
      run: config.run as HandlerDef["run"],
      ...compact({
        retry: config.retry,
        timeoutMs: config.timeoutMs,
        when: config.when as HandlerDef["when"],
        onStart: config.onStart,
        onComplete: config.onComplete as HandlerDef["onComplete"],
        onError: config.onError,
        onRetry: config.onRetry,
      }),
    };
    return attachDef<O>({ kind, id: "" }, def);
  }

  function signal<N extends NeedsMap, S extends StandardSchemaV1>(
    config: SignalConfig<Input, N, S>,
  ): Step<InferSchemaOutput<S>> {
    assertSignalTimeout(config.timeoutMs);
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

  return {
    task: (config) => handler("task", config),
    activity: (config) => handler("activity", config),
    streamingTask: (config) => handler("streaming", config),
    signal,
    subflow,
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

  const finalSteps = rewriteSteps(config.id, built);

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

function rewriteSteps(
  flowId: string,
  map: StepMap,
): Record<string, Step<unknown>> {
  const idByIdentity = new Map<Step<unknown>, string>();
  for (const [id, step] of Object.entries(map)) idByIdentity.set(step, id);

  const out: Record<string, Step<unknown>> = {};
  for (const [id, step] of Object.entries(map)) {
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

    const finalizedDef: StepDef = { ...def, needs: rewrittenNeeds };
    out[id] = attachDef({ kind: finalizedDef.kind, id }, finalizedDef);
  }
  return out;
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
