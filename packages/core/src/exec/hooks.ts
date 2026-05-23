import type { DispatchDeps } from "../dispatch";
import { serializeError } from "../errors";

export interface Hooks {
  fireHook<E>(
    hook: ((event: E) => void | Promise<void>) | undefined,
    event: E,
    hookName: string,
  ): Promise<void>;
  fireStepLifecycle<E>(
    stepHook: ((event: E) => void | Promise<void>) | undefined,
    flowHook: ((event: E) => void | Promise<void>) | undefined,
    names: readonly [step: string, flow: string],
    event: E,
  ): Promise<void>;
}

// The one place user callbacks run. Errors are swallowed (logged) so a throwing
// hook never fails a step or run.
export function makeHooks(deps: DispatchDeps): Hooks {
  async function fireHook<E>(
    hook: ((event: E) => void | Promise<void>) | undefined,
    event: E,
    hookName: string,
  ): Promise<void> {
    if (deps.fireHooks === false) return;
    if (hook === undefined) return;
    try {
      await hook(event);
    } catch (err) {
      const { message, stack } = serializeError(err);
      deps.emitLog({
        level: "error",
        msg: `nagi hook "${hookName}" threw — swallowed`,
        attrs: { error: message, ...(stack !== undefined ? { stack } : {}) },
      });
    }
  }

  async function fireStepLifecycle<E>(
    stepHook: ((event: E) => void | Promise<void>) | undefined,
    flowHook: ((event: E) => void | Promise<void>) | undefined,
    [stepName, flowName]: readonly [step: string, flow: string],
    event: E,
  ): Promise<void> {
    await fireHook(stepHook, event, stepName);
    await fireHook(flowHook, event, flowName);
  }

  return { fireHook, fireStepLifecycle };
}
