import { describe, expect, it } from "vitest";
import { flow } from "./builder";
import { computeBackoff } from "./dispatch";
import { makeHarness, passthroughSchema } from "./test-helpers";
import type {
  FlowCompleteEvent,
  FlowErrorEvent,
  FlowStartEvent,
  Logger,
  RetryPolicy,
  StepErrorEvent,
  StepRetryEvent,
  StepStartEvent,
} from "./types";

describe("computeBackoff", () => {
  const exp: RetryPolicy = {
    maxAttempts: 99,
    backoff: "exponential",
    initialDelayMs: 100,
    maxDelayMs: 10_000,
  };
  const lin: RetryPolicy = {
    maxAttempts: 99,
    backoff: "linear",
    initialDelayMs: 100,
    maxDelayMs: 10_000,
  };
  const fix: RetryPolicy = {
    maxAttempts: 99,
    backoff: "fixed",
    initialDelayMs: 250,
  };

  it.each([
    ["exponential, attempt 1", exp, 1, 100],
    ["exponential, attempt 2", exp, 2, 200],
    ["exponential, attempt 4", exp, 4, 800],
    ["exponential, attempt 8 (capped)", { ...exp, maxDelayMs: 500 }, 8, 500],
    ["linear, attempt 1", lin, 1, 100],
    ["linear, attempt 5", lin, 5, 500],
    ["linear, attempt 200 (capped)", lin, 200, 10_000],
    ["fixed, any attempt", fix, 99, 250],
    ["fixed, capped by maxDelay", { ...fix, maxDelayMs: 100 }, 1, 100],
  ] as const)("%s → %d ms", (_label, policy, attempt, expected) => {
    expect(computeBackoff(policy, attempt)).toBe(expected);
  });
});

describe("dispatchMessage — driver", () => {
  it("happy path: completes a single task and finalizes the flow", async () => {
    const f = flow({
      id: "single",
      input: passthroughSchema<{ x: number }>(),
      build: (b) => ({
        only: b.task({ run: async ({ input }) => ({ result: input.x + 1 }) }),
      }),
    });
    const h = await makeHarness(f);

    const runId = await h.wf.start(f, { x: 41 });
    const processed = await h.drain();

    expect(processed).toBe(1);
    const result = await h.result(runId);
    expect(result.status).toBe("completed");
    expect(result.output("only")).toEqual({ result: 42 });
  });

  it("threads upstream output via needs", async () => {
    const f = flow({
      id: "linear-driver",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => {
        const a = b.task({ run: async () => ({ v: 10 }) });
        const c = b.task({
          needs: { a },
          run: async ({ needs }) => ({ doubled: needs.a.v * 2 }),
        });
        return { a, c };
      },
    });
    const h = await makeHarness(f);
    const runId = await h.wf.start(f, {});

    expect(await h.drainOnce()).toBe(1); // dispatches `a`
    expect((await h.result(runId)).stepStatus("a")).toBe("completed");

    expect(await h.drainOnce()).toBe(1); // dispatches `c`
    const result = await h.result(runId);
    expect(result.output("c")).toEqual({ doubled: 20 });
    expect(result.status).toBe("completed");
  });

  it("retries by re-enqueueing with attempt+1 and a delay", async () => {
    let attempts = 0;
    const f = flow({
      id: "retry-driver",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => ({
        flaky: b.task({
          retry: { maxAttempts: 3, backoff: "fixed", initialDelayMs: 0 },
          run: async () => {
            attempts++;
            if (attempts < 3) throw new Error("nope");
            return { ok: true };
          },
        }),
      }),
    });
    const h = await makeHarness(f);
    const runId = await h.wf.start(f, {});

    await h.drain(); // delay is 0, so all retries drain in one pass

    expect(attempts).toBe(3);
    const result = await h.result(runId);
    expect(result.stepStatus("flaky")).toBe("completed");
    expect(result.factCount("step.retried")).toBe(2);
    expect(result.factCount("step.completed")).toBe(1);
  });

  it("fails terminally after maxAttempts and finalizes flow as failed", async () => {
    const f = flow({
      id: "doomed-driver",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => ({
        doomed: b.task({
          retry: { maxAttempts: 2, backoff: "fixed", initialDelayMs: 0 },
          run: async () => {
            throw new Error("permanent");
          },
        }),
      }),
    });
    const h = await makeHarness(f);
    const runId = await h.wf.start(f, {});

    await h.drain();

    const result = await h.result(runId);
    expect(result.status).toBe("failed");
    expect(result.stepStatus("doomed")).toBe("failed");
    expect(result.error("doomed").message).toBe("permanent");
    expect(result.factCount("flow.failed")).toBe(1);
  });

  it("signal step: marks running and ack; does not invoke a handler", async () => {
    let ran = false;
    const f = flow({
      id: "signal-driver",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => {
        const wait = b.signal({ schema: passthroughSchema<{ ok: boolean }>() });
        const after = b.task({
          needs: { wait },
          run: async () => {
            ran = true;
            return null;
          },
        });
        return { wait, after };
      },
    });
    const h = await makeHarness(f);
    const runId = await h.wf.start(f, {});

    expect(await h.drainOnce()).toBe(1); // dispatches the signal step
    const mid = await h.result(runId);
    expect(mid.stepStatus("wait")).toBe("running");
    expect(ran).toBe(false);

    await h.wf.signal(runId, "wait", { ok: true });
    await h.drain(); // dispatches `after`

    const final = await h.result(runId);
    expect(final.output("wait")).toEqual({ ok: true });
    expect(final.stepStatus("after")).toBe("completed");
    expect(ran).toBe(true);
  });

  it("flow output: explicit `output` fn is computed and persisted on flow.completed", async () => {
    const f = flow({
      id: "with-output",
      input: passthroughSchema<{ x: number }>(),
      build: (b) => {
        const a = b.task({
          run: async ({ input }) => ({ doubled: input.x * 2 }),
        });
        const c = b.task({
          needs: { a },
          run: async ({ needs }) => ({ tripled: needs.a.doubled * 3 }),
        });
        return { a, c };
      },
      output: ({ a, c }) => ({ start: a.doubled / 2, end: c.tripled }),
    });
    const h = await makeHarness(f);
    const runId = await h.wf.start(f, { x: 5 });
    await h.drain();

    const result = await h.result(runId);
    expect(result.status).toBe("completed");
    const completedFacts = result.factsOf("flow.completed");
    expect(completedFacts).toHaveLength(1);
    expect(completedFacts[0]?.output).toEqual({ start: 5, end: 30 });
  });

  it("flow output: defaults to null when no `output` fn is given", async () => {
    const f = flow({
      id: "no-output",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => ({ only: b.task({ run: async () => ({ v: 1 }) }) }),
    });
    const h = await makeHarness(f);
    const runId = await h.wf.start(f, {});
    await h.drain();

    const result = await h.result(runId);
    const completedFacts = result.factsOf("flow.completed");
    expect(completedFacts[0]?.output).toBeNull();
  });

  it("onStepStart hook receives the flow input for a task step", async () => {
    const events: StepStartEvent[] = [];
    const f = flow({
      id: "hook-task-input",
      input: passthroughSchema<{ prompt: string; n: number }>(),
      build: (b) => ({
        only: b.task({ run: async ({ input }) => ({ echoed: input.prompt }) }),
      }),
    });
    const h = await makeHarness(f, {
      hooks: {
        onStepStart: (e) => {
          events.push(e);
        },
      },
    });
    await h.wf.start(f, { prompt: "hello", n: 7 });
    await h.drain();

    expect(events).toHaveLength(1);
    const [start] = events;
    expect(start?.stepId).toBe("only");
    expect(start?.kind).toBe("task");
    expect(start?.input).toEqual({ prompt: "hello", n: 7 });
  });

  it("onStepStart hook passes null for signal and match steps", async () => {
    const events: StepStartEvent[] = [];
    const f = flow({
      id: "hook-signal-null",
      input: passthroughSchema<{ tag: string }>(),
      build: (b) => {
        const wait = b.signal({ schema: passthroughSchema<{ ok: boolean }>() });
        return { wait };
      },
    });
    const h = await makeHarness(f, {
      hooks: {
        onStepStart: (e) => {
          events.push(e);
        },
      },
    });
    const runId = await h.wf.start(f, { tag: "x" });
    await h.drain();

    const signalEvent = events.find((e) => e.stepId === "wait");
    expect(signalEvent).toBeDefined();
    expect(signalEvent?.kind).toBe("signal");
    expect(signalEvent?.input).toBeNull();

    // sanity: signal completes after external payload arrives
    await h.wf.signal(runId, "wait", { ok: true });
    await h.drain();
  });

  it("is idempotent against duplicate dispatch of a completed step", async () => {
    let runs = 0;
    const f = flow({
      id: "idempotent-driver",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => ({
        only: b.task({
          run: async () => {
            runs++;
            return { runs };
          },
        }),
      }),
    });
    const h = await makeHarness(f);
    const runId = await h.wf.start(f, {});

    await h.drain();
    expect(runs).toBe(1);

    // Manually re-enqueue the same step. Dispatcher should detect the
    // terminal state and ack without re-running.
    await h.queue.enqueue(runId, "only");
    await h.drain();

    expect(runs).toBe(1);
  });
});

describe("step-local lifecycle hooks", () => {
  it("fires onComplete on the step config with the typed output", async () => {
    const seen: Array<{ output: { wordCount: number; lang: string } }> = [];
    const f = flow({
      id: "step-oncomplete",
      input: passthroughSchema<{ text: string }>(),
      build: (b) => ({
        transcribe: b.task({
          run: async ({ input }) => ({
            wordCount: input.text.split(" ").length,
            lang: "en" as const,
          }),
          onComplete: (event) => {
            seen.push({ output: event.output });
          },
        }),
      }),
    });
    const h = await makeHarness(f);
    await h.wf.start(f, { text: "hello world from nagi" });
    await h.drain();

    expect(seen).toEqual([{ output: { wordCount: 4, lang: "en" } }]);
  });

  it("fires step.onComplete before the top-level onStepComplete", async () => {
    const order: string[] = [];
    const f = flow({
      id: "ordering",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => ({
        only: b.task({
          run: async () => ({ ok: true }),
          onComplete: () => {
            order.push("step-local");
          },
        }),
      }),
    });
    const h = await makeHarness(f, {
      hooks: {
        onStepComplete: () => {
          order.push("top-level");
        },
      },
    });
    await h.wf.start(f, {});
    await h.drain();

    expect(order).toEqual(["step-local", "top-level"]);
  });

  it("fires onError on terminal failure and onRetry on each retry attempt", async () => {
    let attempts = 0;
    const errors: StepErrorEvent[] = [];
    const retries: StepRetryEvent[] = [];
    const f = flow({
      id: "step-error-and-retry",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => ({
        doomed: b.task({
          retry: { maxAttempts: 3, backoff: "fixed", initialDelayMs: 0 },
          run: async () => {
            attempts++;
            throw new Error(`boom-${attempts}`);
          },
          onError: (event) => {
            errors.push(event);
          },
          onRetry: (event) => {
            retries.push(event);
          },
        }),
      }),
    });
    const h = await makeHarness(f);
    await h.wf.start(f, {});
    await h.drain();

    expect(attempts).toBe(3);
    expect(retries).toHaveLength(2);
    expect(retries[0]?.attempt).toBe(1);
    expect(retries[1]?.attempt).toBe(2);
    expect(errors).toHaveLength(1);
    expect(errors[0]?.error.message).toBe("boom-3");
  });

  it("fires step.onStart with the resolved input alongside top-level", async () => {
    const starts: StepStartEvent[] = [];
    const f = flow({
      id: "step-onstart",
      input: passthroughSchema<{ tag: string }>(),
      build: (b) => ({
        only: b.task({
          run: async ({ input }) => ({ echoed: input.tag }),
          onStart: (event) => {
            starts.push(event);
          },
        }),
      }),
    });
    const h = await makeHarness(f);
    await h.wf.start(f, { tag: "hello" });
    await h.drain();

    expect(starts).toHaveLength(1);
    expect(starts[0]?.input).toEqual({ tag: "hello" });
  });

  it("swallows a thrown step-local hook and continues the run", async () => {
    const logs: string[] = [];
    const logger: Logger = {
      debug: () => {},
      info: () => {},
      warn: () => {},
      error: (m) => logs.push(m),
    };
    const f = flow({
      id: "hook-throws",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => ({
        only: b.task({
          run: async () => ({ ok: true }),
          onComplete: () => {
            throw new Error("operator bug");
          },
        }),
      }),
    });
    const h = await makeHarness(f, { logger });
    const runId = await h.wf.start(f, {});
    await h.drain();

    const result = await h.result(runId);
    expect(result.status).toBe("completed");
    expect(result.output("only")).toEqual({ ok: true });
    expect(logs.some((m) => m.includes("step.onComplete"))).toBe(true);
  });
});

describe("flow-level lifecycle hooks", () => {
  it("fires onStart / onComplete on flow config with typed output", async () => {
    const events: Array<
      | { kind: "start"; input: { x: number } }
      | { kind: "complete"; output: { total: number } }
    > = [];
    const f = flow({
      id: "flow-hooks",
      input: passthroughSchema<{ x: number }>(),
      build: (b) => ({
        a: b.task({ run: async ({ input }) => ({ v: input.x }) }),
      }),
      output: (steps) => ({ total: steps.a.v * 2 }),
      onStart: (event) => {
        events.push({ kind: "start", input: event.input as { x: number } });
      },
      onComplete: (event) => {
        events.push({ kind: "complete", output: event.output });
      },
    });
    const h = await makeHarness(f);
    await h.wf.start(f, { x: 7 });
    await h.drain();

    expect(events).toEqual([
      { kind: "start", input: { x: 7 } },
      { kind: "complete", output: { total: 14 } },
    ]);
  });

  it("fires flow.onError when a step fails terminally", async () => {
    const errors: FlowErrorEvent[] = [];
    const f = flow({
      id: "flow-onerror",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => ({
        doomed: b.task({
          retry: { maxAttempts: 1, backoff: "fixed", initialDelayMs: 0 },
          run: async () => {
            throw new Error("kaput");
          },
        }),
      }),
      onError: (event) => {
        errors.push(event);
      },
    });
    const h = await makeHarness(f);
    await h.wf.start(f, {});
    await h.drain();

    expect(errors).toHaveLength(1);
    expect(errors[0]?.error.message).toBe("kaput");
  });

  it("fires flow.onStart before top-level onFlowStart", async () => {
    const order: string[] = [];
    const starts: FlowStartEvent[] = [];
    const f = flow({
      id: "flow-onstart-order",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => ({ a: b.task({ run: async () => ({}) }) }),
      onStart: (event) => {
        order.push("flow-local");
        starts.push(event);
      },
    });
    const h = await makeHarness(f, {
      hooks: {
        onFlowStart: () => {
          order.push("top-level");
        },
      },
    });
    await h.wf.start(f, {});
    await h.drain();

    expect(order).toEqual(["flow-local", "top-level"]);
    expect(starts).toHaveLength(1);
  });

  it("swallows a thrown flow-local hook and finalizes the run", async () => {
    const logs: string[] = [];
    const logger: Logger = {
      debug: () => {},
      info: () => {},
      warn: () => {},
      error: (m) => logs.push(m),
    };
    const completes: FlowCompleteEvent[] = [];
    const f = flow({
      id: "flow-onComplete-throws",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => ({ a: b.task({ run: async () => ({ v: 1 }) }) }),
      onComplete: () => {
        throw new Error("flow hook bug");
      },
    });
    const h = await makeHarness(f, {
      logger,
      hooks: {
        onFlowComplete: (event) => {
          completes.push(event);
        },
      },
    });
    const runId = await h.wf.start(f, {});
    await h.drain();

    const result = await h.result(runId);
    expect(result.status).toBe("completed");
    expect(logs.some((m) => m.includes("flow.onComplete"))).toBe(true);
    expect(completes).toHaveLength(1);
  });
});
