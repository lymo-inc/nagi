import { afterEach, describe, expect, it, vi } from "vitest";
import { flow } from "../builder";
import { InMemoryQueue, InMemoryStore } from "../memory";
import { nagi } from "../runtime";
import { stepStateOf, stepStatusOf } from "../state";
import type { LogEntry, QueueDequeueOpts, QueueMessage, RunId } from "../types";
import { makeHarness, passthroughSchema, spyOnLog } from "./test-helpers";

// A queue whose dequeue always throws — the one path that rejects worker.run()
// (per-message errors are swallowed). Ported from runtime-run.test.ts; models a
// lost DB connection so we can exercise the "worker exited unexpectedly" site.
class CrashingQueue extends InMemoryQueue {
  override async dequeue(
    _opts: QueueDequeueOpts,
  ): Promise<readonly QueueMessage[]> {
    throw new Error("queue connection lost");
  }
}

const echo = flow({
  id: "echo",
  input: passthroughSchema<{ x: number }>(),
  build: (b) => ({
    step: b.task({ run: async ({ input }) => ({ y: input.x }) }),
  }),
});

// ---------------------------------------------------------------------------
// Record shape & level routing (anchored to the real diagnostic sites)
// ---------------------------------------------------------------------------

describe("onLog — record shape & level routing", () => {
  it("cancel-skipped: one info entry with the exact msg and attrs", async () => {
    const { onLog, entries } = spyOnLog();
    const h = await makeHarness(echo, { onLog });
    const runId = await h.wf.start(echo, { x: 1 });
    await h.drain();
    // Run is completed (terminal); cancelling now hits the skip path.
    await h.wf.cancel(runId, { reason: "after-terminal" });

    const skipped = entries.filter(
      (e) => e.msg === "nagi: cancel skipped — run already terminal",
    );
    expect(skipped).toHaveLength(1);
    const entry = skipped[0] as LogEntry;
    expect(entry.level).toBe("info");
    expect(entry.attrs).toMatchObject({ runId, status: "completed" });
  });

  it("worker-exited-unexpectedly: exactly one error entry with the exact msg", async () => {
    const { onLog, entries } = spyOnLog();
    const handle = await nagi.run({
      flows: [echo],
      store: new InMemoryStore(),
      queue: new CrashingQueue(),
      worker: { pollIntervalMs: 5 },
      onLog,
    });
    await vi.waitFor(() =>
      expect(entries.filter((e) => e.level === "error")).toHaveLength(1),
    );
    const crash = entries.find((e) => e.level === "error") as LogEntry;
    expect(crash.msg).toBe("nagi.run: worker exited unexpectedly");
    expect(crash.attrs).toMatchObject({
      error: expect.stringContaining("queue connection lost"),
    });
    await handle.stop();
  });

  it("hook-threw: one error entry naming the hook, with serialized error attrs", async () => {
    const { onLog, entries } = spyOnLog();
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
    const h = await makeHarness(f, { onLog });
    const runId = await h.wf.start(f, {});
    await h.drain();

    expect((await h.result(runId)).status).toBe("completed");
    const hookThrew = entries.filter(
      (e) => e.msg === 'nagi hook "step.onComplete" threw — swallowed',
    );
    expect(hookThrew).toHaveLength(1);
    const entry = hookThrew[0] as LogEntry;
    expect(entry.level).toBe("error");
    expect(entry.attrs).toMatchObject({ error: "operator bug" });
  });

  it("not-in-flow: one warn entry with the exact msg", async () => {
    const { onLog, entries } = spyOnLog();
    const h = await makeHarness(echo, { onLog });
    const runId = await h.wf.start(echo, { x: 1 });
    // Enqueue a message for a step id that does not exist in the flow.
    await h.queue.enqueue(runId, "ghost-step");
    await h.drain();

    const warns = entries.filter((e) => e.level === "warn");
    expect(warns).toHaveLength(1);
    expect(warns[0]?.msg).toBe(
      `dispatch: step "ghost-step" not in flow "echo"; ack and skip`,
    );
  });

  it("signal-after-resolved: one info entry once the step has completed", async () => {
    const { onLog, entries } = spyOnLog();
    const f = flow({
      id: "sig-resolved",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => ({
        wait: b.signal({ schema: passthroughSchema<{ ok: boolean }>() }),
      }),
    });
    const h = await makeHarness(f, { onLog });
    const w = h.startWorker();
    try {
      const runId = await h.wf.start(f, {});
      await h.waitForStep(runId, "wait", "running");
      await h.wf.signal(runId, "wait", { ok: true });
      await h.waitForEnd(runId);
      // Second signal arrives after the step has resolved → the noop diagnostic.
      await h.wf.signal(runId, "wait", { ok: false });

      const late = entries.filter(
        (e) => e.msg === "nagi: signal arrived after step resolved",
      );
      expect(late).toHaveLength(1);
      const entry = late[0] as LogEntry;
      expect(entry.level).toBe("info");
      expect(entry.attrs).toMatchObject({
        runId,
        stepId: "wait",
        signalName: "wait",
      });
    } finally {
      await w.stop();
    }
  });

  it("operator.skip-noop: one info entry when the target step is already terminal", async () => {
    const { onLog, entries } = spyOnLog();
    const f = flow({
      id: "skip-noop",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => ({
        a: b.task({ run: async () => ({ ok: true }) }),
      }),
    });
    const h = await makeHarness(f, { onLog });
    const runId = await h.wf.start(f, {});
    await h.drain();
    // Step "a" is already completed; skipping it is a noop.
    await h.wf.operator().skip(runId, "a", { actor: "tester" });

    const noop = entries.filter(
      (e) => e.msg === "nagi: operator.skip noop — step already terminal",
    );
    expect(noop).toHaveLength(1);
    const entry = noop[0] as LogEntry;
    expect(entry.level).toBe("info");
    expect(entry.attrs).toMatchObject({
      runId,
      stepId: "a",
      status: "completed",
    });
  });

  it("subflow-wake (parent run terminal): info entry when child finishes after parent canceled", async () => {
    const { onLog, entries } = spyOnLog();
    const child = flow({
      id: "wake-child",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => ({
        gate: b.signal({ schema: passthroughSchema<{ ok: boolean }>() }),
      }),
      output: (steps) => steps.gate,
    });
    const parent = flow({
      id: "wake-parent",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => ({
        sub: b.subflow(child, { input: () => ({}) }),
      }),
    });
    const h = await makeHarness([parent, child], { onLog });
    const parentRunId = await h.wf.start(parent, {});
    await h.drain();
    // The subflow step is now running and the child is waiting on its signal.
    const childRunId = (await h.store.listChildren(parentRunId))[0] as RunId;
    expect(childRunId).toBeDefined();
    expect(
      stepStatusOf(stepStateOf(await h.store.loadRunState(parentRunId), "sub")),
    ).toBe("running");

    // Cancel the PARENT while the child is still in flight.
    await h.wf.cancel(parentRunId, { reason: "kill-parent" });
    // Now resolve the child's signal so it completes and tries to wake the parent.
    await h.wf.signal(childRunId, "gate", { ok: true });
    await h.drain();

    const wakeSkipped = entries.filter(
      (e) =>
        e.msg === "nagi: subflow wake skipped — parent run already terminal",
    );
    expect(wakeSkipped).toHaveLength(1);
    const entry = wakeSkipped[0] as LogEntry;
    expect(entry.level).toBe("info");
    expect(entry.attrs).toMatchObject({
      parentRunId,
      parentStepId: "sub",
      childRunId,
      parentStatus: "canceled",
    });
  });

  it("subflow-wake (parent step not running): info entry while the parent run stays live", async () => {
    const { onLog, entries } = spyOnLog();
    const child = flow({
      id: "wake2-child",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => ({
        gate: b.signal({ schema: passthroughSchema<{ ok: boolean }>() }),
      }),
      output: (steps) => steps.gate,
    });
    // Two independent steps: skipping `sub` leaves the run alive because the
    // `keepalive` signal step is still running — so the child wake sees the run
    // live but the parent step no longer "running".
    const parent = flow({
      id: "wake2-parent",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => ({
        sub: b.subflow(child, { input: () => ({}) }),
        keepalive: b.signal({ schema: passthroughSchema<{ done: boolean }>() }),
      }),
    });
    const h = await makeHarness([parent, child], { onLog });
    const parentRunId = await h.wf.start(parent, {});
    await h.drain();
    const childRunId = (await h.store.listChildren(parentRunId))[0] as RunId;

    // Skip the subflow step: it becomes "skipped" (not running), but the run
    // stays live because `keepalive` is still waiting on its signal.
    await h.wf.operator().skip(parentRunId, "sub", { actor: "tester" });
    const parentState = await h.store.loadRunState(parentRunId);
    expect(parentState.phase.tag).toBe("running");
    expect(stepStatusOf(stepStateOf(parentState, "sub"))).toBe("skipped");

    // Drive the child to completion via its own signal, then let it try to wake
    // the parent — whose `sub` step is no longer running.
    await h.wf.signal(childRunId, "gate", { ok: true });
    await h.drain();

    const wakeSkipped = entries.filter(
      (e) =>
        e.msg === "nagi: subflow wake skipped — parent step not awaiting child",
    );
    expect(wakeSkipped).toHaveLength(1);
    const entry = wakeSkipped[0] as LogEntry;
    expect(entry.level).toBe("info");
    expect(entry.attrs).toMatchObject({
      parentRunId,
      parentStepId: "sub",
      childRunId,
      parentStepStatus: "skipped",
    });
  });

  // worker.dispatch threw uncaught (site #9): driven via a run that references an
  // unregistered flow id, so dispatchMessage's flowFor() throws *before* the
  // internal try/catch — surfacing through the worker's dispatchSafely catch as
  // the worker-level "threw uncaught" error.
  it("worker.dispatch threw uncaught: error entry when dispatch throws before its catch", async () => {
    const { onLog, entries } = spyOnLog();
    const h = await makeHarness(echo, { onLog });
    // Seed a flow.started fact for an unregistered flow on a fresh run id, then
    // enqueue a step against it. flowFor() will fail to resolve "not-registered".
    const ghostRunId = "run-ghost-unregistered" as RunId;
    await h.store.appendFact(ghostRunId, {
      kind: "flow.started",
      runId: ghostRunId,
      flowId: "not-registered-anywhere",
      input: null,
      at: new Date(),
    });
    await h.queue.enqueue(ghostRunId, "whatever");

    // runOnce({ maxSteps: 1 }) routes a single message through dispatchSafely
    // (the worker-level catch) and returns — so the throw surfaces exactly once
    // without the polling loop re-nacking and re-dispatching it forever.
    const result = await h.wf.worker().runOnce({ maxSteps: 1 });
    expect(result.processed).toBe(1);

    const uncaught = entries.filter(
      (e) => e.msg === "worker.dispatch threw uncaught",
    );
    expect(uncaught).toHaveLength(1);
    const entry = uncaught[0] as LogEntry;
    expect(entry.level).toBe("error");
    expect(entry.attrs).toMatchObject({
      error: expect.stringContaining("not-registered-anywhere"),
    });
  });
});

// ---------------------------------------------------------------------------
// attrs contract (passthrough / D4 undefined / conditional stack)
// ---------------------------------------------------------------------------

describe("onLog — attrs contract", () => {
  it("passthrough: a diagnostic that carries attrs delivers them", async () => {
    const { onLog, entries } = spyOnLog();
    const h = await makeHarness(echo, { onLog });
    const runId = await h.wf.start(echo, { x: 1 });
    await h.drain();
    await h.wf.cancel(runId, { reason: "x" });

    const entry = entries.find(
      (e) => e.msg === "nagi: cancel skipped — run already terminal",
    ) as LogEntry;
    expect(entry.attrs).toBeDefined();
    expect(entry.attrs).toEqual({ runId, status: "completed" });
  });

  it("D4: the not-in-flow warn has attrs === undefined (never coerced to {})", async () => {
    const { onLog, entries } = spyOnLog();
    const h = await makeHarness(echo, { onLog });
    const runId = await h.wf.start(echo, { x: 1 });
    await h.queue.enqueue(runId, "ghost-step");
    await h.drain();

    const warn = entries.find((e) => e.level === "warn") as LogEntry;
    expect(warn.attrs).toBeUndefined();
    expect("attrs" in warn).toBe(false);
  });

  it("conditional stack: a hook throwing an Error includes a string stack", async () => {
    const { onLog, entries } = spyOnLog();
    const f = flow({
      id: "hook-stack",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => ({
        only: b.task({
          run: async () => ({ ok: true }),
          onComplete: () => {
            throw new Error("with stack");
          },
        }),
      }),
    });
    const h = await makeHarness(f, { onLog });
    await h.wf.start(f, {});
    await h.drain();

    const entry = entries.find((e) =>
      e.msg.includes("step.onComplete"),
    ) as LogEntry;
    expect(entry.attrs).toMatchObject({ error: "with stack" });
    expect(typeof entry.attrs?.["stack"]).toBe("string");
  });

  it("conditional stack: a hook throwing a non-Error omits the stack key", async () => {
    const { onLog, entries } = spyOnLog();
    const f = flow({
      id: "hook-no-stack",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => ({
        only: b.task({
          run: async () => ({ ok: true }),
          onComplete: () => {
            // Throw a non-Error value: serializeError yields no `stack` key.
            throw "just a string";
          },
        }),
      }),
    });
    const h = await makeHarness(f, { onLog });
    await h.wf.start(f, {});
    await h.drain();

    const entry = entries.find((e) =>
      e.msg.includes("step.onComplete"),
    ) as LogEntry;
    expect(entry.attrs).toBeDefined();
    expect(entry.attrs).not.toHaveProperty("stack");
    expect("stack" in (entry.attrs as object)).toBe(false);
  });
});

// ---------------------------------------------------------------------------
// Silent by default (D3) — the consoleLogger-removal regression suite
// ---------------------------------------------------------------------------

describe("onLog — silent by default (D3)", () => {
  function spyConsole() {
    const debug = vi.spyOn(console, "debug").mockImplementation(() => {});
    const info = vi.spyOn(console, "info").mockImplementation(() => {});
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    const error = vi.spyOn(console, "error").mockImplementation(() => {});
    const log = vi.spyOn(console, "log").mockImplementation(() => {});
    return {
      assertSilent() {
        expect(debug).not.toHaveBeenCalled();
        expect(info).not.toHaveBeenCalled();
        expect(warn).not.toHaveBeenCalled();
        expect(error).not.toHaveBeenCalled();
        expect(log).not.toHaveBeenCalled();
      },
    };
  }

  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("a diagnostic path with no onLog does not throw", async () => {
    const h = await makeHarness(echo); // no onLog
    const runId = await h.wf.start(echo, { x: 1 });
    await h.drain();
    // cancel-skipped diagnostic path — must be a silent no-op, never throwing.
    await expect(h.wf.cancel(runId, { reason: "x" })).resolves.toBeUndefined();
  });

  it("no onLog: a diagnostic never touches console.*", async () => {
    const console = spyConsole();
    const h = await makeHarness(echo); // no onLog
    const runId = await h.wf.start(echo, { x: 1 });
    await h.drain();
    await h.wf.cancel(runId, { reason: "x" }); // cancel-skipped path
    await h.queue.enqueue(runId, "ghost"); // not-in-flow warn path
    await h.drain();
    console.assertSilent();
  });

  it("no onLog: a normal run still completes", async () => {
    const h = await makeHarness(echo); // no onLog
    const runId = await h.wf.start(echo, { x: 42 });
    await h.drain();
    const result = await h.result(runId);
    expect(result.status).toBe("completed");
    expect(result.output("step")).toEqual({ y: 42 });
  });

  it("no onLog: a handler calling ctx.logger.info produces NO console output", async () => {
    const console = spyConsole();
    const f = flow({
      id: "handler-logs-silent",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => ({
        a: b.task({
          run: async ({ ctx }) => {
            // This is the regression the consoleLogger removal fixes: with no
            // onLog this used to write to console.* — it must now be silent.
            ctx.logger.info("inside handler", { detail: 1 });
            ctx.logger.error("also error", { detail: 2 });
            return { ok: true };
          },
        }),
      }),
    });
    const h = await makeHarness(f); // no onLog
    const runId = await h.wf.start(f, {});
    await h.drain();
    expect((await h.result(runId)).status).toBe("completed");
    console.assertSilent();
  });
});

// ---------------------------------------------------------------------------
// Single channel — onLog is the only sink (no hidden console writes)
// ---------------------------------------------------------------------------

describe("onLog — single channel", () => {
  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("with onLog provided, a diagnostic does NOT also hit console.*", async () => {
    const debug = vi.spyOn(console, "debug").mockImplementation(() => {});
    const info = vi.spyOn(console, "info").mockImplementation(() => {});
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    const error = vi.spyOn(console, "error").mockImplementation(() => {});
    const log = vi.spyOn(console, "log").mockImplementation(() => {});

    const { onLog, entries } = spyOnLog();
    const h = await makeHarness(echo, { onLog });
    const runId = await h.wf.start(echo, { x: 1 });
    await h.drain();
    await h.wf.cancel(runId, { reason: "x" });
    await h.queue.enqueue(runId, "ghost");
    await h.drain();

    expect(entries.length).toBeGreaterThan(0); // sink received them
    expect(debug).not.toHaveBeenCalled();
    expect(info).not.toHaveBeenCalled();
    expect(warn).not.toHaveBeenCalled();
    expect(error).not.toHaveBeenCalled();
    expect(log).not.toHaveBeenCalled();
  });

  it("an in-step ctx.logger call routes to onLog and not to console.*", async () => {
    const info = vi.spyOn(console, "info").mockImplementation(() => {});
    const error = vi.spyOn(console, "error").mockImplementation(() => {});

    const { onLog, entries } = spyOnLog();
    const f = flow({
      id: "ctx-single-channel",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => ({
        a: b.task({
          run: async ({ ctx }) => {
            ctx.logger.info("hello");
            return { ok: true };
          },
        }),
      }),
    });
    const h = await makeHarness(f, { onLog });
    await h.wf.start(f, {});
    await h.drain();

    expect(entries.some((e) => e.msg === "hello")).toBe(true);
    expect(info).not.toHaveBeenCalled();
    expect(error).not.toHaveBeenCalled();
  });
});

// ---------------------------------------------------------------------------
// Exactly-once / ordering
// ---------------------------------------------------------------------------

describe("onLog — exactly-once & ordering", () => {
  it("one diagnostic ⇒ onLog called exactly once", async () => {
    const { onLog, entries } = spyOnLog();
    const h = await makeHarness(echo, { onLog });
    const runId = await h.wf.start(echo, { x: 1 });
    await h.drain();
    await h.wf.cancel(runId, { reason: "x" });

    expect(
      entries.filter(
        (e) => e.msg === "nagi: cancel skipped — run already terminal",
      ),
    ).toHaveLength(1);
  });

  it("a fully successful run emits zero error/warn entries", async () => {
    const { onLog, entries } = spyOnLog();
    const h = await makeHarness(echo, { onLog });
    const runId = await h.wf.start(echo, { x: 7 });
    await h.drain();

    expect((await h.result(runId)).status).toBe("completed");
    expect(entries.filter((e) => e.level === "error")).toHaveLength(0);
    expect(entries.filter((e) => e.level === "warn")).toHaveLength(0);
  });

  it("a worker crash yields exactly one error entry", async () => {
    const { onLog, entries } = spyOnLog();
    const handle = await nagi.run({
      flows: [echo],
      store: new InMemoryStore(),
      queue: new CrashingQueue(),
      worker: { pollIntervalMs: 5 },
      onLog,
    });
    await vi.waitFor(() =>
      expect(entries.filter((e) => e.level === "error")).toHaveLength(1),
    );
    // Give the loop a couple more poll cycles to prove it stays at exactly one.
    await new Promise((r) => setTimeout(r, 30));
    expect(entries.filter((e) => e.level === "error")).toHaveLength(1);
    await handle.stop();
  });

  it("two distinct diagnostics arrive in emission order", async () => {
    const { onLog, entries } = spyOnLog();
    const h = await makeHarness(echo, { onLog });
    const runId = await h.wf.start(echo, { x: 1 });
    await h.drain();
    // 1) not-in-flow warn, then 2) cancel-skipped info.
    await h.queue.enqueue(runId, "ghost");
    await h.drain();
    await h.wf.cancel(runId, { reason: "x" });

    const ordered = entries.filter(
      (e) => e.msg.includes("not in flow") || e.msg.includes("cancel skipped"),
    );
    expect(ordered.map((e) => e.level)).toEqual(["warn", "info"]);
  });
});

// ---------------------------------------------------------------------------
// Throw isolation (O4) — a throwing sink cannot corrupt the engine
// ---------------------------------------------------------------------------

describe("onLog — throw isolation (O4)", () => {
  it("an always-throwing onLog still lets the run reach completion", async () => {
    const onLog = () => {
      throw new Error("sink exploded");
    };
    const f = flow({
      id: "throwing-sink-completes",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => ({
        a: b.task({
          run: async ({ ctx }) => {
            ctx.logger.info("emit one"); // forces the sink to throw mid-step
            return { ok: true };
          },
        }),
      }),
    });
    const h = await makeHarness(f, { onLog });
    const runId = await h.wf.start(f, {});
    await h.drain();
    const result = await h.result(runId);
    expect(result.status).toBe("completed");
    expect(result.output("a")).toEqual({ ok: true });
  });

  it("a throwing sink does not abort/fail the in-flight step", async () => {
    const onLog = () => {
      throw new Error("sink exploded");
    };
    const f = flow({
      id: "throwing-sink-step-ok",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => ({
        a: b.task({
          run: async ({ ctx }) => {
            ctx.logger.warn("noisy");
            return { value: 99 };
          },
        }),
      }),
    });
    const h = await makeHarness(f, { onLog });
    const runId = await h.wf.start(f, {});
    await h.drain();
    const result = await h.result(runId);
    expect(result.stepStatus("a")).toBe("completed");
    expect(result.output("a")).toEqual({ value: 99 });
  });

  it("a throw on one emission does not stop a later emission from being attempted", async () => {
    let calls = 0;
    const seen: string[] = [];
    // Throws on the first emission, records every subsequent one.
    const onLog = (entry: LogEntry) => {
      calls++;
      if (calls === 1) throw new Error("first emission boom");
      seen.push(entry.msg);
    };
    const f = flow({
      id: "throw-once-then-record",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => ({
        a: b.task({
          run: async ({ ctx }) => {
            ctx.logger.info("first");
            ctx.logger.info("second");
            ctx.logger.info("third");
            return { ok: true };
          },
        }),
      }),
    });
    const h = await makeHarness(f, { onLog });
    const runId = await h.wf.start(f, {});
    await h.drain();

    expect((await h.result(runId)).status).toBe("completed");
    expect(calls).toBeGreaterThanOrEqual(3);
    // The throw on call #1 did not suppress later emissions.
    expect(seen).toContain("second");
    expect(seen).toContain("third");
  });
});

// ---------------------------------------------------------------------------
// In-step ctx.logger (O1 method shape + O2 enrichment)
// ---------------------------------------------------------------------------

describe("ctx.logger — in-step surface (O1 + O2)", () => {
  it("merges caller attrs AND runId/stepId/attempt into one info entry", async () => {
    const { onLog, entries } = spyOnLog();
    const f = flow({
      id: "enrich-merge",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => ({
        a: b.task({
          run: async ({ ctx }) => {
            ctx.logger.info("merged", { k: "v" });
            return { ok: true };
          },
        }),
      }),
    });
    const h = await makeHarness(f, { onLog });
    const runId = await h.wf.start(f, {});
    await h.drain();

    const merged = entries.filter((e) => e.msg === "merged");
    expect(merged).toHaveLength(1);
    const entry = merged[0] as LogEntry;
    expect(entry.level).toBe("info");
    expect(entry.attrs).toMatchObject({
      k: "v",
      runId,
      stepId: "a",
      attempt: 1,
    });
  });

  it("each of the four levels routes through onLog with the right level", async () => {
    const { onLog, entries } = spyOnLog();
    const f = flow({
      id: "all-levels",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => ({
        a: b.task({
          run: async ({ ctx }) => {
            ctx.logger.debug("d");
            ctx.logger.info("i");
            ctx.logger.warn("w");
            ctx.logger.error("e");
            return { ok: true };
          },
        }),
      }),
    });
    const h = await makeHarness(f, { onLog });
    await h.wf.start(f, {});
    await h.drain();

    const byMsg = (m: string) => entries.find((e) => e.msg === m) as LogEntry;
    expect(byMsg("d").level).toBe("debug");
    expect(byMsg("i").level).toBe("info");
    expect(byMsg("w").level).toBe("warn");
    expect(byMsg("e").level).toBe("error");
  });

  it("runtime wins on collision: a handler-supplied attrs.runId cannot override the real one", async () => {
    const { onLog, entries } = spyOnLog();
    const f = flow({
      id: "collision",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => ({
        a: b.task({
          run: async ({ ctx }) => {
            ctx.logger.info("clobber attempt", {
              runId: "FAKE",
              stepId: "FAKE-STEP",
              attempt: 999,
            });
            return { ok: true };
          },
        }),
      }),
    });
    const h = await makeHarness(f, { onLog });
    const runId = await h.wf.start(f, {});
    await h.drain();

    const entry = entries.find((e) => e.msg === "clobber attempt") as LogEntry;
    expect(entry.attrs?.["runId"]).toBe(runId);
    expect(entry.attrs?.["runId"]).not.toBe("FAKE");
    expect(entry.attrs?.["stepId"]).toBe("a");
    expect(entry.attrs?.["attempt"]).toBe(1);
  });

  it("attempt reflects the real attempt number on a retried task", async () => {
    const { onLog, entries } = spyOnLog();
    let runs = 0;
    const f = flow({
      id: "retry-attempt",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => ({
        a: b.task({
          retry: { maxAttempts: 3, backoff: "fixed", initialDelayMs: 0 },
          run: async ({ ctx }) => {
            runs++;
            ctx.logger.info("attempt-log");
            if (runs === 1) throw new Error("fail once");
            return { ok: true };
          },
        }),
      }),
    });
    const h = await makeHarness(f, { onLog });
    const runId = await h.wf.start(f, {});
    await h.drain();

    expect((await h.result(runId)).status).toBe("completed");
    const logs = entries.filter((e) => e.msg === "attempt-log");
    expect(logs).toHaveLength(2);
    expect(logs[0]?.attrs?.["attempt"]).toBe(1);
    expect(logs[1]?.attrs?.["attempt"]).toBe(2);
    expect(logs[0]?.attrs?.["runId"]).toBe(runId);
    expect(logs[1]?.attrs?.["runId"]).toBe(runId);
  });

  it("with no onLog, ctx.logger.info is a silent no-op (no console)", async () => {
    const info = vi.spyOn(console, "info").mockImplementation(() => {});
    const log = vi.spyOn(console, "log").mockImplementation(() => {});
    const debug = vi.spyOn(console, "debug").mockImplementation(() => {});
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    const error = vi.spyOn(console, "error").mockImplementation(() => {});
    try {
      const f = flow({
        id: "ctx-noop",
        input: passthroughSchema<Record<string, never>>(),
        build: (b) => ({
          a: b.task({
            run: async ({ ctx }) => {
              ctx.logger.info("should-vanish", { a: 1 });
              return { ok: true };
            },
          }),
        }),
      });
      const h = await makeHarness(f); // no onLog
      const runId = await h.wf.start(f, {});
      await h.drain();
      expect((await h.result(runId)).status).toBe("completed");
      expect(info).not.toHaveBeenCalled();
      expect(log).not.toHaveBeenCalled();
      expect(debug).not.toHaveBeenCalled();
      expect(warn).not.toHaveBeenCalled();
      expect(error).not.toHaveBeenCalled();
    } finally {
      vi.restoreAllMocks();
    }
  });
});
