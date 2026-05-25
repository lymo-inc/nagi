import { describe, expect, it } from "vitest";
import { flow } from "../builder";
import { InMemoryClock, InMemoryQueue, InMemoryStore } from "../memory";
import { nagi } from "../runtime";
import { isTerminalRun } from "../state";
import { passthroughSchema, runFlow } from "./test-helpers";

describe("activity steps", () => {
  it("runs the body, completes, and produces output", async () => {
    let runs = 0;
    const f = flow({
      id: "act-basic",
      input: passthroughSchema<{ n: number }>(),
      build: (b) => {
        const a = b.activity({
          run: async ({ input }) => {
            runs += 1;
            return { doubled: input.n * 2 };
          },
        });
        return { a };
      },
    });

    const result = await runFlow(f, { n: 21 });
    expect(result.status).toBe("completed");
    expect(result.output("a")).toEqual({ doubled: 42 });
    expect(runs).toBe(1);
  });

  it("has no ctx.tx — accessing it throws (body runs outside the durable tx)", async () => {
    let activityThrew = false;
    const f = flow({
      id: "act-no-tx",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => {
        const a = b.activity({
          run: async ({ ctx }) => {
            try {
              // @ts-expect-error ActivityCtx has no tx; runtime getter is the backstop
              void ctx.tx;
            } catch {
              activityThrew = true;
            }
            return { ok: true };
          },
        });
        return { a };
      },
    });

    const result = await runFlow(f, {});
    expect(result.status).toBe("completed");
    expect(activityThrew).toBe(true);
  });

  it("contrast: a task's ctx.tx does NOT throw", async () => {
    let taskThrew = false;
    const f = flow({
      id: "task-tx-ok",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => {
        const a = b.task({
          run: async ({ ctx }) => {
            try {
              void ctx.tx;
            } catch {
              taskThrew = true;
            }
            return { ok: true };
          },
        });
        return { a };
      },
    });

    const result = await runFlow(f, {});
    expect(result.status).toBe("completed");
    expect(taskThrew).toBe(false);
  });

  it("feeds a downstream task via needs", async () => {
    const f = flow({
      id: "act-downstream",
      input: passthroughSchema<{ n: number }>(),
      build: (b) => {
        const a = b.activity({
          run: async ({ input }) => ({ v: input.n + 1 }),
        });
        const t = b.task({
          needs: { a },
          run: async ({ needs }) => ({ v: needs.a.v * 10 }),
        });
        return { a, t };
      },
    });

    const result = await runFlow(f, { n: 4 });
    expect(result.output("t")).toEqual({ v: 50 });
  });

  it("retries on failure per its retry policy", async () => {
    let attempts = 0;
    const f = flow({
      id: "act-retry",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => {
        const a = b.activity({
          retry: { maxAttempts: 3, backoff: "fixed", initialDelayMs: 1 },
          run: async () => {
            attempts += 1;
            if (attempts < 3) throw new Error("transient");
            return { ok: true };
          },
        });
        return { a };
      },
    });

    const result = await runFlow(f, {});
    expect(result.status).toBe("completed");
    expect(attempts).toBe(3);
  });

  it("a slow activity keeps its message leased (heartbeat) and runs once", async () => {
    let runs = 0;
    const f = flow({
      id: "act-slow",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => {
        const a = b.activity({
          run: async () => {
            runs += 1;
            await new Promise((r) => setTimeout(r, 120));
            return { ok: true };
          },
        });
        return { a };
      },
    });

    const store = new InMemoryStore();
    const queue = new InMemoryQueue({ leaseMs: 30 });
    const clock = new InMemoryClock();
    const wf = await nagi({
      flows: [f],
      store,
      queue,
      clock,
      heartbeatIntervalMs: 10,
      heartbeatLeaseMs: 50,
    });

    const ac = new AbortController();
    const worker = wf.worker({ pollIntervalMs: 5, signal: ac.signal });
    const done = worker.run();

    try {
      const runId = await wf.start(f, {});
      const start = Date.now();
      while (Date.now() - start < 3_000) {
        if (isTerminalRun(await store.loadRunState(runId))) break;
        await new Promise((r) => setTimeout(r, 5));
      }
      expect(isTerminalRun(await store.loadRunState(runId))).toBe(true);
      expect(runs).toBe(1);
    } finally {
      ac.abort();
      await done;
    }
  });
});
