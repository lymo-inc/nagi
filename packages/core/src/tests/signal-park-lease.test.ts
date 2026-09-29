import { describe, expect, it, vi } from "vitest";
import { flow } from "../builder";
import { InMemoryClock, InMemoryQueue, InMemoryStore } from "../memory";
import { nagi } from "../runtime";
import { stepStateOf } from "../state";
import { passthroughSchema } from "./test-helpers";

const f = flow({
  id: "signal-park-lease",
  input: passthroughSchema<Record<string, never>>(),
  build: (b) => ({
    gate: b.signal({
      timeoutMs: "unbounded" as const,
      schema: passthroughSchema<{ ok: boolean }>(),
    }),
  }),
});

// A parked signal step's lease used to expire and get reaped every lease
// period for the whole wait. This pins that the park releases the lease so
// the reaper has nothing to reap.
describe("a parked signal step releases its lease", () => {
  it("is not re-dispatched by the reaper while it waits", async () => {
    const store = new InMemoryStore({ leaseMs: 5 });
    const queue = new InMemoryQueue();
    const wf = await nagi({
      flows: [f],
      store,
      queue,
      clock: new InMemoryClock(),
    });

    const ac = new AbortController();
    const done = wf
      .worker({ pollIntervalMs: 5, reaperIntervalMs: 10, signal: ac.signal })
      .run();

    try {
      const runId = await wf.start(f, {});

      await vi.waitFor(async () => {
        const state = await store.loadRunState(runId);
        expect(stepStateOf(state, "gate").tag).toBe("awaitingSignal");
      });

      await new Promise((r) => setTimeout(r, 150));

      const parked = await store.loadRunState(runId);
      expect(
        parked.facts.filter((x) => x.kind === "lease.reaped"),
      ).toHaveLength(0);
      expect(
        parked.facts.filter(
          (x) => x.kind === "step.started" && x.stepId === "gate",
        ),
      ).toHaveLength(1);

      await wf.signal(runId, "gate", { ok: true });

      await vi.waitFor(async () => {
        const state = await store.loadRunState(runId);
        expect(state.phase.tag).toBe("completed");
      });
    } finally {
      ac.abort();
      await done;
    }
  });
});
