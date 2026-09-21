import { describe, expect, it } from "vitest";
import { flow } from "../builder";
import type { RunEventEnvelope } from "../types";
import { emptySchema, makeHarness, passthroughSchema } from "./test-helpers";

function types(events: readonly RunEventEnvelope[]): string[] {
  return events.map((e) => e.type);
}

describe("wf.watchRun", () => {
  it("reports the lifecycle of a run in order", async () => {
    const f = flow({
      id: "watch-happy",
      input: emptySchema(),
      build: (b) => {
        const a = b.task({ run: async () => ({ v: 1 }) });
        const c = b.task({ needs: { a }, run: async () => ({ v: 2 }) });
        return { a, c };
      },
      output: (s) => s.c,
    });
    const h = await makeHarness(f);
    const runId = await h.wf.startById("watch-happy", {});

    const seen: RunEventEnvelope[] = [];
    h.wf.watchRun(runId, (e) => seen.push(e));
    await h.drain();

    expect(types(seen)).toEqual([
      "step.started",
      "step.completed",
      "step.started",
      "step.completed",
      "flow.completed",
    ]);
    expect(seen.every((e) => e.runId === runId)).toBe(true);
  });

  it("carries step output and the terminating flow output", async () => {
    const f = flow({
      id: "watch-payload",
      input: emptySchema(),
      build: (b) => ({ only: b.task({ run: async () => ({ ok: true }) }) }),
      output: (s) => s.only,
    });
    const h = await makeHarness(f);
    const runId = await h.wf.startById("watch-payload", {});
    const seen: RunEventEnvelope[] = [];
    h.wf.watchRun(runId, (e) => seen.push(e));
    await h.drain();

    const completed = seen.find((e) => e.type === "step.completed");
    expect(completed).toMatchObject({ stepId: "only", output: { ok: true } });
    expect(seen.at(-1)).toMatchObject({
      type: "flow.completed",
      output: { ok: true },
    });
  });

  it("reports a terminal step failure with its error and attempt", async () => {
    const f = flow({
      id: "watch-failure",
      input: emptySchema(),
      build: (b) => ({
        boom: b.task({
          retry: { maxAttempts: 1, backoff: "fixed" },
          run: async () => {
            throw new Error("nope");
          },
        }),
      }),
    });
    const h = await makeHarness(f);
    const runId = await h.wf.startById("watch-failure", {});
    const seen: RunEventEnvelope[] = [];
    h.wf.watchRun(runId, (e) => seen.push(e));
    await h.drain();

    const failed = seen.find((e) => e.type === "step.failed");
    expect(failed).toMatchObject({ stepId: "boom", attempt: 1 });
    expect(types(seen)).toContain("flow.failed");
  });

  it("stops delivering after the disposer is called", async () => {
    const f = flow({
      id: "watch-dispose",
      input: emptySchema(),
      build: (b) => {
        const a = b.task({ run: async () => ({}) });
        const c = b.task({ needs: { a }, run: async () => ({}) });
        return { a, c };
      },
    });
    const h = await makeHarness(f);
    const runId = await h.wf.startById("watch-dispose", {});
    const seen: RunEventEnvelope[] = [];
    const off = h.wf.watchRun(runId, (e) => {
      seen.push(e);
      off();
    });
    await h.drain();
    expect(seen).toHaveLength(1);
  });

  it("a throwing handler never breaks the run it observes", async () => {
    const f = flow({
      id: "watch-throwing",
      input: emptySchema(),
      build: (b) => ({ only: b.task({ run: async () => ({ ok: true }) }) }),
      output: (s) => s.only,
    });
    const h = await makeHarness(f);
    const runId = await h.wf.startById("watch-throwing", {});
    h.wf.watchRun(runId, () => {
      throw new Error("subscriber exploded");
    });
    await h.drain();
    const r = await h.result(runId);
    expect(r.status).toBe("completed");
  });

  it("drops its handlers once the run is terminal, so watchers do not leak", async () => {
    const f = flow({
      id: "watch-autodispose",
      input: emptySchema(),
      build: (b) => ({ only: b.task({ run: async () => ({}) }) }),
    });
    const h = await makeHarness(f);
    const runId = await h.wf.startById("watch-autodispose", {});
    const seen: RunEventEnvelope[] = [];
    h.wf.watchRun(runId, (e) => seen.push(e));
    await h.drain();
    const afterTerminal = seen.length;

    // Reopening the settled run must not reach the auto-disposed watcher.
    await h.wf.operator().retry(runId, "only", { actor: "ops", scope: "step" });
    await h.drain();
    expect(seen).toHaveLength(afterTerminal);
  });
});

describe("wf.watchRuns", () => {
  it("observes every run, not just one", async () => {
    const f = flow({
      id: "watch-all",
      input: passthroughSchema<{ n: number }>(),
      build: (b) => ({ only: b.task({ run: async () => ({}) }) }),
    });
    const h = await makeHarness(f);
    const seen: RunEventEnvelope[] = [];
    h.wf.watchRuns((e) => seen.push(e));

    const a = await h.wf.start(f, { n: 1 });
    const c = await h.wf.start(f, { n: 2 });
    await h.drain();

    const runIds = new Set(seen.map((e) => e.runId));
    expect(runIds.has(a)).toBe(true);
    expect(runIds.has(c)).toBe(true);
    expect(seen.filter((e) => e.type === "flow.completed")).toHaveLength(2);
  });

  it("sees flow.started, which a per-run watcher registered later misses", async () => {
    const f = flow({
      id: "watch-started",
      input: emptySchema(),
      build: (b) => ({ only: b.task({ run: async () => ({}) }) }),
    });
    const h = await makeHarness(f);
    const seen: RunEventEnvelope[] = [];
    h.wf.watchRuns((e) => seen.push(e));
    await h.wf.startById("watch-started", {});
    expect(types(seen)).toContain("flow.started");
  });
});

describe("concurrency supersession", () => {
  it("is observable — the event core alone could not have emitted", async () => {
    // flow.canceled(concurrency) is minted INSIDE the store's transaction, so
    // this is the case a core-side decorator would have missed entirely.
    const f = flow({
      id: "watch-supersede",
      input: passthroughSchema<{ k: string }>(),
      concurrency: {
        keyFn: (i: { k: string }) => i.k,
        mode: "cancel-in-progress",
      },
      build: (b) => ({ only: b.task({ run: async () => ({}) }) }),
    });
    const h = await makeHarness(f);
    const seen: RunEventEnvelope[] = [];
    h.wf.watchRuns((e) => seen.push(e));

    const first = await h.wf.start(f, { k: "same" });
    const second = await h.wf.start(f, { k: "same" });

    const canceled = seen.find(
      (e) => e.type === "flow.canceled" && e.runId === first,
    );
    expect(canceled).toMatchObject({
      type: "flow.canceled",
      cause: "concurrency",
      canceledByRunId: second,
    });
  });
});
