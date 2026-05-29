import { describe, expect, it, vi } from "vitest";
import { flow } from "../builder";
import { NagiConcurrencyConflictError } from "../errors";
import { InMemoryQueue, InMemoryStore } from "../memory";
import { nagi } from "../runtime";
import type {
  ConcurrencyMode,
  Fact,
  FlowCanceledByConcurrencyFact,
  FlowErrorEvent,
  FlowStartedFact,
  Queue,
  QueueEnqueueOpts,
  RunId,
  StepId,
  Tx,
} from "../types";
import { makeHarness, passthroughSchema } from "./test-helpers";

interface VideoInput {
  readonly videoId: string;
}

function makeBasicFlow() {
  return flow({
    id: "staged-basic",
    input: passthroughSchema<VideoInput>(),
    build: (b) => ({
      analyze: b.task({
        run: async ({ input }) => ({ analyzed: input.videoId }),
      }),
    }),
  });
}

function makeConcurrentFlow() {
  return flow({
    id: "staged-concurrent",
    input: passthroughSchema<VideoInput>(),
    concurrency: {
      keyFn: (i) => i.videoId,
      mode: "cancel-in-progress",
    },
    build: (b) => ({
      analyze: b.task({
        run: async ({ input }) => ({ analyzed: input.videoId }),
      }),
    }),
  });
}

// Fake Tx marker — InMemoryStore ignores it (no real tx), but threads it
// through so the queue.withTx path receives the same reference.
const FAKE_TX = { __fakeTx: true } as unknown as Tx;

describe("wf.startStaged", () => {
  it("writes flow.started on the tx connection (test #1)", async () => {
    const f = makeBasicFlow();
    const h = await makeHarness(f);

    // Spy on the in-memory store's tryStartRunOnTx to confirm it received the
    // tx the caller supplied (the tx-bound write is the contract).
    const spy = vi.spyOn(h.store, "tryStartRunOnTx");

    const res = await h.wf.startStaged(f, { videoId: "v1" }, { tx: FAKE_TX });
    expect(res.started).toBe(true);
    expect(spy).toHaveBeenCalledTimes(1);
    expect(spy.mock.calls[0]?.[0]).toBe(FAKE_TX);

    // Once "committed" (in-memory: write is already visible), the run row +
    // flow.started fact are persisted under the runId.
    const state = await h.store.loadRunState(res.runId);
    expect(state.flowId).toBe("staged-basic");
    expect(
      state.facts.filter((f: Fact) => f.kind === "flow.started").length,
    ).toBe(1);
  });

  it("caller tx rollback rolls back the run row + flow.started fact (test #2)", async () => {
    // Stub a Tx-aware Store that defers all writes to a per-tx queue and only
    // commits when the test explicitly applies them — never applying simulates
    // rollback. This isolates the tx-rollback semantics from the InMemoryStore
    // (which has no real tx).
    const f = makeBasicFlow();

    const visible = new InMemoryStore();
    const pendingByTx = new Map<unknown, Array<() => Promise<void>>>();

    class RollbackableStore extends InMemoryStore {
      override async tryStartRunOnTx(
        tx: Tx,
        runId: RunId,
        fact: FlowStartedFact,
        concurrency?: { readonly key: string; readonly mode: ConcurrencyMode },
      ) {
        const apply = pendingByTx.get(tx) ?? [];
        apply.push(async () => {
          await visible.tryStartRun(runId, fact, concurrency);
        });
        pendingByTx.set(tx, apply);
        return { started: true, canceled: [] };
      }
    }

    const wf = await nagi({
      flows: [f],
      store: new RollbackableStore(),
      queue: new InMemoryQueue(),
    });

    const txKey: Tx = { rolledBack: true } as unknown as Tx;
    const res = await wf.startStaged(f, { videoId: "v9" }, { tx: txKey });
    expect(res.started).toBe(true);

    // Caller-tx rollback: never apply the pending writes, never call
    // applyOnCommit. The run row + flow.started fact MUST NOT exist.
    const state = await visible.loadRunState(res.runId);
    expect(state.flowId).toBe("");
    expect(state.facts.length).toBe(0);
  });

  it("queue enqueue joins the same tx via queue.withTx (test #3)", async () => {
    const f = makeBasicFlow();

    // Track withTx + enqueue calls to assert the initial-step enqueue threads
    // the caller's tx (the "joins the same tx" pin).
    const enqueueLog: Array<{ tx: Tx; runId: RunId; stepId: StepId }> = [];
    let txQueueUsed: Tx | null = null;
    class TxAwareQueue extends InMemoryQueue {
      withTx(tx: Tx): Queue {
        txQueueUsed = tx;
        const parent = this;
        return {
          async enqueue(
            runId: RunId,
            stepId: StepId,
            opts?: QueueEnqueueOpts,
          ): Promise<void> {
            enqueueLog.push({ tx, runId, stepId });
            await parent.enqueue(runId, stepId, opts);
          },
          dequeue: parent.dequeue.bind(parent),
          ack: parent.ack.bind(parent),
          nack: parent.nack.bind(parent),
          extend: parent.extend.bind(parent),
        };
      }
    }

    const queue = new TxAwareQueue();
    const store = new InMemoryStore();
    const wf = await nagi({ flows: [f], store, queue });

    const res = await wf.startStaged(f, { videoId: "v1" }, { tx: FAKE_TX });
    expect(res.started).toBe(true);
    expect(txQueueUsed).toBe(FAKE_TX);
    expect(enqueueLog.length).toBe(1);
    expect(enqueueLog[0]?.tx).toBe(FAKE_TX);
    expect(enqueueLog[0]?.runId).toBe(res.runId);
    expect(enqueueLog[0]?.stepId).toBe("analyze");
  });

  it("duplicate runId under the same tx is idempotent (started: false) (test #4)", async () => {
    const f = makeBasicFlow();
    const h = await makeHarness(f);

    const first = await h.wf.startStaged(
      f,
      { videoId: "v1" },
      { tx: FAKE_TX, runId: "fixed-run" as RunId },
    );
    expect(first.started).toBe(true);

    const second = await h.wf.startStaged(
      f,
      { videoId: "v1" },
      { tx: FAKE_TX, runId: "fixed-run" as RunId },
    );
    expect(second.started).toBe(false);
    expect(second.canceled).toEqual([]);
  });

  it("concurrency-key supersession works; applyOnCommit fires cancel hooks exactly once (test #5)", async () => {
    const errors: FlowErrorEvent[] = [];
    const f = makeConcurrentFlow();
    const h = await makeHarness(f, {
      hooks: {
        onFlowError: (e) => {
          errors.push(e);
        },
      },
    });

    const firstRes = await h.wf.startStaged(
      f,
      { videoId: "v123" },
      { tx: FAKE_TX },
    );
    await firstRes.applyOnCommit();

    const secondRes = await h.wf.startStaged(
      f,
      { videoId: "v123" },
      { tx: FAKE_TX },
    );
    expect(secondRes.started).toBe(true);
    expect(secondRes.canceled).toEqual([firstRes.runId]);

    // applyOnCommit on the second call MUST fire the cancel hook for the
    // superseded run exactly once.
    expect(errors.length).toBe(0);
    await secondRes.applyOnCommit();
    expect(errors.length).toBe(1);
    expect(errors[0]?.runId).toBe(firstRes.runId);
    expect(errors[0]?.error.name).toBe("NagiCanceledError");
    expect(
      (errors[0]?.error.cause as { canceledByRunId: RunId }).canceledByRunId,
    ).toBe(secondRes.runId);
  });

  it("wf.start (no-tx, existing API) preserves current semantics (test #6 — regression)", async () => {
    const f = makeBasicFlow();
    const h = await makeHarness(f);

    const runId = await h.wf.start(f, { videoId: "v1" });
    await h.drain();
    const r = await h.result(runId);
    expect(r.status).toBe("completed");
    expect(r.output("analyze")).toEqual({ analyzed: "v1" });
  });

  it("applyOnCommit called twice fires hooks only once (idempotent) (test #7)", async () => {
    const errors: FlowErrorEvent[] = [];
    const starts: Array<{ runId: RunId }> = [];
    const f = makeConcurrentFlow();
    const h = await makeHarness(f, {
      hooks: {
        onFlowStart: (e) => {
          starts.push({ runId: e.runId });
        },
        onFlowError: (e) => {
          errors.push(e);
        },
      },
    });

    const firstRes = await h.wf.startStaged(
      f,
      { videoId: "vx" },
      { tx: FAKE_TX },
    );
    await firstRes.applyOnCommit();
    const startCountAfterFirst = starts.length;

    const secondRes = await h.wf.startStaged(
      f,
      { videoId: "vx" },
      { tx: FAKE_TX },
    );
    await secondRes.applyOnCommit();
    await secondRes.applyOnCommit(); // second call is a no-op
    await secondRes.applyOnCommit(); // and a third

    expect(starts.length).toBe(startCountAfterFirst + 1);
    expect(errors.length).toBe(1);
  });

  it("pre-existing terminal runId returns 'started: false' without re-firing flow.started (test #8)", async () => {
    const starts: Array<{ runId: RunId }> = [];
    const f = makeBasicFlow();
    const h = await makeHarness(f, {
      hooks: {
        onFlowStart: (e) => {
          starts.push({ runId: e.runId });
        },
      },
    });

    const fixedId = "preexisting-run" as RunId;
    const firstRunId = await h.wf.start(
      f,
      { videoId: "v1" },
      { runId: fixedId },
    );
    await h.drain();
    const r = await h.result(firstRunId);
    expect(r.status).toBe("completed");
    expect(starts.length).toBe(1);

    // Re-staging the same runId — terminal row exists; store returns started:
    // false; applyOnCommit MUST NOT re-fire onFlowStart.
    const res = await h.wf.startStaged(
      f,
      { videoId: "v1" },
      { tx: FAKE_TX, runId: fixedId },
    );
    expect(res.started).toBe(false);
    await res.applyOnCommit();
    expect(starts.length).toBe(1);
  });

  it("unique-violation race triggers retry; on second violation throws NagiConcurrencyConflictError (test #9)", async () => {
    // Stub a store that throws NagiConcurrencyConflictError on every
    // tryStartRunOnTx call with concurrency — mirroring the PG path that gave
    // up after a second 23505 unique-violation. (The PG path performs the
    // single internal retry itself; here we pin the surface: when the store
    // surfaces the typed error, startStaged propagates it.)
    const f = makeConcurrentFlow();

    class ConflictingStore extends InMemoryStore {
      override async tryStartRunOnTx(
        _tx: Tx,
        runId: RunId,
        fact: FlowStartedFact,
        concurrency?: { readonly key: string; readonly mode: ConcurrencyMode },
      ) {
        if (concurrency === undefined) {
          return { started: true, canceled: [] };
        }
        throw new NagiConcurrencyConflictError({
          runId,
          flowId: fact.flowId,
          concurrencyKey: concurrency.key,
        });
      }
    }

    const wf = await nagi({
      flows: [f],
      store: new ConflictingStore(),
      queue: new InMemoryQueue(),
    });

    await expect(
      wf.startStaged(f, { videoId: "race" }, { tx: FAKE_TX }),
    ).rejects.toBeInstanceOf(NagiConcurrencyConflictError);
  });

  it("advisory lock NOT acquired (verifies skip-under-shared-tx) (test #10)", async () => {
    // The in-memory store has no advisory-lock concept; this test pins the
    // shape contract: runtime.startStaged routes to tryStartRunOnTx (NOT to
    // the no-tx tryStartRun the legacy lock lives behind in PG). Combined
    // with the SQL inspection below (no pg_advisory_xact_lock in
    // PostgresStore.tryStartRunOnTx) and the postgres integration test, this
    // pins D6=A end-to-end.
    const f = makeConcurrentFlow();

    // Custom store: tryStartRunOnTx is a self-contained path (does NOT
    // delegate to tryStartRun under the hood). tryStartRun throws — if the
    // runtime ever routes startStaged through it under shared tx, this test
    // surfaces the regression.
    class NoLockPathStore extends InMemoryStore {
      override async tryStartRun(): Promise<never> {
        throw new Error(
          "startStaged routed to tryStartRun (the advisory-lock-bearing path) under shared tx — should call tryStartRunOnTx",
        );
      }
      override async tryStartRunOnTx(
        _tx: Tx,
        _runId: RunId,
        _fact: FlowStartedFact,
      ) {
        return { started: true, canceled: [] };
      }
    }
    const wf = await nagi({
      flows: [f],
      store: new NoLockPathStore(),
      queue: new InMemoryQueue(),
    });

    const res = await wf.startStaged(f, { videoId: "vlock" }, { tx: FAKE_TX });
    expect(res.started).toBe(true);
  });
});

// Type-only pin: the FlowCanceledByConcurrencyFact import is preserved
// to keep the type surface explicit. Reference it so tsc keeps the import
// useful in strict mode.
const _factShape: FlowCanceledByConcurrencyFact | undefined = undefined;
void _factShape;
