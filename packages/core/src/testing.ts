import { Facts } from "./facts";
import { InMemoryQueue } from "./memory";
import { stepStateOf, stepStatusOf } from "./state";
import type {
  AttemptNumber,
  Json,
  Millis,
  PrunableStatus,
  RunId,
  StandardSchemaV1,
  StepId,
  StepKind,
  Store,
} from "./types";

export function passthroughSchema<T>(): StandardSchemaV1<T, T> {
  return {
    "~standard": {
      version: 1,
      vendor: "nagi-test",
      validate: (value: unknown) => ({ value: value as T }),
    },
  };
}

export interface StoreContractHarness {
  // MUST return an empty store whose claimStep lease lasts `leaseMs`.
  makeStore(opts: { readonly leaseMs: Millis }): Promise<Store>;
}

export interface StoreContractCase {
  readonly name: string;
  run(harness: StoreContractHarness): Promise<void>;
}

class StoreContractViolation extends Error {
  constructor(message: string) {
    super(message);
    this.name = "StoreContractViolation";
  }
}

function ok(cond: boolean, msg: string): asserts cond {
  if (!cond) throw new StoreContractViolation(msg);
}

function eq(actual: unknown, expected: unknown, msg: string): void {
  if (!deepEqual(actual, expected)) {
    throw new StoreContractViolation(
      `${msg}\n  expected: ${show(expected)}\n  actual:   ${show(actual)}`,
    );
  }
}

async function rejects(p: Promise<unknown>, msg: string): Promise<void> {
  let threw = false;
  try {
    await p;
  } catch {
    threw = true;
  }
  ok(threw, msg);
}

function show(v: unknown): string {
  return v instanceof Error ? String(v) : JSON.stringify(v);
}

function deepEqual(a: unknown, b: unknown): boolean {
  if (Object.is(a, b)) return true;
  if (a instanceof Date || b instanceof Date) {
    return (
      a instanceof Date && b instanceof Date && a.getTime() === b.getTime()
    );
  }
  if (Array.isArray(a) || Array.isArray(b)) {
    return (
      Array.isArray(a) &&
      Array.isArray(b) &&
      a.length === b.length &&
      a.every((v, i) => deepEqual(v, b[i]))
    );
  }
  if (typeof a === "object" && typeof b === "object" && a && b) {
    const ra = a as Record<string, unknown>;
    const rb = b as Record<string, unknown>;
    const ka = Object.keys(ra).filter((k) => ra[k] !== undefined);
    const kb = Object.keys(rb).filter((k) => rb[k] !== undefined);
    return ka.length === kb.length && ka.every((k) => deepEqual(ra[k], rb[k]));
  }
  return false;
}

const LEASE_MS: Millis = 60_000;
const SHORT_LEASE_MS: Millis = 40;
const PAST_LEASE_MS = 90;
const HOUR_MS = 3_600_000;

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
const rid = (): RunId => `run-${crypto.randomUUID()}` as RunId;
const fid = (): string => `flow-${crypto.randomUUID()}`;
const A1 = 1 as AttemptNumber;
const A2 = 2 as AttemptNumber;

async function startRun(
  s: Store,
  runId: RunId,
  opts: {
    readonly flowId?: string;
    readonly input?: Json;
    readonly at?: Date;
    readonly parent?: { readonly runId: RunId; readonly stepId: StepId };
    readonly concurrencyKey?: string;
  } = {},
) {
  const fact = Facts.flowStarted({
    runId,
    flowId: opts.flowId ?? "f",
    input: opts.input ?? {},
    at: opts.at ?? new Date(),
    ...(opts.parent !== undefined ? { parent: opts.parent } : {}),
  });
  return s.tryStartRun(
    runId,
    fact,
    opts.concurrencyKey !== undefined
      ? { key: opts.concurrencyKey, mode: "cancel-in-progress" }
      : undefined,
  );
}

async function startStep(
  s: Store,
  runId: RunId,
  stepId: StepId,
  opts: { readonly attempt?: AttemptNumber; readonly kind?: StepKind } = {},
): Promise<void> {
  await s.appendFact(
    runId,
    Facts.stepStarted(
      runId,
      stepId,
      opts.attempt ?? A1,
      opts.kind ?? "task",
      new Date(),
    ),
  );
}

async function stepStatus(s: Store, runId: RunId, stepId: StepId) {
  return stepStatusOf(stepStateOf(await s.loadRunState(runId), stepId));
}

async function endRun(
  s: Store,
  runId: RunId,
  status: PrunableStatus,
  at = new Date(),
): Promise<void> {
  const fact =
    status === "completed"
      ? Facts.flowCompleted(runId, null, at)
      : status === "failed"
        ? Facts.flowFailed(runId, { name: "E", message: "x" }, at)
        : Facts.flowCanceled(runId, { cause: "explicit", reason: "test" }, at);
  await s.appendFact(runId, fact);
}

async function stepView(s: Store, runId: RunId, stepId: StepId) {
  const d = await s.describe(runId);
  ok(d !== null, `describe(${runId}) returned null`);
  return d.steps.find((st) => st.stepId === stepId);
}

async function claimableAgain(
  s: Store,
  runId: RunId,
  stepId: StepId,
  what: string,
): Promise<void> {
  ok(
    (await s.claimStep(runId, stepId, A1)) !== null,
    `${what} must release the lease: claimStep at the same attempt still returns null`,
  );
}

export const storeContract: ReadonlyArray<StoreContractCase> = [
  {
    name: "tryStartRun: concurrent calls with the same runId produce exactly one flow.started",
    async run(h) {
      const s = await h.makeStore({ leaseMs: LEASE_MS });
      const runId = rid();
      const results = await Promise.all(
        Array.from({ length: 8 }, () => startRun(s, runId)),
      );
      eq(
        results.filter((r) => r.started).length,
        1,
        "exactly one call reports started:true",
      );
      const state = await s.loadRunState(runId);
      eq(
        state.facts.filter((f) => f.kind === "flow.started").length,
        1,
        "exactly one flow.started fact",
      );
    },
  },
  {
    name: "tryStartRun: a second call for a known runId is a no-op that keeps the original input",
    async run(h) {
      const s = await h.makeStore({ leaseMs: LEASE_MS });
      const runId = rid();
      await startRun(s, runId, { input: { x: 1 } });
      const again = await startRun(s, runId, { input: { x: 999 } });
      eq(again, { started: false, canceled: [] }, "second start");
      eq((await s.loadRunState(runId)).input, { x: 1 }, "input unchanged");
    },
  },
  {
    name: "tryStartRun: with concurrency, cancels the prior active run on the same (flowId, key) and returns its fact",
    async run(h) {
      const s = await h.makeStore({ leaseMs: LEASE_MS });
      const flowId = fid();
      const a = rid();
      const b = rid();
      await startRun(s, a, { flowId, concurrencyKey: "k" });
      const res = await startRun(s, b, { flowId, concurrencyKey: "k" });
      ok(res.started, "b started");
      eq(res.canceled.length, 1, "one prior run canceled");
      const c = res.canceled[0];
      ok(c !== undefined, "canceled entry");
      eq(c.runId, a, "canceled runId");
      eq(c.fact.kind, "flow.canceled", "fact kind");
      eq(c.fact.cause, "concurrency", "fact cause");
      eq(c.fact.canceledByRunId, b, "fact canceledByRunId");
      eq(c.fact.concurrencyKey, "k", "fact concurrencyKey");
      eq((await s.loadRunState(a)).phase.tag, "canceled", "a folds canceled");
      eq((await s.describe(a))?.run.canceledByRunId, b, "describe(a)");
      eq((await s.describe(a))?.run.concurrencyKey, "k", "describe(a) key");
    },
  },
  {
    name: "tryStartRun: a different key or flowId cancels nothing",
    async run(h) {
      const s = await h.makeStore({ leaseMs: LEASE_MS });
      const flowId = fid();
      await startRun(s, rid(), { flowId, concurrencyKey: "k1" });
      const other = await startRun(s, rid(), { flowId, concurrencyKey: "k2" });
      eq(other.canceled, [], "different key");
      const otherFlow = await startRun(s, rid(), {
        flowId: fid(),
        concurrencyKey: "k1",
      });
      eq(otherFlow.canceled, [], "different flowId");
    },
  },
  {
    name: "flow.completed / flow.failed / flow.canceled release the (flowId, key) slot — a later start cancels nothing",
    async run(h) {
      const s = await h.makeStore({ leaseMs: LEASE_MS });
      for (const status of ["completed", "failed", "canceled"] as const) {
        const flowId = fid();
        const a = rid();
        await startRun(s, a, { flowId, concurrencyKey: "k" });
        await endRun(s, a, status);
        const res = await startRun(s, rid(), { flowId, concurrencyKey: "k" });
        eq(res.canceled, [], `after flow.${status}`);
        eq((await s.loadRunState(a)).phase.tag, status, "a keeps its status");
      }
    },
  },
  {
    name: "claimStep: returns a token, then null while the lease is live",
    async run(h) {
      const s = await h.makeStore({ leaseMs: LEASE_MS });
      const runId = rid();
      ok((await s.claimStep(runId, "s", A1)) !== null, "first claim");
      eq(await s.claimStep(runId, "s", A1), null, "second claim");
    },
  },
  {
    name: "claimStep: re-acquires after the lease expires",
    async run(h) {
      const s = await h.makeStore({ leaseMs: SHORT_LEASE_MS });
      const runId = rid();
      ok((await s.claimStep(runId, "s", A1)) !== null, "first claim");
      await sleep(PAST_LEASE_MS);
      ok((await s.claimStep(runId, "s", A1)) !== null, "claim after expiry");
    },
  },
  {
    name: "claimStep: leases are keyed per attempt",
    async run(h) {
      const s = await h.makeStore({ leaseMs: LEASE_MS });
      const runId = rid();
      ok((await s.claimStep(runId, "s", A1)) !== null, "attempt 1");
      ok((await s.claimStep(runId, "s", A2)) !== null, "attempt 2");
    },
  },
  {
    name: "extendLease: keeps a live lease claimed past its original expiry",
    async run(h) {
      const s = await h.makeStore({ leaseMs: SHORT_LEASE_MS });
      const runId = rid();
      ok((await s.claimStep(runId, "s", A1)) !== null, "claim");
      await s.extendLease(runId, "s", A1, 10_000);
      await sleep(PAST_LEASE_MS);
      eq(await s.claimStep(runId, "s", A1), null, "still leased");
    },
  },
  {
    name: "extendLease: is a no-op, never a throw, without a lease",
    async run(h) {
      const s = await h.makeStore({ leaseMs: LEASE_MS });
      const runId = rid();
      await s.extendLease(runId, "s", A1, 10_000);
      ok((await s.claimStep(runId, "s", A1)) !== null, "claim afterwards");
    },
  },
  {
    name: "settleStep(step.completed): releases the lease — describe() drops lease.expiresAt and claimStep re-acquires",
    async run(h) {
      const s = await h.makeStore({ leaseMs: LEASE_MS });
      const runId = rid();
      await startRun(s, runId);
      await startStep(s, runId, "s");
      ok((await s.claimStep(runId, "s", A1)) !== null, "claim");
      const live = await stepView(s, runId, "s");
      ok(live?.lease?.expiresAt instanceof Date, "lease visible while held");
      await s.settleStep(
        runId,
        "s",
        Facts.stepCompleted(runId, "s", A1, { ok: 1 }, new Date()),
      );
      const done = await stepView(s, runId, "s");
      eq(done?.status, "completed", "status");
      eq(done?.output, { ok: 1 }, "output");
      eq(done?.lease, undefined, "lease gone from describe()");
      await claimableAgain(s, runId, "s", "settleStep(step.completed)");
    },
  },
  {
    name: "settleStep(step.failed): releases the lease",
    async run(h) {
      const s = await h.makeStore({ leaseMs: LEASE_MS });
      const runId = rid();
      await startRun(s, runId);
      await startStep(s, runId, "s");
      ok((await s.claimStep(runId, "s", A1)) !== null, "claim");
      await s.settleStep(
        runId,
        "s",
        Facts.stepFailed(
          runId,
          "s",
          A1,
          { name: "E", message: "x" },
          new Date(),
        ),
      );
      eq(await stepStatus(s, runId, "s"), "failed", "status");
      eq((await stepView(s, runId, "s"))?.lease, undefined, "lease");
      await claimableAgain(s, runId, "s", "settleStep(step.failed)");
    },
  },
  {
    name: "runStep: returns the output, persists the fact and releases the lease for completed / failed / canceled",
    async run(h) {
      const s = await h.makeStore({ leaseMs: LEASE_MS });
      const runId = rid();
      await startRun(s, runId);
      const at = new Date();
      const cases = [
        {
          stepId: "done",
          fact: Facts.stepCompleted(runId, "done", A1, { v: 1 }, at),
          status: "completed",
        },
        {
          stepId: "bad",
          fact: Facts.stepFailed(
            runId,
            "bad",
            A1,
            { name: "E", message: "x" },
            at,
          ),
          status: "failed",
        },
        {
          stepId: "gone",
          fact: Facts.stepCanceled(runId, "gone", A1, at),
          status: "canceled",
        },
      ] as const;
      for (const c of cases) {
        await startStep(s, runId, c.stepId);
        ok((await s.claimStep(runId, c.stepId, A1)) !== null, "claim");
        const out = await s.runStep(runId, c.stepId, A1, async () => ({
          output: { v: 1 },
          fact: c.fact,
        }));
        eq(out, { v: 1 }, `${c.stepId}: output`);
        eq(
          await stepStatus(s, runId, c.stepId),
          c.status,
          `${c.stepId}: status`,
        );
        await claimableAgain(s, runId, c.stepId, `runStep(${c.fact.kind})`);
      }
    },
  },
  {
    name: "runStep: a throwing body persists nothing",
    async run(h) {
      const s = await h.makeStore({ leaseMs: LEASE_MS });
      const runId = rid();
      await startRun(s, runId);
      await startStep(s, runId, "s");
      await rejects(
        s.runStep(runId, "s", A1, async () => {
          throw new Error("boom");
        }),
        "runStep rethrows",
      );
      eq(await stepStatus(s, runId, "s"), "running", "step untouched");
      const facts = (await s.loadRunState(runId)).facts;
      eq(
        facts.filter(
          (f) => f.kind !== "flow.started" && f.kind !== "step.started",
        ).length,
        0,
        "no extra fact",
      );
    },
  },
  {
    name: "settleSignal(deliver): releases the lease and the armed timer",
    async run(h) {
      const s = await h.makeStore({ leaseMs: LEASE_MS });
      const runId = rid();
      await startRun(s, runId);
      await startStep(s, runId, "gate", { kind: "signal" });
      ok((await s.claimStep(runId, "gate", A1)) !== null, "claim");
      await s.upsertTimer(runId, "gate", new Date(Date.now() - 1000));
      const res = await s.settleSignal({
        runId,
        stepId: "gate",
        at: new Date(),
        incoming: { payload: { go: true } },
      });
      eq(res.tag, "delivered", "delivered");
      eq(await stepStatus(s, runId, "gate"), "completed", "status");
      await claimableAgain(s, runId, "gate", "settleSignal(deliver)");
      eq(
        await s.sweepSignalTimeouts({ now: new Date(Date.now() + HOUR_MS) }),
        [],
        "timer released on delivery",
      );
    },
  },
  {
    name: "sweepSignalTimeouts: fails an awaiting step past its deadline with NagiSignalTimeoutError, releases its lease, consumes the timer",
    async run(h) {
      const s = await h.makeStore({ leaseMs: LEASE_MS });
      const runId = rid();
      await startRun(s, runId);
      await startStep(s, runId, "gate", { kind: "signal" });
      ok((await s.claimStep(runId, "gate", A1)) !== null, "claim");
      await s.upsertTimer(runId, "gate", new Date(Date.now() - 1000));
      const timedOut = await s.sweepSignalTimeouts({ now: new Date() });
      eq(timedOut.length, 1, "one step timed out");
      eq(timedOut[0]?.runId, runId, "runId");
      eq(timedOut[0]?.stepId, "gate", "stepId");
      eq(timedOut[0]?.attempt, A1, "attempt");
      const step = stepStateOf(await s.loadRunState(runId), "gate");
      ok(step.tag === "failed", "step failed");
      eq(step.error.name, "NagiSignalTimeoutError", "error name");
      await claimableAgain(s, runId, "gate", "sweepSignalTimeouts");
      eq(
        await s.sweepSignalTimeouts({ now: new Date() }),
        [],
        "timer consumed",
      );
    },
  },
  {
    name: "appendFact(step.canceled): releases the lease",
    async run(h) {
      const s = await h.makeStore({ leaseMs: LEASE_MS });
      const runId = rid();
      await startRun(s, runId);
      await startStep(s, runId, "s");
      ok((await s.claimStep(runId, "s", A1)) !== null, "claim");
      await s.appendFact(runId, Facts.stepCanceled(runId, "s", A1, new Date()));
      eq(await stepStatus(s, runId, "s"), "canceled", "status");
      await claimableAgain(s, runId, "s", "appendFact(step.canceled)");
    },
  },
  {
    name: "appendFact(step.reset): releases the lease and the timer so the next upsertTimer arms fresh",
    async run(h) {
      const s = await h.makeStore({ leaseMs: LEASE_MS });
      const runId = rid();
      await startRun(s, runId);
      await startStep(s, runId, "gate", { kind: "signal" });
      ok((await s.claimStep(runId, "gate", A1)) !== null, "claim");
      await s.upsertTimer(runId, "gate", new Date(Date.now() + HOUR_MS));
      await s.appendFact(
        runId,
        Facts.stepReset({ runId, stepId: "gate", at: new Date() }),
      );
      eq(await stepStatus(s, runId, "gate"), "pending", "reset to pending");
      await claimableAgain(s, runId, "gate", "appendFact(step.reset)");
      await startStep(s, runId, "gate", { attempt: A2, kind: "signal" });
      await s.upsertTimer(runId, "gate", new Date(Date.now() - 1000));
      const timedOut = await s.sweepSignalTimeouts({ now: new Date() });
      eq(
        timedOut.map((t) => t.attempt),
        [A2],
        "fresh deadline fired",
      );
    },
  },
  {
    name: "upsertTimer: insert-if-absent — the first armed deadline wins",
    async run(h) {
      const s = await h.makeStore({ leaseMs: LEASE_MS });
      const early = rid();
      await startRun(s, early);
      await startStep(s, early, "gate", { kind: "signal" });
      await s.upsertTimer(early, "gate", new Date(Date.now() - 1000));
      await s.upsertTimer(early, "gate", new Date(Date.now() + HOUR_MS));
      eq(
        (await s.sweepSignalTimeouts({ now: new Date() })).map((t) => t.runId),
        [early],
        "first (due) deadline kept",
      );
      const late = rid();
      await startRun(s, late);
      await startStep(s, late, "gate", { kind: "signal" });
      await s.upsertTimer(late, "gate", new Date(Date.now() + HOUR_MS));
      await s.upsertTimer(late, "gate", new Date(Date.now() - 1000));
      eq(
        await s.sweepSignalTimeouts({ now: new Date() }),
        [],
        "first (future) deadline kept",
      );
    },
  },
  {
    name: "sweepSignalTimeouts: a due timer whose step is no longer awaiting is consumed as a no-op",
    async run(h) {
      const s = await h.makeStore({ leaseMs: LEASE_MS });
      const runId = rid();
      await startRun(s, runId);
      await startStep(s, runId, "gate", { kind: "signal" });
      await s.upsertTimer(runId, "gate", new Date(Date.now() - 1000));
      await s.appendFact(
        runId,
        Facts.stepCanceled(runId, "gate", A1, new Date()),
      );
      eq(await s.sweepSignalTimeouts({ now: new Date() }), [], "noop");
      eq(await stepStatus(s, runId, "gate"), "canceled", "status untouched");
      eq(await s.sweepSignalTimeouts({ now: new Date() }), [], "consumed");
    },
  },
  {
    name: "sweepSignalTimeouts: honors limit and returns the rest on the next sweep",
    async run(h) {
      const s = await h.makeStore({ leaseMs: LEASE_MS });
      for (let i = 0; i < 3; i++) {
        const runId = rid();
        await startRun(s, runId);
        await startStep(s, runId, "gate", { kind: "signal" });
        await s.upsertTimer(runId, "gate", new Date(Date.now() - 1000));
      }
      eq(
        (await s.sweepSignalTimeouts({ now: new Date(), limit: 2 })).length,
        2,
        "first",
      );
      eq(
        (await s.sweepSignalTimeouts({ now: new Date(), limit: 2 })).length,
        1,
        "rest",
      );
    },
  },
  {
    name: "settleSignal: buffers a signal for a step that has not started, then the consume call delivers it",
    async run(h) {
      const s = await h.makeStore({ leaseMs: LEASE_MS });
      const runId = rid();
      await startRun(s, runId);
      const early = await s.settleSignal({
        runId,
        stepId: "gate",
        at: new Date(),
        incoming: { payload: { n: 1 }, signalName: "alias" },
      });
      eq(early, { tag: "buffered" }, "buffered");
      eq(
        (await s.loadRunState(runId)).bufferedSignals["gate"],
        { payload: { n: 1 }, signalName: "alias" },
        "parked in run state",
      );
      await startStep(s, runId, "gate", { kind: "signal" });
      const consumed = await s.settleSignal({
        runId,
        stepId: "gate",
        at: new Date(),
      });
      eq(
        consumed,
        {
          tag: "delivered",
          attempt: A1,
          payload: { n: 1 },
          signalName: "alias",
        },
        "consume delivers the buffered signal",
      );
      const step = stepStateOf(await s.loadRunState(runId), "gate");
      ok(step.tag === "completed", "step completed");
      eq(step.output, { n: 1 }, "output is the payload");
    },
  },
  {
    name: "settleSignal: delivers straight to an awaiting step and writes signal.received + step.completed",
    async run(h) {
      const s = await h.makeStore({ leaseMs: LEASE_MS });
      const runId = rid();
      await startRun(s, runId);
      await startStep(s, runId, "gate", { kind: "signal" });
      const res = await s.settleSignal({
        runId,
        stepId: "gate",
        at: new Date(),
        incoming: { payload: "hi" },
      });
      eq(res, { tag: "delivered", attempt: A1, payload: "hi" }, "delivered");
      const kinds = (await s.loadRunState(runId)).facts.map((f) => f.kind);
      eq(
        kinds.filter((k) => k === "signal.received").length,
        1,
        "signal.received",
      );
      eq(
        kinds.filter((k) => k === "step.completed").length,
        1,
        "step.completed",
      );
    },
  },
  {
    name: "settleSignal: no-ops once the step resolved, and on a consume call with nothing buffered",
    async run(h) {
      const s = await h.makeStore({ leaseMs: LEASE_MS });
      const runId = rid();
      await startRun(s, runId);
      eq(
        await s.settleSignal({ runId, stepId: "gate", at: new Date() }),
        { tag: "noop" },
        "consume with nothing buffered",
      );
      await startStep(s, runId, "gate", { kind: "signal" });
      await s.settleSignal({
        runId,
        stepId: "gate",
        at: new Date(),
        incoming: { payload: 1 },
      });
      eq(
        await s.settleSignal({
          runId,
          stepId: "gate",
          at: new Date(),
          incoming: { payload: 2 },
        }),
        { tag: "noop" },
        "late signal",
      );
      const step = stepStateOf(await s.loadRunState(runId), "gate");
      ok(step.tag === "completed", "completed");
      eq(step.output, 1, "first delivery stands");
    },
  },
  {
    name: "settleSignal: the first buffered signal wins",
    async run(h) {
      const s = await h.makeStore({ leaseMs: LEASE_MS });
      const runId = rid();
      await startRun(s, runId);
      await s.settleSignal({
        runId,
        stepId: "gate",
        at: new Date(),
        incoming: { payload: 1 },
      });
      eq(
        await s.settleSignal({
          runId,
          stepId: "gate",
          at: new Date(),
          incoming: { payload: 2 },
        }),
        { tag: "buffered" },
        "second still reports buffered",
      );
      const state = await s.loadRunState(runId);
      eq(state.bufferedSignals["gate"]?.payload, 1, "first payload parked");
      eq(
        state.facts.filter((f) => f.kind === "signal.buffered").length,
        1,
        "one fact",
      );
    },
  },
  {
    name: "sweepLeases: reaps an expired lease on a running step — deletes it, writes lease.reaped, re-enqueues at attempt+1 with flowId",
    async run(h) {
      const s = await h.makeStore({ leaseMs: SHORT_LEASE_MS });
      const queue = new InMemoryQueue();
      const runId = rid();
      const flowId = fid();
      await startRun(s, runId, { flowId });
      await startStep(s, runId, "s");
      ok((await s.claimStep(runId, "s", A1)) !== null, "claim");
      await sleep(PAST_LEASE_MS);
      const reaped = await s.sweepLeases({ now: new Date(), queue });
      eq(
        reaped,
        [{ runId, stepId: "s", attempt: A1, nextAttempt: A2 }],
        "reaped",
      );
      const facts = (await s.loadRunState(runId)).facts;
      eq(
        facts.filter((f) => f.kind === "lease.reaped").length,
        1,
        "audit fact",
      );
      const [msg] = await queue.dequeue({ count: 1 });
      eq(msg?.runId, runId, "re-enqueued runId");
      eq(msg?.stepId, "s", "re-enqueued stepId");
      eq(msg?.attempt, A2, "re-enqueued attempt");
      eq(msg?.flowId, flowId, "re-enqueued flowId");
      await claimableAgain(s, runId, "s", "sweepLeases");
      eq(await s.sweepLeases({ now: new Date(), queue }), [], "restart-safe");
    },
  },
  {
    name: "sweepLeases: skips live leases and the released lease of a settled step",
    async run(h) {
      const s = await h.makeStore({ leaseMs: SHORT_LEASE_MS });
      const queue = new InMemoryQueue();
      const live = rid();
      await startRun(s, live);
      await startStep(s, live, "s");
      ok((await s.claimStep(live, "s", A1)) !== null, "claim live");
      await s.extendLease(live, "s", A1, LEASE_MS);
      const settled = rid();
      await startRun(s, settled);
      await startStep(s, settled, "s");
      ok((await s.claimStep(settled, "s", A1)) !== null, "claim settled");
      await s.settleStep(
        settled,
        "s",
        Facts.stepCompleted(settled, "s", A1, null, new Date()),
      );
      await sleep(PAST_LEASE_MS);
      eq(await s.sweepLeases({ now: new Date(), queue }), [], "nothing reaped");
      eq(
        (await s.loadRunState(settled)).facts.filter(
          (f) => f.kind === "lease.reaped",
        ),
        [],
        "no audit fact on the settled run",
      );
    },
  },
  {
    name: "sweepLeases: filters on expiry before applying limit — an expired lease behind `limit` live ones is still reaped",
    async run(h) {
      const s = await h.makeStore({ leaseMs: SHORT_LEASE_MS });
      const queue = new InMemoryQueue();
      for (let i = 0; i < 5; i++) {
        const runId = rid();
        await startRun(s, runId);
        await startStep(s, runId, "s");
        ok((await s.claimStep(runId, "s", A1)) !== null, "claim live");
        await s.extendLease(runId, "s", A1, LEASE_MS);
      }
      const expired = rid();
      await startRun(s, expired);
      await startStep(s, expired, "s");
      ok((await s.claimStep(expired, "s", A1)) !== null, "claim expiring");
      await sleep(PAST_LEASE_MS);
      const reaped = await s.sweepLeases({ now: new Date(), queue, limit: 3 });
      eq(
        reaped.map((r) => r.runId),
        [expired],
        "expired lease reaped under limit",
      );
    },
  },
  {
    name: "sweepLeases: skips a subflow step while its child is active, reaps once the child is terminal",
    async run(h) {
      const s = await h.makeStore({ leaseMs: SHORT_LEASE_MS });
      const queue = new InMemoryQueue();
      const parent = rid();
      const child = rid();
      await startRun(s, parent);
      await startStep(s, parent, "sub", { kind: "subflow" });
      ok((await s.claimStep(parent, "sub", A1)) !== null, "claim");
      await startRun(s, child, { parent: { runId: parent, stepId: "sub" } });
      await sleep(PAST_LEASE_MS);
      eq(await s.sweepLeases({ now: new Date(), queue }), [], "child active");
      await endRun(s, child, "completed");
      eq(
        (await s.sweepLeases({ now: new Date(), queue })).map((r) => r.runId),
        [parent],
        "child terminal",
      );
    },
  },
  {
    name: "sweepLeases: honors limit and reaps the rest on the next sweep",
    async run(h) {
      const s = await h.makeStore({ leaseMs: SHORT_LEASE_MS });
      const queue = new InMemoryQueue();
      for (let i = 0; i < 3; i++) {
        const runId = rid();
        await startRun(s, runId);
        await startStep(s, runId, "s");
        ok((await s.claimStep(runId, "s", A1)) !== null, "claim");
      }
      await sleep(PAST_LEASE_MS);
      eq(
        (await s.sweepLeases({ now: new Date(), queue, limit: 2 })).length,
        2,
        "first",
      );
      eq(
        (await s.sweepLeases({ now: new Date(), queue, limit: 2 })).length,
        1,
        "rest",
      );
    },
  },
  {
    name: "recordOnce / getOnce: first write wins; null when absent",
    async run(h) {
      const s = await h.makeStore({ leaseMs: LEASE_MS });
      const runId = rid();
      eq(await s.getOnce(runId, "s", "scope"), null, "absent");
      await s.recordOnce(runId, "s", "scope", { value: 1 });
      await s.recordOnce(runId, "s", "scope", { value: 2 });
      eq(
        await s.getOnce(runId, "s", "scope"),
        { value: 1 },
        "first write wins",
      );
      eq(await s.getOnce(runId, "s", "other"), null, "scoped");
    },
  },
  {
    name: "upsertSnapshot: idempotent on flowHash; loadSnapshot null for an unknown hash",
    async run(h) {
      const s = await h.makeStore({ leaseMs: LEASE_MS });
      const flowHash = `h-${crypto.randomUUID()}`;
      eq(await s.loadSnapshot(flowHash), null, "unknown");
      await s.upsertSnapshot({ flowHash, flowId: "f", dag: { v: 1 } });
      await s.upsertSnapshot({ flowHash, flowId: "f", dag: { v: 2 } });
      eq(
        await s.loadSnapshot(flowHash),
        { flowId: "f", dag: { v: 1 } },
        "first wins",
      );
    },
  },
  {
    name: "getRef / setRef: null until set, then the latest of the snapshots it points at",
    async run(h) {
      const s = await h.makeStore({ leaseMs: LEASE_MS });
      const flowId = fid();
      eq(await s.getRef(flowId), null, "unset");
      // A ref points at a stored snapshot (Postgres enforces it as a FK).
      await s.upsertSnapshot({ flowHash: "h1", flowId, dag: { v: 1 } });
      await s.upsertSnapshot({ flowHash: "h2", flowId, dag: { v: 2 } });
      await s.setRef(flowId, "h1");
      await s.setRef(flowId, "h2");
      eq(await s.getRef(flowId), "h2", "latest");
    },
  },
  {
    name: "appendGlobalFact: accepts a flow_ref.updated fact",
    async run(h) {
      const s = await h.makeStore({ leaseMs: LEASE_MS });
      await s.appendGlobalFact(
        Facts.flowRefUpdated({
          flowId: fid(),
          from: null,
          to: "h1",
          at: new Date(),
        }),
      );
    },
  },
  {
    name: "loadRunState: folds facts in append order; an unknown runId folds to an empty pending state",
    async run(h) {
      const s = await h.makeStore({ leaseMs: LEASE_MS });
      const unknown = await s.loadRunState(rid());
      eq(unknown.phase, { tag: "pending" }, "unknown phase");
      eq(unknown.facts, [], "unknown facts");
      const runId = rid();
      await startRun(s, runId, { flowId: "f", input: { a: 1 } });
      await startStep(s, runId, "s");
      await s.settleStep(
        runId,
        "s",
        Facts.stepCompleted(runId, "s", A1, 7, new Date()),
      );
      await endRun(s, runId, "completed");
      const state = await s.loadRunState(runId);
      eq(state.flowId, "f", "flowId");
      eq(state.input, { a: 1 }, "input");
      eq(
        state.facts.map((f) => f.kind),
        ["flow.started", "step.started", "step.completed", "flow.completed"],
        "append order",
      );
      eq(state.phase.tag, "completed", "phase");
    },
  },
  {
    name: "queryRuns: orders by (startedAt DESC, runId DESC)",
    async run(h) {
      const s = await h.makeStore({ leaseMs: LEASE_MS });
      const flowId = fid();
      const t0 = new Date(1_700_000_000_000);
      const t1 = new Date(1_700_000_001_000);
      const base = crypto.randomUUID();
      const a = `run-${base}-a` as RunId;
      const b = `run-${base}-b` as RunId;
      const c = `run-${base}-c` as RunId;
      const later = `run-${base}-0` as RunId;
      for (const runId of [b, a, c])
        await startRun(s, runId, { flowId, at: t0 });
      await startRun(s, later, { flowId, at: t1 });
      const r = await s.queryRuns({ where: { flowId } });
      eq(
        r.runs.map((x) => x.runId),
        [later, c, b, a],
        "order",
      );
      eq(r.cursor, null, "single page");
    },
  },
  {
    name: "queryRuns: input containment follows jsonb @> — subset, nested, arrays, empty object, type mismatch",
    async run(h) {
      const s = await h.makeStore({ leaseMs: LEASE_MS });
      const flowId = fid();
      await startRun(s, rid(), {
        flowId,
        input: { videoId: "abc", userId: 7 },
      });
      await startRun(s, rid(), {
        flowId,
        input: { customer: { id: 1, plan: { seats: 5 } } },
      });
      await startRun(s, rid(), { flowId, input: { tags: ["a", "b", "c"] } });
      await startRun(s, rid(), { flowId, input: { x: "string" } });
      const count = async (input: Record<string, Json>) =>
        (await s.queryRuns({ where: { flowId, input } })).runs.length;
      eq(await count({ videoId: "abc" }), 1, "subset of keys");
      eq(await count({ videoId: "abc", userId: 7 }), 1, "all keys");
      eq(await count({ videoId: "abc", missing: 1 }), 0, "superset of keys");
      eq(await count({ videoId: "MISS" }), 0, "value mismatch");
      eq(await count({ customer: { plan: { seats: 5 } } }), 1, "nested");
      eq(await count({ customer: {} }), 1, "empty object matches any object");
      eq(await count({ tags: ["a", "b"] }), 1, "array subset");
      eq(await count({ tags: ["b", "a"] }), 1, "array order-insensitive");
      eq(await count({ tags: ["a", "z"] }), 0, "array with a missing element");
      eq(await count({ x: 42 }), 0, "type mismatch");
      eq(await count({}), 4, "empty filter matches all");
    },
  },
  {
    name: "queryRuns: filters by flowId and status",
    async run(h) {
      const s = await h.makeStore({ leaseMs: LEASE_MS });
      const flowId = fid();
      const running = rid();
      const completed = rid();
      const failed = rid();
      await startRun(s, running, { flowId });
      await startRun(s, completed, { flowId });
      await endRun(s, completed, "completed");
      await startRun(s, failed, { flowId });
      await endRun(s, failed, "failed");
      await startRun(s, rid(), { flowId: fid() });
      eq((await s.queryRuns({ where: { flowId } })).runs.length, 3, "flowId");
      const terminal = await s.queryRuns({
        where: { flowId, status: ["completed", "failed"] },
      });
      eq(
        terminal.runs.map((r) => r.runId).sort(),
        [completed, failed].sort(),
        "status array",
      );
      const one = await s.queryRuns({
        where: { flowId, status: ["completed"] },
      });
      eq(
        one.runs.map((r) => r.status),
        ["completed"],
        "single status",
      );
      ok(one.runs[0]?.completedAt instanceof Date, "completedAt populated");
    },
  },
  {
    name: "queryRuns: latest:true returns the single newest matching run and no cursor",
    async run(h) {
      const s = await h.makeStore({ leaseMs: LEASE_MS });
      const flowId = fid();
      await startRun(s, rid(), { flowId, at: new Date(1_700_000_000_000) });
      const newest = rid();
      await startRun(s, newest, { flowId, at: new Date(1_700_000_005_000) });
      await startRun(s, rid(), { flowId, at: new Date(1_700_000_002_000) });
      const r = await s.queryRuns({ where: { flowId }, latest: true });
      eq(
        r.runs.map((x) => x.runId),
        [newest],
        "newest",
      );
      eq(r.cursor, null, "no cursor");
      eq(
        await s.queryRuns({ where: { flowId: fid() }, latest: true }),
        { runs: [], cursor: null },
        "no match",
      );
    },
  },
  {
    name: "queryRuns: cursor pagination visits every row exactly once and ends with a null cursor",
    async run(h) {
      const s = await h.makeStore({ leaseMs: LEASE_MS });
      const flowId = fid();
      const seeded: RunId[] = [];
      for (let i = 0; i < 5; i++) {
        const runId = rid();
        seeded.push(runId);
        await startRun(s, runId, {
          flowId,
          at: new Date(1_700_000_000_000 + i * 1000),
        });
      }
      const seen: RunId[] = [];
      let cursor: string | null = null;
      let pages = 0;
      do {
        const page = await s.queryRuns({
          where: { flowId },
          limit: 2,
          ...(cursor !== null ? { cursor } : {}),
        });
        ok(page.runs.length <= 2, "page size");
        seen.push(...page.runs.map((r) => r.runId));
        cursor = page.cursor;
        pages++;
      } while (cursor !== null && pages < 10);
      eq(pages, 3, "three pages");
      eq([...seen].sort(), [...seeded].sort(), "every row once");
    },
  },
  {
    name: "queryRuns: rejects a malformed cursor",
    async run(h) {
      const s = await h.makeStore({ leaseMs: LEASE_MS });
      await rejects(s.queryRuns({ cursor: "not-a-cursor" }), "throws");
    },
  },
  {
    name: "queryRuns: a non-positive or non-integer limit falls back to the default",
    async run(h) {
      const s = await h.makeStore({ leaseMs: LEASE_MS });
      const flowId = fid();
      for (let i = 0; i < 3; i++) await startRun(s, rid(), { flowId });
      eq(
        (await s.queryRuns({ where: { flowId }, limit: 0 })).runs.length,
        3,
        "0",
      );
      eq(
        (await s.queryRuns({ where: { flowId }, limit: 1.5 })).runs.length,
        3,
        "1.5",
      );
      eq(
        (await s.queryRuns({ where: { flowId }, limit: 2 })).runs.length,
        2,
        "2",
      );
    },
  },
  {
    name: "describe: returns null for an unknown runId",
    async run(h) {
      const s = await h.makeStore({ leaseMs: LEASE_MS });
      eq(await s.describe(rid()), null, "unknown");
    },
  },
  {
    name: "describe / listChildren: reflect run, steps, parent and children",
    async run(h) {
      const s = await h.makeStore({ leaseMs: LEASE_MS });
      const parent = rid();
      const child = rid();
      const startedAt = new Date(1_700_000_000_000);
      await startRun(s, parent, {
        flowId: "p",
        input: { n: 1 },
        at: startedAt,
      });
      await startRun(s, child, {
        flowId: "c",
        parent: { runId: parent, stepId: "sub" },
      });
      await startStep(s, parent, "sub", { kind: "subflow" });
      await s.settleStep(
        parent,
        "sub",
        Facts.stepCompleted(
          parent,
          "sub",
          A1,
          { childRunId: child },
          new Date(),
        ),
      );
      await s.appendFact(
        parent,
        Facts.flowCompleted(parent, { done: true }, new Date()),
      );

      const p = await s.describe(parent);
      ok(p !== null, "parent described");
      eq(p.run.runId, parent, "runId");
      eq(p.run.flowId, "p", "flowId");
      eq(p.run.status, "completed", "status");
      eq(p.run.startedAt, startedAt, "startedAt");
      eq(p.run.input, { n: 1 }, "input");
      eq(p.run.output, { done: true }, "output");
      ok(p.run.completedAt instanceof Date, "completedAt");
      eq(p.run.children, [child], "children");
      eq(p.run.parent, undefined, "root has no parent");
      eq(p.steps.length, 1, "one step");
      const step = p.steps[0];
      eq(step?.stepId, "sub", "stepId");
      eq(step?.attempt, A1, "attempt");
      eq(step?.status, "completed", "step status");
      eq(step?.output, { childRunId: child }, "step output");
      ok(step?.startedAt instanceof Date, "step startedAt");
      ok(step?.completedAt instanceof Date, "step completedAt");

      const c = await s.describe(child);
      eq(c?.run.parent, { runId: parent, stepId: "sub" }, "child parent link");
      eq(c?.run.children, [], "child has no children");
      eq(await s.listChildren(parent), [child], "listChildren");
      eq(await s.listChildren(child), [], "listChildren leaf");
    },
  },
  {
    name: "pruneFacts: never prunes non-terminal runs; respects olderThan and statuses",
    async run(h) {
      const s = await h.makeStore({ leaseMs: LEASE_MS });
      const old = new Date(1_700_000_000_000);
      const cutoff = new Date(1_700_000_100_000);
      const recent = new Date(1_700_000_200_000);
      const oldCompleted = rid();
      const recentCompleted = rid();
      const oldFailed = rid();
      const running = rid();
      await startRun(s, oldCompleted, { at: old });
      await endRun(s, oldCompleted, "completed", old);
      await startRun(s, recentCompleted, { at: old });
      await endRun(s, recentCompleted, "completed", recent);
      await startRun(s, oldFailed, { at: old });
      await endRun(s, oldFailed, "failed", old);
      await startRun(s, running, { at: old });

      const first = await s.pruneFacts({
        olderThan: cutoff,
        statuses: ["completed"],
        batchSize: 100,
        keepSummary: true,
      });
      eq(
        first,
        { runsPruned: 1, factsPruned: 2 },
        "only the old completed run",
      );
      eq((await s.loadRunState(oldCompleted)).facts, [], "pruned facts gone");
      ok(
        (await s.loadRunState(recentCompleted)).facts.length > 0,
        "recent kept",
      );
      ok((await s.loadRunState(oldFailed)).facts.length > 0, "failed kept");
      ok((await s.loadRunState(running)).facts.length > 0, "running kept");

      const second = await s.pruneFacts({
        olderThan: cutoff,
        statuses: ["failed", "canceled"],
        batchSize: 100,
        keepSummary: true,
      });
      eq(second.runsPruned, 1, "failed pruned by statuses");
      ok(
        (await s.loadRunState(running)).facts.length > 0,
        "running still kept",
      );
    },
  },
  {
    name: "pruneFacts: one call drains every eligible run regardless of batchSize",
    async run(h) {
      const s = await h.makeStore({ leaseMs: LEASE_MS });
      const old = new Date(1_700_000_000_000);
      for (let i = 0; i < 5; i++) {
        const runId = rid();
        await startRun(s, runId, { at: old });
        await endRun(s, runId, "completed", new Date(old.getTime() + i));
      }
      const r = await s.pruneFacts({
        olderThan: new Date(1_700_000_100_000),
        statuses: ["completed"],
        batchSize: 2,
        keepSummary: true,
      });
      eq(r, { runsPruned: 5, factsPruned: 10 }, "all drained");
    },
  },
  {
    name: "pruneFacts keepSummary:true: the run stays queryable and describable with no steps, and its runId stays reserved",
    async run(h) {
      const s = await h.makeStore({ leaseMs: LEASE_MS });
      const flowId = fid();
      const runId = rid();
      const old = new Date(1_700_000_000_000);
      await startRun(s, runId, { flowId, input: { keep: 1 }, at: old });
      await startStep(s, runId, "s");
      await s.settleStep(
        runId,
        "s",
        Facts.stepCompleted(runId, "s", A1, null, old),
      );
      await endRun(s, runId, "completed", old);
      await s.pruneFacts({
        olderThan: new Date(1_700_000_100_000),
        statuses: ["completed"],
        batchSize: 100,
        keepSummary: true,
      });
      const q = await s.queryRuns({ where: { flowId } });
      eq(q.runs.length, 1, "still listed");
      eq(q.runs[0]?.runId, runId, "runId");
      eq(q.runs[0]?.status, "completed", "status");
      eq(q.runs[0]?.input, { keep: 1 }, "input");
      const d = await s.describe(runId);
      eq(d?.run.status, "completed", "describe status");
      eq(d?.run.input, { keep: 1 }, "describe input");
      eq(d?.steps, [], "no steps");
      eq(
        (await startRun(s, runId, { flowId })).started,
        false,
        "runId reserved",
      );
      eq((await s.loadRunState(runId)).facts, [], "facts gone");
    },
  },
  {
    name: "pruneFacts keepSummary:false: the run disappears entirely",
    async run(h) {
      const s = await h.makeStore({ leaseMs: LEASE_MS });
      const flowId = fid();
      const runId = rid();
      const old = new Date(1_700_000_000_000);
      await startRun(s, runId, { flowId, at: old });
      await endRun(s, runId, "completed", old);
      await s.pruneFacts({
        olderThan: new Date(1_700_000_100_000),
        statuses: ["completed"],
        batchSize: 100,
        keepSummary: false,
      });
      eq((await s.queryRuns({ where: { flowId } })).runs, [], "not listed");
      eq(await s.describe(runId), null, "not describable");
      eq(
        (await startRun(s, runId, { flowId })).started,
        true,
        "runId free again",
      );
    },
  },
  {
    name: "pruneFacts: cascades to leases, timers and onces of the pruned run",
    async run(h) {
      const s = await h.makeStore({ leaseMs: LEASE_MS });
      const runId = rid();
      const old = new Date(1_700_000_000_000);
      await startRun(s, runId, { at: old });
      await startStep(s, runId, "gate", { kind: "signal" });
      ok((await s.claimStep(runId, "gate", A1)) !== null, "claim");
      await s.upsertTimer(runId, "gate", new Date(Date.now() - 1000));
      await s.recordOnce(runId, "gate", "scope", { v: 1 });
      await endRun(s, runId, "completed", old);
      await s.pruneFacts({
        olderThan: new Date(1_700_000_100_000),
        statuses: ["completed"],
        batchSize: 100,
        keepSummary: false,
      });
      eq(await s.getOnce(runId, "gate", "scope"), null, "once gone");
      await claimableAgain(s, runId, "gate", "pruneFacts");
      eq(await s.sweepSignalTimeouts({ now: new Date() }), [], "timer gone");
    },
  },
];
