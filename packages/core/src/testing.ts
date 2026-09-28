import { NagiConcurrencyConflictError } from "./errors";
import { Facts, isRunEnd } from "./facts";
import { InMemoryQueue } from "./memory";
import { stepStateOf, stepStatusOf } from "./state";
import type {
  AttemptNumber,
  Json,
  Millis,
  PrunableStatus,
  Queue,
  RunEventEnvelope,
  RunId,
  StandardSchemaV1,
  StepId,
  StepKind,
  Store,
  StreamEvent,
  Tx,
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
  // MUST return an empty store whose claimStep lease lasts `leaseMs`, with any
  // `events` / `stream` transport it offers already live. Cases for a transport
  // the store does not offer pass vacuously.
  makeStore(opts: { readonly leaseMs: Millis }): Promise<Store>;
  // Runs `body` on a transaction the store's *OnTx methods accept, committing
  // when it resolves.
  withTx<T>(store: Store, body: (tx: Tx) => Promise<T>): Promise<T>;
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
  await s.endRun(runId, fact);
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

const TRANSPORT_WAIT_MS = 5_000;

// Transports may deliver asynchronously (Postgres: LISTEN/NOTIFY), so event and
// stream assertions poll up to a deadline instead of reading once.
async function eventually(cond: () => boolean, msg: string): Promise<void> {
  const deadline = Date.now() + TRANSPORT_WAIT_MS;
  while (!cond()) {
    ok(Date.now() < deadline, msg);
    await sleep(10);
  }
}

function recordRuns(s: Store): readonly RunEventEnvelope[] | null {
  if (s.events === undefined) return null;
  const seen: RunEventEnvelope[] = [];
  s.events.watchRuns((e) => seen.push(e));
  return seen;
}

function typesFor(seen: readonly RunEventEnvelope[], runId: RunId): string[] {
  return seen.filter((e) => e.runId === runId).map((e) => e.type);
}

async function drain(
  stream: AsyncIterable<StreamEvent<Json>>,
  what: string,
): Promise<StreamEvent<Json>[]> {
  const out: StreamEvent<Json>[] = [];
  const iter = stream[Symbol.asyncIterator]();
  for (;;) {
    const next = await Promise.race([
      iter.next(),
      sleep(TRANSPORT_WAIT_MS).then(() => null),
    ]);
    if (next === null) {
      await iter.return?.();
      throw new StoreContractViolation(`${what}: the stream never closed`);
    }
    if (next.done === true) return out;
    out.push(next.value);
  }
}

function failingQueue(): Queue {
  const fail = async (): Promise<never> => {
    throw new Error("queue unavailable");
  };
  const q: Queue = {
    enqueue: fail,
    dequeue: fail,
    ack: fail,
    nack: fail,
    extend: fail,
    withTx: () => q,
  };
  return q;
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
    // nagi#29: a phantom superseder is a canceled_by_run_id pointing at a run
    // row that does not exist. The canonical stores cannot produce one because
    // cancel-prior and insert-new share a transaction; this pins that property
    // on the CONTRACT so a custom Store cannot regress into the bug silently.
    name: "tryStartRun: a superseded run's canceledByRunId always references a run that exists",
    async run(h) {
      const s = await h.makeStore({ leaseMs: LEASE_MS });
      const flowId = fid();
      const a = rid();
      const b = rid();
      await startRun(s, a, { flowId, concurrencyKey: "k" });
      await startRun(s, b, { flowId, concurrencyKey: "k" });

      const superseder = (await s.describe(a))?.run.canceledByRunId;
      ok(superseder !== undefined, "a records a superseder");
      ok(
        (await s.describe(superseder as RunId)) !== null,
        "the superseder run row must exist — a dangling reference is nagi#29",
      );
      eq(
        (await s.loadRunState(superseder as RunId)).facts.length > 0,
        true,
        "the superseder must have a durable fact log, not just a reference",
      );
    },
  },
  {
    // The other half of the same atomicity claim: a start that does NOT happen
    // must not cancel anything. An implementation that cancels priors before
    // confirming its own insert leaves exactly the orphaned state nagi#29 saw.
    name: "tryStartRun: a refused start cancels no prior run",
    async run(h) {
      const s = await h.makeStore({ leaseMs: LEASE_MS });
      const flowId = fid();
      const a = rid();
      const b = rid();
      await startRun(s, a, { flowId, concurrencyKey: "k" });
      await startRun(s, b, { flowId, concurrencyKey: "k2" });

      // b is already known, so this start is refused. It must not take the
      // "k" slot from a on the way out.
      const refused = await startRun(s, b, { flowId, concurrencyKey: "k" });
      eq(refused.started, false, "second start for a known runId is refused");
      eq(refused.canceled.length, 0, "a refused start cancels nothing");
      eq(
        (await s.loadRunState(a)).phase.tag,
        "running",
        "the prior run on that key survives a refused start",
      );
    },
  },
  {
    // The third face of nagi#29, and the one the issue's hypothesis list
    // misses: tryStartRun is atomic, but nothing keeps the superseder ALIVE.
    // Retention that prunes "completed" while keeping "canceled" for audit
    // deletes the superseder and strands the victim's reference.
    name: "pruneFacts: pruning a superseder leaves no dangling canceledByRunId",
    async run(h) {
      const s = await h.makeStore({ leaseMs: LEASE_MS });
      const flowId = fid();
      const a = rid();
      const b = rid();
      await startRun(s, a, { flowId, concurrencyKey: "k" });
      await startRun(s, b, { flowId, concurrencyKey: "k" });
      eq((await s.describe(a))?.run.canceledByRunId, b, "b superseded a");

      await endRun(s, b, "completed");
      await s.pruneFacts({
        olderThan: new Date(Date.now() + 60_000),
        statuses: ["completed"],
        batchSize: 100,
        keepSummary: false,
      });

      const victim = await s.describe(a);
      ok(victim !== null, "a canceled run survives a completed-only policy");
      const ref = victim.run.canceledByRunId;
      if (ref !== undefined)
        ok(
          (await s.describe(ref)) !== null,
          "canceledByRunId must not outlive the run it names — nagi#29",
        );
    },
  },
  {
    name: "endRun: a run ends once — of racing ends exactly one is admitted, and a later one (or one on a never-started run) is refused and writes nothing",
    async run(h) {
      const s = await h.makeStore({ leaseMs: LEASE_MS });
      const runId = rid();
      await startRun(s, runId);
      const won = await Promise.all([
        s.endRun(
          runId,
          Facts.flowFailed(runId, { name: "E", message: "x" }, new Date()),
        ),
        s.endRun(
          runId,
          Facts.flowCanceled(
            runId,
            { cause: "explicit", reason: "race" },
            new Date(),
          ),
        ),
      ]);
      eq(won.filter(Boolean).length, 1, "exactly one racing end is admitted");
      const winner = won[0] === true ? "failed" : "canceled";

      ok(
        !(await s.endRun(runId, Facts.flowCompleted(runId, null, new Date()))),
        "an end on an already-ended run must resolve false",
      );
      const state = await s.loadRunState(runId);
      eq(
        state.facts.filter(isRunEnd).length,
        1,
        "a refused end is not persisted",
      );
      eq(state.phase.tag, winner, "the fold keeps the winner's end");
      eq((await s.describe(runId))?.run.status, winner, "so does the row");

      const ghost = rid();
      ok(
        !(await s.endRun(ghost, Facts.flowCompleted(ghost, null, new Date()))),
        "a run that never started cannot end",
      );
      eq((await s.loadRunState(ghost)).facts, [], "and nothing is written");
    },
  },
  {
    // NagiCanceledError is public: a handler can name any run as its canceler
    // and classifyFailure turns that claim into a concurrency-cause fact. The
    // fact is the record; the VIEW must not present an unresolvable claim as a
    // reference, or every consumer's audit finds nagi#29's orphan.
    name: "endRun(flow.canceled): a canceler that never existed is not surfaced as a reference",
    async run(h) {
      const s = await h.makeStore({ leaseMs: LEASE_MS });
      const victim = rid();
      await startRun(s, victim);
      await s.endRun(
        victim,
        Facts.flowCanceledByConcurrency({
          runId: victim,
          canceledByRunId: rid(),
          concurrencyKey: "k",
          at: new Date(),
        }),
      );

      const d = await s.describe(victim);
      ok(d !== null, "the canceled run is still describable");
      eq(d.run.status, "canceled", "the cancellation itself stands");
      eq(
        d.run.canceledByRunId,
        undefined,
        "an unresolvable canceler is omitted, not surfaced — nagi#29",
      );
      const facts = (await s.loadRunState(victim)).facts;
      eq(
        facts.some((f) => f.kind === "flow.canceled"),
        true,
        "the fact log keeps the claim verbatim",
      );
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
    name: "appendFact(step.completed): releases the lease — describe() drops lease.expiresAt and claimStep re-acquires",
    async run(h) {
      const s = await h.makeStore({ leaseMs: LEASE_MS });
      const runId = rid();
      await startRun(s, runId);
      await startStep(s, runId, "s");
      ok((await s.claimStep(runId, "s", A1)) !== null, "claim");
      const live = await stepView(s, runId, "s");
      ok(live?.lease?.expiresAt instanceof Date, "lease visible while held");
      await s.appendFact(
        runId,
        Facts.stepCompleted(runId, "s", A1, { ok: 1 }, new Date()),
      );
      const done = await stepView(s, runId, "s");
      eq(done?.status, "completed", "status");
      eq(done?.output, { ok: 1 }, "output");
      eq(done?.lease, undefined, "lease gone from describe()");
      await claimableAgain(s, runId, "s", "appendFact(step.completed)");
    },
  },
  {
    name: "appendFact(step.failed): releases the lease",
    async run(h) {
      const s = await h.makeStore({ leaseMs: LEASE_MS });
      const runId = rid();
      await startRun(s, runId);
      await startStep(s, runId, "s");
      ok((await s.claimStep(runId, "s", A1)) !== null, "claim");
      await s.appendFact(
        runId,
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
      await claimableAgain(s, runId, "s", "appendFact(step.failed)");
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
    name: "appendFact(step.reset): reopens a completed / failed run to running in both the fold and the read model; canceled stays canceled",
    async run(h) {
      const s = await h.makeStore({ leaseMs: LEASE_MS });
      for (const status of ["completed", "failed"] as const) {
        const flowId = fid();
        const runId = rid();
        await startRun(s, runId, { flowId });
        await startStep(s, runId, "s");
        await endRun(s, runId, status);
        await s.appendFact(
          runId,
          Facts.stepReset({ runId, stepId: "s", at: new Date() }),
        );
        eq(
          (await s.loadRunState(runId)).phase.tag,
          "running",
          `fold after flow.${status}`,
        );
        const { runs } = await s.queryRuns({
          where: { flowId, status: ["running"] },
        });
        eq(
          runs.map((r) => r.runId),
          [runId],
          `read model after flow.${status}`,
        );
      }
      const runId = rid();
      await startRun(s, runId);
      await startStep(s, runId, "s");
      await endRun(s, runId, "canceled");
      await s.appendFact(
        runId,
        Facts.stepReset({ runId, stepId: "s", at: new Date() }),
      );
      eq((await s.loadRunState(runId)).phase.tag, "canceled", "canceled");
    },
  },
  {
    name: "appendFact(step.reset): a reopened run re-takes its (flowId, key) slot — refused with NagiConcurrencyConflictError while another active run holds it",
    async run(h) {
      const s = await h.makeStore({ leaseMs: LEASE_MS });
      const flowId = fid();
      const a = rid();
      const b = rid();
      await startRun(s, a, { flowId, concurrencyKey: "k" });
      await startStep(s, a, "s");
      await endRun(s, a, "completed");
      eq(
        (await startRun(s, b, { flowId, concurrencyKey: "k" })).canceled,
        [],
        "slot free after a completed",
      );
      const reset = Facts.stepReset({ runId: a, stepId: "s", at: new Date() });
      let err: unknown;
      try {
        await s.appendFact(a, reset);
      } catch (e) {
        err = e;
      }
      ok(
        err instanceof NagiConcurrencyConflictError,
        "reopen while b holds the key must throw NagiConcurrencyConflictError",
      );
      eq((await s.loadRunState(a)).phase.tag, "completed", "a untouched");

      await endRun(s, b, "completed");
      await s.appendFact(a, reset);
      eq((await s.loadRunState(a)).phase.tag, "running", "a reopened");
      const res = await startRun(s, rid(), { flowId, concurrencyKey: "k" });
      eq(
        res.canceled.map((c) => c.runId),
        [a],
        "the reopened run holds the slot again",
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
      // Before re-claiming: the re-claim takes a fresh SHORT_LEASE_MS lease a
      // slow runner can let expire before a later sweep.
      eq(await s.sweepLeases({ now: new Date(), queue }), [], "restart-safe");
      await claimableAgain(s, runId, "s", "sweepLeases");
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
      await s.appendFact(
        settled,
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
    name: "recordOnce / getOnce: first write wins; miss when absent; a recorded null is a hit",
    async run(h) {
      const s = await h.makeStore({ leaseMs: LEASE_MS });
      const runId = rid();
      eq(await s.getOnce(runId, "s", "scope"), { tag: "miss" }, "absent");
      await s.recordOnce(runId, "s", "scope", { value: 1 });
      await s.recordOnce(runId, "s", "scope", { value: 2 });
      eq(
        await s.getOnce(runId, "s", "scope"),
        { tag: "hit", value: { value: 1 } },
        "first write wins",
      );
      eq(await s.getOnce(runId, "s", "other"), { tag: "miss" }, "scoped");
      await s.recordOnce(runId, "s", "nil", null);
      await s.recordOnce(runId, "s", "nil", { value: 3 });
      eq(
        await s.getOnce(runId, "s", "nil"),
        { tag: "hit", value: null },
        "recorded null",
      );
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
      await s.appendFact(
        runId,
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
      await s.appendFact(
        parent,
        Facts.stepCompleted(
          parent,
          "sub",
          A1,
          { childRunId: child },
          new Date(),
        ),
      );
      await s.endRun(
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
    name: "describe: a retried step is ONE view of its latest attempt, timestamped by its facts' `at`",
    async run(h) {
      const s = await h.makeStore({ leaseMs: LEASE_MS });
      const flowId = fid();
      const runId = rid();
      const t = (n: number) => new Date(1_700_000_000_000 + n * 1000);
      const error = { name: "E", message: "boom" };
      await startRun(s, runId, { flowId, at: t(0) });
      await s.appendFact(
        runId,
        Facts.stepStarted(runId, "s", A1, "task", t(1)),
      );
      await s.appendFact(
        runId,
        Facts.stepRetried(runId, "s", A1, t(3), error, t(2)),
      );
      eq(
        (await s.describe(runId))?.steps,
        [
          {
            stepId: "s",
            attempt: A1,
            status: "running",
            startedAt: t(1),
            error,
          },
        ],
        "backing off: the failed attempt, still running, with its error",
      );

      await s.appendFact(
        runId,
        Facts.stepStarted(runId, "s", A2, "task", t(3)),
      );
      await s.appendFact(
        runId,
        Facts.stepStarted(runId, "s", A1, "task", t(4)),
      );
      eq(
        (await s.describe(runId))?.steps,
        [{ stepId: "s", attempt: A2, status: "running", startedAt: t(3) }],
        "attempt 2 supersedes attempt 1; a stale attempt-1 start changes nothing",
      );
      eq(
        (await s.queryRuns({ where: { flowId, status: ["running"] } })).runs
          .length,
        1,
        "a retry leaves the run running",
      );

      await s.appendFact(
        runId,
        Facts.stepCompleted(runId, "s", A2, { v: 2 }, t(5)),
      );
      await endRun(s, runId, "completed", t(6));
      const d = await s.describe(runId);
      eq(
        d?.steps,
        [
          {
            stepId: "s",
            attempt: A2,
            status: "completed",
            startedAt: t(3),
            completedAt: t(5),
            output: { v: 2 },
          },
        ],
        "one view per step",
      );
      eq(d?.run.startedAt, t(0), "run startedAt");
      eq(d?.run.completedAt, t(6), "run completedAt");
      const [summary] = (
        await s.queryRuns({ where: { flowId, status: ["completed"] } })
      ).runs;
      eq(summary?.completedAt, t(6), "queryRuns completedAt");
    },
  },
  {
    name: "describe: a skipped step settles its view; a settled step changes only by reset",
    async run(h) {
      const s = await h.makeStore({ leaseMs: LEASE_MS });
      const runId = rid();
      const t = (n: number) => new Date(1_700_000_000_000 + n * 1000);
      const skip = (stepId: StepId, at: Date) =>
        s.appendFact(
          runId,
          Facts.stepSkipped({ runId, stepId, reason: "manual", at }),
        );
      await startRun(s, runId, { at: t(0) });
      await skip("never", t(1));
      await s.appendFact(
        runId,
        Facts.stepStarted(runId, "inflight", A1, "task", t(2)),
      );
      await skip("inflight", t(3));
      await s.appendFact(
        runId,
        Facts.stepCompleted(runId, "inflight", A1, { late: true }, t(4)),
      );
      eq(
        (await s.describe(runId))?.steps,
        [
          {
            stepId: "inflight",
            attempt: A1,
            status: "skipped",
            startedAt: t(2),
            completedAt: t(3),
          },
          {
            stepId: "never",
            attempt: 0,
            status: "skipped",
            completedAt: t(1),
          },
        ],
        "skipped views; the late completion is ignored",
      );

      await s.appendFact(
        runId,
        Facts.stepReset({ runId, stepId: "inflight", at: t(5) }),
      );
      eq(
        (await s.describe(runId))?.steps.map((st) => st.stepId),
        ["never"],
        "a reset step has no view until it restarts",
      );
      await s.appendFact(
        runId,
        Facts.stepStarted(runId, "inflight", A1, "task", t(6)),
      );
      eq(
        await stepView(s, runId, "inflight"),
        { stepId: "inflight", attempt: A1, status: "running", startedAt: t(6) },
        "restarted",
      );
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
      await s.appendFact(runId, Facts.stepCompleted(runId, "s", A1, null, old));
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
      eq(await s.getOnce(runId, "gate", "scope"), { tag: "miss" }, "once gone");
      await claimableAgain(s, runId, "gate", "pruneFacts");
      eq(await s.sweepSignalTimeouts({ now: new Date() }), [], "timer gone");
    },
  },
  {
    name: "tryStartRunOnTx: starts the run on the caller's tx; a second call for the same runId is a no-op",
    async run(h) {
      const s = await h.makeStore({ leaseMs: LEASE_MS });
      const runId = rid();
      const fact = Facts.flowStarted({
        runId,
        flowId: fid(),
        input: { x: 1 },
        at: new Date(),
      });
      const first = await h.withTx(s, (tx) =>
        s.tryStartRunOnTx(tx, runId, fact),
      );
      eq(first, { started: true, canceled: [] }, "first start");
      const again = await h.withTx(s, (tx) =>
        s.tryStartRunOnTx(tx, runId, { ...fact, input: { x: 2 } }),
      );
      eq(again, { started: false, canceled: [] }, "second start");
      const state = await s.loadRunState(runId);
      eq(state.phase.tag, "running", "running after commit");
      eq(state.input, { x: 1 }, "input unchanged");
      eq((await s.describe(runId))?.run.status, "running", "read model");
    },
  },
  {
    name: "tryStartRunOnTx: with concurrency, cancels the prior active run on the same (flowId, key) and returns its fact",
    async run(h) {
      const s = await h.makeStore({ leaseMs: LEASE_MS });
      const flowId = fid();
      const a = rid();
      const b = rid();
      await startRun(s, a, { flowId, concurrencyKey: "k" });
      const res = await h.withTx(s, (tx) =>
        s.tryStartRunOnTx(
          tx,
          b,
          Facts.flowStarted({ runId: b, flowId, input: {}, at: new Date() }),
          { key: "k", mode: "cancel-in-progress" },
        ),
      );
      ok(res.started, "b started");
      eq(
        res.canceled.map((c) => [c.runId, c.fact.canceledByRunId]),
        [[a, b]],
        "a canceled by b",
      );
      eq((await s.loadRunState(a)).phase.tag, "canceled", "a folds canceled");
      eq((await s.describe(a))?.run.canceledByRunId, b, "describe(a)");
    },
  },
  {
    name: "events: watchRuns sees flow.started from both tryStartRun and tryStartRunOnTx",
    async run(h) {
      const s = await h.makeStore({ leaseMs: LEASE_MS });
      const seen = recordRuns(s);
      if (seen === null) return;
      const own = rid();
      const onTx = rid();
      await startRun(s, own);
      await h.withTx(s, (tx) =>
        s.tryStartRunOnTx(
          tx,
          onTx,
          Facts.flowStarted({
            runId: onTx,
            flowId: "f",
            input: {},
            at: new Date(),
          }),
        ),
      );
      await eventually(
        () => typesFor(seen, own).includes("flow.started"),
        "tryStartRun must announce flow.started",
      );
      await eventually(
        () => typesFor(seen, onTx).includes("flow.started"),
        "tryStartRunOnTx must announce flow.started once its tx commits",
      );
    },
  },
  {
    name: "events: watchRun sees step events in order and ends on the terminal event",
    async run(h) {
      const s = await h.makeStore({ leaseMs: LEASE_MS });
      if (s.events === undefined) return;
      const runId = rid();
      const seen: RunEventEnvelope[] = [];
      s.events.watchRun(runId, (e) => seen.push(e));
      await startRun(s, runId);
      await startStep(s, runId, "s");
      await s.appendFact(
        runId,
        Facts.stepCompleted(runId, "s", A1, { ok: 1 }, new Date()),
      );
      await s.appendFact(
        runId,
        Facts.matchArmSelected(runId, "m", "arm", new Date()),
      );
      await endRun(s, runId, "completed");
      const want = [
        "flow.started",
        "step.started",
        "step.completed",
        "flow.completed",
      ];
      await eventually(() => seen.length >= want.length, "all four events");
      eq(
        seen.map((e) => e.type),
        want,
        "bookkeeping facts are not events",
      );
    },
  },
  {
    name: "events: a superseded run announces flow.canceled naming its superseder",
    async run(h) {
      const s = await h.makeStore({ leaseMs: LEASE_MS });
      const seen = recordRuns(s);
      if (seen === null) return;
      const flowId = fid();
      const a = rid();
      const b = rid();
      await startRun(s, a, { flowId, concurrencyKey: "k" });
      await startRun(s, b, { flowId, concurrencyKey: "k" });
      await eventually(
        () => seen.some((e) => e.runId === a && e.type === "flow.canceled"),
        "a's cancellation is announced",
      );
      eq(
        seen.find((e) => e.runId === a && e.type === "flow.canceled"),
        {
          type: "flow.canceled",
          cause: "concurrency",
          canceledByRunId: b,
          runId: a,
        },
        "cancel event",
      );
    },
  },
  {
    name: "stream: a subscriber receives published chunks, then closes on step.completed",
    async run(h) {
      const s = await h.makeStore({ leaseMs: LEASE_MS });
      if (s.stream === undefined) return;
      const runId = rid();
      await startRun(s, runId);
      await startStep(s, runId, "s", { kind: "streaming" });
      const events = drain(s.stream.subscribeStream(runId, "s"), "completed");
      s.stream.publishChunk(runId, "s", "a");
      s.stream.publishChunk(runId, "s", "b");
      await s.appendFact(
        runId,
        Facts.stepCompleted(runId, "s", A1, null, new Date()),
      );
      eq(
        await events,
        [
          { kind: "chunk", chunk: "a" },
          { kind: "chunk", chunk: "b" },
        ],
        "chunks in order, then closed",
      );
    },
  },
  {
    name: "stream: step.failed closes with the error; a run end closes a step that never emitted",
    async run(h) {
      const s = await h.makeStore({ leaseMs: LEASE_MS });
      if (s.stream === undefined) return;
      const runId = rid();
      await startRun(s, runId);
      await startStep(s, runId, "bad", { kind: "streaming" });
      await startStep(s, runId, "quiet", { kind: "streaming" });
      const bad = drain(s.stream.subscribeStream(runId, "bad"), "failed");
      const quiet = drain(s.stream.subscribeStream(runId, "quiet"), "run end");
      const error = { name: "E", message: "x" };
      await s.appendFact(
        runId,
        Facts.stepFailed(runId, "bad", A1, error, new Date()),
      );
      eq(await bad, [{ kind: "error", error }], "error event, then closed");
      await endRun(s, runId, "canceled");
      eq(await quiet, [], "closed by the run end");
    },
  },
  {
    name: "stream: subscribing to an already-settled step closes instead of hanging",
    async run(h) {
      const s = await h.makeStore({ leaseMs: LEASE_MS });
      if (s.stream === undefined) return;
      const runId = rid();
      await startRun(s, runId);
      await startStep(s, runId, "s", { kind: "streaming" });
      await s.appendFact(
        runId,
        Facts.stepCompleted(runId, "s", A1, null, new Date()),
      );
      eq(
        await drain(s.stream.subscribeStream(runId, "s"), "settled step"),
        [],
        "empty and closed",
      );
    },
  },
  {
    name: "sweepLeases: a failing enqueue leaves nothing half-reaped — the lease stays reapable and no lease.reaped is written",
    async run(h) {
      const s = await h.makeStore({ leaseMs: SHORT_LEASE_MS });
      const runId = rid();
      await startRun(s, runId);
      await startStep(s, runId, "s");
      ok((await s.claimStep(runId, "s", A1)) !== null, "claim");
      await sleep(PAST_LEASE_MS);
      await rejects(
        s.sweepLeases({ now: new Date(), queue: failingQueue() }),
        "the enqueue failure surfaces",
      );
      eq(
        (await s.loadRunState(runId)).facts.filter(
          (f) => f.kind === "lease.reaped",
        ).length,
        0,
        "no lease.reaped",
      );
      eq(
        (
          await s.sweepLeases({ now: new Date(), queue: new InMemoryQueue() })
        ).map((r) => r.runId),
        [runId],
        "the next sweep still reaps it",
      );
    },
  },
];
