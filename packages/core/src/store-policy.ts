import type {
  Fact,
  Json,
  PrunableStatus,
  RunId,
  RunStatus,
  StepId,
} from "./types";

// Store policy the adapters must not re-decide. An adapter owns only its
// transaction boundary (locks, rows, maps); every decision below is pure and
// shared, so two stores can only disagree by not calling it. Companions:
// decideSignal / decideTimeout (signals.ts), decideExpiredLeaseAction
// (lease-reaper.ts). `storeContract` (./testing) asserts each one per adapter.

export const QUERY_RUNS_DEFAULT_LIMIT = 50;
export const QUERY_RUNS_MAX_LIMIT = 500;

export function clampQueryLimit(limit: number | undefined): number {
  if (limit === undefined) return QUERY_RUNS_DEFAULT_LIMIT;
  if (!Number.isInteger(limit) || limit < 1) return QUERY_RUNS_DEFAULT_LIMIT;
  return Math.min(limit, QUERY_RUNS_MAX_LIMIT);
}

export interface RunCursor {
  readonly startedAt: Date;
  readonly runId: RunId;
}

// queryRuns pages by (startedAt DESC, runId DESC); the cursor is the last row
// of the previous page and the next page is every row that sorts after it.
export function compareRunOrder(a: RunCursor, b: RunCursor): number {
  const t = b.startedAt.getTime() - a.startedAt.getTime();
  if (t !== 0) return t;
  return a.runId < b.runId ? 1 : a.runId > b.runId ? -1 : 0;
}

export function isPastCursor(row: RunCursor, cursor: RunCursor): boolean {
  return compareRunOrder(row, cursor) > 0;
}

// Wire format `{t, r}` (base64url JSON) is a Store.queryRuns contract: a cursor
// minted by one adapter must decode in another.
export function encodeRunCursor(c: RunCursor): string {
  const bytes = new TextEncoder().encode(
    JSON.stringify({ t: c.startedAt.getTime(), r: c.runId }),
  );
  let binary = "";
  for (const b of bytes) binary += String.fromCharCode(b);
  return btoa(binary)
    .replace(/\+/g, "-")
    .replace(/\//g, "_")
    .replace(/=+$/, "");
}

export function decodeRunCursor(s: string): RunCursor {
  try {
    const binary = atob(s.replace(/-/g, "+").replace(/_/g, "/"));
    const bytes = Uint8Array.from(binary, (ch) => ch.charCodeAt(0));
    const parsed: unknown = JSON.parse(new TextDecoder().decode(bytes));
    if (
      typeof parsed === "object" &&
      parsed !== null &&
      typeof (parsed as { t?: unknown }).t === "number" &&
      typeof (parsed as { r?: unknown }).r === "string"
    ) {
      const { t, r } = parsed as { t: number; r: string };
      return { startedAt: new Date(t), runId: r as RunId };
    }
    throw new Error("malformed cursor body");
  } catch (err) {
    throw new Error(
      `queryRuns: invalid cursor — ${err instanceof Error ? err.message : String(err)}`,
    );
  }
}

// Reference semantics for QueryRunsWhere.input: Postgres jsonb `@>`. Objects
// match when every needle key is contained; arrays when every needle element
// is contained in some haystack element; scalars by equality.
export function jsonContains(haystack: Json, needle: Json): boolean {
  if (Array.isArray(needle)) {
    if (!Array.isArray(haystack)) return false;
    return needle.every((n) => haystack.some((h) => jsonContains(h, n)));
  }
  if (needle !== null && typeof needle === "object") {
    if (
      haystack === null ||
      Array.isArray(haystack) ||
      typeof haystack !== "object"
    )
      return false;
    return Object.entries(needle).every(
      ([k, v]) => k in haystack && jsonContains(haystack[k] as Json, v as Json),
    );
  }
  return haystack === needle;
}

export const DEFAULT_SWEEP_LIMIT = 100;

// Expiry filter BEFORE limit (Postgres: `WHERE deadline < now LIMIT n`): a live
// row never occupies a slot, so an expired one cannot starve behind `limit`
// live ones however the adapter happens to iterate.
export function selectExpired<T>(
  items: Iterable<T>,
  args: {
    readonly now: Date;
    readonly limit: number;
    readonly deadline: (item: T) => Date;
  },
): T[] {
  const nowMs = args.now.getTime();
  const out: T[] = [];
  for (const item of items) {
    if (out.length >= args.limit) break;
    if (args.deadline(item).getTime() < nowMs) out.push(item);
  }
  return out;
}

export interface PruneCandidate {
  readonly runId: RunId;
  readonly status: RunStatus;
  readonly completedAt: Date | null;
}

// One pruneFacts call drains every eligible run; the adapter loops this until
// it returns empty, each batch its own atomic unit. Oldest-first ordering keeps
// concurrent pruners from contending on the same rows.
export function selectPruneBatch<T extends PruneCandidate>(
  candidates: Iterable<T>,
  opts: {
    readonly olderThan: Date;
    readonly statuses: ReadonlyArray<PrunableStatus>;
    readonly batchSize: number;
  },
): T[] {
  const cutoff = opts.olderThan.getTime();
  const wanted = new Set<RunStatus>(opts.statuses);
  const eligible: Array<{ readonly item: T; readonly at: number }> = [];
  for (const item of candidates) {
    if (item.completedAt === null || !wanted.has(item.status)) continue;
    const at = item.completedAt.getTime();
    if (at < cutoff) eligible.push({ item, at });
  }
  eligible.sort(
    (a, b) =>
      a.at - b.at ||
      (a.item.runId < b.item.runId ? -1 : a.item.runId > b.item.runId ? 1 : 0),
  );
  return eligible.slice(0, opts.batchSize).map((e) => e.item);
}

// What a fact releases besides being appended. Both stores apply this at their
// fact write, so "which settle path also drops the lease" is not a per-adapter
// question. `timer` is the signal-timeout deadline: released when the step
// resolves by delivery or is restarted. A deadline that fires consumes its own
// row (sweepSignalTimeouts), and the other terminal paths leave a stale timer
// to fire as a no-op rather than contend with the sweeper's lock order.
export type FactEffects =
  | { readonly tag: "none" }
  | {
      readonly tag: "release-step";
      readonly stepId: StepId;
      readonly timer: boolean;
    }
  | { readonly tag: "release-run" };

const NONE: FactEffects = { tag: "none" };
const RELEASE_RUN: FactEffects = { tag: "release-run" };

export function factEffects(fact: Fact): FactEffects {
  switch (fact.kind) {
    case "step.completed":
    case "step.reset":
      return { tag: "release-step", stepId: fact.stepId, timer: true };
    case "step.failed":
    case "step.canceled":
      return { tag: "release-step", stepId: fact.stepId, timer: false };
    case "flow.completed":
    case "flow.failed":
    case "flow.canceled":
      return RELEASE_RUN;
    case "flow.started":
    case "step.started":
    case "step.retried":
    case "step.skipped":
    case "step.abort-requested":
    case "signal.sent":
    case "signal.received":
    case "signal.buffered":
    case "once.recorded":
    case "match.arm-selected":
    case "lease.reaped":
      return NONE;
  }
}
