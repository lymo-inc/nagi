import {
  getDef,
  matchArms,
  needsStepIds,
  type MatchArmDef,
  type MatchDef,
  type SignalDef,
  type StepDef,
  type TaskDef,
} from "./internal";
import type {
  BackoffStrategy,
  Flow,
  Millis,
  RetryPolicy,
  StandardSchemaV1,
  StepId,
  StepKind,
} from "./types";

/**
 * Canonical retry shape. Defaults are materialized explicitly so equivalent
 * retry configs hash identically regardless of source-side omission style.
 * `retryOn` is dropped — it's a function, not serializable.
 */
export interface CanonicalRetryPolicy {
  readonly maxAttempts: number;
  readonly backoff: BackoffStrategy;
  readonly initialDelayMs: Millis;
  readonly maxDelayMs: Millis;
}

/**
 * Canonical schema fingerprint. v0 hashes `validate.toString()` alongside
 * runtime-stable `vendor` + `version` — best-effort, breaks under minification.
 * Documented as a known limit in RFC 0001.
 */
export interface CanonicalSchema {
  readonly vendor: string;
  readonly version: number;
  readonly validateHash: string;
}

export interface CanonicalMatchArm {
  readonly id: string;
  readonly otherwise?: true;
  readonly whenHash?: string;
  /** Namespaced IDs of nested steps for this arm, sorted lexicographically. */
  readonly stepIds: readonly StepId[];
}

export interface CanonicalStep {
  readonly id: StepId;
  readonly kind: StepKind;
  /** Upstream step ids the step depends on, sorted lexicographically. */
  readonly needs: readonly StepId[];
  readonly whenHash?: string;
  readonly retry?: CanonicalRetryPolicy;
  readonly timeoutMs?: Millis;
  // signal-only
  readonly signalSchema?: CanonicalSchema;
  // match-only
  readonly matchMode?: "discriminator" | "guard";
  readonly matchOnHash?: string;
  readonly matchArms?: readonly CanonicalMatchArm[];
}

export interface CanonicalDag {
  readonly flowId: string;
  readonly inputSchema: CanonicalSchema;
  /** Steps sorted by `id` lexicographically — insertion order is ignored. */
  readonly steps: readonly CanonicalStep[];
}

/**
 * Default retry policy. Mirrors `DEFAULT_RETRY` in `dispatch.ts` — kept in
 * sync manually; both must drift together if either changes.
 */
const DEFAULT_RETRY: CanonicalRetryPolicy = {
  maxAttempts: 3,
  backoff: "exponential",
  initialDelayMs: 1_000,
  maxDelayMs: 60_000,
};

let warnedAboutSourceHash = false;
function warnSourceHashOnce(): void {
  if (warnedAboutSourceHash) return;
  warnedAboutSourceHash = true;
  // biome-ignore lint/suspicious/noConsole: one-time runtime advisory by design
  console.warn(
    "[nagi] canonicalize: hashing `when`/schema source via Function.prototype.toString(). " +
      "Cosmetic source changes (minification, whitespace, identifier renames) will flip " +
      "the flow hash even when behavior is unchanged. See RFC 0001.",
  );
}

/**
 * Build the canonical DAG for a flow.
 *
 * Stable across cosmetic source changes (step-key reorder, needs-key reorder)
 * and different across topology changes (added/removed steps, edge changes,
 * `when` flips, retry edits, timeout edits).
 *
 * Handler `run` bodies are intentionally NOT part of the hash — see RFC 0001
 * "Topology vs handler code." Use `nagi({ codeVersion })` (typically a git
 * SHA) to capture handler-code drift orthogonally.
 *
 * Async because predicate/schema source bodies are sha256'd via
 * `crypto.subtle` so the stored `dag` JSON stays compact.
 */
export async function canonicalize(flow: Flow): Promise<CanonicalDag> {
  const ids = Object.keys(flow.steps).sort();
  const steps: CanonicalStep[] = [];
  for (const id of ids) {
    const step = flow.steps[id];
    if (step === undefined) continue;
    steps.push(await canonicalizeStep(id, getDef(step)));
  }
  return {
    flowId: flow.id,
    inputSchema: await canonicalizeSchema(flow.input),
    steps,
  };
}

async function canonicalizeStep(
  id: StepId,
  def: StepDef,
): Promise<CanonicalStep> {
  const needs = [...needsStepIds(def)].sort();
  const base: CanonicalStep = { id, kind: def.kind, needs };
  if (def.kind === "task") return canonicalizeTask(base, def);
  if (def.kind === "signal") return canonicalizeSignal(base, def);
  return canonicalizeMatch(base, def);
}

async function canonicalizeTask(
  base: CanonicalStep,
  def: TaskDef,
): Promise<CanonicalStep> {
  const out: Mutable<CanonicalStep> = { ...base };
  if (def.when !== undefined) out.whenHash = await hashFnSource(def.when);
  if (def.retry !== undefined) out.retry = normalizeRetry(def.retry);
  if (def.timeoutMs !== undefined) out.timeoutMs = def.timeoutMs;
  return out;
}

async function canonicalizeSignal(
  base: CanonicalStep,
  def: SignalDef,
): Promise<CanonicalStep> {
  const out: Mutable<CanonicalStep> = { ...base };
  if (def.when !== undefined) out.whenHash = await hashFnSource(def.when);
  if (def.timeoutMs !== undefined) out.timeoutMs = def.timeoutMs;
  out.signalSchema = await canonicalizeSchema(def.schema);
  return out;
}

async function canonicalizeMatch(
  base: CanonicalStep,
  def: MatchDef,
): Promise<CanonicalStep> {
  const out: Mutable<CanonicalStep> = { ...base };
  out.matchMode = def.mode;
  if (def.mode === "discriminator") {
    out.matchOnHash = await hashFnSource(def.on);
  }
  const arms: CanonicalMatchArm[] = [];
  for (const arm of matchArms(def)) {
    arms.push(await canonicalizeArm(arm));
  }
  out.matchArms = arms;
  return out;
}

async function canonicalizeArm(arm: MatchArmDef): Promise<CanonicalMatchArm> {
  const out: Mutable<CanonicalMatchArm> = {
    id: arm.id,
    stepIds: [...arm.stepIds].sort(),
  };
  if (arm.otherwise) out.otherwise = true;
  if (arm.when !== undefined) out.whenHash = await hashFnSource(arm.when);
  return out;
}

function normalizeRetry(retry: RetryPolicy): CanonicalRetryPolicy {
  return {
    maxAttempts: retry.maxAttempts,
    backoff: retry.backoff,
    initialDelayMs: retry.initialDelayMs ?? DEFAULT_RETRY.initialDelayMs,
    maxDelayMs: retry.maxDelayMs ?? DEFAULT_RETRY.maxDelayMs,
  };
}

async function canonicalizeSchema(
  schema: StandardSchemaV1,
): Promise<CanonicalSchema> {
  const props = schema["~standard"];
  return {
    vendor: props.vendor,
    version: props.version,
    validateHash: await hashFnSource(props.validate),
  };
}

async function hashFnSource(fn: (...args: never[]) => unknown): Promise<string> {
  warnSourceHashOnce();
  return sha256Hex(fn.toString());
}

/**
 * Compute the sha256 hex digest of a canonical DAG. Uses Web Crypto
 * (`crypto.subtle.digest`) which is available in Node ≥ 19, modern browsers,
 * Cloudflare Workers, Deno, and Bun.
 *
 * The DAG is serialized via {@link stableStringify} so object-key order is
 * deterministic. Two equivalent DAGs produce byte-identical input bytes.
 */
export async function sha256Canonical(dag: CanonicalDag): Promise<string> {
  return sha256Hex(stableStringify(dag));
}

/**
 * Deterministic JSON serializer. Object keys are emitted in ascending
 * lexicographic order; `undefined` entries are skipped (matching `JSON.stringify`).
 *
 * Arrays preserve order — canonicalize.ts is responsible for sorting any
 * array whose hash-relevant ordering is otherwise unstable (steps, needs,
 * arm step ids). Match arms preserve declaration order because top-to-bottom
 * evaluation is semantically meaningful for guard mode.
 */
export function stableStringify(value: unknown): string {
  if (value === undefined) return "null";
  if (value === null || typeof value !== "object") return JSON.stringify(value);
  if (Array.isArray(value)) {
    return `[${value.map(stableStringify).join(",")}]`;
  }
  const entries = Object.entries(value as Record<string, unknown>)
    .filter(([, v]) => v !== undefined)
    .sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0));
  return `{${entries
    .map(([k, v]) => `${JSON.stringify(k)}:${stableStringify(v)}`)
    .join(",")}}`;
}

async function sha256Hex(input: string): Promise<string> {
  const bytes = new TextEncoder().encode(input);
  const digest = await crypto.subtle.digest("SHA-256", bytes);
  return hex(new Uint8Array(digest));
}

function hex(bytes: Uint8Array): string {
  let s = "";
  for (let i = 0; i < bytes.length; i++) {
    s += (bytes[i] as number).toString(16).padStart(2, "0");
  }
  return s;
}

type Mutable<T> = { -readonly [K in keyof T]: T[K] };
