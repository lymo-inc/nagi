import {
  getDef,
  type MatchArmDef,
  type MatchDef,
  matchArms,
  needsStepIds,
  type SignalDef,
  type StepDef,
  type SubflowDef,
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

export interface CanonicalRetryPolicy {
  readonly maxAttempts: number;
  readonly backoff: BackoffStrategy;
  readonly initialDelayMs: Millis;
  readonly maxDelayMs: Millis;
}

export interface CanonicalSchema {
  readonly vendor: string;
  readonly version: number;
  readonly validateHash: string;
}

export interface CanonicalMatchArm {
  readonly id: string;
  readonly otherwise?: true;
  readonly whenHash?: string;
  readonly stepIds: readonly StepId[];
}

export interface CanonicalStep {
  readonly id: StepId;
  readonly kind: StepKind;
  readonly needs: readonly StepId[];
  readonly whenHash?: string;
  readonly retry?: CanonicalRetryPolicy;
  readonly timeoutMs?: Millis;
  readonly signalSchema?: CanonicalSchema;
  readonly signalNames?: readonly string[];
  readonly matchMode?: "discriminator" | "guard";
  readonly matchOnHash?: string;
  readonly matchArms?: readonly CanonicalMatchArm[];
  readonly childFlowId?: string;
  readonly subflowInputHash?: string;
}

export interface CanonicalDag {
  readonly flowId: string;
  readonly inputSchema: CanonicalSchema;
  readonly steps: readonly CanonicalStep[];
}

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
      "the flow hash even when behavior is unchanged.",
  );
}

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
  if (def.kind === "subflow") return canonicalizeSubflow(base, def);
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
  if (def.names !== undefined) {
    out.signalNames = [...def.names].sort();
  }
  return out;
}

async function canonicalizeSubflow(
  base: CanonicalStep,
  def: SubflowDef,
): Promise<CanonicalStep> {
  const out: Mutable<CanonicalStep> = { ...base };
  if (def.when !== undefined) out.whenHash = await hashFnSource(def.when);
  if (def.timeoutMs !== undefined) out.timeoutMs = def.timeoutMs;
  out.childFlowId = def.childFlowId;
  out.subflowInputHash = await hashFnSource(def.buildInput);
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

async function hashFnSource(
  fn: (...args: never[]) => unknown,
): Promise<string> {
  warnSourceHashOnce();
  return sha256Hex(fn.toString());
}

export async function sha256Canonical(dag: CanonicalDag): Promise<string> {
  return sha256Hex(stableStringify(dag));
}

export async function fingerprintFlows(
  flows: ReadonlyArray<Flow>,
): Promise<string> {
  const entries: Array<readonly [string, string]> = [];
  for (const f of flows) {
    const dag = await canonicalize(f);
    entries.push([f.id, await sha256Canonical(dag)]);
  }
  entries.sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0));
  return sha256Hex(stableStringify(entries));
}

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
