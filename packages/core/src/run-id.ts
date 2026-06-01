import { validationError } from "./errors";
import type { RunId as RunIdType } from "./types";

// Runtime constructor for the RunId brand. `parse` validates for consumer
// input; `fromTrusted` brands a DB-read string without re-validation. We
// re-export the brand type from this module so consumers can use a single
// `RunId` identifier as both type and value.
export type RunId = RunIdType;

export const RunId = {
  parse(s: string): RunId {
    if (typeof s !== "string" || s.length === 0) {
      throw validationError("RunId.parse: must be a non-empty string", [
        "runId",
      ]);
    }
    if (/\s/.test(s)) {
      throw validationError("RunId.parse: must not contain whitespace", [
        "runId",
      ]);
    }
    return s as RunId;
  },
  fromTrusted(s: string): RunId {
    return s as RunId;
  },
} as const;

// Deterministic, name-based RunId for a subflow child, derived from its parent
// (runId, stepId) and a replay `generation`. Durable execution is
// at-least-once: a parent subflow step is re-dispatched on redelivery, on
// lease-reap (which re-enqueues at attempt+1), and on the durable child-wake —
// all for the SAME logical spawn. Keying on `attempt` would mint a different id
// on a reaper re-dispatch, and cancel-in-progress would self-supersede the
// first child, failing the parent (the 2026-05-29 incident). So the id is
// INVARIANT under attempt: any re-dispatch of the same generation resolves to
// the SAME child and tryStartRun re-attaches (idempotent). `generation` is the
// count of step.reset facts for this step — it is 0 on the original run, +1 per
// replay (nagi#6), and unchanged by reaping/retries — so a genuine replay gets
// a FRESH child while a reaper re-dispatch does not. RFC-4122 v5-shaped
// (SHA-256 truncated to 16 bytes; version + variant bits set) so it is
// indistinguishable from a minted random id. Async + Web-Crypto (crypto.subtle)
// to stay runtime-agnostic, matching canonicalize.
export async function deriveChildRunId(key: {
  readonly runId: RunId;
  readonly stepId: string;
  readonly generation: number;
}): Promise<RunId> {
  const seed = `nagi:subflow:${key.runId}:${key.stepId}:gen${key.generation}`;
  const digest = await crypto.subtle.digest(
    "SHA-256",
    new TextEncoder().encode(seed),
  );
  const bytes = new Uint8Array(digest).subarray(0, 16);
  bytes[6] = ((bytes[6] ?? 0) & 0x0f) | 0x50; // version 5
  bytes[8] = ((bytes[8] ?? 0) & 0x3f) | 0x80; // RFC 4122 variant
  const hex = Array.from(bytes, (b) => b.toString(16).padStart(2, "0")).join(
    "",
  );
  const uuid = `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20, 32)}`;
  return `run-${uuid}` as RunId;
}
