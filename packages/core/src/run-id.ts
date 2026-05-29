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
// (runId, stepId, attempt). Durable execution is at-least-once: a parent
// subflow step can be re-delivered and re-execute its spawn. Minting a random
// id each time would create a second child that cancel-in-progress then
// self-supersedes, failing the parent. A deterministic id means a re-delivery
// of the SAME attempt resolves to the SAME child (tryStartRun re-attaches), so
// the spawn is idempotent — while a genuine retry (new attempt) still gets a
// fresh child. RFC-4122 v5-shaped (SHA-256 truncated to 16 bytes; version and
// variant bits set) so it is indistinguishable from a minted random id. Async
// + Web-Crypto (crypto.subtle) to stay runtime-agnostic, matching canonicalize.
export async function deriveChildRunId(parent: {
  readonly runId: RunId;
  readonly stepId: string;
  readonly attempt: number;
}): Promise<RunId> {
  const seed = `nagi:subflow:${parent.runId}:${parent.stepId}:${parent.attempt}`;
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
