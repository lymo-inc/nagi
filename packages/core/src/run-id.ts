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
