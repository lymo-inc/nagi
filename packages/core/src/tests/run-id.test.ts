import { describe, expect, it } from "vitest";
import { NagiValidationError } from "../errors";
import { RunId } from "../run-id";

describe("RunId.parse", () => {
  it("accepts a non-empty string and returns a branded RunId", () => {
    const id = RunId.parse("run-abc-123");
    expect(id).toBe("run-abc-123");
  });

  it("rejects empty / whitespace-only strings with NagiValidationError", () => {
    expect(() => RunId.parse("")).toThrow(NagiValidationError);
    expect(() => RunId.parse(" ")).toThrow(NagiValidationError);
    expect(() => RunId.parse("\t")).toThrow(NagiValidationError);
    expect(() => RunId.parse("run with spaces")).toThrow(NagiValidationError);
  });
});

describe("RunId.fromTrusted", () => {
  it("brands without validation", () => {
    // bypass mode — intentionally weird input still brands
    const id = RunId.fromTrusted("anything goes here");
    expect(id).toBe("anything goes here");
  });
});

describe("RunId brand", () => {
  it("survives JSON round-trip", () => {
    const id = RunId.parse("run-roundtrip-1");
    const json = JSON.stringify({ runId: id });
    const parsed = JSON.parse(json) as { runId: string };
    const back = RunId.parse(parsed.runId);
    expect(back).toBe(id);
  });
});
