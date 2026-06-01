import { describe, expect, it } from "vitest";
import { NagiValidationError } from "../errors";
import { deriveChildRunId, RunId } from "../run-id";

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

describe("deriveChildRunId", () => {
  const base = {
    runId: RunId.fromTrusted("run-parent-1"),
    stepId: "sub",
    generation: 0,
  };

  it("is deterministic for the same (runId, stepId, generation)", async () => {
    expect(await deriveChildRunId(base)).toBe(await deriveChildRunId(base));
  });

  it("differs by generation, stepId, and parent runId", async () => {
    const id = await deriveChildRunId(base);
    // A replay (new generation) gets a fresh child...
    expect(await deriveChildRunId({ ...base, generation: 1 })).not.toBe(id);
    expect(await deriveChildRunId({ ...base, stepId: "other" })).not.toBe(id);
    expect(
      await deriveChildRunId({
        ...base,
        runId: RunId.fromTrusted("run-parent-2"),
      }),
    ).not.toBe(id);
  });

  it("produces a run- prefixed, RFC-4122 v5-shaped id", async () => {
    expect(await deriveChildRunId(base)).toMatch(
      /^run-[0-9a-f]{8}-[0-9a-f]{4}-5[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/,
    );
  });
});
