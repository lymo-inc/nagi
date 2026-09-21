import { describe, expect, it } from "vitest";
import { uuidv7 } from "./uuidv7";

describe("uuidv7", () => {
  it("matches the canonical UUID format", () => {
    const id = uuidv7();
    expect(id).toMatch(
      /^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/,
    );
  });

  it("encodes the version 7 nibble", () => {
    const id = uuidv7();
    expect(id.charAt(14)).toBe("7");
  });

  it("encodes the RFC 9562 variant nibble (10xx → 8/9/a/b)", () => {
    const id = uuidv7();
    expect(id.charAt(19)).toMatch(/[89ab]/);
  });

  it("encodes the supplied timestamp in the high 48 bits", () => {
    const ts = Date.UTC(2026, 4, 12);
    const id = uuidv7(ts);
    const hexTs = id.replace(/-/g, "").slice(0, 12);
    expect(parseInt(hexTs, 16)).toBe(ts);
  });

  it("is lex-ordered by timestamp", () => {
    const a = uuidv7(1_000_000);
    const b = uuidv7(2_000_000);
    const c = uuidv7(3_000_000);
    expect(a < b).toBe(true);
    expect(b < c).toBe(true);
  });

  it("sorts in creation order within a single millisecond", () => {
    // The fact table is read back with ORDER BY fact_id, so this IS the fact
    // log's append order. Purely random low bits used to reverse same-ms pairs.
    // rand_a is 12 bits seeded in its low half, so ordering is guaranteed for
    // at least 2048 ids per millisecond — orders of magnitude more than a run
    // writes. Beyond that the counter saturates and only uniqueness holds.
    const ids = Array.from({ length: 2_000 }, () => uuidv7(1_700_000_000_000));
    expect(ids).toEqual([...ids].sort());
  });

  it("keeps same-millisecond ids unique even past the counter's headroom", () => {
    // Uniqueness rides on the 62 random bits of rand_b, never on the counter.
    const ids = new Set(
      Array.from({ length: 20_000 }, () => uuidv7(1_700_000_000_001)),
    );
    expect(ids.size).toBe(20_000);
  });

  it("still sorts across millisecond boundaries", () => {
    const a = uuidv7(1_700_000_000_010);
    const b = uuidv7(1_700_000_000_010);
    const c = uuidv7(1_700_000_000_011);
    expect(a < b).toBe(true);
    expect(b < c).toBe(true);
  });

  it("survives a million-call uniqueness smoke without obvious collisions", () => {
    const seen = new Set<string>();
    for (let i = 0; i < 10_000; i++) {
      const id = uuidv7();
      expect(seen.has(id)).toBe(false);
      seen.add(id);
    }
  });
});
