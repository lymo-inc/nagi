import { describe, expectTypeOf, it } from "vitest";
import type {
  CanonicalDag,
  CanonicalMatchArm,
  CanonicalRetryPolicy,
  CanonicalSchema,
  CanonicalStep,
} from "./canonicalize";
import { canonicalize, sha256Canonical } from "./canonicalize";

describe("CanonicalDag — type shape", () => {
  it("CanonicalDag has flowId, inputSchema, steps", () => {
    expectTypeOf<CanonicalDag["flowId"]>().toEqualTypeOf<string>();
    expectTypeOf<CanonicalDag["inputSchema"]>().toEqualTypeOf<CanonicalSchema>();
    expectTypeOf<CanonicalDag["steps"]>().toEqualTypeOf<
      readonly CanonicalStep[]
    >();
  });

  it("CanonicalSchema is { vendor, version, validateHash }", () => {
    expectTypeOf<CanonicalSchema>().toEqualTypeOf<{
      readonly vendor: string;
      readonly version: number;
      readonly validateHash: string;
    }>();
  });

  it("CanonicalRetryPolicy fields are all required after normalization", () => {
    expectTypeOf<CanonicalRetryPolicy["maxAttempts"]>().toEqualTypeOf<number>();
    expectTypeOf<
      CanonicalRetryPolicy["initialDelayMs"]
    >().toEqualTypeOf<number>();
    expectTypeOf<CanonicalRetryPolicy["maxDelayMs"]>().toEqualTypeOf<number>();
  });

  it("CanonicalMatchArm has id, stepIds, optional whenHash and otherwise", () => {
    expectTypeOf<CanonicalMatchArm["id"]>().toEqualTypeOf<string>();
    expectTypeOf<CanonicalMatchArm["stepIds"]>().toEqualTypeOf<
      readonly string[]
    >();
    expectTypeOf<
      CanonicalMatchArm["whenHash"]
    >().toEqualTypeOf<string | undefined>();
    expectTypeOf<
      CanonicalMatchArm["otherwise"]
    >().toEqualTypeOf<true | undefined>();
  });

  it("canonicalize returns Promise<CanonicalDag>", () => {
    expectTypeOf<ReturnType<typeof canonicalize>>().toEqualTypeOf<
      Promise<CanonicalDag>
    >();
  });

  it("sha256Canonical returns Promise<string>", () => {
    expectTypeOf<ReturnType<typeof sha256Canonical>>().toEqualTypeOf<
      Promise<string>
    >();
  });
});
