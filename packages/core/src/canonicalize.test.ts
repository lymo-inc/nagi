import { describe, expect, it } from "vitest";
import { flow } from "./builder";
import { canonicalize, sha256Canonical, stableStringify } from "./canonicalize";
import { passthroughSchema } from "./test-helpers";

async function hashOf(f: Parameters<typeof canonicalize>[0]): Promise<string> {
  return sha256Canonical(await canonicalize(f));
}

describe("canonicalize — byte-stability invariants (same hash)", () => {
  it("is idempotent for the same flow", async () => {
    const make = () =>
      flow({
        id: "stable",
        input: passthroughSchema<{ x: number }>(),
        build: (b) => ({
          a: b.task({ run: async () => ({ v: 1 }) }),
          b: b.task({ run: async () => ({ v: 2 }) }),
        }),
      });
    expect(await hashOf(make())).toBe(await hashOf(make()));
  });

  it("ignores step-key insertion order", async () => {
    const f1 = flow({
      id: "order",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => {
        const a = b.task({ run: async () => ({ v: 1 }) });
        const z = b.task({ run: async () => ({ v: 2 }) });
        return { a, z };
      },
    });
    const f2 = flow({
      id: "order",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => {
        const z = b.task({ run: async () => ({ v: 2 }) });
        const a = b.task({ run: async () => ({ v: 1 }) });
        return { z, a };
      },
    });
    expect(await hashOf(f1)).toBe(await hashOf(f2));
  });

  it("ignores needs-key insertion order", async () => {
    const f1 = flow({
      id: "needs-order",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => {
        const a = b.task({ run: async () => ({ v: 1 }) });
        const c = b.task({ run: async () => ({ v: 2 }) });
        const z = b.task({
          needs: { x: a, y: c },
          run: async () => null,
        });
        return { a, c, z };
      },
    });
    const f2 = flow({
      id: "needs-order",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => {
        const a = b.task({ run: async () => ({ v: 1 }) });
        const c = b.task({ run: async () => ({ v: 2 }) });
        const z = b.task({
          needs: { y: c, x: a },
          run: async () => null,
        });
        return { a, c, z };
      },
    });
    expect(await hashOf(f1)).toBe(await hashOf(f2));
  });

  it("ignores `run` handler body changes (topology-only hashing)", async () => {
    const f1 = flow({
      id: "handler-ignored",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => ({
        only: b.task({ run: async () => ({ v: 1 }) }),
      }),
    });
    const f2 = flow({
      id: "handler-ignored",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => ({
        only: b.task({ run: async () => ({ v: 999, extra: "stuff" }) }),
      }),
    });
    expect(await hashOf(f1)).toBe(await hashOf(f2));
  });

  it("ignores per-handler closure shape for retry.retryOn (function, not serializable)", async () => {
    const f1 = flow({
      id: "retry-on",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => ({
        only: b.task({
          retry: {
            maxAttempts: 3,
            backoff: "exponential",
            retryOn: (e) => e instanceof Error,
          },
          run: async () => null,
        }),
      }),
    });
    const f2 = flow({
      id: "retry-on",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => ({
        only: b.task({
          retry: {
            maxAttempts: 3,
            backoff: "exponential",
            retryOn: (e) => typeof e === "object" && e !== null,
          },
          run: async () => null,
        }),
      }),
    });
    expect(await hashOf(f1)).toBe(await hashOf(f2));
  });
});

describe("canonicalize — byte-difference invariants (different hash)", () => {
  function single() {
    return flow({
      id: "base",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => ({
        only: b.task({ run: async () => ({ v: 1 }) }),
      }),
    });
  }

  it("differs when a step is added", async () => {
    const f1 = single();
    const f2 = flow({
      id: "base",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => ({
        only: b.task({ run: async () => ({ v: 1 }) }),
        extra: b.task({ run: async () => ({ v: 2 }) }),
      }),
    });
    expect(await hashOf(f1)).not.toBe(await hashOf(f2));
  });

  it("differs when an edge is added", async () => {
    const f1 = flow({
      id: "edges",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => {
        const a = b.task({ run: async () => ({ v: 1 }) });
        const z = b.task({ run: async () => ({ v: 2 }) });
        return { a, z };
      },
    });
    const f2 = flow({
      id: "edges",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => {
        const a = b.task({ run: async () => ({ v: 1 }) });
        const z = b.task({
          needs: { a },
          run: async () => ({ v: 2 }),
        });
        return { a, z };
      },
    });
    expect(await hashOf(f1)).not.toBe(await hashOf(f2));
  });

  it("differs when a `when` predicate is flipped", async () => {
    const f1 = flow({
      id: "when-flip",
      input: passthroughSchema<{ ok: boolean }>(),
      build: (b) => ({
        only: b.task({
          when: ({ input }) => input.ok === true,
          run: async () => null,
        }),
      }),
    });
    const f2 = flow({
      id: "when-flip",
      input: passthroughSchema<{ ok: boolean }>(),
      build: (b) => ({
        only: b.task({
          when: ({ input }) => input.ok === false,
          run: async () => null,
        }),
      }),
    });
    expect(await hashOf(f1)).not.toBe(await hashOf(f2));
  });

  it("differs when retry.maxAttempts changes", async () => {
    const f1 = flow({
      id: "retry",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => ({
        only: b.task({
          retry: { maxAttempts: 3, backoff: "exponential" },
          run: async () => null,
        }),
      }),
    });
    const f2 = flow({
      id: "retry",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => ({
        only: b.task({
          retry: { maxAttempts: 5, backoff: "exponential" },
          run: async () => null,
        }),
      }),
    });
    expect(await hashOf(f1)).not.toBe(await hashOf(f2));
  });

  it("differs when flow.id changes", async () => {
    const f1 = single();
    const f2 = flow({
      id: "renamed",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => ({
        only: b.task({ run: async () => ({ v: 1 }) }),
      }),
    });
    expect(await hashOf(f1)).not.toBe(await hashOf(f2));
  });

  it("differs when timeoutMs changes", async () => {
    const f1 = flow({
      id: "timeout",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => ({
        only: b.task({ timeoutMs: 1000, run: async () => null }),
      }),
    });
    const f2 = flow({
      id: "timeout",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => ({
        only: b.task({ timeoutMs: 5000, run: async () => null }),
      }),
    });
    expect(await hashOf(f1)).not.toBe(await hashOf(f2));
  });

  it("differs when a signal schema validator body changes", async () => {
    const f1 = flow({
      id: "signal-schema",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => ({
        s: b.signal({
          schema: {
            "~standard": {
              version: 1 as const,
              vendor: "test",
              validate: (v: unknown) => ({ value: v as { ok: boolean } }),
            },
          },
        }),
      }),
    });
    const f2 = flow({
      id: "signal-schema",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => ({
        s: b.signal({
          schema: {
            "~standard": {
              version: 1 as const,
              vendor: "test",
              validate: (v: unknown) => {
                if (typeof v !== "object" || v === null) {
                  return { issues: [{ message: "must be object" }] };
                }
                return { value: v as { ok: boolean } };
              },
            },
          },
        }),
      }),
    });
    expect(await hashOf(f1)).not.toBe(await hashOf(f2));
  });

  it("differs when the signal schema's vendor changes", async () => {
    const f1 = flow({
      id: "signal-vendor",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => ({
        s: b.signal({
          schema: {
            "~standard": {
              version: 1 as const,
              vendor: "alpha",
              validate: (v: unknown) => ({ value: v as { ok: boolean } }),
            },
          },
        }),
      }),
    });
    const f2 = flow({
      id: "signal-vendor",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => ({
        s: b.signal({
          schema: {
            "~standard": {
              version: 1 as const,
              vendor: "beta",
              validate: (v: unknown) => ({ value: v as { ok: boolean } }),
            },
          },
        }),
      }),
    });
    expect(await hashOf(f1)).not.toBe(await hashOf(f2));
  });

  it("differs when match arms add a new step inside an arm", async () => {
    const f1 = flow({
      id: "match-arm",
      input: passthroughSchema<{ kind: "a" | "b" }>(),
      build: (b) =>
        ({
          m: b.match({
            on: ({ input }) => input.kind,
            cases: {
              a: (b1) => ({ x: b1.task({ run: async () => null }) }),
              b: (b1) => ({ y: b1.task({ run: async () => null }) }),
            },
          }),
        }) as never,
    });
    const f2 = flow({
      id: "match-arm",
      input: passthroughSchema<{ kind: "a" | "b" }>(),
      build: (b) =>
        ({
          m: b.match({
            on: ({ input }) => input.kind,
            cases: {
              a: (b1) => ({
                x: b1.task({ run: async () => null }),
                z: b1.task({ run: async () => null }),
              }),
              b: (b1) => ({ y: b1.task({ run: async () => null }) }),
            },
          }),
        }) as never,
    });
    expect(await hashOf(f1)).not.toBe(await hashOf(f2));
  });
});

describe("canonicalize — shape", () => {
  it("sorts step ids and needs lexicographically", async () => {
    const f = flow({
      id: "sorted",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => {
        const z = b.task({ run: async () => null });
        const a = b.task({ run: async () => null });
        const m = b.task({
          needs: { zRef: z, aRef: a },
          run: async () => null,
        });
        return { z, a, m };
      },
    });
    const dag = await canonicalize(f);
    expect(dag.steps.map((s) => s.id)).toEqual(["a", "m", "z"]);
    const mStep = dag.steps.find((s) => s.id === "m");
    expect(mStep?.needs).toEqual(["a", "z"]);
  });

  it("normalizes retry by filling in default initialDelayMs / maxDelayMs", async () => {
    const f = flow({
      id: "retry-normalize",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => ({
        only: b.task({
          retry: { maxAttempts: 5, backoff: "linear" },
          run: async () => null,
        }),
      }),
    });
    const dag = await canonicalize(f);
    expect(dag.steps[0]?.retry).toEqual({
      maxAttempts: 5,
      backoff: "linear",
      initialDelayMs: 1_000,
      maxDelayMs: 60_000,
    });
  });

  it("emits a stable hex string from sha256Canonical (64 chars)", async () => {
    const f = flow({
      id: "hex",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => ({ x: b.task({ run: async () => null }) }),
    });
    const hash = await sha256Canonical(await canonicalize(f));
    expect(hash).toMatch(/^[0-9a-f]{64}$/);
  });
});

describe("stableStringify", () => {
  it("sorts object keys lexicographically", () => {
    expect(stableStringify({ b: 1, a: 2 })).toBe('{"a":2,"b":1}');
  });

  it("preserves array order", () => {
    expect(stableStringify([3, 1, 2])).toBe("[3,1,2]");
  });

  it("skips undefined entries", () => {
    expect(stableStringify({ a: 1, b: undefined, c: 3 })).toBe('{"a":1,"c":3}');
  });

  it("handles nested objects deterministically", () => {
    expect(stableStringify({ z: { b: 1, a: 2 }, a: [{ y: 1, x: 2 }] })).toBe(
      '{"a":[{"x":2,"y":1}],"z":{"a":2,"b":1}}',
    );
  });

  it("encodes null primitives correctly", () => {
    expect(stableStringify(null)).toBe("null");
    expect(stableStringify("x")).toBe('"x"');
    expect(stableStringify(42)).toBe("42");
    expect(stableStringify(true)).toBe("true");
  });
});
