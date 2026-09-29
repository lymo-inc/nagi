import { describe, expect, it } from "vitest";
import { flow, optional } from "../builder";
import {
  canonicalize,
  fingerprintFlows,
  sha256Canonical,
} from "../canonicalize";
import { passthroughSchema } from "./test-helpers";

// Live runs are pinned by flow hash: any change to canonical form flips these
// and drifts every in-flight run. Update the pins only for a deliberate,
// announced hash break.

const child = flow({
  id: "pin-child",
  input: passthroughSchema<{ x: number }>(),
  build: (b) => ({
    double: b.task({ run: async ({ input }) => ({ doubled: input.x * 2 }) }),
  }),
});

const kitchenSink = flow({
  id: "pin-kitchen-sink",
  input: passthroughSchema<{ n: number; tenant: string; ok: boolean }>(),
  concurrency: "tenant",
  build: (b) => {
    const fetch = b.task({
      retry: { maxAttempts: 3, backoff: "exponential", initialDelayMs: 100 },
      timeoutMs: 5_000,
      run: async () => ({ v: 1 }),
    });
    const act = b.activity({
      needs: { fetch },
      retry: { maxAttempts: 2, backoff: "fixed" },
      run: async () => ({ a: 1 }),
    });
    const approval = b.signal({
      needs: { act },
      schema: passthroughSchema<{ approved: boolean }>(),
      names: ["approve", "approval-alias"],
      timeoutMs: 60_000,
    });
    const park = b.signal({
      schema: passthroughSchema<{ go: true }>(),
      timeoutMs: "unbounded",
    });
    const guarded = b.task({
      needs: { approval, park: optional(park) },
      when: ({ input }) => input.ok,
      run: async () => ({ g: 1 }),
    });
    const sub = b.subflow(child, {
      needs: { guarded: optional(guarded) },
      when: ({ input }) => input.n > 0,
      input: ({ input }) => ({ x: input.n }),
    });
    const stream = b.streamingTask({
      needs: { sub },
      timeoutMs: 30_000,
      run: async () => ({ s: 1 }),
    });
    return { fetch, act, approval, park, guarded, sub, stream };
  },
});

const linear = flow({
  id: "pin-linear",
  input: passthroughSchema<Record<string, never>>(),
  concurrency: { keyFn: () => "k", mode: "cancel-in-progress" },
  build: (b) => {
    const a = b.task({ run: async () => ({ v: 1 }) });
    const z = b.task({ needs: { a }, run: async () => ({ v: 2 }) });
    return { a, z };
  },
});

async function hashOf(f: Parameters<typeof canonicalize>[0]): Promise<string> {
  return sha256Canonical(await canonicalize(f));
}

describe("flow hash pins", () => {
  it("keeps per-flow hashes byte-identical", async () => {
    expect({
      child: await hashOf(child),
      kitchenSink: await hashOf(kitchenSink),
      linear: await hashOf(linear),
    }).toEqual({
      child: "aefa367e7f4c0997f880f3e45320fab4d3998aa06cebf63062a59b1442de445d",
      kitchenSink:
        "1192370c3e66f9e0c34460bd7ac0ff56fb4633189eb9fcc0a2d510f7553687f3",
      linear:
        "781accd292c9c04cac0b3c669e77a07f6a9b1cf26a242585d32fd57025073420",
    });
  });

  it("keeps the fleet fingerprint byte-identical", async () => {
    expect(await fingerprintFlows([kitchenSink, child, linear])).toBe(
      "d096f295316f21715080fc98236c8d8497f3067fb9c442a0caf9f117219ec78d",
    );
  });
});
