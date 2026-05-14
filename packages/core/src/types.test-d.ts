import { describe, expectTypeOf, it } from "vitest";
import { flow } from "./builder";
import type { Wf } from "./runtime";
import { passthroughSchema } from "./test-helpers";
import type {
  Builder,
  Fact,
  FlowOutput,
  Json,
  MatchArm,
  NeedsOutputs,
  RunId,
  Step,
  StepCtx,
  StepMap,
  StepOutput,
  Tx,
} from "./types";

declare const taskStep: Step<{ doubled: number }>;
declare const transcribeStep: Step<{ text: string; recipientEmail: string }>;
declare const classifyStep: Step<{ category: "hot" | "warm" | "cold" }>;
declare const scoreStep: Step<{ value: number; intent: boolean }>;

declare const ctxCall: StepCtx<{ callId: string }>;
declare const builderX: Builder<{ x: number }>;
declare const builderU: Builder<unknown>;
declare const factEx: Fact;
declare const wfEx: Wf;

describe("Step output inference", () => {
  it("StepOutput extracts the Output type parameter", () => {
    expectTypeOf<StepOutput<typeof taskStep>>().toEqualTypeOf<{
      doubled: number;
    }>();
    expectTypeOf<StepOutput<typeof classifyStep>>().toEqualTypeOf<{
      category: "hot" | "warm" | "cold";
    }>();
  });

  it("StepOutput on a non-Step is never", () => {
    expectTypeOf<StepOutput<string>>().toBeNever();
    expectTypeOf<StepOutput<number>>().toBeNever();
  });
});

describe("NeedsOutputs", () => {
  it("maps each named step to its Output", () => {
    type N = { recording: typeof taskStep; transcript: typeof transcribeStep };
    expectTypeOf<NeedsOutputs<N>>().toEqualTypeOf<{
      readonly recording: { doubled: number };
      readonly transcript: { text: string; recipientEmail: string };
    }>();
  });

  it("rename-safe: local key in the handler differs from upstream identifier", () => {
    type N = { foo: typeof taskStep };
    expectTypeOf<NeedsOutputs<N>["foo"]>().toEqualTypeOf<{ doubled: number }>();
  });
});

describe("StepCtx shape", () => {
  it("input has the flow input type", () => {
    expectTypeOf(ctxCall.input).toEqualTypeOf<{ callId: string }>();
  });

  it("tx is the module-augmented type (defaults to unknown)", () => {
    expectTypeOf(ctxCall.tx).toEqualTypeOf<Tx>();
  });

  it("idempotencyKey takes a scope string and returns a string", () => {
    expectTypeOf(ctxCall.idempotencyKey).parameter(0).toBeString();
    expectTypeOf(ctxCall.idempotencyKey).returns.toBeString();
  });

  it("signal is an AbortSignal for graceful drain", () => {
    expectTypeOf(ctxCall.signal).toEqualTypeOf<AbortSignal>();
  });

  it("runId is branded — a raw string is not assignable", () => {
    // @ts-expect-error — RunId is `string & { __brand: "RunId" }`.
    const x: typeof ctxCall.runId = "raw-string";
    void x;
  });
});

describe("Builder.task", () => {
  it("infers Output from the run handler's return type", () => {
    const step = builderX.task({
      run: async ({ input }) => ({ doubled: input.x * 2 }),
    });
    expectTypeOf(step).toEqualTypeOf<Step<{ doubled: number }>>();
  });

  it("threads upstream Output via needs", () => {
    const upstream = builderX.task({
      run: async () => ({ recordingUrl: "url" }),
    });
    const downstream = builderX.task({
      needs: { rec: upstream },
      run: async ({ needs }) => {
        expectTypeOf(needs.rec).toEqualTypeOf<{ recordingUrl: string }>();
        return { ok: true };
      },
    });
    expectTypeOf(downstream).toEqualTypeOf<Step<{ ok: boolean }>>();
  });

  it("when predicate sees both input and needs", () => {
    const upstream = builderX.task({ run: async () => ({ approved: true }) });
    builderX.task({
      needs: { review: upstream },
      when: ({ input, needs }) => {
        expectTypeOf(input).toEqualTypeOf<{ x: number }>();
        expectTypeOf(needs.review).toEqualTypeOf<{ approved: boolean }>();
        return needs.review.approved;
      },
      run: async () => null,
    });
  });
});

describe("Builder.match — discriminator mode", () => {
  it("output is the union of per-arm StepMap outputs", () => {
    const route = builderU.match({
      needs: { c: classifyStep },
      on: ({ needs }) => needs.c.category,
      cases: {
        hot: (b) => ({
          f: b.task({ run: async () => ({ kind: "hot" as const }) }),
        }),
        warm: (b) => ({
          f: b.task({ run: async () => ({ kind: "warm" as const }) }),
        }),
        cold: (b) => ({
          f: b.task({ run: async () => ({ kind: "cold" as const }) }),
        }),
      },
    });
    expectTypeOf<StepOutput<typeof route>>().toEqualTypeOf<
      | { readonly f: { kind: "hot" } }
      | { readonly f: { kind: "warm" } }
      | { readonly f: { kind: "cold" } }
    >();
  });

  it("missing a case is a compile error (exhaustiveness)", () => {
    // @ts-expect-error — `cold` case is missing from `cases`.
    builderU.match({
      needs: { c: classifyStep },
      on: ({ needs }) => needs.c.category,
      cases: {
        hot: (b) => ({ f: b.task({ run: async () => null }) }),
        warm: (b) => ({ f: b.task({ run: async () => null }) }),
      },
    });
  });
});

describe("Builder.match — guard mode", () => {
  it("guard arms can use predicates over needs and produce an arm output type", () => {
    const route = builderU.match({
      needs: { s: scoreStep },
      arms: [
        {
          when: ({ needs }) => needs.s.value >= 90,
          build: (b) => ({
            f: b.task({ run: async () => ({ tier: "premium" as const }) }),
          }),
        },
        {
          otherwise: true,
          build: (b) => ({
            f: b.task({ run: async () => ({ tier: "default" as const }) }),
          }),
        },
      ],
    });
    expectTypeOf<StepOutput<typeof route>>().toMatchTypeOf<{
      readonly f: { tier: "premium" } | { tier: "default" };
    }>();
  });

  it("an arm cannot have both `when` and `otherwise`", () => {
    // The MatchArm union rejects an object that sets both `when` and `otherwise`.
    // Asserting against the union directly so `@ts-expect-error` lands on the
    // offending property rather than getting swallowed by an outer overload failure.
    type ScoreNeeds = { s: typeof scoreStep };
    // @ts-expect-error — `when` and `otherwise` are mutually exclusive on one arm.
    const _badArm: MatchArm<unknown, ScoreNeeds, StepMap> = {
      when: (args: { input: unknown; needs: NeedsOutputs<ScoreNeeds> }) =>
        args.needs.s.intent,
      otherwise: true,
      build: (b) => ({ f: b.task({ run: async () => null }) }),
    };
    void _badArm;
  });
});

describe("Flow output inference", () => {
  it("FlowOutput extracts the Output type param of the flow", () => {
    const f = flow({
      id: "out",
      input: passthroughSchema<{ x: number }>(),
      build: (b) => ({
        a: b.task({ run: async ({ input }) => ({ doubled: input.x * 2 }) }),
      }),
      output: ({ a }) => ({ result: a.doubled }),
    });
    expectTypeOf<FlowOutput<typeof f>>().toEqualTypeOf<{ result: number }>();
  });

  it("FlowOutput on a flow with no output fn is `unknown` (the default)", () => {
    const f = flow({
      id: "no-out",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => ({ a: b.task({ run: async () => null }) }),
    });
    expectTypeOf<FlowOutput<typeof f>>().toEqualTypeOf<unknown>();
  });

  it("FlowOutput on a non-Flow is never", () => {
    expectTypeOf<FlowOutput<string>>().toBeNever();
  });

  it("output fn sees each step's typed Output via the M parameter", () => {
    flow({
      id: "see-steps",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => {
        const a = b.task({ run: async () => ({ v: 1 }) });
        const c = b.task({ run: async () => ({ s: "x" }) });
        return { a, c };
      },
      output: ({ a, c }) => {
        expectTypeOf(a).toEqualTypeOf<{ v: number }>();
        expectTypeOf(c).toEqualTypeOf<{ s: string }>();
        return null;
      },
    });
  });
});

describe("Flow literal id (const Id)", () => {
  it("id narrows to the literal string", () => {
    const f = flow({
      id: "checkout",
      input: passthroughSchema<{ x: number }>(),
      build: (b) => ({ a: b.task({ run: async () => ({ ok: true }) }) }),
    });
    expectTypeOf(f.id).toEqualTypeOf<"checkout">();
  });

  it("keyof steps narrows to the literal step keys", () => {
    const f = flow({
      id: "checkout",
      input: passthroughSchema<{ x: number }>(),
      build: (b) => ({
        a: b.task({ run: async () => ({ ok: true }) }),
        b: b.task({ run: async () => ({ ok: true }) }),
      }),
    });
    expectTypeOf<keyof typeof f.steps>().toEqualTypeOf<"a" | "b">();
  });
});

describe("Builder.step chain (RFC 0002)", () => {
  it("step return-map has typed Step<Output> per key", () => {
    const f = flow({
      id: "step-types",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) =>
        b
          .step("a", { run: async () => ({ y: 1 }) })
          .step("c", { run: async () => ({ z: "x" }) }),
    });

    expectTypeOf(f.steps).toEqualTypeOf<{
      readonly a: Step<{ y: number }>;
      readonly c: Step<{ z: string }>;
    }>();
  });

  it("flow.output sees typed step outputs from b.step chain", () => {
    const f = flow({
      id: "step-output",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) =>
        b
          .step("a", { run: async () => ({ v: 1 }) })
          .step("c", { needs: ["a"], run: async () => ({ s: "x" }) }),
      output: ({ a, c }) => ({ total: a.v, label: c.s }),
    });
    expectTypeOf<FlowOutput<typeof f>>().toEqualTypeOf<{
      total: number;
      label: string;
    }>();
  });

  it("keyof steps narrows to the literal chain keys", () => {
    const f = flow({
      id: "step-keys",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) =>
        b
          .step("fetchRecording", { run: async () => ({ url: "x" }) })
          .step("transcribe", {
            needs: ["fetchRecording"],
            run: async () => ({ text: "y" }),
          }),
    });
    expectTypeOf<keyof typeof f.steps>().toEqualTypeOf<
      "fetchRecording" | "transcribe"
    >();
  });

  it("needs.<sibling> is auto-typed inside the run handler", () => {
    flow({
      id: "step-needs-typed",
      input: passthroughSchema<{ start: number }>(),
      build: (b) =>
        b
          .step("a", {
            run: async ({ input }) => ({ doubled: input.start * 2 }),
          })
          .step("b", {
            needs: ["a"],
            run: async ({ input, needs }) => {
              expectTypeOf(input).toEqualTypeOf<{ start: number }>();
              expectTypeOf(needs.a).toEqualTypeOf<{ doubled: number }>();
              return { next: input.start + needs.a.doubled };
            },
          }),
    });
  });

  it("needs.<sibling> is auto-typed inside the when predicate", () => {
    flow({
      id: "step-when-typed",
      input: passthroughSchema<{ on: boolean }>(),
      build: (b) =>
        b
          .step("gate", { run: async () => ({ enabled: true }) })
          .step("branch", {
            needs: ["gate"],
            when: ({ input, needs }) => {
              expectTypeOf(input).toEqualTypeOf<{ on: boolean }>();
              expectTypeOf(needs.gate).toEqualTypeOf<{ enabled: boolean }>();
              return input.on && needs.gate.enabled;
            },
            run: async () => null,
          }),
    });
  });

  it("rejects an unknown sibling in needs[] at compile time", () => {
    flow({
      id: "step-typo",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) =>
        b.step("a", { run: async () => ({ y: 1 }) }).step("b", {
          // @ts-expect-error - "wrong" is not in the accumulator
          needs: ["wrong"],
          run: async () => null,
        }),
    });
  });

  it("rejects a duplicate chain key at compile time", () => {
    flow({
      id: "step-dup",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) =>
        b.step("a", { run: async () => ({ y: 1 }) }).step(
          // @ts-expect-error - "a" is already in the accumulator
          "a",
          { run: async () => null },
        ),
    });
  });

  it("include() brings a pre-built Step under a chain key", () => {
    flow({
      id: "step-include",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => {
        const pre = b.task({ run: async () => ({ pre: "v" }) });
        return b
          .step("a", { run: async () => ({ y: 1 }) })
          .include("pre", pre)
          .step("c", {
            needs: ["a", "pre"],
            run: async ({ needs }) => {
              expectTypeOf(needs.a).toEqualTypeOf<{ y: number }>();
              expectTypeOf(needs.pre).toEqualTypeOf<{ pre: string }>();
              return null;
            },
          });
      },
    });
  });
});

describe("Wf.start signature", () => {
  it("accepts (flow, input) — opts is optional (back-compat)", () => {
    const f = flow({
      id: "sig-back-compat",
      input: passthroughSchema<{ x: number }>(),
      build: (b) => ({ a: b.task({ run: async () => null }) }),
    });
    expectTypeOf(wfEx.start(f, { x: 1 })).toEqualTypeOf<Promise<RunId>>();
  });

  it("accepts (flow, input, { runId })", () => {
    const f = flow({
      id: "sig-with-opts",
      input: passthroughSchema<{ x: number }>(),
      build: (b) => ({ a: b.task({ run: async () => null }) }),
    });
    const id = "run-supplied" as RunId;
    expectTypeOf(wfEx.start(f, { x: 1 }, { runId: id })).toEqualTypeOf<
      Promise<RunId>
    >();
  });

  it("opts.runId must be a RunId (branded string), not a raw string", () => {
    const f = flow({
      id: "sig-brand",
      input: passthroughSchema<{ x: number }>(),
      build: (b) => ({ a: b.task({ run: async () => null }) }),
    });
    // @ts-expect-error — RunId is branded; a raw string literal isn't assignable.
    void wfEx.start(f, { x: 1 }, { runId: "raw-string" });
  });
});

describe("Fact discriminated union", () => {
  it("exhaustive switch on `kind` narrows correctly", () => {
    switch (factEx.kind) {
      case "flow.started":
        expectTypeOf(factEx.input).toEqualTypeOf<Json>();
        expectTypeOf(factEx.flowId).toBeString();
        break;
      case "flow.completed":
        expectTypeOf(factEx.output).toEqualTypeOf<Json>();
        break;
      case "flow.failed":
        expectTypeOf(factEx.error.message).toBeString();
        break;
      case "step.started":
      case "step.completed":
      case "step.failed":
      case "step.retried":
      case "step.skipped":
        expectTypeOf(factEx.stepId).toBeString();
        break;
      case "signal.sent":
      case "signal.received":
        expectTypeOf(factEx.payload).toEqualTypeOf<Json>();
        break;
      case "once.recorded":
        expectTypeOf(factEx.scope).toBeString();
        expectTypeOf(factEx.value).toEqualTypeOf<Json>();
        break;
      case "match.arm-selected":
        expectTypeOf(factEx.arm).toBeString();
        break;
    }
  });
});
