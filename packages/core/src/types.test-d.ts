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
    // @ts-expect-error
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
    type ScoreNeeds = { s: typeof scoreStep };
    // @ts-expect-error
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
    // @ts-expect-error
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
