import { describe, expectTypeOf, it } from "vitest";
import { flow } from "../builder";
import { nagi, type Wf } from "../runtime";
import type {
  Builder,
  Fact,
  FlowIdOf,
  FlowOutput,
  Json,
  MatchArm,
  NeedsOutputs,
  Queue,
  RunId,
  RunSummary,
  Step,
  StepCtx,
  StepMap,
  StepOutput,
  Store,
  Tx,
} from "../types";
import { passthroughSchema } from "./test-helpers";

declare const taskStep: Step<{ doubled: number }>;
declare const transcribeStep: Step<{ text: string; recipientEmail: string }>;
declare const classifyStep: Step<{ category: "hot" | "warm" | "cold" }>;
declare const scoreStep: Step<{ value: number; intent: boolean }>;

declare const ctxCall: StepCtx<{ callId: string }>;
declare const builderX: Builder<{ x: number }>;
declare const builderU: Builder<unknown>;
declare const factEx: Fact;
declare const wfEx: Wf;

// Stubs to construct `nagi(...)` in type-level tests — bodies never run.
declare const store: Store;
declare const queue: Queue;

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

describe("Wf parameterized over registered flows (RFC 0018)", () => {
  const a = flow({
    id: "a",
    input: passthroughSchema<{ x: number }>(),
    build: (b) => ({ s: b.task({ run: async () => null }) }),
  });
  const b = flow({
    id: "b",
    input: passthroughSchema<{ y: string }>(),
    build: (bld) => ({ s: bld.task({ run: async () => null }) }),
  });

  it("nagi({ flows: [a, b] }) — no `as const` — narrows flowId to the union", async () => {
    const wf = await nagi({ flows: [a, b], store, queue });
    type Runs = Awaited<ReturnType<typeof wf.queryRuns>>["runs"];
    expectTypeOf<Runs[number]["flowId"]>().toEqualTypeOf<"a" | "b">();
  });

  it("single-flow tuple [a] narrows to the lone literal (not widened)", async () => {
    const wf = await nagi({ flows: [a], store, queue });
    type Runs = Awaited<ReturnType<typeof wf.queryRuns>>["runs"];
    expectTypeOf<Runs[number]["flowId"]>().toEqualTypeOf<"a">();
  });

  it("bare Wf (no type arg) keeps flowId as string", () => {
    type Runs = Awaited<ReturnType<typeof wfEx.queryRuns>>["runs"];
    expectTypeOf<Runs[number]["flowId"]>().toBeString();
  });

  it("bare RunSummary (no type arg) keeps flowId as string", () => {
    expectTypeOf<RunSummary["flowId"]>().toBeString();
  });

  it("FlowIdOf<typeof flows> resolves to the literal union", () => {
    const flows = [a, b] as const;
    expectTypeOf<FlowIdOf<typeof flows>>().toEqualTypeOf<"a" | "b">();
  });

  it("where.flowId accepts a registered id", async () => {
    const wf = await nagi({ flows: [a, b], store, queue });
    const r = wf.queryRuns({ where: { flowId: "a" } });
    expectTypeOf(r).toEqualTypeOf<ReturnType<typeof wf.queryRuns>>();
  });

  it("where.flowId rejects a non-registered id (strict union, no escape hatch)", async () => {
    const wf = await nagi({ flows: [a, b], store, queue });
    // @ts-expect-error "notAFlow" is not a registered flow id
    void wf.queryRuns({ where: { flowId: "notAFlow" } });
  });

  it("start still infers FlowInput for a registered flow", async () => {
    const wf = await nagi({ flows: [a, b], store, queue });
    expectTypeOf(wf.start).parameter(0).toEqualTypeOf<typeof a | typeof b>();
    expectTypeOf(wf.start(a, { x: 1 })).toEqualTypeOf<Promise<RunId>>();
    expectTypeOf(wf.start(b, { y: "ok" })).toEqualTypeOf<Promise<RunId>>();
  });

  it("start rejects a flow not in the nagi({ flows }) array (Q2 constraint)", async () => {
    const wf = await nagi({ flows: [a, b], store, queue });
    const unregistered = flow({
      id: "unregistered",
      input: passthroughSchema<{ z: boolean }>(),
      build: (bld) => ({ s: bld.task({ run: async () => null }) }),
    });
    // @ts-expect-error `unregistered` was not passed to nagi({ flows })
    void wf.start(unregistered, { z: true });
  });

  it("RunSummary.input is still Json", () => {
    expectTypeOf<RunSummary["input"]>().toEqualTypeOf<Json>();
    expectTypeOf<RunSummary<"a" | "b">["input"]>().toEqualTypeOf<Json>();
  });
});

describe("Flow concurrency shorthand typing", () => {
  it("bare string that is a string-valued key typechecks", () => {
    flow({
      id: "conc-bare-ok",
      input: passthroughSchema<{ videoId: string; count: number }>(),
      concurrency: "videoId",
      build: (b) => ({ a: b.task({ run: async () => null }) }),
    });
  });

  it("misspelled key is rejected", () => {
    flow({
      id: "conc-bare-typo",
      input: passthroughSchema<{ videoId: string; count: number }>(),
      // @ts-expect-error misspelled key is not in StringKeyOf<Input>
      concurrency: "videold",
      build: (b) => ({ a: b.task({ run: async () => null }) }),
    });
  });

  it("numeric-valued key is rejected (strict string-only)", () => {
    flow({
      id: "conc-bare-numeric",
      input: passthroughSchema<{ videoId: string; count: number }>(),
      // @ts-expect-error numeric-valued key is excluded by StringKeyOf
      concurrency: "count",
      build: (b) => ({ a: b.task({ run: async () => null }) }),
    });
  });

  it("an arbitrary string value is rejected (StringKeyOf, not string)", () => {
    const k: string = "videoId";
    flow({
      id: "conc-bare-widestring",
      input: passthroughSchema<{ videoId: string; count: number }>(),
      // @ts-expect-error a plain `string` is wider than StringKeyOf<Input>
      concurrency: k,
      build: (b) => ({ a: b.task({ run: async () => null }) }),
    });
  });

  it("keyFn object form typechecks without mode", () => {
    flow({
      id: "conc-keyfn-no-mode",
      input: passthroughSchema<{ videoId: string; count: number }>(),
      concurrency: { keyFn: (input) => input.videoId },
      build: (b) => ({ a: b.task({ run: async () => null }) }),
    });
  });

  it("keyFn object form typechecks with mode", () => {
    flow({
      id: "conc-keyfn-with-mode",
      input: passthroughSchema<{ videoId: string; count: number }>(),
      concurrency: {
        keyFn: (input) => input.videoId,
        mode: "cancel-in-progress",
      },
      build: (b) => ({ a: b.task({ run: async () => null }) }),
    });
  });

  it("invalid mode literal is rejected", () => {
    flow({
      id: "conc-keyfn-bad-mode",
      input: passthroughSchema<{ videoId: string; count: number }>(),
      concurrency: {
        keyFn: (input) => input.videoId,
        // @ts-expect-error "nope" is not a ConcurrencyMode
        mode: "nope",
      },
      build: (b) => ({ a: b.task({ run: async () => null }) }),
    });
  });
});
