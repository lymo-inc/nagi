import { describe, expectTypeOf, it } from "vitest";
import type {
  AttemptNumber,
  Builder,
  SerializedError,
  Step,
  StepKind,
  StreamEvent,
  StreamingStepCtx,
  Tx,
} from "../types";

declare const builderX: Builder<{ x: number }>;
declare const streamEvent: StreamEvent<{ token: string }>;

describe("Builder.streamingTask — output inference", () => {
  it("infers Output from the run handler's return type (not the chunk type)", () => {
    const step = builderX.streamingTask({
      run: async ({ input }) => ({ text: `len:${input.x}` }),
    });
    expectTypeOf(step).toEqualTypeOf<Step<{ text: string }>>();
  });

  it("a streaming Step is assignable to Step<O>", () => {
    const step = builderX.streamingTask({
      run: async () => ({ done: true }),
    });
    const asStep: Step<{ done: boolean }> = step;
    void asStep;
  });

  it("downstream needs sees the durable Output, never the chunk type", () => {
    const upstream = builderX.streamingTask<
      Record<string, never>,
      { final: string },
      { token: string }
    >({
      run: async ({ ctx }) => {
        await ctx.emit({ token: "hi" });
        return { final: "all done" };
      },
    });
    builderX.task({
      needs: { gen: upstream },
      run: async ({ needs }) => {
        expectTypeOf(needs.gen).toEqualTypeOf<{ final: string }>();
        return null;
      },
    });
  });
});

describe("StreamingStepCtx.emit", () => {
  it("accepts the declared chunk type and returns Promise<void>", () => {
    builderX.streamingTask<Record<string, never>, void, { token: string }>({
      run: async ({ ctx }) => {
        expectTypeOf(ctx.emit).parameter(0).toEqualTypeOf<{ token: string }>();
        expectTypeOf(ctx.emit({ token: "a" })).toEqualTypeOf<Promise<void>>();
      },
    });
  });

  it("rejects a chunk of the wrong type", () => {
    builderX.streamingTask<Record<string, never>, void, { token: string }>({
      run: async ({ ctx }) => {
        // @ts-expect-error number is not the declared chunk type { token: string }
        await ctx.emit(123);
      },
    });
  });

  it("defaults the chunk type to Json when C is not supplied", () => {
    builderX.streamingTask({
      run: async ({ ctx }) => {
        // a Json-compatible value is accepted under the default
        await ctx.emit({ partial: "ok" });
        return null;
      },
    });
  });
});

describe("StreamingStepCtx — inherited StepCtx surface", () => {
  it("still exposes input/tx/runId/attempt/signal/once/idempotencyKey", () => {
    builderX.streamingTask({
      run: async ({ ctx }) => {
        expectTypeOf(ctx.input).toEqualTypeOf<{ x: number }>();
        expectTypeOf(ctx.tx).toEqualTypeOf<Tx>();
        expectTypeOf(ctx.runId).toEqualTypeOf<
          StreamingStepCtx<{ x: number }>["runId"]
        >();
        expectTypeOf(ctx.attempt).toEqualTypeOf<AttemptNumber>();
        expectTypeOf(ctx.signal).toEqualTypeOf<AbortSignal>();
        expectTypeOf(ctx.once).toBeFunction();
        expectTypeOf(ctx.once<{ ok: boolean }>).returns.toEqualTypeOf<
          Promise<{ ok: boolean }>
        >();
        expectTypeOf(ctx.idempotencyKey).parameter(0).toBeString();
        expectTypeOf(ctx.idempotencyKey).returns.toBeString();
        return null;
      },
    });
  });

  it("a plain StepCtx (b.task) has no emit member", () => {
    builderX.task({
      run: async ({ ctx }) => {
        // @ts-expect-error emit exists only on StreamingStepCtx, not StepCtx
        void ctx.emit;
        return null;
      },
    });
  });
});

describe("StepKind", () => {
  it('includes "streaming"', () => {
    // "streaming" is a member: assignable into StepKind, and StepKind extends it.
    const k: StepKind = "streaming";
    void k;
    expectTypeOf<"streaming">().toMatchTypeOf<StepKind>();
    expectTypeOf<StepKind>().toEqualTypeOf<
      "task" | "signal" | "match" | "subflow" | "streaming"
    >();
  });
});

describe("StreamEvent<C> discriminated union", () => {
  it("narrows each arm on `kind`", () => {
    switch (streamEvent.kind) {
      case "chunk":
        expectTypeOf(streamEvent.chunk).toEqualTypeOf<{ token: string }>();
        break;
      case "dropped":
        expectTypeOf(streamEvent.count).toBeNumber();
        break;
      case "retry":
        expectTypeOf(streamEvent.attempt).toEqualTypeOf<AttemptNumber>();
        break;
      case "error":
        expectTypeOf(streamEvent.error).toEqualTypeOf<SerializedError>();
        break;
    }
  });

  it("defaults C to Json", () => {
    const ev: StreamEvent = { kind: "chunk", chunk: { any: "json" } };
    if (ev.kind === "chunk") {
      expectTypeOf(ev.chunk).toEqualTypeOf<import("../types").Json>();
    }
  });
});
