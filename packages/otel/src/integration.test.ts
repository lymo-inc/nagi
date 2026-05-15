/**
 * Drives `otelHooks()` through a real `nagi()` runtime end-to-end with in-memory
 * deps. Verifies that the flow + per-step span hierarchy and core attributes
 * land in an OTel exporter when the hooks are wired the way users will wire them.
 */
import {
  flow,
  InMemoryClock,
  InMemoryQueue,
  InMemoryStore,
  nagi,
  type RunId,
} from "@nagi-js/core";
import {
  BasicTracerProvider,
  InMemorySpanExporter,
  SimpleSpanProcessor,
} from "@opentelemetry/sdk-trace-base";
import { afterEach, beforeAll, describe, expect, it } from "vitest";
import { otelHooks } from "./hooks";

const exporter = new InMemorySpanExporter();
let provider: BasicTracerProvider;

beforeAll(() => {
  provider = new BasicTracerProvider({
    spanProcessors: [new SimpleSpanProcessor(exporter)],
  });
});

afterEach(() => {
  exporter.reset();
});

function passthroughSchema<T>() {
  return {
    "~standard": {
      version: 1 as const,
      vendor: "nagi-test",
      validate: (value: unknown) => ({ value: value as T }),
    },
  };
}

async function runToEnd(
  wf: Awaited<ReturnType<typeof nagi>>,
  store: InMemoryStore,
  runId: RunId,
  timeoutMs = 5_000,
) {
  const ac = new AbortController();
  const worker = wf.worker({ pollIntervalMs: 1, signal: ac.signal });
  const done = worker.run();
  try {
    const start = Date.now();
    while (Date.now() - start < timeoutMs) {
      const state = await store.loadRunState(runId);
      if (state.status === "completed" || state.status === "failed")
        return state;
      await new Promise((res) => setTimeout(res, 5));
    }
    throw new Error("runToEnd: timeout");
  } finally {
    ac.abort();
    await done;
  }
}

describe("@nagi-js/otel — end-to-end against a real nagi runtime", () => {
  it("emits one flow span with two step children for a 2-step flow", async () => {
    const f = flow({
      id: "otel-two-step",
      input: passthroughSchema<{ x: number }>(),
      build: (b) => {
        const a = b.task({
          run: async ({ input }) => ({ doubled: input.x * 2 }),
        });
        const c = b.task({
          needs: { a },
          run: async ({ needs }) => ({ tripled: needs.a.doubled * 3 }),
        });
        return { a, c };
      },
    });

    const store = new InMemoryStore();
    const wf = await nagi({
      store,
      queue: new InMemoryQueue(),
      clock: new InMemoryClock(),
      flows: [f],
      hooks: otelHooks({
        tracer: provider.getTracer("@nagi-js/otel-integration"),
        defaultAttributes: { "deployment.environment": "test" },
      }),
    });

    const runId = await wf.start(f, { x: 7 });
    const state = await runToEnd(wf, store, runId);
    expect(state.status).toBe("completed");

    const spans = exporter.getFinishedSpans();
    const flowSpan = spans.find((s) => s.name === "flow otel-two-step");
    expect(flowSpan).toBeDefined();

    const stepSpans = spans.filter((s) => s.name.startsWith("step "));
    expect(stepSpans.length).toBe(2);

    // Every step is a child of the flow span and shares the same trace id.
    for (const s of stepSpans) {
      expect(s.parentSpanId).toBe(flowSpan!.spanContext().spanId);
      expect(s.spanContext().traceId).toBe(flowSpan!.spanContext().traceId);
      expect(s.attributes["nagi.flow.id"]).toBe("otel-two-step");
      expect(s.attributes["nagi.run.id"]).toBe(runId);
      expect(s.attributes["deployment.environment"]).toBe("test");
    }

    // The two step span IDs are the local keys from `build`.
    const stepIds = stepSpans.map((s) => s.attributes["nagi.step.id"]).sort();
    expect(stepIds).toEqual(["a", "c"]);
  }, 10_000);
});
