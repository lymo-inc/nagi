import { describe, expect, it } from "vitest";
import { flow } from "./builder";
import { canonicalize, sha256Canonical } from "./canonicalize";
import { makeHarness, passthroughSchema } from "./test-helpers";
import type { Logger, SignalReceivedFact } from "./types";

interface LogEntry {
  readonly level: "debug" | "info" | "warn" | "error";
  readonly msg: string;
  readonly attrs?: Record<string, unknown>;
}

function memoryLogger(): { logger: Logger; entries: LogEntry[] } {
  const entries: LogEntry[] = [];
  const push =
    (level: LogEntry["level"]) =>
    (msg: string, attrs?: Record<string, unknown>): void => {
      entries.push(
        attrs !== undefined ? { level, msg, attrs } : { level, msg },
      );
    };
  return {
    logger: {
      debug: push("debug"),
      info: push("info"),
      warn: push("warn"),
      error: push("error"),
    },
    entries,
  };
}

describe("b.signal — single-name back-compat", () => {
  it("default (no name / names): resolves via step id, fact has no signalName", async () => {
    const f = flow({
      id: "single-default",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => ({
        review: b.signal({
          schema: passthroughSchema<{ ok: boolean }>(),
        }),
      }),
    });

    const h = await makeHarness(f);
    const w = h.startWorker();
    try {
      const runId = await h.wf.start(f, {});
      await h.waitForStep(runId, "review", "running");
      await h.wf.signal(runId, "review", { ok: true });
      const result = await h.waitForEnd(runId);
      expect(result.output("review")).toEqual({ ok: true });
      const facts = result.factsOf("signal.received");
      expect(facts).toHaveLength(1);
      expect(facts[0]?.stepId).toBe("review");
      expect((facts[0] as SignalReceivedFact).signalName).toBeUndefined();
    } finally {
      await w.stop();
    }
  });

  it("explicit single name: resolves via that name, fact carries signalName", async () => {
    const f = flow({
      id: "single-explicit",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => ({
        review: b.signal({
          name: "approval",
          schema: passthroughSchema<{ ok: boolean }>(),
        }),
      }),
    });

    const h = await makeHarness(f);
    const w = h.startWorker();
    try {
      const runId = await h.wf.start(f, {});
      await h.waitForStep(runId, "review", "running");
      await h.wf.signal(runId, "approval", { ok: true });
      const result = await h.waitForEnd(runId);
      const facts = result.factsOf("signal.received");
      expect(facts[0]?.stepId).toBe("review");
      expect((facts[0] as SignalReceivedFact).signalName).toBe("approval");
    } finally {
      await w.stop();
    }
  });

  it("explicit single name: the step id is NOT an accepted name", async () => {
    const f = flow({
      id: "single-explicit-renamed",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => ({
        review: b.signal({
          name: "approval",
          schema: passthroughSchema<{ ok: boolean }>(),
        }),
      }),
    });

    const h = await makeHarness(f);
    const w = h.startWorker();
    try {
      const runId = await h.wf.start(f, {});
      await h.waitForStep(runId, "review", "running");
      await expect(h.wf.signal(runId, "review", { ok: true })).rejects.toThrow(
        /no signal step accepting "review"/,
      );
    } finally {
      await w.stop();
    }
  });
});

describe("b.signal — multi-name", () => {
  function dualSourceFlow() {
    return flow({
      id: "dual-source",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => ({
        transcript: b.signal({
          names: ["audioReady", "recordingReady"],
          schema: passthroughSchema<
            { audioUrl: string } | { transcript: string }
          >(),
        }),
      }),
    });
  }

  it("first arrival wins (audioReady)", async () => {
    const f = dualSourceFlow();
    const h = await makeHarness(f);
    const w = h.startWorker();
    try {
      const runId = await h.wf.start(f, {});
      await h.waitForStep(runId, "transcript", "running");
      await h.wf.signal(runId, "audioReady", { audioUrl: "u" });
      const result = await h.waitForEnd(runId);
      expect(result.output("transcript")).toEqual({ audioUrl: "u" });
      expect(
        (result.factsOf("signal.received")[0] as SignalReceivedFact).signalName,
      ).toBe("audioReady");
    } finally {
      await w.stop();
    }
  });

  it("first arrival wins (recordingReady)", async () => {
    const f = dualSourceFlow();
    const h = await makeHarness(f);
    const w = h.startWorker();
    try {
      const runId = await h.wf.start(f, {});
      await h.waitForStep(runId, "transcript", "running");
      await h.wf.signal(runId, "recordingReady", { transcript: "t" });
      const result = await h.waitForEnd(runId);
      expect(result.output("transcript")).toEqual({ transcript: "t" });
      expect(
        (result.factsOf("signal.received")[0] as SignalReceivedFact).signalName,
      ).toBe("recordingReady");
    } finally {
      await w.stop();
    }
  });

  it("late loser is a no-op + logged (no throw, no second fact)", async () => {
    const f = dualSourceFlow();
    const { logger, entries } = memoryLogger();
    const h = await makeHarness(f, { logger });
    const w = h.startWorker();
    try {
      const runId = await h.wf.start(f, {});
      await h.waitForStep(runId, "transcript", "running");
      await h.wf.signal(runId, "audioReady", { audioUrl: "u" });
      await h.waitForEnd(runId);

      await expect(
        h.wf.signal(runId, "recordingReady", { transcript: "late" }),
      ).resolves.toBeUndefined();

      const state = await h.store.loadRunState(runId);
      const received = state.facts.filter((f) => f.kind === "signal.received");
      expect(received).toHaveLength(1);
      expect(
        entries.some(
          (e) =>
            e.level === "info" &&
            e.msg.includes("signal arrived after step resolved"),
        ),
      ).toBe(true);
    } finally {
      await w.stop();
    }
  });

  it("unknown signal name throws", async () => {
    const f = dualSourceFlow();
    const h = await makeHarness(f);
    const w = h.startWorker();
    try {
      const runId = await h.wf.start(f, {});
      await h.waitForStep(runId, "transcript", "running");
      await expect(
        h.wf.signal(runId, "nopeNotADeclaredAlias", { audioUrl: "u" }),
      ).rejects.toThrow(/no signal step accepting "nopeNotADeclaredAlias"/);
    } finally {
      await w.stop();
    }
  });
});

describe("b.signal — construction-time uniqueness", () => {
  it("rejects alias overlapping with a step id elsewhere in the flow", () => {
    expect(() =>
      flow({
        id: "collide-alias-vs-step-id",
        input: passthroughSchema<Record<string, never>>(),
        build: (b) => ({
          // Names "x" via alias.
          transcript: b.signal({
            names: ["x"],
            schema: passthroughSchema<{ v: number }>(),
          }),
          // And "x" again as a step id.
          x: b.signal({
            schema: passthroughSchema<{ v: number }>(),
          }),
        }),
      }),
    ).toThrow(/signal name "x" is declared as both/);
  });

  it("rejects two signal steps declaring overlapping aliases", () => {
    expect(() =>
      flow({
        id: "collide-alias-vs-alias",
        input: passthroughSchema<Record<string, never>>(),
        build: (b) => ({
          a: b.signal({
            names: ["shared", "onlyA"],
            schema: passthroughSchema<{ v: number }>(),
          }),
          b: b.signal({
            names: ["shared", "onlyB"],
            schema: passthroughSchema<{ v: number }>(),
          }),
        }),
      }),
    ).toThrow(/signal name "shared" is declared as both/);
  });

  it("allows a signal step to list its own id in `names` (no clash)", () => {
    expect(() =>
      flow({
        id: "self-named",
        input: passthroughSchema<Record<string, never>>(),
        build: (b) => ({
          ping: b.signal({
            names: ["ping", "PING"],
            schema: passthroughSchema<{ v: number }>(),
          }),
        }),
      }),
    ).not.toThrow();
  });
});

describe("b.signal — canonical hash invariants", () => {
  function withSchema(name: string | undefined, names?: readonly string[]) {
    return flow({
      id: "hash-target",
      input: passthroughSchema<Record<string, never>>(),
      build: (b) => ({
        only: b.signal(
          names !== undefined
            ? {
                names: names as readonly [string, ...string[]],
                schema: passthroughSchema<{ v: number }>(),
              }
            : name !== undefined
              ? {
                  name,
                  schema: passthroughSchema<{ v: number }>(),
                }
              : {
                  schema: passthroughSchema<{ v: number }>(),
                },
        ),
      }),
    });
  }

  it("default (no name) hashes identically to itself (pre-RFC byte-stability)", async () => {
    const a = await sha256Canonical(await canonicalize(withSchema(undefined)));
    const b = await sha256Canonical(await canonicalize(withSchema(undefined)));
    expect(a).toBe(b);
  });

  it("default vs explicit single name (same value as step id) → different hash", async () => {
    // Explicitly setting name === stepId still moves the hash, because the
    // declaration itself is structural: the caller has opted into named routing.
    const noName = await sha256Canonical(
      await canonicalize(withSchema(undefined)),
    );
    const explicit = await sha256Canonical(
      await canonicalize(withSchema("only")),
    );
    expect(noName).not.toBe(explicit);
  });

  it("multi-name list is order-independent in the hash", async () => {
    const ab = await sha256Canonical(
      await canonicalize(withSchema(undefined, ["a", "b"])),
    );
    const ba = await sha256Canonical(
      await canonicalize(withSchema(undefined, ["b", "a"])),
    );
    expect(ab).toBe(ba);
  });

  it("changing the set of accepted names moves the hash", async () => {
    const ab = await sha256Canonical(
      await canonicalize(withSchema(undefined, ["a", "b"])),
    );
    const ac = await sha256Canonical(
      await canonicalize(withSchema(undefined, ["a", "c"])),
    );
    expect(ab).not.toBe(ac);
  });
});
