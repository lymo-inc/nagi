import { describe, expect, it } from "vitest";
import { makeIdempotencyKey, makeOnce } from "./idempotency";
import { InMemoryStore } from "./memory";
import type { RunId } from "./types";

const RUN: RunId = "run-test" as RunId;

describe("makeIdempotencyKey", () => {
  it("encodes runId, stepId, and scope into the key", () => {
    const key = makeIdempotencyKey("run-abc" as RunId, "transcribe");
    expect(key("upload")).toBe("nagi:run-abc:transcribe:upload");
  });

  it("returns the same key across calls with the same scope", () => {
    const key = makeIdempotencyKey(RUN, "step");
    expect(key("foo")).toBe(key("foo"));
  });

  it("different scopes produce different keys", () => {
    const key = makeIdempotencyKey(RUN, "step");
    expect(key("a")).not.toBe(key("b"));
  });

  it("different stepIds within the same run produce different keys", () => {
    const a = makeIdempotencyKey(RUN, "stepA")("scope");
    const b = makeIdempotencyKey(RUN, "stepB")("scope");
    expect(a).not.toBe(b);
  });
});

describe("makeOnce", () => {
  it("invokes fn on first call and caches the value", async () => {
    const store = new InMemoryStore();
    let calls = 0;
    const once = makeOnce({ runId: RUN, stepId: "s", store });

    const first = await once("scope", async () => {
      calls++;
      return { v: 42 };
    });
    const second = await once("scope", async () => {
      calls++;
      return { v: 99 };
    });

    expect(calls).toBe(1);
    expect(first).toEqual({ v: 42 });
    expect(second).toEqual({ v: 42 });
  });

  it("scopes are independent: different scopes invoke fn separately", async () => {
    const store = new InMemoryStore();
    let calls = 0;
    const once = makeOnce({ runId: RUN, stepId: "s", store });

    await once("a", async () => {
      calls++;
      return { v: 1 };
    });
    await once("b", async () => {
      calls++;
      return { v: 2 };
    });

    expect(calls).toBe(2);
  });

  it("re-uses store across instances bound to the same runId/stepId", async () => {
    const store = new InMemoryStore();
    const once1 = makeOnce({ runId: RUN, stepId: "s", store });
    const once2 = makeOnce({ runId: RUN, stepId: "s", store });

    await once1("scope", async () => ({ v: 1 }));
    const second = await once2("scope", async () => ({ v: 999 }));

    expect(second).toEqual({ v: 1 });
  });
});
