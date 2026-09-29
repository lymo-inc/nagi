import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { every } from "../step-exec";
import type { Millis } from "../types";

function gate(): { readonly wait: Promise<void>; readonly open: () => void } {
  let open: () => void = () => {};
  const wait = new Promise<void>((r) => {
    open = r;
  });
  return { wait, open };
}

describe("every", () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });
  afterEach(() => {
    vi.useRealTimers();
  });

  it("first tick fires after intervalMs; the next is armed only once the previous settles", async () => {
    const g = gate();
    let calls = 0;
    let active = 0;
    let maxActive = 0;
    const loop = every(100 as Millis, async () => {
      calls++;
      active++;
      maxActive = Math.max(maxActive, active);
      if (calls === 1) await g.wait;
      active--;
      return undefined;
    });

    await vi.advanceTimersByTimeAsync(99);
    expect(calls).toBe(0);
    await vi.advanceTimersByTimeAsync(1);
    expect(calls).toBe(1);

    await vi.advanceTimersByTimeAsync(1_000);
    expect(calls).toBe(1);

    g.open();
    await vi.advanceTimersByTimeAsync(100);
    expect(calls).toBe(2);
    expect(maxActive).toBe(1);

    await loop.stop();
  });

  it('a tick returning "stop" ends the loop', async () => {
    let calls = 0;
    const loop = every(100 as Millis, async () => {
      calls++;
      return "stop";
    });

    await vi.advanceTimersByTimeAsync(1_000);
    expect(calls).toBe(1);
    expect(vi.getTimerCount()).toBe(0);
    await loop.stop();
  });

  it("stop() waits for an in-flight tick, is idempotent, and no tick starts after it", async () => {
    const g = gate();
    let calls = 0;
    const loop = every(100 as Millis, async () => {
      calls++;
      await g.wait;
      return undefined;
    });
    await vi.advanceTimersByTimeAsync(100);
    expect(calls).toBe(1);

    let stopped = false;
    const first = loop.stop().then(() => {
      stopped = true;
    });
    const second = loop.stop();
    await vi.advanceTimersByTimeAsync(1_000);
    expect(stopped).toBe(false);

    g.open();
    await first;
    await second;
    await vi.advanceTimersByTimeAsync(1_000);
    expect(calls).toBe(1);
    expect(vi.getTimerCount()).toBe(0);
    await loop.stop();
  });

  it("a throwing tick is skipped — the loop keeps going and stop() still resolves", async () => {
    const tick = vi
      .fn<() => Promise<"stop" | undefined>>()
      .mockImplementationOnce(() => {
        throw new Error("sync boom");
      })
      .mockRejectedValueOnce(new Error("async boom"))
      .mockResolvedValue(undefined);
    const loop = every(100 as Millis, tick);

    await vi.advanceTimersByTimeAsync(300);
    expect(tick).toHaveBeenCalledTimes(3);
    await expect(loop.stop()).resolves.toBeUndefined();
  });
});
