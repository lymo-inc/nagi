import type { Kysely } from "kysely";
import { describe, expect, it, vi } from "vitest";
import { postgresStore } from "./store";
import type { StreamListener } from "./stream";

const fakeDb = {} as Kysely<unknown>;

function fakeListener(): {
  listener: StreamListener;
  disposers: ReturnType<typeof vi.fn>[];
} {
  const disposers: ReturnType<typeof vi.fn>[] = [];
  return {
    listener: {
      async listen() {
        const disposer = vi.fn(async () => undefined);
        disposers.push(disposer);
        return disposer;
      },
    },
    disposers,
  };
}

describe("postgresStore — close()", () => {
  it("disposes both LISTEN channels exactly once, even called twice", async () => {
    const { listener, disposers } = fakeListener();
    const store = postgresStore({ db: fakeDb, listener });
    await store.ready();
    await store.close();
    await store.close();
    expect(disposers).toHaveLength(2);
    for (const disposer of disposers) {
      expect(disposer).toHaveBeenCalledTimes(1);
    }
  });

  it("resolves close() even when the LISTEN rejected", async () => {
    const store = postgresStore({
      db: fakeDb,
      listener: {
        listen: () => Promise.reject(new Error("connection refused")),
      },
    });
    await expect(store.ready()).rejects.toThrow("connection refused");
    await expect(store.close()).resolves.toBeUndefined();
  });

  it("resolves close() with no listener configured", async () => {
    const store = postgresStore({ db: fakeDb });
    await expect(store.close()).resolves.toBeUndefined();
  });
});
