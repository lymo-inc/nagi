import type { Kysely } from "kysely";
import { describe, expect, it } from "vitest";
import { postgresStore } from "./store";
import { postgresTrigger } from "./trigger";

const fakeDb = {} as Kysely<unknown>;
const fakeListen = {
  query: async () => undefined,
  on: () => undefined,
};

describe("postgresStore — config validation", () => {
  it("accepts the default schema (nagi)", () => {
    expect(() => postgresStore({ db: fakeDb })).not.toThrow();
  });

  it.each([
    "custom",
    "Custom",
    "with_underscores",
    "_leading_underscore",
  ])("accepts safe schema name %s", (schema) => {
    expect(() => postgresStore({ db: fakeDb, schema })).not.toThrow();
  });

  it.each([
    "1bad",
    "has space",
    "with-dash",
    'with"quote',
    "has;semicolon",
    "",
  ])("rejects unsafe schema name %s", (schema) => {
    expect(() => postgresStore({ db: fakeDb, schema })).toThrow(
      /invalid schema name/i,
    );
  });
});

describe("postgresTrigger — channel validation", () => {
  it("accepts the default channel (nagi)", () => {
    expect(() => postgresTrigger({ listen: fakeListen })).not.toThrow();
  });

  it.each([
    "1bad",
    "has space",
    "with-dash",
    "DROP TABLE foo",
  ])("rejects unsafe channel %s", (channel) => {
    expect(() => postgresTrigger({ listen: fakeListen, channel })).toThrow(
      /invalid NOTIFY channel/i,
    );
  });

  it("subscribe → unsubscribe is a no-op when never notified", () => {
    const trigger = postgresTrigger({ listen: fakeListen });
    const unsubscribe = trigger.subscribe(() => {
      throw new Error("should not fire");
    });
    unsubscribe();
  });
});
