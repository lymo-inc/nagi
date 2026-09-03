import type { Kysely } from "kysely";
import { describe, expect, it } from "vitest";
import { postgresStore } from "./store";

const fakeDb = {} as Kysely<unknown>;

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
