import { describe, expect, it } from "vitest";
import { migrations } from "./migrations";

describe("migrations", () => {
  it("ships at least one migration", () => {
    expect(migrations.length).toBeGreaterThan(0);
  });

  it("ids are ordered and unique", () => {
    const ids = migrations.map((m) => m.id);
    expect(ids).toEqual([...ids].sort());
    expect(new Set(ids).size).toBe(ids.length);
  });

  it("ids use 4-digit prefix + underscore + snake (sortable)", () => {
    for (const m of migrations) {
      expect(m.id).toMatch(/^\d{4}_[a-z][a-z0-9_]*$/);
    }
  });

  it("interpolates the schema name into the DDL", () => {
    const sql = (migrations[0] as { sql: (schema: string) => string }).sql(
      "custom_schema",
    );
    expect(sql).toContain("CREATE SCHEMA IF NOT EXISTS custom_schema");
    expect(sql).toContain("custom_schema.workflow_run");
    expect(sql).toContain("custom_schema.step_run");
    expect(sql).toContain("custom_schema.fact");
    expect(sql).toContain("custom_schema.lease");
    expect(sql).toContain("custom_schema.timer");
    expect(sql).toContain("custom_schema.dedupe");
  });

  it("emits IF NOT EXISTS for every table — migrations are idempotent on partial runs", () => {
    const sql = (migrations[0] as { sql: (schema: string) => string }).sql(
      "nagi",
    );
    const tableCount = (sql.match(/CREATE TABLE IF NOT EXISTS/g) ?? []).length;
    expect(tableCount).toBeGreaterThanOrEqual(6);
  });

  it("declares all PKs (no missing PRIMARY KEY)", () => {
    const sql = (migrations[0] as { sql: (schema: string) => string }).sql(
      "nagi",
    );
    // 6 tables, each with one PRIMARY KEY declaration (either inline or composite).
    const pkCount = (sql.match(/PRIMARY KEY/g) ?? []).length;
    expect(pkCount).toBeGreaterThanOrEqual(6);
  });
});
