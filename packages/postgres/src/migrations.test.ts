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
    const totalTables = migrations.reduce(
      (n, m) =>
        n + (m.sql("nagi").match(/CREATE TABLE IF NOT EXISTS/g) ?? []).length,
      0,
    );
    expect(totalTables).toBeGreaterThanOrEqual(6);
  });

  it("declares all PKs (no missing PRIMARY KEY)", () => {
    const totalPks = migrations.reduce(
      (n, m) => n + (m.sql("nagi").match(/PRIMARY KEY/g) ?? []).length,
      0,
    );
    expect(totalPks).toBeGreaterThanOrEqual(6);
  });

  it("0002_snapshot_tables adds snapshot store DDL", () => {
    const m = migrations.find((x) => x.id === "0002_snapshot_tables");
    expect(m).toBeDefined();
    const sql = (m as { sql: (schema: string) => string }).sql("custom_schema");
    expect(sql).toContain("custom_schema.flow_snapshot");
    expect(sql).toContain("custom_schema.flow_ref");
    expect(sql).toContain("custom_schema.global_fact");
    expect(sql).toContain("ADD COLUMN IF NOT EXISTS flow_hash");
    expect(sql).toContain("ADD COLUMN IF NOT EXISTS code_version");
    expect(sql).toContain("REFERENCES custom_schema.flow_snapshot(flow_hash)");
  });
});
