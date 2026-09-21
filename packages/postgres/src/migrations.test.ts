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

  describe("0008_canceled_by_run_id_fk", () => {
    const sql = (
      migrations.find((x) => x.id === "0008_canceled_by_run_id_fk") as {
        sql: (schema: string) => string;
      }
    ).sql("custom_schema");

    it("nulls the reference when the referenced run is deleted", () => {
      // Retention deletes run rows. A policy that keeps 'canceled' for audit
      // while dropping 'completed' deletes the superseder, and the victim's
      // reference has to go with it rather than dangle.
      expect(sql).toContain("ON DELETE SET NULL");
    });

    it("is checked immediately — the store never names a row before it exists", () => {
      // Worth pinning: tryStartRun cancels the prior run BEFORE inserting the
      // superseder (the partial unique index only frees the slot once the
      // prior leaves 'pending'/'running'), so the cancel write cannot name it.
      // linkSuperseder resolves the reference after the insert instead, which
      // is what lets this constraint stay immediate rather than deferred.
      expect(sql).not.toContain("DEFERRABLE");
    });

    it("qualifies the orphan-cleanup subquery against the outer row", () => {
      // workflow_run appears on both sides of the cleanup, and the inner table
      // HAS a canceled_by_run_id column: an unqualified reference binds to the
      // inner row and the UPDATE silently matches nothing.
      expect(sql).toContain("WHERE s.run_id = w.canceled_by_run_id");
    });

    it("indexes the referencing column", () => {
      // Every workflow_run DELETE re-checks the FK. pruneFacts deletes in
      // batches, so without this index retention seq-scans the table per row.
      expect(sql).toContain("workflow_run_canceled_by_idx");
    });

    it("is safe to re-apply", () => {
      expect(sql).toContain("DROP CONSTRAINT IF EXISTS");
      expect(sql).toContain("CREATE INDEX IF NOT EXISTS");
    });
  });
});
