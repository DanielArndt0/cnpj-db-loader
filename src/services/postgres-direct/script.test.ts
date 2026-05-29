import { describe, expect, it } from "vitest";

import { generatePostgresDirectScriptFiles } from "./script.js";

const FILE_MTIME = "2026-05-29T00:00:00.000Z";

describe("generatePostgresDirectScriptFiles", () => {
  it("reuses the existing quarantine and checkpoint tables for invalid partner rows", () => {
    const generated = generatePostgresDirectScriptFiles({
      files: [
        {
          dataset: "partners",
          absolutePath: "C:/ReceitaDB/test/Socios0.SOCIOCSV",
          relativePath: "Socios0.SOCIOCSV",
          fileSize: 123,
          fileMtime: FILE_MTIME,
        },
      ],
      validatedPath: "C:/ReceitaDB/test",
      sourceEncoding: "UTF8",
      transactionMode: "phase",
      include: ["partners"],
      skipIndexes: true,
      skipAnalyze: true,
    });

    const setup = generated.scripts["setup.sql"] ?? "";
    const partners = generated.scripts["load-partners.sql"] ?? "";
    const materialize = generated.scripts["materialize.sql"] ?? "";

    expect(setup).toContain("insert into import_plans");
    expect(setup).toContain("insert into import_plan_files");
    expect(partners).toContain("insert into import_checkpoints");
    expect(partners).toContain("insert into import_quarantine");
    expect(partners).toContain("Missing required value for partner_name.");
    expect(partners).toContain("postgres_direct_staging_validation");
    expect(partners).toContain("where (case");
    expect(partners).toContain("is null;");
    expect(materialize).toContain(
      "insert into import_materialization_checkpoints",
    );
  });

  it("generates generic quarantine validation for required and transformed values", () => {
    const generated = generatePostgresDirectScriptFiles({
      files: [
        {
          dataset: "companies",
          absolutePath: "C:/ReceitaDB/test/Empresas0.EMPRECSV",
          relativePath: "Empresas0.EMPRECSV",
          fileSize: 456,
          fileMtime: FILE_MTIME,
        },
      ],
      validatedPath: "C:/ReceitaDB/test",
      sourceEncoding: "UTF8",
      transactionMode: "phase",
      include: ["companies"],
      skipIndexes: true,
      skipAnalyze: true,
    });

    const companies = generated.scripts["load-companies.sql"] ?? "";

    expect(companies).toContain("HYBRID_REQUIRED_VALUE_MISSING");
    expect(companies).toContain("HYBRID_INVALID_NUMERIC_VALUE");
    expect(companies).toContain("pg_temp.cdl_safe_numeric");
    expect(companies).toContain("rows_committed = :hybrid_valid_rows");
  });
});
