import { describe, expect, it } from "vitest";

import { generatePostgresDirectScriptFiles } from "../../services/postgres-direct/script.js";

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

    expect(setup).toContain("insert into planos_importacao");
    expect(setup).toContain("insert into arquivos_plano_importacao");

    expect(partners).toContain("insert into checkpoints_importacao");
    expect(partners).toContain("insert into quarentena_importacao");
    expect(partners).toContain(
      "Missing required value for nome_socio_razao_social.",
    );
    expect(partners).toContain("HYBRID_INVALID_DATE_VALUE");
    expect(partners).toContain("pg_temp.cdl_safe_date");
    expect(partners).toContain("postgres_direct_staging_validation");
    expect(partners).toContain("where (case");
    expect(partners).toContain("is null;");

    expect(materialize).toMatch(/insert into [^\n;]*materializa/i);
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
    expect(companies).toContain("linhas_confirmadas = :hybrid_valid_rows");

    expect(companies).not.toContain(":hybrid_quarantined_rows+ delete");
  });
});
