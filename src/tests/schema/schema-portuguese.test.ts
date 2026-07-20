import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";

import { describe, expect, it } from "vitest";

import {
  companiesLayout,
  establishmentsLayout,
  partnersLayout,
  simplesLayout,
} from "../../dictionary/layouts/index.js";
import {
  generateSchemaSql,
  type SchemaProfile,
} from "../../services/schema.service.js";
import {
  NOMES_TABELAS_DATASET,
  NOMES_TABELAS_LOOKUP,
  NOMES_TABELAS_STAGING,
} from "../../services/schema/table-names.js";

const STATIC_SCHEMA_PATH = fileURLToPath(
  new URL("../../../sql/schema.sql", import.meta.url),
);

const PROFILES: SchemaProfile[] = ["full", "final", "staging"];

const RESIDUOS_INGLESES = [
  "companies",
  "establishments",
  "partners",
  "simples_options",
  "establishment_secondary_cnaes",
  "countries",
  "cities",
  "legal_natures",
  "partner_qualifications",
  "registration_statuses",
  "company_sizes",
  "branch_types",
  "partner_types",
  "age_groups",
  "import_plans",
  "import_plan_files",
  "import_checkpoints",
  "import_materialization_checkpoints",
  "import_quarantine",
  "staging_companies",
  "staging_establishments",
  "staging_partners",
  "staging_simples_options",
  "cnpj_root",
  "cnpj_order",
  "cnpj_check_digits",
  "cnpj_full",
  "company_name",
  "trade_name",
  "main_cnae_code",
  "share_capital",
  "partner_dedupe_key",
  "cnae_code",
  "country_code",
  "created_at",
  "updated_at",
  "batch_size",
  "target_database",
  "file_path",
  "byte_offset",
  "primary key (code)",
  "(code, description)",
];

describe("schema em portugues", () => {
  it("gera de forma deterministica para todos os perfis", () => {
    for (const profile of PROFILES) {
      const first = generateSchemaSql({ profile });
      const second = generateSchemaSql({ profile });
      expect(second).toBe(first);
    }
  });

  it("o artefato estatico espelha o builder do perfil full", () => {
    const staticSchema = readFileSync(STATIC_SCHEMA_PATH, "utf8");
    expect(staticSchema).toBe(`${generateSchemaSql({ profile: "full" })}\n`);
  });

  it("usa nomes de tabela e coluna em portugues no perfil full", () => {
    const sql = generateSchemaSql({ profile: "full" });

    for (const table of [
      "create table if not exists empresas (",
      "create table if not exists estabelecimentos (",
      "create table if not exists estabelecimento_cnaes_secundarios (",
      "create table if not exists socios (",
      "create table if not exists simples (",
      "create table if not exists paises (",
      "create table if not exists municipios (",
      "create table if not exists qualificacoes_socios (",
      "create table if not exists naturezas_juridicas (",
      "create table if not exists motivos_situacao_cadastral (",
      "create table if not exists portes_empresa (",
      "create table if not exists identificadores_matriz_filial (",
      "create table if not exists situacoes_cadastrais (",
      "create table if not exists identificadores_socio (",
      "create table if not exists faixas_etarias (",
      "create table if not exists planos_importacao (",
      "create table if not exists arquivos_plano_importacao (",
      "create table if not exists checkpoints_importacao (",
      "create table if not exists checkpoints_materializacao (",
      "create table if not exists quarentena_importacao (",
      "create unlogged table if not exists staging_empresas (",
      "create unlogged table if not exists staging_estabelecimentos (",
      "create unlogged table if not exists staging_socios (",
      "create unlogged table if not exists staging_simples (",
    ]) {
      expect(sql).toContain(table);
    }

    expect(sql).toContain("primary key (codigo)");
    expect(sql).toContain("(codigo, descricao) values");
  });

  it("nao contem residuos de nomes 2.x em ingles no perfil full", () => {
    const sql = generateSchemaSql({ profile: "full" });

    for (const residuo of RESIDUOS_INGLESES) {
      expect(sql).not.toContain(residuo);
    }
  });

  it("mantem CNPJ como texto e aplica as constraints de charset", () => {
    const sql = generateSchemaSql({ profile: "full" });

    expect(sql).toContain("cnpj_basico text not null");
    expect(sql).toContain("cnpj_ordem text not null");
    expect(sql).toContain("cnpj_dv text not null");
    expect(sql).toContain("cnpj_completo text not null");

    expect(sql).not.toMatch(/cnpj_basico (integer|bigint|numeric)/);
    expect(sql).not.toMatch(/cnpj_completo (integer|bigint|numeric)/);

    expect(sql).toContain(
      "constraint chk_empresas_cnpj_basico check (cnpj_basico ~ '^[0-9A-Z]{8}$')",
    );
    expect(sql).toContain(
      "constraint chk_estabelecimentos_cnpj_ordem check (cnpj_ordem ~ '^[0-9A-Z]{4}$')",
    );
    expect(sql).toContain(
      "constraint chk_estabelecimentos_cnpj_dv check (cnpj_dv ~ '^[0-9]{2}$')",
    );
    expect(sql).toContain(
      "constraint chk_estabelecimentos_cnpj_completo check (cnpj_completo ~ '^[0-9A-Z]{12}[0-9]{2}$')",
    );
    expect(sql).toContain(
      "constraint chk_socios_cnpj_basico check (cnpj_basico ~ '^[0-9A-Z]{8}$')",
    );
    expect(sql).toContain(
      "constraint chk_simples_cnpj_basico check (cnpj_basico ~ '^[0-9A-Z]{8}$')",
    );
  });

  it("preserva PKs, uniques e indices com os novos nomes", () => {
    const sql = generateSchemaSql({ profile: "full" });

    expect(sql).toContain("primary key (cnpj_basico)");
    expect(sql).toContain("primary key (cnpj_completo)");
    expect(sql).toContain("primary key (cnpj_completo, codigo_cnae)");
    expect(sql).toContain("unique (chave_deduplicacao_socio)");

    for (const indice of [
      "idx_estabelecimentos_cnpj_basico on estabelecimentos (cnpj_basico)",
      "idx_estabelecimento_cnaes_secundarios_codigo_cnae on estabelecimento_cnaes_secundarios (codigo_cnae)",
      "idx_socios_cnpj_basico on socios (cnpj_basico)",
      "idx_planos_importacao_status on planos_importacao (status)",
      "idx_arquivos_plano_importacao_plano_id on arquivos_plano_importacao (plano_id)",
      "idx_quarentena_importacao_pode_tentar_novamente on quarentena_importacao (pode_tentar_novamente)",
    ]) {
      expect(sql).toContain(`create index if not exists ${indice};`);
    }
  });

  it("separa corretamente os perfis final e staging", () => {
    const final = generateSchemaSql({ profile: "final" });
    expect(final).toContain("create table if not exists empresas (");
    expect(final).not.toContain("staging_empresas");

    const staging = generateSchemaSql({ profile: "staging" });
    expect(staging).toContain(
      "create unlogged table if not exists staging_empresas (",
    );
    expect(staging).not.toContain("create table if not exists empresas (");
  });
});

describe("modulo central de nomes", () => {
  it("expoe os layouts finais com nomes de tabela em portugues", () => {
    expect(companiesLayout.tableName).toBe("empresas");
    expect(establishmentsLayout.tableName).toBe("estabelecimentos");
    expect(partnersLayout.tableName).toBe("socios");
    expect(simplesLayout.tableName).toBe("simples");
  });

  it("mapeia discriminadores de lookup para nomes em portugues", () => {
    expect(NOMES_TABELAS_LOOKUP.partner_qualifications).toBe(
      "qualificacoes_socios",
    );
    expect(NOMES_TABELAS_LOOKUP.registration_statuses).toBe(
      "situacoes_cadastrais",
    );
    expect(NOMES_TABELAS_LOOKUP.age_groups).toBe("faixas_etarias");
  });

  it("mapeia datasets e staging para nomes em portugues", () => {
    expect(NOMES_TABELAS_DATASET.companies).toBe("empresas");
    expect(NOMES_TABELAS_DATASET.simples_options).toBe("simples");
    expect(NOMES_TABELAS_STAGING.establishments).toBe(
      "staging_estabelecimentos",
    );
    expect(NOMES_TABELAS_STAGING.partners).toBe("staging_socios");
  });
});
