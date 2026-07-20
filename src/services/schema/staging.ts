import {
  companiesLayout,
  establishmentsLayout,
  partnersLayout,
  simplesLayout,
} from "../../dictionary/layouts/index.js";
import { createColumnSql } from "./shared.js";
import {
  TABELA_STAGING_EMPRESAS,
  TABELA_STAGING_ESTABELECIMENTOS,
  TABELA_STAGING_SIMPLES,
  TABELA_STAGING_SOCIOS,
} from "./table-names.js";

function createUnloggedStagingTableSql(
  tableName: string,
  columnsSql: string,
): string {
  return [
    `create unlogged table if not exists ${tableName} (`,
    "  staging_id bigserial primary key,",
    columnsSql,
    ");",
  ].join("\n");
}

export function createStagingCompaniesSql(): string {
  return createUnloggedStagingTableSql(
    TABELA_STAGING_EMPRESAS,
    companiesLayout.fields.map(createColumnSql).join(",\n"),
  );
}

export function createStagingEstablishmentsSql(): string {
  return createUnloggedStagingTableSql(
    TABELA_STAGING_ESTABELECIMENTOS,
    establishmentsLayout.fields.map(createColumnSql).join(",\n"),
  );
}

export function createStagingPartnersSql(): string {
  return createUnloggedStagingTableSql(
    TABELA_STAGING_SOCIOS,
    partnersLayout.fields.map(createColumnSql).join(",\n"),
  );
}

export function createStagingSimplesSql(): string {
  return createUnloggedStagingTableSql(
    TABELA_STAGING_SIMPLES,
    simplesLayout.fields.map(createColumnSql).join(",\n"),
  );
}

export function createStagingSchemaParts(): string[] {
  return [
    "-- Tabelas de staging para importações em massa",
    createStagingCompaniesSql(),
    createStagingEstablishmentsSql(),
    createStagingPartnersSql(),
    createStagingSimplesSql(),
  ];
}
