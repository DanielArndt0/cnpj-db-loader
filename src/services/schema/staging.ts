import {
  companiesLayout,
  establishmentsLayout,
  partnersLayout,
  simplesLayout,
} from "../../dictionary/layouts/index.js";
import { createColumnSql } from "./shared.js";

function createUnloggedStagingTableSql(
  tableName: string,
  columnsSql: string,
): string {
  return [
    `create unlogged table if not exists ${tableName} (`,
    columnsSql,
    ");",
  ].join("\n");
}

export function createStagingCompaniesSql(): string {
  return createUnloggedStagingTableSql(
    "staging_companies",
    companiesLayout.fields.map(createColumnSql).join(",\n"),
  );
}

export function createStagingEstablishmentsSql(): string {
  return createUnloggedStagingTableSql(
    "staging_establishments",
    establishmentsLayout.fields.map(createColumnSql).join(",\n"),
  );
}

export function createStagingPartnersSql(): string {
  return createUnloggedStagingTableSql(
    "staging_partners",
    partnersLayout.fields.map(createColumnSql).join(",\n"),
  );
}

export function createStagingSimplesSql(): string {
  return createUnloggedStagingTableSql(
    "staging_simples_options",
    simplesLayout.fields.map(createColumnSql).join(",\n"),
  );
}

export function createStagingSecondaryCnaesSql(): string {
  return createUnloggedStagingTableSql(
    "staging_establishment_secondary_cnaes",
    [
      "  establishment_cnpj_full text not null",
      "  cnae_code text not null",
      "  source_order integer not null",
    ].join(",\n"),
  );
}

export function createStagingSchemaParts(): string[] {
  return [
    "-- Staging tables for bulk-oriented imports",
    createStagingCompaniesSql(),
    createStagingEstablishmentsSql(),
    createStagingPartnersSql(),
    createStagingSimplesSql(),
    createStagingSecondaryCnaesSql(),
  ];
}
