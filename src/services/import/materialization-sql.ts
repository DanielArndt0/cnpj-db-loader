import {
  companiesLayout,
  establishmentsLayout,
  partnersLayout,
  simplesLayout,
} from "../../dictionary/layouts/index.js";
import { getConflictClause } from "./sql.js";
import type { ImportDatasetType, ImportSchemaCapabilities } from "./types.js";

export type MaterializationDataset =
  | "companies"
  | "establishments"
  | "partners"
  | "simples_options";

type MaterializationChunkQuery = {
  text: string;
  values: readonly unknown[];
};

const MATERIALIZATION_COLUMNS: Record<
  MaterializationDataset,
  readonly string[]
> = {
  companies: companiesLayout.fields.map((field) => field.columnName),
  establishments: establishmentsLayout.fields.map((field) => field.columnName),
  partners: partnersLayout.fields.map((field) => field.columnName),
  simples_options: simplesLayout.fields.map((field) => field.columnName),
};

function buildPartnerDedupeExpression(alias: string): string {
  return [
    `md5(`,
    `      coalesce(${alias}.cnpj_root, '') || '|' ||`,
    `      coalesce(${alias}.partner_type_code, '') || '|' ||`,
    `      coalesce(${alias}.partner_name, '') || '|' ||`,
    `      coalesce(${alias}.partner_document, '') || '|' ||`,
    `      coalesce(${alias}.partner_qualification_code, '') || '|' ||`,
    `      coalesce((${alias}.entry_date - date '2000-01-01')::text, '') || '|' ||`,
    `      coalesce(${alias}.country_code, '') || '|' ||`,
    `      coalesce(${alias}.legal_representative_document, '') || '|' ||`,
    `      coalesce(${alias}.legal_representative_name, '') || '|' ||`,
    `      coalesce(${alias}.legal_representative_qualification_code, '') || '|' ||`,
    `      coalesce(${alias}.age_group_code, '')`,
    `    )`,
  ].join("\n");
}

function buildChunkInsertSql(input: {
  stagingTable: string;
  targetTable: string;
  insertColumns: readonly string[];
  selectColumns: readonly string[];
  conflictClause: string;
  lastStagingId: number;
  chunkSize: number;
  extraSelects?: readonly string[];
}): MaterializationChunkQuery {
  const extraSelects = input.extraSelects ?? [];
  const chunkSelectList = [
    "source.staging_id",
    ...input.selectColumns.map((column) => `source.${column}`),
    ...extraSelects,
  ].join(",\n    ");
  const insertSelectList = input.insertColumns.join(", ");

  return {
    text: [
      "with chunked as (",
      `  select\n    ${chunkSelectList}`,
      `  from ${input.stagingTable} source`,
      "  where source.staging_id > $1",
      "  order by source.staging_id asc",
      "  limit $2",
      "),",
      "inserted as (",
      `  insert into ${input.targetTable} (${input.insertColumns.join(", ")})`,
      `  select ${insertSelectList}`,
      "  from chunked",
      ...(input.conflictClause ? [input.conflictClause] : []),
      "  returning 1",
      ")",
      "select",
      "  coalesce((select max(staging_id)::bigint from chunked), $1::bigint) as max_staging_id,",
      "  coalesce((select count(*)::bigint from chunked), 0)::bigint as source_rows,",
      "  coalesce((select count(*)::bigint from inserted), 0)::bigint as affected_rows;",
    ].join("\n"),
    values: [input.lastStagingId, input.chunkSize],
  };
}

export function buildMaterializationChunkQuery(input: {
  dataset: MaterializationDataset;
  schemaCapabilities: ImportSchemaCapabilities;
  lastStagingId: number;
  chunkSize: number;
}): MaterializationChunkQuery {
  const baseColumns = MATERIALIZATION_COLUMNS[input.dataset];

  switch (input.dataset) {
    case "partners": {
      const insertColumns = input.schemaCapabilities
        .includePartnerDedupeKeyInInsert
        ? [...baseColumns, "partner_dedupe_key"]
        : [...baseColumns];
      return buildChunkInsertSql({
        stagingTable: "staging_partners",
        targetTable: "partners",
        insertColumns,
        selectColumns: baseColumns,
        conflictClause: getConflictClause("partners", [...insertColumns]),
        lastStagingId: input.lastStagingId,
        chunkSize: input.chunkSize,
        extraSelects: input.schemaCapabilities.includePartnerDedupeKeyInInsert
          ? [`${buildPartnerDedupeExpression("source")} as partner_dedupe_key`]
          : [],
      });
    }
    case "companies":
      return buildChunkInsertSql({
        stagingTable: "staging_companies",
        targetTable: "companies",
        insertColumns: baseColumns,
        selectColumns: baseColumns,
        conflictClause: getConflictClause("companies", [...baseColumns]),
        lastStagingId: input.lastStagingId,
        chunkSize: input.chunkSize,
      });
    case "establishments":
      return buildChunkInsertSql({
        stagingTable: "staging_establishments",
        targetTable: "establishments",
        insertColumns: baseColumns,
        selectColumns: baseColumns,
        conflictClause: getConflictClause("establishments", [...baseColumns]),
        lastStagingId: input.lastStagingId,
        chunkSize: input.chunkSize,
      });
    case "simples_options":
      return buildChunkInsertSql({
        stagingTable: "staging_simples_options",
        targetTable: "simples_options",
        insertColumns: baseColumns,
        selectColumns: baseColumns,
        conflictClause: getConflictClause("simples_options", [...baseColumns]),
        lastStagingId: input.lastStagingId,
        chunkSize: input.chunkSize,
      });
  }
}

export function buildSecondaryCnaesMaterializationChunkQuery(input: {
  lastStagingId: number;
  chunkSize: number;
}): MaterializationChunkQuery {
  return {
    text: [
      "with chunked as (",
      "  select",
      "    source.staging_id,",
      "    source.establishment_cnpj_full,",
      "    source.cnae_code,",
      "    source.source_order",
      "  from staging_establishment_secondary_cnaes source",
      "  where source.staging_id > $1",
      "  order by source.staging_id asc",
      "  limit $2",
      "),",
      "inserted as (",
      "  insert into establishment_secondary_cnaes (establishment_cnpj_full, cnae_code, source_order)",
      "  select establishment_cnpj_full, cnae_code, source_order",
      "  from chunked",
      "  on conflict (establishment_cnpj_full, cnae_code) do update set source_order = excluded.source_order",
      "  returning 1",
      ")",
      "select",
      "  coalesce((select max(staging_id)::bigint from chunked), $1::bigint) as max_staging_id,",
      "  coalesce((select count(*)::bigint from chunked), 0)::bigint as source_rows,",
      "  coalesce((select count(*)::bigint from inserted), 0)::bigint as affected_rows;",
    ].join("\n"),
    values: [input.lastStagingId, input.chunkSize],
  };
}

export function isMaterializationDataset(
  dataset: ImportDatasetType,
): dataset is MaterializationDataset {
  return [
    "companies",
    "establishments",
    "partners",
    "simples_options",
  ].includes(dataset);
}
