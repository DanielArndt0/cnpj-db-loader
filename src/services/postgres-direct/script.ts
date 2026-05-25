import path from "node:path";

import {
  companiesLayout,
  establishmentsLayout,
  partnersLayout,
  simplesLayout,
} from "../../dictionary/layouts/index.js";
import type { FieldDefinition } from "../../dictionary/layouts/index.js";
import type { ImportDatasetType } from "../import/types.js";
import { DATASET_LAYOUTS } from "../import/types.js";
import type { PostgresCsvFile, PostgresDirectSourceFile } from "./types.js";

type CsvScriptGenerationInput = {
  files: PostgresCsvFile[];
};

type SanitizedScriptGenerationInput = {
  files: PostgresDirectSourceFile[];
  sourceEncoding: string;
};

const STAGING_DATASETS: readonly ImportDatasetType[] = [
  "companies",
  "establishments",
  "partners",
  "simples_options",
];

const DOMAIN_DATASETS: readonly ImportDatasetType[] = [
  "partner_qualifications",
  "legal_natures",
  "countries",
  "cities",
  "reasons",
  "cnaes",
];

const STAGING_TABLE_BY_DATASET: Partial<Record<ImportDatasetType, string>> = {
  companies: "staging_companies",
  establishments: "staging_establishments",
  partners: "staging_partners",
  simples_options: "staging_simples_options",
};

function quoteSqlLiteral(value: string): string {
  return `'${value.replace(/'/g, "''")}'`;
}

function quoteIdentifier(value: string): string {
  return `"${value.replace(/"/g, '""')}"`;
}

function normalizePathForPsql(filePath: string): string {
  return path.resolve(filePath).replace(/\\/g, "/");
}

function csvCopyCommand(
  tableName: string,
  columns: readonly string[],
  filePath: string,
): string {
  const normalizedFilePath = normalizePathForPsql(filePath);
  return `\\copy ${tableName} (${columns.join(", ")}) from ${quoteSqlLiteral(normalizedFilePath)} with (format csv, header true, delimiter ',', quote '"', escape '"', null '')`;
}

function receitaCopyCommand(
  tableName: string,
  columns: readonly string[],
  filePath: string,
): string {
  const normalizedFilePath = normalizePathForPsql(filePath);
  return `\\copy ${tableName} (${columns.join(", ")}) from ${quoteSqlLiteral(normalizedFilePath)} with (format csv, header false, delimiter ';', quote '"', escape '"')`;
}

function datasetColumns(dataset: ImportDatasetType): string[] {
  return DATASET_LAYOUTS[dataset].fields.map((field) => field.columnName);
}

function updateAssignments(
  columns: readonly string[],
  excludedColumns: readonly string[],
): string {
  return columns
    .filter((column) => !excludedColumns.includes(column))
    .map((column) => `${column} = excluded.${column}`)
    .concat(["updated_at = now()"])
    .join(",\n  ");
}

function partnerDedupeExpression(alias: string): string {
  return [
    "md5(",
    `  coalesce(${alias}.cnpj_root, '') || '|' ||`,
    `  coalesce(${alias}.partner_type_code, '') || '|' ||`,
    `  coalesce(${alias}.partner_name, '') || '|' ||`,
    `  coalesce(${alias}.partner_document, '') || '|' ||`,
    `  coalesce(${alias}.partner_qualification_code, '') || '|' ||`,
    `  coalesce((${alias}.entry_date - date '2000-01-01')::text, '') || '|' ||`,
    `  coalesce(${alias}.country_code, '') || '|' ||`,
    `  coalesce(${alias}.legal_representative_document, '') || '|' ||`,
    `  coalesce(${alias}.legal_representative_name, '') || '|' ||`,
    `  coalesce(${alias}.legal_representative_qualification_code, '') || '|' ||`,
    `  coalesce(${alias}.age_group_code, '')`,
    ")",
  ].join("\n");
}

function materializeCompaniesSql(): string {
  const columns = companiesLayout.fields.map((field) => field.columnName);

  return [
    "\\echo 'Materializing companies...'",
    "with source as (",
    "  select",
    `    ${columns.map((column) => `source.${column}`).join(",\n    ")},`,
    "    row_number() over (partition by source.cnpj_root order by source.staging_id desc) as dedupe_rank",
    "  from staging_companies source",
    "),",
    "deduped as (",
    "  select * from source where dedupe_rank = 1",
    ")",
    `insert into companies (${columns.join(", ")})`,
    `select ${columns.join(", ")}`,
    "from deduped",
    "on conflict (cnpj_root) do update set",
    `  ${updateAssignments(columns, ["cnpj_root"])};`,
  ].join("\n");
}

function materializeEstablishmentsSql(): string {
  const baseColumns = establishmentsLayout.fields.map(
    (field) => field.columnName,
  );
  const insertColumns = [...baseColumns, "cnpj_full"];

  return [
    "\\echo 'Materializing establishments and secondary CNAEs...'",
    "with source as (",
    "  select",
    `    ${baseColumns.map((column) => `source.${column}`).join(",\n    ")},`,
    "    source.cnpj_root || source.cnpj_order || source.cnpj_check_digits as cnpj_full,",
    "    row_number() over (partition by source.cnpj_root || source.cnpj_order || source.cnpj_check_digits order by source.staging_id desc) as dedupe_rank",
    "  from staging_establishments source",
    "),",
    "deduped as (",
    "  select * from source where dedupe_rank = 1",
    "),",
    "upserted as (",
    `  insert into establishments (${insertColumns.join(", ")})`,
    `  select ${insertColumns.join(", ")}`,
    "  from deduped",
    "  on conflict (cnpj_full) do update set",
    `    ${updateAssignments(insertColumns, ["cnpj_root", "cnpj_order", "cnpj_check_digits", "cnpj_full"])}`,
    "  returning cnpj_full",
    "),",
    "deleted_secondary_cnaes as (",
    "  delete from establishment_secondary_cnaes target",
    "  using (select cnpj_full from deduped) source_keys",
    "  where target.cnpj_full = source_keys.cnpj_full",
    "  returning 1",
    "),",
    "secondary_cnaes_source as (",
    "  select distinct",
    "    deduped.cnpj_full,",
    "    btrim(cnae_code) as cnae_code",
    "  from deduped",
    "  cross join lateral unnest(string_to_array(deduped.secondary_cnaes_raw, ',')) as cnae_code",
    "  where deduped.secondary_cnaes_raw is not null",
    "    and deduped.secondary_cnaes_raw <> ''",
    "    and btrim(cnae_code) <> ''",
    ")",
    "insert into establishment_secondary_cnaes (cnpj_full, cnae_code)",
    "select cnpj_full, cnae_code",
    "from secondary_cnaes_source",
    "on conflict (cnpj_full, cnae_code) do nothing;",
  ].join("\n");
}

function materializePartnersSql(): string {
  const baseColumns = partnersLayout.fields.map((field) => field.columnName);
  const insertColumns = [...baseColumns, "partner_dedupe_key"];

  return [
    "\\echo 'Materializing partners...'",
    "with source as (",
    "  select",
    `    ${baseColumns.map((column) => `source.${column}`).join(",\n    ")},`,
    `    ${partnerDedupeExpression("source")} as partner_dedupe_key`,
    "  from staging_partners source",
    "),",
    "ranked as (",
    "  select",
    "    source.*,",
    "    row_number() over (partition by source.partner_dedupe_key order by source.cnpj_root asc) as dedupe_rank",
    "  from source",
    "),",
    "deduped as (",
    "  select * from ranked where dedupe_rank = 1",
    ")",
    `insert into partners (${insertColumns.join(", ")})`,
    `select ${insertColumns.join(", ")}`,
    "from deduped",
    "on conflict (partner_dedupe_key) do update set",
    `  ${updateAssignments(insertColumns, ["partner_dedupe_key"])};`,
  ].join("\n");
}

function materializeSimplesSql(): string {
  const columns = simplesLayout.fields.map((field) => field.columnName);

  return [
    "\\echo 'Materializing simples options...'",
    "with source as (",
    "  select",
    `    ${columns.map((column) => `source.${column}`).join(",\n    ")},`,
    "    row_number() over (partition by source.cnpj_root order by source.staging_id desc) as dedupe_rank",
    "  from staging_simples_options source",
    "),",
    "deduped as (",
    "  select * from source where dedupe_rank = 1",
    ")",
    `insert into simples_options (${columns.join(", ")})`,
    `select ${columns.join(", ")}`,
    "from deduped",
    "on conflict (cnpj_root) do update set",
    `  ${updateAssignments(columns, ["cnpj_root"])};`,
  ].join("\n");
}

function copyDomainSql(
  dataset: ImportDatasetType,
  files: readonly PostgresCsvFile[],
): string[] {
  if (files.length === 0) {
    return [];
  }

  const columns = datasetColumns(dataset);
  const tempTable = `tmp_hybrid_${dataset}`;
  const lines = [
    `\\echo 'Loading ${dataset} lookup data...'`,
    `drop table if exists ${tempTable};`,
    `create temporary table ${tempTable} (code text, description text);`,
  ];

  for (const file of files) {
    lines.push(csvCopyCommand(tempTable, columns, file.absolutePath));
  }

  lines.push(
    `insert into ${dataset} (${columns.join(", ")})`,
    `select distinct on (code) ${columns.join(", ")}`,
    `from ${tempTable}`,
    "where code is not null and code <> ''",
    "order by code",
    "on conflict (code) do update set description = excluded.description;",
  );

  return lines;
}

function copyStagingSql(
  dataset: ImportDatasetType,
  files: readonly PostgresCsvFile[],
): string[] {
  if (files.length === 0) {
    return [];
  }

  const tableName = STAGING_TABLE_BY_DATASET[dataset];
  if (!tableName) {
    return [];
  }

  const columns = datasetColumns(dataset);
  return [
    `\\echo 'Loading ${dataset} staging data...'`,
    ...files.map((file) =>
      csvCopyCommand(tableName, columns, file.absolutePath),
    ),
  ];
}

function csvFilesByDataset(
  files: readonly PostgresCsvFile[],
): Partial<Record<ImportDatasetType, PostgresCsvFile[]>> {
  const grouped: Partial<Record<ImportDatasetType, PostgresCsvFile[]>> = {};

  for (const file of files) {
    const items = grouped[file.dataset] ?? [];
    items.push(file);
    grouped[file.dataset] = items;
  }

  return grouped;
}

function directFilesByDataset(
  files: readonly PostgresDirectSourceFile[],
): Partial<Record<ImportDatasetType, PostgresDirectSourceFile[]>> {
  const grouped: Partial<
    Record<ImportDatasetType, PostgresDirectSourceFile[]>
  > = {};

  for (const file of files) {
    const items = grouped[file.dataset] ?? [];
    items.push(file);
    grouped[file.dataset] = items;
  }

  return grouped;
}

function rawTableName(dataset: ImportDatasetType): string {
  return `tmp_hybrid_raw_${dataset}`;
}

function createRawTempTableSql(dataset: ImportDatasetType): string {
  const columns = DATASET_LAYOUTS[dataset].fields
    .map((field) => `  ${quoteIdentifier(field.columnName)} text`)
    .join(",\n");

  return [
    `drop table if exists ${rawTableName(dataset)};`,
    `create temporary table ${rawTableName(dataset)} (`,
    columns,
    ");",
  ].join("\n");
}

function textExpression(alias: string, column: string): string {
  return `nullif(btrim(${alias}.${quoteIdentifier(column)}), '')`;
}

function dateExpression(alias: string, column: string): string {
  const value = `btrim(${alias}.${quoteIdentifier(column)})`;
  return [
    "case",
    `  when ${value} = '' or ${value} = '00000000' then null`,
    `  when ${value} ~ '^\\d{8}$' then to_date(${value}, 'YYYYMMDD')`,
    "  else null",
    "end",
  ].join(" ");
}

function numericExpression(alias: string, column: string): string {
  const value = `btrim(${alias}.${quoteIdentifier(column)})`;
  return [
    "case",
    `  when ${value} = '' then null`,
    `  when ${value} like '%,%' and ${value} like '%.%' then replace(replace(${value}, '.', ''), ',', '.')::numeric`,
    `  when ${value} like '%,%' then replace(${value}, ',', '.')::numeric`,
    `  else ${value}::numeric`,
    "end",
  ].join(" ");
}

function integerExpression(alias: string, column: string): string {
  const value = `btrim(${alias}.${quoteIdentifier(column)})`;
  return [
    "case",
    `  when ${value} = '' then null`,
    `  when ${value} ~ '^-?\\d+$' then ${value}::integer`,
    "  else null",
    "end",
  ].join(" ");
}

function booleanExpression(alias: string, column: string): string {
  const value = `lower(btrim(${alias}.${quoteIdentifier(column)}))`;
  return [
    "case",
    `  when ${value} in ('1', 'true', 't', 'y', 'yes', 's') then true`,
    `  when ${value} in ('0', 'false', 'f', 'n', 'no') then false`,
    "  else null",
    "end",
  ].join(" ");
}

function fieldExpression(
  dataset: ImportDatasetType,
  field: FieldDefinition,
  alias: string,
): string {
  const column = field.columnName;

  if (dataset === "companies" && column === "company_size_code") {
    return `coalesce(${textExpression(alias, column)}, '00')`;
  }

  if (dataset === "establishments" && column === "branch_type_code") {
    return `coalesce(${textExpression(alias, column)}, '1')`;
  }

  if (dataset === "establishments" && column === "registration_status_code") {
    return `coalesce(${textExpression(alias, column)}, '01')`;
  }

  switch (field.dataType) {
    case "date":
      return dateExpression(alias, column);
    case "numeric":
      return numericExpression(alias, column);
    case "integer":
      return integerExpression(alias, column);
    case "boolean":
      return booleanExpression(alias, column);
    default:
      return textExpression(alias, column);
  }
}

function rawDomainSql(
  dataset: ImportDatasetType,
  files: readonly PostgresDirectSourceFile[],
): string[] {
  if (files.length === 0) {
    return [];
  }

  const layout = DATASET_LAYOUTS[dataset];
  const columns = layout.fields.map((field) => field.columnName);
  const tableName = rawTableName(dataset);

  const lines = [
    `\\echo 'Loading ${dataset} lookup data directly from sanitized Receita files...'`,
    createRawTempTableSql(dataset),
  ];

  for (const file of files) {
    lines.push(receitaCopyCommand(tableName, columns, file.absolutePath));
  }

  lines.push(
    `insert into ${dataset} (${columns.join(", ")})`,
    "select distinct on (code)",
    "  nullif(btrim(code), '') as code,",
    "  nullif(btrim(description), '') as description",
    `from ${tableName}`,
    "where nullif(btrim(code), '') is not null",
    "order by code",
    "on conflict (code) do update set description = excluded.description;",
  );

  return lines;
}

function rawStagingSql(
  dataset: ImportDatasetType,
  files: readonly PostgresDirectSourceFile[],
): string[] {
  if (files.length === 0) {
    return [];
  }

  const targetTable = STAGING_TABLE_BY_DATASET[dataset];
  if (!targetTable) {
    return [];
  }

  const layout = DATASET_LAYOUTS[dataset];
  const columns = layout.fields.map((field) => field.columnName);
  const tableName = rawTableName(dataset);
  const alias = "source";
  const expressions = layout.fields.map(
    (field) =>
      `  ${fieldExpression(dataset, field, alias)} as ${field.columnName}`,
  );

  const lines = [
    `\\echo 'Loading ${dataset} staging data directly from sanitized Receita files...'`,
    createRawTempTableSql(dataset),
  ];

  for (const file of files) {
    lines.push(receitaCopyCommand(tableName, columns, file.absolutePath));
  }

  lines.push(
    `insert into ${targetTable} (${columns.join(", ")})`,
    "select",
    expressions.join(",\n"),
    `from ${tableName} ${alias};`,
  );

  return lines;
}

export function generatePostgresDirectImportScript(
  input: CsvScriptGenerationInput,
): string {
  const grouped = csvFilesByDataset(input.files);

  const lines = [
    "-- CNPJ DB Loader hybrid PostgreSQL import script",
    "-- Generated from PostgreSQL-ready CSV files exported by cnpj-db-loader postgres export-csv.",
    "-- Execute with psql, for example:",
    '--   psql "postgres://postgres:postgres@localhost:5432/cnpj" -f import-postgres-direct.sql',
    "",
    "\\set ON_ERROR_STOP on",
    "\\echo 'Starting CNPJ DB Loader hybrid PostgreSQL import...'",
    "",
    "begin;",
    "",
    "-- Keep the final schema and seed data managed by sql/schema.sql.",
    "-- This script only resets staging tables and then upserts final data.",
    "truncate table staging_companies restart identity;",
    "truncate table staging_establishments restart identity;",
    "truncate table staging_partners restart identity;",
    "truncate table staging_simples_options restart identity;",
    "",
  ];

  for (const dataset of DOMAIN_DATASETS) {
    lines.push(...copyDomainSql(dataset, grouped[dataset] ?? []), "");
  }

  for (const dataset of STAGING_DATASETS) {
    lines.push(...copyStagingSql(dataset, grouped[dataset] ?? []), "");
  }

  lines.push(...materializationAndAnalyzeSql());

  return lines.join("\n");
}

export function generatePostgresSanitizedDirectImportScript(
  input: SanitizedScriptGenerationInput,
): string {
  const grouped = directFilesByDataset(input.files);

  const lines = [
    "-- CNPJ DB Loader direct PostgreSQL import script",
    "-- Generated from sanitized Receita files by cnpj-db-loader postgres generate-script.",
    "-- This path avoids rewriting the dataset into a second CSV tree.",
    "-- Execute with psql, for example:",
    '--   psql "postgres://postgres:postgres@localhost:5432/cnpj" -f import-postgres-direct.sql',
    "",
    "\\set ON_ERROR_STOP on",
    `\\echo 'Using source file encoding ${input.sourceEncoding} for psql copy operations...'`,
    `set client_encoding to ${quoteSqlLiteral(input.sourceEncoding)};`,
    "\\echo 'Starting CNPJ DB Loader direct PostgreSQL import from sanitized files...'",
    "",
    "begin;",
    "",
    "-- Keep the final schema and seed data managed by sql/schema.sql.",
    "-- This script copies sanitized Receita files into temporary raw tables,",
    "-- transforms values inside PostgreSQL, resets staging tables and upserts final data.",
    "truncate table staging_companies restart identity;",
    "truncate table staging_establishments restart identity;",
    "truncate table staging_partners restart identity;",
    "truncate table staging_simples_options restart identity;",
    "",
  ];

  for (const dataset of DOMAIN_DATASETS) {
    lines.push(...rawDomainSql(dataset, grouped[dataset] ?? []), "");
  }

  for (const dataset of STAGING_DATASETS) {
    lines.push(...rawStagingSql(dataset, grouped[dataset] ?? []), "");
  }

  lines.push(...materializationAndAnalyzeSql());

  return lines.join("\n");
}

function materializationAndAnalyzeSql(): string[] {
  return [
    materializeCompaniesSql(),
    "",
    materializeEstablishmentsSql(),
    "",
    materializePartnersSql(),
    "",
    materializeSimplesSql(),
    "",
    "\\echo 'Refreshing planner statistics...'",
    "analyze companies;",
    "analyze establishments;",
    "analyze establishment_secondary_cnaes;",
    "analyze partners;",
    "analyze simples_options;",
    "analyze cnaes;",
    "analyze cities;",
    "analyze countries;",
    "analyze legal_natures;",
    "analyze partner_qualifications;",
    "analyze reasons;",
    "",
    "commit;",
    "",
    "\\echo 'CNPJ DB Loader hybrid PostgreSQL import completed.'",
    "",
  ];
}
