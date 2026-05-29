import { createHash } from "node:crypto";
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
import type {
  PostgresCsvFile,
  PostgresDirectIncludeTarget,
  PostgresDirectScriptStep,
  PostgresDirectSourceFile,
  PostgresDirectTransactionMode,
} from "./types.js";

type CsvScriptGenerationInput = {
  files: PostgresCsvFile[];
};

type SanitizedScriptGenerationInput = {
  files: PostgresDirectSourceFile[];
  validatedPath?: string | undefined;
  sourceEncoding: string;
  transactionMode: PostgresDirectTransactionMode;
  include: readonly PostgresDirectIncludeTarget[];
  skipIndexes: boolean;
  skipAnalyze: boolean;
};

export type GeneratedPostgresDirectScripts = {
  scripts: Record<string, string>;
  steps: PostgresDirectScriptStep[];
  sourceFingerprint: string;
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

const STEP_ORDER = [
  "setup",
  "load-domains",
  "load-companies",
  "load-establishments",
  "load-partners",
  "load-simples",
  "materialize",
  "materialize-secondary-cnaes",
  "indexes",
  "analyze",
] as const;

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

function echo(message: string): string {
  return `\\echo ${quoteSqlLiteral(message)}`;
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
    echo("[materialize] Materializing companies..."),
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
    echo("[materialize] Companies materialization completed."),
  ].join("\n");
}

function materializeEstablishmentsSql(): string {
  const baseColumns = establishmentsLayout.fields.map(
    (field) => field.columnName,
  );
  const insertColumns = [...baseColumns, "cnpj_full"];

  return [
    echo("[materialize] Materializing establishments..."),
    "with source as (",
    "  select",
    `    ${baseColumns.map((column) => `source.${column}`).join(",\n    ")},`,
    "    source.cnpj_root || source.cnpj_order || source.cnpj_check_digits as cnpj_full,",
    "    row_number() over (partition by source.cnpj_root || source.cnpj_order || source.cnpj_check_digits order by source.staging_id desc) as dedupe_rank",
    "  from staging_establishments source",
    "),",
    "deduped as (",
    "  select * from source where dedupe_rank = 1",
    ")",
    `insert into establishments (${insertColumns.join(", ")})`,
    `select ${insertColumns.join(", ")}`,
    "from deduped",
    "on conflict (cnpj_full) do update set",
    `  ${updateAssignments(insertColumns, ["cnpj_root", "cnpj_order", "cnpj_check_digits", "cnpj_full"])};`,
    echo("[materialize] Establishments materialization completed."),
  ].join("\n");
}

function materializeSecondaryCnaesSql(): string {
  return [
    echo(
      "[materialize-secondary-cnaes] Materializing establishment secondary CNAEs...",
    ),
    "with source as (",
    "  select",
    "    staging.cnpj_root || staging.cnpj_order || staging.cnpj_check_digits as cnpj_full,",
    "    staging.secondary_cnaes_raw,",
    "    row_number() over (partition by staging.cnpj_root || staging.cnpj_order || staging.cnpj_check_digits order by staging.staging_id desc) as dedupe_rank",
    "  from staging_establishments staging",
    "),",
    "deduped as (",
    "  select * from source where dedupe_rank = 1",
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
    echo(
      "[materialize-secondary-cnaes] Secondary CNAEs materialization completed.",
    ),
  ].join("\n");
}

function materializePartnersSql(): string {
  const baseColumns = partnersLayout.fields.map((field) => field.columnName);
  const insertColumns = [...baseColumns, "partner_dedupe_key"];

  return [
    echo("[materialize] Materializing partners..."),
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
    echo("[materialize] Partners materialization completed."),
  ].join("\n");
}

function materializeSimplesSql(): string {
  const columns = simplesLayout.fields.map((field) => field.columnName);

  return [
    echo("[materialize] Materializing simples options..."),
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
    echo("[materialize] Simples options materialization completed."),
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
    echo(`[load-domains] Loading ${dataset} lookup data...`),
    `drop table if exists ${tempTable};`,
    `create temporary table ${tempTable} (code text, description text);`,
  ];

  for (const [index, file] of files.entries()) {
    lines.push(
      echo(
        `[load-domains] Loading ${dataset} file ${index + 1} of ${files.length}: ${file.relativePath}`,
      ),
      csvCopyCommand(tempTable, columns, file.absolutePath),
      echo(
        `[load-domains] Loaded ${dataset} file ${index + 1} of ${files.length}.`,
      ),
    );
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
  const lines = [echo(`[load-${dataset}] Loading ${dataset} staging data...`)];

  for (const [index, file] of files.entries()) {
    lines.push(
      echo(
        `[load-${dataset}] Loading file ${index + 1} of ${files.length}: ${file.relativePath}`,
      ),
      csvCopyCommand(tableName, columns, file.absolutePath),
      echo(`[load-${dataset}] Loaded file ${index + 1} of ${files.length}.`),
    );
  }

  return lines;
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
    "set client_min_messages to warning;",
    `drop table if exists ${rawTableName(dataset)};`,
    "reset client_min_messages;",
    `create temporary table ${rawTableName(dataset)} (`,
    columns,
    ");",
  ].join("\n");
}

function safeConversionFunctionsSql(): string[] {
  return [
    `create or replace function pg_temp.cdl_safe_date(value text)
returns date
language plpgsql
immutable
as $$
declare
  normalized text := btrim(value);
begin
  if normalized = '' or normalized = '00000000' then
    return null;
  end if;

  if normalized !~ '^\\d{8}$' then
    return null;
  end if;

  return (
    substring(normalized, 1, 4) || '-' ||
    substring(normalized, 5, 2) || '-' ||
    substring(normalized, 7, 2)
  )::date;
exception when others then
  return null;
end;
$$;`,
    `create or replace function pg_temp.cdl_safe_numeric(value text)
returns numeric
language plpgsql
immutable
as $$
declare
  normalized text := btrim(value);
begin
  if normalized = '' then
    return null;
  end if;

  if position(',' in normalized) > 0 and position('.' in normalized) > 0 then
    normalized := replace(replace(normalized, '.', ''), ',', '.');
  elsif position(',' in normalized) > 0 then
    normalized := replace(normalized, ',', '.');
  end if;

  return normalized::numeric;
exception when others then
  return null;
end;
$$;`,
  ];
}

function textExpression(alias: string, column: string): string {
  return `nullif(btrim(${alias}.${quoteIdentifier(column)}), '')`;
}

function dateExpression(alias: string, column: string): string {
  return `pg_temp.cdl_safe_date(${alias}.${quoteIdentifier(column)})`;
}

function numericExpression(alias: string, column: string): string {
  return `pg_temp.cdl_safe_numeric(${alias}.${quoteIdentifier(column)})`;
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

type HybridValidationRule = {
  condition: string;
  code: string;
  category: string;
  message: string;
};

function rawTrimExpression(alias: string, column: string): string {
  return `btrim(${alias}.${quoteIdentifier(column)})`;
}

function hasDefaultValue(dataset: ImportDatasetType, column: string): boolean {
  return (
    (dataset === "companies" && column === "company_size_code") ||
    (dataset === "establishments" && column === "branch_type_code") ||
    (dataset === "establishments" && column === "registration_status_code")
  );
}

function validationRules(
  dataset: ImportDatasetType,
  alias: string,
): HybridValidationRule[] {
  const layout = DATASET_LAYOUTS[dataset];
  const rules: HybridValidationRule[] = [];

  for (const field of layout.fields) {
    const rawValue = rawTrimExpression(alias, field.columnName);
    const parsedValue = fieldExpression(dataset, field, alias);

    if (field.dataType === "date") {
      rules.push({
        condition: `${rawValue} <> '' and ${rawValue} <> '00000000' and ${parsedValue} is null`,
        code: "HYBRID_INVALID_DATE_VALUE",
        category: "transform_error",
        message: `Invalid date value for ${field.columnName}.`,
      });
    }

    if (field.dataType === "numeric") {
      rules.push({
        condition: `${rawValue} <> '' and ${parsedValue} is null`,
        code: "HYBRID_INVALID_NUMERIC_VALUE",
        category: "transform_error",
        message: `Invalid numeric value for ${field.columnName}.`,
      });
    }

    if (field.dataType === "integer") {
      rules.push({
        condition: `${rawValue} <> '' and ${parsedValue} is null`,
        code: "HYBRID_INVALID_INTEGER_VALUE",
        category: "transform_error",
        message: `Invalid integer value for ${field.columnName}.`,
      });
    }

    if (field.dataType === "boolean") {
      rules.push({
        condition: `${rawValue} <> '' and ${parsedValue} is null`,
        code: "HYBRID_INVALID_BOOLEAN_VALUE",
        category: "transform_error",
        message: `Invalid boolean value for ${field.columnName}.`,
      });
    }

    if (
      dataset === "simples_options" &&
      ["simples_option_flag", "mei_option_flag"].includes(field.columnName)
    ) {
      rules.push({
        condition: `${rawValue} <> '' and upper(${rawValue}) not in ('S', 'N')`,
        code: "HYBRID_INVALID_ALLOWED_VALUE",
        category: "transform_error",
        message: `Invalid allowed value for ${field.columnName}.`,
      });
    }

    if (!field.nullable && !hasDefaultValue(dataset, field.columnName)) {
      rules.push({
        condition: `${parsedValue} is null`,
        code: "HYBRID_REQUIRED_VALUE_MISSING",
        category: "not_null_violation",
        message: `Missing required value for ${field.columnName}.`,
      });
    }
  }

  return rules;
}

function validationIssueExpression(
  dataset: ImportDatasetType,
  alias: string,
): string {
  const rules = validationRules(dataset, alias);

  return [
    "case",
    ...rules.map(
      (rule) =>
        `  when ${rule.condition} then jsonb_build_object('code', ${quoteSqlLiteral(rule.code)}, 'category', ${quoteSqlLiteral(rule.category)}, 'message', ${quoteSqlLiteral(rule.message)})`,
    ),
    "  else null",
    "end",
  ].join("\n");
}

function domainValidationIssueExpression(alias: string): string {
  return [
    "case",
    `  when nullif(btrim(${alias}.code), '') is null then jsonb_build_object('code', 'HYBRID_REQUIRED_VALUE_MISSING', 'category', 'not_null_violation', 'message', 'Missing required value for code.')`,
    `  when nullif(btrim(${alias}.description), '') is null then jsonb_build_object('code', 'HYBRID_REQUIRED_VALUE_MISSING', 'category', 'not_null_violation', 'message', 'Missing required value for description.')`,
    "  else null",
    "end",
  ].join("\n");
}

function hybridSourceFingerprint(
  files: readonly PostgresDirectSourceFile[],
): string {
  const payload = files
    .map((file) =>
      [
        file.dataset,
        normalizePathForPsql(file.absolutePath),
        String(file.fileSize),
        file.fileMtime,
      ].join("|"),
    )
    .sort()
    .join("\n");

  return `hybrid:${createHash("sha256").update(payload).digest("hex")}`;
}

function hybridValidatedPath(input: SanitizedScriptGenerationInput): string {
  if (input.validatedPath) {
    return normalizePathForPsql(input.validatedPath);
  }

  const firstFile = input.files[0];
  return firstFile
    ? normalizePathForPsql(path.dirname(firstFile.absolutePath))
    : ".";
}

function hybridPlanIdSql(sourceFingerprint: string): string {
  return `(select id from import_plans where source_fingerprint = ${quoteSqlLiteral(sourceFingerprint)})`;
}

function hybridPlanPhaseSql(
  sourceFingerprint: string,
  phase: string,
  options: {
    status?: string;
    loadStatus?: string;
    materializationStatus?: string;
  } = {},
): string {
  const assignments = [
    `last_phase = ${quoteSqlLiteral(phase)}`,
    "updated_at = now()",
    "last_used_at = now()",
  ];

  if (options.status) {
    assignments.push(`status = ${quoteSqlLiteral(options.status)}`);
  }
  if (options.loadStatus) {
    assignments.push(`load_status = ${quoteSqlLiteral(options.loadStatus)}`);
  }
  if (options.materializationStatus) {
    assignments.push(
      `materialization_status = ${quoteSqlLiteral(options.materializationStatus)}`,
    );
  }

  return [
    "update import_plans",
    `set ${assignments.join(",\n    ")}`,
    `where source_fingerprint = ${quoteSqlLiteral(sourceFingerprint)};`,
  ].join("\n");
}

function hybridPlanSetupSql(
  input: SanitizedScriptGenerationInput,
  sourceFingerprint: string,
): string[] {
  const validatedPath = hybridValidatedPath(input);
  const datasets = [...new Set(input.files.map((file) => file.dataset))];
  const executionOrder = JSON.stringify(datasets);
  const planId = hybridPlanIdSql(sourceFingerprint);
  const lines = [
    echo(
      "[setup] Registering hybrid import plan using the existing import control tables...",
    ),
    `insert into import_plans (
  source_fingerprint,
  input_path,
  validated_path,
  batch_size,
  target_database,
  total_datasets,
  total_files,
  total_rows,
  total_batches,
  execution_order,
  status,
  load_status,
  materialization_status,
  last_phase,
  last_error,
  created_at,
  updated_at,
  last_used_at
) values (
  ${quoteSqlLiteral(sourceFingerprint)},
  ${quoteSqlLiteral(validatedPath)},
  ${quoteSqlLiteral(validatedPath)},
  0,
  current_database(),
  ${datasets.length},
  ${input.files.length},
  0,
  0,
  ${quoteSqlLiteral(executionOrder)}::jsonb,
  'planned',
  'pending',
  'pending',
  'postgres-direct-setup',
  null,
  now(),
  now(),
  now()
)
on conflict (source_fingerprint)
do update set
  input_path = excluded.input_path,
  validated_path = excluded.validated_path,
  target_database = excluded.target_database,
  total_datasets = excluded.total_datasets,
  total_files = excluded.total_files,
  execution_order = excluded.execution_order,
  status = 'planned',
  load_status = 'pending',
  materialization_status = 'pending',
  last_phase = 'postgres-direct-setup',
  last_error = null,
  updated_at = now(),
  last_used_at = now();`,
  ];

  for (const [fileIndex, file] of input.files.entries()) {
    const datasetIndex = datasets.indexOf(file.dataset) + 1;
    lines.push(
      `insert into import_plan_files (
  plan_id,
  dataset,
  dataset_index,
  file_index,
  file_path,
  file_display_path,
  file_size,
  file_mtime,
  total_rows,
  total_batches
)
select
  ${planId},
  ${quoteSqlLiteral(file.dataset)},
  ${datasetIndex},
  ${fileIndex + 1},
  ${quoteSqlLiteral(normalizePathForPsql(file.absolutePath))},
  ${quoteSqlLiteral(file.relativePath)},
  ${file.fileSize},
  ${quoteSqlLiteral(file.fileMtime)}::timestamptz,
  0,
  0
on conflict (plan_id, file_path)
do update set
  dataset = excluded.dataset,
  dataset_index = excluded.dataset_index,
  file_index = excluded.file_index,
  file_display_path = excluded.file_display_path,
  file_size = excluded.file_size,
  file_mtime = excluded.file_mtime;`,
    );
  }

  lines.push(echo("[setup] Hybrid import plan registered."));
  return lines;
}

function importCheckpointStartSql(
  dataset: ImportDatasetType,
  file: PostgresDirectSourceFile,
): string {
  const filePath = normalizePathForPsql(file.absolutePath);
  return `insert into import_checkpoints (
  dataset,
  file_path,
  file_size,
  file_mtime,
  byte_offset,
  rows_committed,
  status,
  last_error,
  updated_at
) values (
  ${quoteSqlLiteral(dataset)},
  ${quoteSqlLiteral(filePath)},
  ${file.fileSize},
  ${quoteSqlLiteral(file.fileMtime)}::timestamptz,
  0,
  0,
  'in_progress',
  null,
  now()
)
on conflict (dataset, file_path)
do update set
  file_size = excluded.file_size,
  file_mtime = excluded.file_mtime,
  byte_offset = 0,
  rows_committed = 0,
  status = 'in_progress',
  last_error = null,
  updated_at = now();`;
}

function importCheckpointCompletedSql(
  dataset: ImportDatasetType,
  file: PostgresDirectSourceFile,
): string {
  const filePath = normalizePathForPsql(file.absolutePath);
  return `update import_checkpoints
set byte_offset = ${file.fileSize},
    rows_committed = :hybrid_valid_rows,
    status = 'completed',
    last_error = null,
    updated_at = now()
where dataset = ${quoteSqlLiteral(dataset)}
  and file_path = ${quoteSqlLiteral(filePath)};`;
}

function quarantineAndCountSql(input: {
  dataset: ImportDatasetType;
  file: PostgresDirectSourceFile;
  tableName: string;
  alias: string;
  issueExpression: string;
  stepName: string;
}): string[] {
  const filePath = normalizePathForPsql(input.file.absolutePath);
  const issueExpression = input.issueExpression;

  return [
    `select
  count(*) filter (where (${issueExpression}) is null) as hybrid_valid_rows,
  count(*) filter (where (${issueExpression}) is not null) as hybrid_quarantined_rows
from ${input.tableName} ${input.alias}
\\gset`,
    `\\echo '[${input.stepName}] Valid rows:' :hybrid_valid_rows '- quarantined rows:' :hybrid_quarantined_rows`,
    `delete from import_quarantine
where dataset = ${quoteSqlLiteral(input.dataset)}
  and file_path = ${quoteSqlLiteral(filePath)}
  and error_stage = 'postgres_direct_staging_validation';`,
    `with invalid_rows as (
  select
    ${input.alias}.*,
    row_number() over () as __hybrid_row_number,
    ${issueExpression} as __hybrid_issue
  from ${input.tableName} ${input.alias}
)
insert into import_quarantine (
  dataset,
  file_path,
  row_number,
  checkpoint_offset,
  error_code,
  error_category,
  error_stage,
  error_message,
  raw_line,
  parsed_payload,
  sanitizations_applied,
  retry_count,
  can_retry_later,
  created_at
)
select
  ${quoteSqlLiteral(input.dataset)},
  ${quoteSqlLiteral(filePath)},
  invalid_rows.__hybrid_row_number,
  null,
  invalid_rows.__hybrid_issue ->> 'code',
  invalid_rows.__hybrid_issue ->> 'category',
  'postgres_direct_staging_validation',
  invalid_rows.__hybrid_issue ->> 'message',
  (to_jsonb(invalid_rows) - '__hybrid_row_number' - '__hybrid_issue')::text,
  to_jsonb(invalid_rows) - '__hybrid_row_number' - '__hybrid_issue',
  null,
  0,
  false,
  now()
from invalid_rows
where invalid_rows.__hybrid_issue is not null;`,
  ];
}

function materializationCheckpointStartSql(
  sourceFingerprint: string,
  dataset: ImportDatasetType,
  targetTable: string,
): string {
  return `insert into import_materialization_checkpoints (
  plan_id,
  dataset,
  target_table,
  status,
  rows_materialized,
  last_staging_id,
  chunks_completed,
  last_error,
  started_at,
  completed_at,
  updated_at
) values (
  ${hybridPlanIdSql(sourceFingerprint)},
  ${quoteSqlLiteral(dataset)},
  ${quoteSqlLiteral(targetTable)},
  'in_progress',
  0,
  0,
  0,
  null,
  now(),
  null,
  now()
)
on conflict (plan_id, dataset)
do update set
  target_table = excluded.target_table,
  status = 'in_progress',
  rows_materialized = 0,
  last_staging_id = 0,
  chunks_completed = 0,
  last_error = null,
  started_at = now(),
  completed_at = null,
  updated_at = now();`;
}

function materializationCheckpointCompletedSql(
  sourceFingerprint: string,
  dataset: ImportDatasetType,
  stagingTable: string,
): string {
  return `update import_materialization_checkpoints
set status = 'completed',
    rows_materialized = (select coalesce(max(staging_id), 0) from ${stagingTable}),
    last_staging_id = (select coalesce(max(staging_id), 0) from ${stagingTable}),
    chunks_completed = 1,
    last_error = null,
    completed_at = now(),
    updated_at = now()
where plan_id = ${hybridPlanIdSql(sourceFingerprint)}
  and dataset = ${quoteSqlLiteral(dataset)};`;
}

function secondaryCnaesCheckpointStartSql(sourceFingerprint: string): string {
  return `update import_materialization_checkpoints
set lookup_reconciliation_status = 'in_progress',
    lookup_reconciliation_completed_at = null,
    updated_at = now()
where plan_id = ${hybridPlanIdSql(sourceFingerprint)}
  and dataset = 'establishments';`;
}

function secondaryCnaesCheckpointCompletedSql(
  sourceFingerprint: string,
): string {
  return `update import_materialization_checkpoints
set lookup_reconciliation_status = 'completed',
    lookup_reconciliation_max_staging_id_verified = (select coalesce(max(staging_id), 0) from staging_establishments),
    lookup_reconciliation_completed_at = now(),
    updated_at = now()
where plan_id = ${hybridPlanIdSql(sourceFingerprint)}
  and dataset = 'establishments';`;
}

function rawDomainSql(
  dataset: ImportDatasetType,
  files: readonly PostgresDirectSourceFile[],
  sourceFingerprint: string,
): string[] {
  if (files.length === 0) {
    return [];
  }

  const layout = DATASET_LAYOUTS[dataset];
  const columns = layout.fields.map((field) => field.columnName);
  const tableName = rawTableName(dataset);
  const alias = "source";
  const issueExpression = domainValidationIssueExpression(alias);
  const stepName = "load-domains";
  const lines = [
    echo(
      `[load-domains] Loading ${dataset} lookup data directly from sanitized Receita files...`,
    ),
    hybridPlanPhaseSql(sourceFingerprint, `load-domains:${dataset}`, {
      status: "in_progress",
      loadStatus: "in_progress",
    }),
    createRawTempTableSql(dataset),
  ];

  for (const [index, file] of files.entries()) {
    lines.push(
      `truncate table ${tableName};`,
      importCheckpointStartSql(dataset, file),
      echo(
        `[load-domains] Loading ${dataset} file ${index + 1} of ${files.length}: ${file.relativePath}`,
      ),
      receitaCopyCommand(tableName, columns, file.absolutePath),
      echo(
        `[load-domains] Loaded ${dataset} file ${index + 1} of ${files.length}.`,
      ),
      ...quarantineAndCountSql({
        dataset,
        file,
        tableName,
        alias,
        issueExpression,
        stepName,
      }),
      `insert into ${dataset} (${columns.join(", ")})
select distinct on (code)
  nullif(btrim(code), '') as code,
  nullif(btrim(description), '') as description
from ${tableName} ${alias}
where (${issueExpression}) is null
order by code
on conflict (code) do update set description = excluded.description;`,
      importCheckpointCompletedSql(dataset, file),
      echo(
        `[load-domains] Completed ${dataset} file ${index + 1} of ${files.length}.`,
      ),
    );
  }

  lines.push(
    hybridPlanPhaseSql(sourceFingerprint, `load-domains:${dataset}:completed`),
    echo(`[load-domains] ${dataset} lookup data completed.`),
  );

  return lines;
}

function rawStagingSql(
  dataset: ImportDatasetType,
  files: readonly PostgresDirectSourceFile[],
  sourceFingerprint: string,
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
  const issueExpression = validationIssueExpression(dataset, alias);
  const stepName = loadStepName(dataset);

  const lines = [
    echo(
      `[${stepName}] Loading ${dataset} staging data directly from sanitized Receita files...`,
    ),
    hybridPlanPhaseSql(sourceFingerprint, stepName, {
      status: "in_progress",
      loadStatus: "in_progress",
    }),
    `truncate table ${targetTable} restart identity;`,
    ...safeConversionFunctionsSql(),
    createRawTempTableSql(dataset),
  ];

  for (const [index, file] of files.entries()) {
    lines.push(
      `truncate table ${tableName};`,
      importCheckpointStartSql(dataset, file),
      echo(
        `[${stepName}] Loading file ${index + 1} of ${files.length}: ${file.relativePath}`,
      ),
      receitaCopyCommand(tableName, columns, file.absolutePath),
      echo(`[${stepName}] Loaded file ${index + 1} of ${files.length}.`),
      ...quarantineAndCountSql({
        dataset,
        file,
        tableName,
        alias,
        issueExpression,
        stepName,
      }),
      echo(
        `[${stepName}] Transforming valid ${dataset} rows from file ${index + 1} into ${targetTable}...`,
      ),
      `insert into ${targetTable} (${columns.join(", ")})
select
${expressions.join(",\n")}
from ${tableName} ${alias}
where (${issueExpression}) is null;`,
      importCheckpointCompletedSql(dataset, file),
      echo(`[${stepName}] Completed file ${index + 1} of ${files.length}.`),
    );
  }

  lines.push(
    hybridPlanPhaseSql(sourceFingerprint, `${stepName}:completed`),
    echo(`[${stepName}] ${dataset} staging load completed.`),
  );

  return lines;
}

function loadStepName(dataset: ImportDatasetType): string {
  switch (dataset) {
    case "companies":
      return "load-companies";
    case "establishments":
      return "load-establishments";
    case "partners":
      return "load-partners";
    case "simples_options":
      return "load-simples";
    default:
      return `load-${dataset}`;
  }
}

function scriptHeader(title: string, sourceEncoding?: string): string[] {
  return [
    `-- ${title}`,
    "-- Generated by cnpj-db-loader postgres generate-script.",
    "\\set ON_ERROR_STOP on",
    ...(sourceEncoding
      ? [
          echo(
            `Using source file encoding ${sourceEncoding} for psql copy operations...`,
          ),
          `set client_encoding to ${quoteSqlLiteral(sourceEncoding)};`,
        ]
      : []),
    "",
  ];
}

function wrapTransaction(
  lines: readonly string[],
  mode: PostgresDirectTransactionMode,
  shouldWrap: boolean,
): string[] {
  if (!shouldWrap || mode !== "phase") {
    return [...lines];
  }

  return ["begin;", "", ...lines, "", "commit;"];
}

function buildStepScript(
  title: string,
  body: readonly string[],
  input: SanitizedScriptGenerationInput,
  wrapInPhaseTransaction: boolean,
): string {
  return [
    ...scriptHeader(title, input.sourceEncoding),
    ...wrapTransaction(body, input.transactionMode, wrapInPhaseTransaction),
    "",
  ].join("\n");
}

function includeSet(
  input: SanitizedScriptGenerationInput,
): Set<PostgresDirectIncludeTarget> {
  const selected = new Set(input.include);

  if (input.skipIndexes) {
    selected.delete("indexes");
  }

  if (input.skipAnalyze) {
    selected.delete("analyze");
  }

  return selected;
}

function hasAnyFinalMaterialization(
  selected: ReadonlySet<PostgresDirectIncludeTarget>,
): boolean {
  return (
    selected.has("companies") ||
    selected.has("establishments") ||
    selected.has("partners") ||
    selected.has("simples")
  );
}

function materializeSql(
  selected: ReadonlySet<PostgresDirectIncludeTarget>,
  sourceFingerprint: string,
): string[] {
  const lines = [
    echo("[materialize] Starting final table materialization..."),
    hybridPlanPhaseSql(sourceFingerprint, "materialize", {
      status: "in_progress",
      loadStatus: "completed",
      materializationStatus: "in_progress",
    }),
  ];

  if (selected.has("companies")) {
    lines.push(
      materializationCheckpointStartSql(
        sourceFingerprint,
        "companies",
        "companies",
      ),
      materializeCompaniesSql(),
      materializationCheckpointCompletedSql(
        sourceFingerprint,
        "companies",
        "staging_companies",
      ),
      "",
    );
  }

  if (selected.has("establishments")) {
    lines.push(
      materializationCheckpointStartSql(
        sourceFingerprint,
        "establishments",
        "establishments",
      ),
      materializeEstablishmentsSql(),
      materializationCheckpointCompletedSql(
        sourceFingerprint,
        "establishments",
        "staging_establishments",
      ),
      "",
    );
  }

  if (selected.has("partners")) {
    lines.push(
      materializationCheckpointStartSql(
        sourceFingerprint,
        "partners",
        "partners",
      ),
      materializePartnersSql(),
      materializationCheckpointCompletedSql(
        sourceFingerprint,
        "partners",
        "staging_partners",
      ),
      "",
    );
  }

  if (selected.has("simples")) {
    lines.push(
      materializationCheckpointStartSql(
        sourceFingerprint,
        "simples_options",
        "simples_options",
      ),
      materializeSimplesSql(),
      materializationCheckpointCompletedSql(
        sourceFingerprint,
        "simples_options",
        "staging_simples_options",
      ),
      "",
    );
  }

  lines.push(
    hybridPlanPhaseSql(sourceFingerprint, "materialize:completed", {
      materializationStatus: "completed",
    }),
    echo("[materialize] Final table materialization completed."),
  );

  return lines;
}

function indexesSql(): string[] {
  return [
    echo(
      "[indexes] No additional index operations are generated in this beta.",
    ),
    "-- Indexes are expected to be managed by the schema generated by cnpj-db-loader schema generate.",
    "-- A future fast-rebuild mode may generate DROP/CREATE INDEX operations here.",
  ];
}

function analyzeSql(
  selected: ReadonlySet<PostgresDirectIncludeTarget>,
): string[] {
  const tables = new Set<string>();

  if (selected.has("companies")) {
    tables.add("companies");
  }

  if (selected.has("establishments")) {
    tables.add("establishments");
  }

  if (selected.has("secondary-cnaes")) {
    tables.add("establishment_secondary_cnaes");
  }

  if (selected.has("partners")) {
    tables.add("partners");
  }

  if (selected.has("simples")) {
    tables.add("simples_options");
  }

  if (selected.has("domains")) {
    for (const dataset of DOMAIN_DATASETS) {
      tables.add(dataset);
    }
  }

  return [
    echo("[analyze] Refreshing planner statistics..."),
    ...[...tables].map((table) => `analyze ${table};`),
    echo("[analyze] Planner statistics refreshed."),
  ];
}

function step(
  name: string,
  file: string,
  dependsOn: string[],
  included: boolean,
): PostgresDirectScriptStep {
  return { name, file, dependsOn, included };
}

export function generatePostgresDirectScriptFiles(
  input: SanitizedScriptGenerationInput,
): GeneratedPostgresDirectScripts {
  const grouped = directFilesByDataset(input.files);
  const selected = includeSet(input);
  const sourceFingerprint = hybridSourceFingerprint(input.files);
  if (!DOMAIN_DATASETS.some((dataset) => (grouped[dataset] ?? []).length > 0)) {
    selected.delete("domains");
  }
  if ((grouped.companies ?? []).length === 0) {
    selected.delete("companies");
  }
  if ((grouped.establishments ?? []).length === 0) {
    selected.delete("establishments");
    selected.delete("secondary-cnaes");
  }
  if ((grouped.partners ?? []).length === 0) {
    selected.delete("partners");
  }
  if ((grouped.simples_options ?? []).length === 0) {
    selected.delete("simples");
  }
  const scripts: Record<string, string> = {};
  const steps: PostgresDirectScriptStep[] = [];

  const setupIncluded = true;
  steps.push(step("setup", "setup.sql", [], setupIncluded));
  scripts["setup.sql"] = [
    ...scriptHeader(
      "CNPJ DB Loader PostgreSQL direct import setup",
      input.sourceEncoding,
    ),
    echo("[setup] Preparing PostgreSQL direct import session..."),
    ...hybridPlanSetupSql(input, sourceFingerprint),
    "-- The database schema must be applied before running these scripts.",
    "-- This setup script configures the psql session used by the generated orchestrator.",
    echo("[setup] Setup completed."),
    "",
  ].join("\n");

  const domainsIncluded =
    selected.has("domains") &&
    DOMAIN_DATASETS.some((dataset) => (grouped[dataset] ?? []).length > 0);
  steps.push(
    step("load-domains", "load-domains.sql", ["setup"], domainsIncluded),
  );
  if (domainsIncluded) {
    const lines = [echo("[load-domains] Starting domain tables load...")];
    for (const dataset of DOMAIN_DATASETS) {
      lines.push(
        ...rawDomainSql(dataset, grouped[dataset] ?? [], sourceFingerprint),
        "",
      );
    }
    lines.push(echo("[load-domains] Domain tables load completed."));
    scripts["load-domains.sql"] = buildStepScript(
      "CNPJ DB Loader PostgreSQL direct import domains step",
      lines,
      input,
      true,
    );
  }

  const datasetSteps: Array<{
    dataset: ImportDatasetType;
    name: string;
    file: string;
    include: PostgresDirectIncludeTarget;
  }> = [
    {
      dataset: "companies",
      name: "load-companies",
      file: "load-companies.sql",
      include: "companies",
    },
    {
      dataset: "establishments",
      name: "load-establishments",
      file: "load-establishments.sql",
      include: "establishments",
    },
    {
      dataset: "partners",
      name: "load-partners",
      file: "load-partners.sql",
      include: "partners",
    },
    {
      dataset: "simples_options",
      name: "load-simples",
      file: "load-simples.sql",
      include: "simples",
    },
  ];

  for (const item of datasetSteps) {
    const files = grouped[item.dataset] ?? [];
    const included = selected.has(item.include) && files.length > 0;
    steps.push(step(item.name, item.file, ["setup"], included));

    if (included) {
      scripts[item.file] = buildStepScript(
        `CNPJ DB Loader PostgreSQL direct import ${item.name} step`,
        rawStagingSql(item.dataset, files, sourceFingerprint),
        input,
        true,
      );
    }
  }

  const materializeIncluded = hasAnyFinalMaterialization(selected);
  steps.push(
    step(
      "materialize",
      "materialize.sql",
      datasetSteps
        .filter((item) => selected.has(item.include))
        .map((item) => item.name),
      materializeIncluded,
    ),
  );
  if (materializeIncluded) {
    scripts["materialize.sql"] = buildStepScript(
      "CNPJ DB Loader PostgreSQL direct import materialization step",
      materializeSql(selected, sourceFingerprint),
      input,
      true,
    );
  }

  const secondaryIncluded =
    selected.has("secondary-cnaes") && selected.has("establishments");
  steps.push(
    step(
      "materialize-secondary-cnaes",
      "materialize-secondary-cnaes.sql",
      ["load-establishments"],
      secondaryIncluded,
    ),
  );
  if (secondaryIncluded) {
    scripts["materialize-secondary-cnaes.sql"] = buildStepScript(
      "CNPJ DB Loader PostgreSQL direct import secondary CNAEs step",
      [
        secondaryCnaesCheckpointStartSql(sourceFingerprint),
        materializeSecondaryCnaesSql(),
        secondaryCnaesCheckpointCompletedSql(sourceFingerprint),
      ],
      input,
      true,
    );
  }

  const indexesIncluded = selected.has("indexes");
  steps.push(
    step(
      "indexes",
      "indexes.sql",
      materializeIncluded ? ["materialize"] : ["setup"],
      indexesIncluded,
    ),
  );
  if (indexesIncluded) {
    scripts["indexes.sql"] = buildStepScript(
      "CNPJ DB Loader PostgreSQL direct import indexes step",
      indexesSql(),
      input,
      true,
    );
  }

  const analyzeIncluded = selected.has("analyze");
  const analyzeDependencies = [
    ...(domainsIncluded ? ["load-domains"] : []),
    ...(materializeIncluded ? ["materialize"] : []),
    ...(secondaryIncluded ? ["materialize-secondary-cnaes"] : []),
  ];
  steps.push(
    step(
      "analyze",
      "analyze.sql",
      analyzeDependencies.length > 0 ? analyzeDependencies : ["setup"],
      analyzeIncluded,
    ),
  );
  if (analyzeIncluded) {
    scripts["analyze.sql"] = buildStepScript(
      "CNPJ DB Loader PostgreSQL direct import analyze step",
      analyzeSql(selected),
      input,
      true,
    );
  }

  const orchestratorLines = [
    "-- CNPJ DB Loader direct PostgreSQL import orchestrator",
    "-- Generated from sanitized Receita files by cnpj-db-loader postgres generate-script.",
    "-- Execute with psql, for example:",
    '--   psql -d "postgres://postgres:postgres@localhost:5432/cnpj" -f import-postgres-direct.sql',
    "",
    "\\set ON_ERROR_STOP on",
    echo(
      `Using source file encoding ${input.sourceEncoding} for psql copy operations...`,
    ),
    `set client_encoding to ${quoteSqlLiteral(input.sourceEncoding)};`,
    echo(
      `Starting CNPJ DB Loader direct PostgreSQL import using transaction mode ${input.transactionMode}...`,
    ),
    "",
    ...(input.transactionMode === "single" ? ["begin;", ""] : []),
  ];

  for (const name of STEP_ORDER) {
    const currentStep = steps.find((item) => item.name === name);
    if (!currentStep?.included) {
      continue;
    }

    orchestratorLines.push(
      echo(
        `[orchestrator] Running ${currentStep.name} (${currentStep.file})...`,
      ),
      `\\ir ${currentStep.file}`,
      echo(`[orchestrator] Completed ${currentStep.name}.`),
      "",
    );
  }

  orchestratorLines.push(
    hybridPlanPhaseSql(sourceFingerprint, "completed", {
      status: "completed",
      loadStatus: "completed",
      materializationStatus: materializeIncluded ? "completed" : "pending",
    }),
    ...(input.transactionMode === "single" ? ["commit;", ""] : []),
    echo("CNPJ DB Loader hybrid PostgreSQL import completed."),
    "",
  );

  scripts["import-postgres-direct.sql"] = orchestratorLines.join("\n");

  return { scripts, steps, sourceFingerprint };
}

export function generatePostgresDirectImportScript(
  input: CsvScriptGenerationInput,
): string {
  const grouped = csvFilesByDataset(input.files);

  const lines = [
    "-- CNPJ DB Loader hybrid PostgreSQL import script",
    "-- Generated from PostgreSQL-ready CSV files exported by cnpj-db-loader postgres export-csv.",
    "-- Execute with psql, for example:",
    '--   psql -d "postgres://postgres:postgres@localhost:5432/cnpj" -f import-postgres-direct.sql',
    "",
    "\\set ON_ERROR_STOP on",
    echo("Starting CNPJ DB Loader hybrid PostgreSQL import..."),
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
  input: Omit<
    SanitizedScriptGenerationInput,
    "transactionMode" | "include" | "skipIndexes" | "skipAnalyze"
  >,
): string {
  const generated = generatePostgresDirectScriptFiles({
    ...input,
    transactionMode: "single",
    include: [
      "domains",
      "companies",
      "establishments",
      "partners",
      "simples",
      "secondary-cnaes",
      "indexes",
      "analyze",
    ],
    skipIndexes: false,
    skipAnalyze: false,
  });

  return generated.scripts["import-postgres-direct.sql"] ?? "";
}

function materializationAndAnalyzeSql(): string[] {
  return [
    materializeCompaniesSql(),
    "",
    materializeEstablishmentsSql(),
    "",
    materializeSecondaryCnaesSql(),
    "",
    materializePartnersSql(),
    "",
    materializeSimplesSql(),
    "",
    echo("Refreshing planner statistics..."),
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
    echo("CNPJ DB Loader hybrid PostgreSQL import completed."),
    "",
  ];
}
