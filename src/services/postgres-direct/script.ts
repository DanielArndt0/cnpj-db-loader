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
  sourceEncoding: string;
  transactionMode: PostgresDirectTransactionMode;
  include: readonly PostgresDirectIncludeTarget[];
  skipIndexes: boolean;
  skipAnalyze: boolean;
};

export type GeneratedPostgresDirectScripts = {
  scripts: Record<string, string>;
  steps: PostgresDirectScriptStep[];
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
    echo(
      `[load-domains] Loading ${dataset} lookup data directly from sanitized Receita files...`,
    ),
    createRawTempTableSql(dataset),
  ];

  for (const [index, file] of files.entries()) {
    lines.push(
      echo(
        `[load-domains] Loading ${dataset} file ${index + 1} of ${files.length}: ${file.relativePath}`,
      ),
      receitaCopyCommand(tableName, columns, file.absolutePath),
      echo(
        `[load-domains] Loaded ${dataset} file ${index + 1} of ${files.length}.`,
      ),
    );
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
    echo(`[load-domains] ${dataset} lookup data completed.`),
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
  const stepName = loadStepName(dataset);

  const lines = [
    echo(
      `[${stepName}] Loading ${dataset} staging data directly from sanitized Receita files...`,
    ),
    `truncate table ${targetTable} restart identity;`,
    createRawTempTableSql(dataset),
  ];

  for (const [index, file] of files.entries()) {
    lines.push(
      echo(
        `[${stepName}] Loading file ${index + 1} of ${files.length}: ${file.relativePath}`,
      ),
      receitaCopyCommand(tableName, columns, file.absolutePath),
      echo(`[${stepName}] Loaded file ${index + 1} of ${files.length}.`),
    );
  }

  lines.push(
    echo(
      `[${stepName}] Transforming ${dataset} raw rows into ${targetTable}...`,
    ),
    `insert into ${targetTable} (${columns.join(", ")})`,
    "select",
    expressions.join(",\n"),
    `from ${tableName} ${alias};`,
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
): string[] {
  const lines = [echo("[materialize] Starting final table materialization...")];

  if (selected.has("companies")) {
    lines.push(materializeCompaniesSql(), "");
  }

  if (selected.has("establishments")) {
    lines.push(materializeEstablishmentsSql(), "");
  }

  if (selected.has("partners")) {
    lines.push(materializePartnersSql(), "");
  }

  if (selected.has("simples")) {
    lines.push(materializeSimplesSql(), "");
  }

  lines.push(echo("[materialize] Final table materialization completed."));

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
      lines.push(...rawDomainSql(dataset, grouped[dataset] ?? []), "");
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
        rawStagingSql(item.dataset, files),
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
      materializeSql(selected),
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
      [materializeSecondaryCnaesSql()],
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
    ...(input.transactionMode === "single" ? ["commit;", ""] : []),
    echo("CNPJ DB Loader hybrid PostgreSQL import completed."),
    "",
  );

  scripts["import-postgres-direct.sql"] = orchestratorLines.join("\n");

  return { scripts, steps };
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
