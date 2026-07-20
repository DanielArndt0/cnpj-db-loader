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
import { NOMES_TABELAS_DATASET } from "../schema/table-names.js";
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
  companies: "staging_empresas",
  establishments: "staging_estabelecimentos",
  partners: "staging_socios",
  simples_options: "staging_simples",
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
    .concat(["atualizado_em = now()"])
    .join(",\n  ");
}

function partnerDedupeExpression(alias: string): string {
  return [
    "md5(",
    `  coalesce(${alias}.cnpj_basico, '') || '|' ||`,
    `  coalesce(${alias}.identificador_socio, '') || '|' ||`,
    `  coalesce(${alias}.nome_socio_razao_social, '') || '|' ||`,
    `  coalesce(${alias}.cnpj_cpf_socio, '') || '|' ||`,
    `  coalesce(${alias}.codigo_qualificacao_socio, '') || '|' ||`,
    `  coalesce((${alias}.data_entrada_sociedade - date '2000-01-01')::text, '') || '|' ||`,
    `  coalesce(${alias}.codigo_pais, '') || '|' ||`,
    `  coalesce(${alias}.cpf_representante_legal, '') || '|' ||`,
    `  coalesce(${alias}.nome_representante_legal, '') || '|' ||`,
    `  coalesce(${alias}.codigo_qualificacao_representante_legal, '') || '|' ||`,
    `  coalesce(${alias}.codigo_faixa_etaria, '')`,
    ")",
  ].join("\n");
}

function materializeCompaniesSql(): string {
  const columns = companiesLayout.fields.map((field) => field.columnName);

  return [
    echo("[materialize] Materializando empresas..."),
    "with source as (",
    "  select",
    `    ${columns.map((column) => `source.${column}`).join(",\n    ")},`,
    "    row_number() over (partition by source.cnpj_basico order by source.staging_id desc) as dedupe_rank",
    "  from staging_empresas source",
    "),",
    "deduped as (",
    "  select * from source where dedupe_rank = 1",
    ")",
    `insert into empresas (${columns.join(", ")})`,
    `select ${columns.join(", ")}`,
    "from deduped",
    "on conflict (cnpj_basico) do update set",
    `  ${updateAssignments(columns, ["cnpj_basico"])};`,
    echo("[materialize] Materialização de empresas concluída."),
  ].join("\n");
}

function materializeEstablishmentsSql(): string {
  const baseColumns = establishmentsLayout.fields.map(
    (field) => field.columnName,
  );
  const insertColumns = [...baseColumns, "cnpj_completo"];

  return [
    echo("[materialize] Materializando estabelecimentos..."),
    "with source as (",
    "  select",
    `    ${baseColumns.map((column) => `source.${column}`).join(",\n    ")},`,
    "    source.cnpj_basico || source.cnpj_ordem || source.cnpj_dv as cnpj_completo,",
    "    row_number() over (partition by source.cnpj_basico || source.cnpj_ordem || source.cnpj_dv order by source.staging_id desc) as dedupe_rank",
    "  from staging_estabelecimentos source",
    "),",
    "deduped as (",
    "  select * from source where dedupe_rank = 1",
    ")",
    `insert into estabelecimentos (${insertColumns.join(", ")})`,
    `select ${insertColumns.join(", ")}`,
    "from deduped",
    "on conflict (cnpj_completo) do update set",
    `  ${updateAssignments(insertColumns, ["cnpj_basico", "cnpj_ordem", "cnpj_dv", "cnpj_completo"])};`,
    echo("[materialize] Materialização de estabelecimentos concluída."),
  ].join("\n");
}

function materializeSecondaryCnaesSql(): string {
  return [
    echo(
      "[materialize-secondary-cnaes] Materializando CNAEs secundários dos estabelecimentos...",
    ),
    "with source as (",
    "  select",
    "    staging.cnpj_basico || staging.cnpj_ordem || staging.cnpj_dv as cnpj_completo,",
    "    staging.cnae_fiscal_secundaria_raw,",
    "    row_number() over (partition by staging.cnpj_basico || staging.cnpj_ordem || staging.cnpj_dv order by staging.staging_id desc) as dedupe_rank",
    "  from staging_estabelecimentos staging",
    "),",
    "deduped as (",
    "  select * from source where dedupe_rank = 1",
    "),",
    "deleted_secondary_cnaes as (",
    "  delete from estabelecimento_cnaes_secundarios target",
    "  using (select cnpj_completo from deduped) source_keys",
    "  where target.cnpj_completo = source_keys.cnpj_completo",
    "  returning 1",
    "),",
    "secondary_cnaes_source as (",
    "  select distinct",
    "    deduped.cnpj_completo,",
    "    btrim(codigo_cnae) as codigo_cnae",
    "  from deduped",
    "  cross join lateral unnest(string_to_array(deduped.cnae_fiscal_secundaria_raw, ',')) as codigo_cnae",
    "  where deduped.cnae_fiscal_secundaria_raw is not null",
    "    and deduped.cnae_fiscal_secundaria_raw <> ''",
    "    and btrim(codigo_cnae) <> ''",
    ")",
    "insert into estabelecimento_cnaes_secundarios (cnpj_completo, codigo_cnae)",
    "select cnpj_completo, codigo_cnae",
    "from secondary_cnaes_source",
    "on conflict (cnpj_completo, codigo_cnae) do nothing;",
    echo(
      "[materialize-secondary-cnaes] Materialização dos CNAEs secundários concluída.",
    ),
  ].join("\n");
}

function materializePartnersSql(): string {
  const baseColumns = partnersLayout.fields.map((field) => field.columnName);
  const insertColumns = [...baseColumns, "chave_deduplicacao_socio"];

  return [
    echo("[materialize] Materializando sócios..."),
    "with source as (",
    "  select",
    `    ${baseColumns.map((column) => `source.${column}`).join(",\n    ")},`,
    `    ${partnerDedupeExpression("source")} as chave_deduplicacao_socio`,
    "  from staging_socios source",
    "),",
    "ranked as (",
    "  select",
    "    source.*,",
    "    row_number() over (partition by source.chave_deduplicacao_socio order by source.cnpj_basico asc) as dedupe_rank",
    "  from source",
    "),",
    "deduped as (",
    "  select * from ranked where dedupe_rank = 1",
    ")",
    `insert into socios (${insertColumns.join(", ")})`,
    `select ${insertColumns.join(", ")}`,
    "from deduped",
    "on conflict (chave_deduplicacao_socio) do update set",
    `  ${updateAssignments(insertColumns, ["chave_deduplicacao_socio"])};`,
    echo("[materialize] Materialização de sócios concluída."),
  ].join("\n");
}

function materializeSimplesSql(): string {
  const columns = simplesLayout.fields.map((field) => field.columnName);

  return [
    echo("[materialize] Materializando opções do Simples..."),
    "with source as (",
    "  select",
    `    ${columns.map((column) => `source.${column}`).join(",\n    ")},`,
    "    row_number() over (partition by source.cnpj_basico order by source.staging_id desc) as dedupe_rank",
    "  from staging_simples source",
    "),",
    "deduped as (",
    "  select * from source where dedupe_rank = 1",
    ")",
    `insert into simples (${columns.join(", ")})`,
    `select ${columns.join(", ")}`,
    "from deduped",
    "on conflict (cnpj_basico) do update set",
    `  ${updateAssignments(columns, ["cnpj_basico"])};`,
    echo("[materialize] Materialização das opções do Simples concluída."),
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
    echo(`[load-domains] Carregando dados de domínio de ${dataset}...`),
    `drop table if exists ${tempTable};`,
    `create temporary table ${tempTable} (codigo text, descricao text);`,
  ];

  for (const [index, file] of files.entries()) {
    lines.push(
      echo(
        `[load-domains] Carregando arquivo ${index + 1} de ${files.length} de ${dataset}: ${file.relativePath}`,
      ),
      csvCopyCommand(tempTable, columns, file.absolutePath),
      echo(
        `[load-domains] Arquivo ${index + 1} de ${files.length} de ${dataset} carregado.`,
      ),
    );
  }

  lines.push(
    `insert into ${NOMES_TABELAS_DATASET[dataset]} (${columns.join(", ")})`,
    `select distinct on (codigo) ${columns.join(", ")}`,
    `from ${tempTable}`,
    "where codigo is not null and codigo <> ''",
    "order by codigo",
    "on conflict (codigo) do update set descricao = excluded.descricao;",
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
  const lines = [
    echo(`[load-${dataset}] Carregando dados de staging de ${dataset}...`),
  ];

  for (const [index, file] of files.entries()) {
    lines.push(
      echo(
        `[load-${dataset}] Carregando arquivo ${index + 1} de ${files.length}: ${file.relativePath}`,
      ),
      csvCopyCommand(tableName, columns, file.absolutePath),
      echo(
        `[load-${dataset}] Arquivo ${index + 1} de ${files.length} carregado.`,
      ),
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

  if (dataset === "companies" && column === "codigo_porte_empresa") {
    return `coalesce(${textExpression(alias, column)}, '00')`;
  }

  if (
    dataset === "establishments" &&
    column === "identificador_matriz_filial"
  ) {
    return `coalesce(${textExpression(alias, column)}, '1')`;
  }

  if (dataset === "establishments" && column === "situacao_cadastral") {
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
    (dataset === "companies" && column === "codigo_porte_empresa") ||
    (dataset === "establishments" &&
      column === "identificador_matriz_filial") ||
    (dataset === "establishments" && column === "situacao_cadastral")
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
      ["opcao_simples", "opcao_mei"].includes(field.columnName)
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
    `  when nullif(btrim(${alias}.codigo), '') is null then jsonb_build_object('code', 'HYBRID_REQUIRED_VALUE_MISSING', 'category', 'not_null_violation', 'message', 'Missing required value for codigo.')`,
    `  when nullif(btrim(${alias}.descricao), '') is null then jsonb_build_object('code', 'HYBRID_REQUIRED_VALUE_MISSING', 'category', 'not_null_violation', 'message', 'Missing required value for descricao.')`,
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
  return `(select id from planos_importacao where impressao_digital_origem = ${quoteSqlLiteral(sourceFingerprint)})`;
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
    `ultima_fase = ${quoteSqlLiteral(phase)}`,
    "atualizado_em = now()",
    "ultimo_uso_em = now()",
  ];

  if (options.status) {
    assignments.push(`status = ${quoteSqlLiteral(options.status)}`);
  }
  if (options.loadStatus) {
    assignments.push(`status_carga = ${quoteSqlLiteral(options.loadStatus)}`);
  }
  if (options.materializationStatus) {
    assignments.push(
      `status_materializacao = ${quoteSqlLiteral(options.materializationStatus)}`,
    );
  }

  return [
    "update planos_importacao",
    `set ${assignments.join(",\n    ")}`,
    `where impressao_digital_origem = ${quoteSqlLiteral(sourceFingerprint)};`,
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
      "[setup] Registrando o plano de importação híbrido usando as tabelas de controle de importação existentes...",
    ),
    `insert into planos_importacao (
  impressao_digital_origem,
  caminho_entrada,
  caminho_validado,
  tamanho_lote,
  banco_destino,
  total_conjuntos,
  total_arquivos,
  total_linhas,
  total_lotes,
  ordem_execucao,
  status,
  status_carga,
  status_materializacao,
  ultima_fase,
  ultimo_erro,
  criado_em,
  atualizado_em,
  ultimo_uso_em
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
on conflict (impressao_digital_origem)
do update set
  caminho_entrada = excluded.caminho_entrada,
  caminho_validado = excluded.caminho_validado,
  banco_destino = excluded.banco_destino,
  total_conjuntos = excluded.total_conjuntos,
  total_arquivos = excluded.total_arquivos,
  ordem_execucao = excluded.ordem_execucao,
  status = 'planned',
  status_carga = 'pending',
  status_materializacao = 'pending',
  ultima_fase = 'postgres-direct-setup',
  ultimo_erro = null,
  atualizado_em = now(),
  ultimo_uso_em = now();`,
  ];

  for (const [fileIndex, file] of input.files.entries()) {
    const datasetIndex = datasets.indexOf(file.dataset) + 1;
    lines.push(
      `insert into arquivos_plano_importacao (
  plano_id,
  conjunto,
  indice_conjunto,
  indice_arquivo,
  caminho_arquivo,
  caminho_exibicao_arquivo,
  tamanho_arquivo,
  modificado_em,
  total_linhas,
  total_lotes
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
on conflict (plano_id, caminho_arquivo)
do update set
  conjunto = excluded.conjunto,
  indice_conjunto = excluded.indice_conjunto,
  indice_arquivo = excluded.indice_arquivo,
  caminho_exibicao_arquivo = excluded.caminho_exibicao_arquivo,
  tamanho_arquivo = excluded.tamanho_arquivo,
  modificado_em = excluded.modificado_em;`,
    );
  }

  lines.push(echo("[setup] Plano de importação híbrido registrado."));
  return lines;
}

function importCheckpointStartSql(
  dataset: ImportDatasetType,
  file: PostgresDirectSourceFile,
): string {
  const filePath = normalizePathForPsql(file.absolutePath);
  return `insert into checkpoints_importacao (
  conjunto,
  caminho_arquivo,
  tamanho_arquivo,
  modificado_em,
  deslocamento_bytes,
  linhas_confirmadas,
  status,
  ultimo_erro,
  atualizado_em
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
on conflict (conjunto, caminho_arquivo)
do update set
  tamanho_arquivo = excluded.tamanho_arquivo,
  modificado_em = excluded.modificado_em,
  deslocamento_bytes = 0,
  linhas_confirmadas = 0,
  status = 'in_progress',
  ultimo_erro = null,
  atualizado_em = now();`;
}

function importCheckpointCompletedSql(
  dataset: ImportDatasetType,
  file: PostgresDirectSourceFile,
): string {
  const filePath = normalizePathForPsql(file.absolutePath);
  return `update checkpoints_importacao
set deslocamento_bytes = ${file.fileSize},
    linhas_confirmadas = :hybrid_valid_rows,
    status = 'completed',
    ultimo_erro = null,
    atualizado_em = now()
where conjunto = ${quoteSqlLiteral(dataset)}
  and caminho_arquivo = ${quoteSqlLiteral(filePath)};`;
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
    `\\echo '[${input.stepName}] Linhas válidas:' :hybrid_valid_rows '- linhas em quarentena:' :hybrid_quarantined_rows`,
    `delete from quarentena_importacao
where conjunto = ${quoteSqlLiteral(input.dataset)}
  and caminho_arquivo = ${quoteSqlLiteral(filePath)}
  and etapa_erro = 'postgres_direct_staging_validation';`,
    `with invalid_rows as (
  select
    ${input.alias}.*,
    row_number() over () as __hybrid_row_number,
    ${issueExpression} as __hybrid_issue
  from ${input.tableName} ${input.alias}
)
insert into quarentena_importacao (
  conjunto,
  caminho_arquivo,
  numero_linha,
  deslocamento_checkpoint,
  codigo_erro,
  categoria_erro,
  etapa_erro,
  mensagem_erro,
  linha_bruta,
  payload_parseado,
  sanitizacoes_aplicadas,
  total_tentativas,
  pode_tentar_novamente,
  criado_em
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
  return `insert into checkpoints_materializacao (
  plano_id,
  conjunto,
  tabela_destino,
  status,
  linhas_materializadas,
  ultimo_staging_id,
  blocos_concluidos,
  ultimo_erro,
  iniciado_em,
  concluido_em,
  atualizado_em
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
on conflict (plano_id, conjunto)
do update set
  tabela_destino = excluded.tabela_destino,
  status = 'in_progress',
  linhas_materializadas = 0,
  ultimo_staging_id = 0,
  blocos_concluidos = 0,
  ultimo_erro = null,
  iniciado_em = now(),
  concluido_em = null,
  atualizado_em = now();`;
}

function materializationCheckpointCompletedSql(
  sourceFingerprint: string,
  dataset: ImportDatasetType,
  stagingTable: string,
): string {
  return `update checkpoints_materializacao
set status = 'completed',
    linhas_materializadas = (select coalesce(max(staging_id), 0) from ${stagingTable}),
    ultimo_staging_id = (select coalesce(max(staging_id), 0) from ${stagingTable}),
    blocos_concluidos = 1,
    ultimo_erro = null,
    concluido_em = now(),
    atualizado_em = now()
where plano_id = ${hybridPlanIdSql(sourceFingerprint)}
  and conjunto = ${quoteSqlLiteral(dataset)};`;
}

function secondaryCnaesCheckpointStartSql(sourceFingerprint: string): string {
  return `update checkpoints_materializacao
set status_reconciliacao_dominio = 'in_progress',
    reconciliacao_dominio_concluida_em = null,
    atualizado_em = now()
where plano_id = ${hybridPlanIdSql(sourceFingerprint)}
  and conjunto = 'establishments';`;
}

function secondaryCnaesCheckpointCompletedSql(
  sourceFingerprint: string,
): string {
  return `update checkpoints_materializacao
set status_reconciliacao_dominio = 'completed',
    reconciliacao_dominio_max_staging_id_verificado = (select coalesce(max(staging_id), 0) from staging_estabelecimentos),
    reconciliacao_dominio_concluida_em = now(),
    atualizado_em = now()
where plano_id = ${hybridPlanIdSql(sourceFingerprint)}
  and conjunto = 'establishments';`;
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
      `[load-domains] Carregando dados de domínio de ${dataset} diretamente dos arquivos sanitizados da Receita...`,
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
        `[load-domains] Carregando arquivo ${index + 1} de ${files.length} de ${dataset}: ${file.relativePath}`,
      ),
      receitaCopyCommand(tableName, columns, file.absolutePath),
      echo(
        `[load-domains] Arquivo ${index + 1} de ${files.length} de ${dataset} carregado.`,
      ),
      ...quarantineAndCountSql({
        dataset,
        file,
        tableName,
        alias,
        issueExpression,
        stepName,
      }),
      `insert into ${NOMES_TABELAS_DATASET[dataset]} (${columns.join(", ")})
select distinct on (codigo)
  nullif(btrim(codigo), '') as codigo,
  nullif(btrim(descricao), '') as descricao
from ${tableName} ${alias}
where (${issueExpression}) is null
order by codigo
on conflict (codigo) do update set descricao = excluded.descricao;`,
      importCheckpointCompletedSql(dataset, file),
      echo(
        `[load-domains] Arquivo ${index + 1} de ${files.length} de ${dataset} concluído.`,
      ),
    );
  }

  lines.push(
    hybridPlanPhaseSql(sourceFingerprint, `load-domains:${dataset}:completed`),
    echo(`[load-domains] Dados de domínio de ${dataset} concluídos.`),
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
      `[${stepName}] Carregando dados de staging de ${dataset} diretamente dos arquivos sanitizados da Receita...`,
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
        `[${stepName}] Carregando arquivo ${index + 1} de ${files.length}: ${file.relativePath}`,
      ),
      receitaCopyCommand(tableName, columns, file.absolutePath),
      echo(`[${stepName}] Arquivo ${index + 1} de ${files.length} carregado.`),
      ...quarantineAndCountSql({
        dataset,
        file,
        tableName,
        alias,
        issueExpression,
        stepName,
      }),
      echo(
        `[${stepName}] Transformando as linhas válidas de ${dataset} do arquivo ${index + 1} para ${targetTable}...`,
      ),
      `insert into ${targetTable} (${columns.join(", ")})
select
${expressions.join(",\n")}
from ${tableName} ${alias}
where (${issueExpression}) is null;`,
      importCheckpointCompletedSql(dataset, file),
      echo(`[${stepName}] Arquivo ${index + 1} de ${files.length} concluído.`),
    );
  }

  lines.push(
    hybridPlanPhaseSql(sourceFingerprint, `${stepName}:completed`),
    echo(`[${stepName}] Carga de staging de ${dataset} concluída.`),
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
    "-- Gerado por cnpj-db-loader postgres generate-script.",
    "\\set ON_ERROR_STOP on",
    ...(sourceEncoding
      ? [
          echo(
            `Usando o encoding de origem ${sourceEncoding} nas operações de copy do psql...`,
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
    echo("[materialize] Iniciando a materialização das tabelas finais..."),
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
        "staging_empresas",
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
        "staging_estabelecimentos",
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
        "staging_socios",
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
        "staging_simples",
      ),
      "",
    );
  }

  lines.push(
    hybridPlanPhaseSql(sourceFingerprint, "materialize:completed", {
      materializationStatus: "completed",
    }),
    echo("[materialize] Materialização das tabelas finais concluída."),
  );

  return lines;
}

function indexesSql(): string[] {
  return [
    echo("[indexes] Nenhuma operação adicional de índice é gerada nesta beta."),
    "-- Espera-se que os índices sejam gerenciados pelo schema gerado por cnpj-db-loader schema generate.",
    "-- Um futuro modo de reconstrução rápida poderá gerar operações DROP/CREATE INDEX aqui.",
  ];
}

function analyzeSql(
  selected: ReadonlySet<PostgresDirectIncludeTarget>,
): string[] {
  const tables = new Set<string>();

  if (selected.has("companies")) {
    tables.add("empresas");
  }

  if (selected.has("establishments")) {
    tables.add("estabelecimentos");
  }

  if (selected.has("secondary-cnaes")) {
    tables.add("estabelecimento_cnaes_secundarios");
  }

  if (selected.has("partners")) {
    tables.add("socios");
  }

  if (selected.has("simples")) {
    tables.add("simples");
  }

  if (selected.has("domains")) {
    for (const dataset of DOMAIN_DATASETS) {
      tables.add(NOMES_TABELAS_DATASET[dataset]);
    }
  }

  return [
    echo("[analyze] Atualizando as estatísticas do planejador..."),
    ...[...tables].map((table) => `analyze ${table};`),
    echo("[analyze] Estatísticas do planejador atualizadas."),
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
      "CNPJ DB Loader — setup da importação direta PostgreSQL",
      input.sourceEncoding,
    ),
    echo("[setup] Preparando a sessão de importação direta do PostgreSQL..."),
    ...hybridPlanSetupSql(input, sourceFingerprint),
    "-- O schema do banco deve ser aplicado antes de executar estes scripts.",
    "-- Este script de setup configura a sessão psql usada pelo orquestrador gerado.",
    echo("[setup] Setup concluído."),
    "",
  ].join("\n");

  const domainsIncluded =
    selected.has("domains") &&
    DOMAIN_DATASETS.some((dataset) => (grouped[dataset] ?? []).length > 0);
  steps.push(
    step("load-domains", "load-domains.sql", ["setup"], domainsIncluded),
  );
  if (domainsIncluded) {
    const lines = [
      echo("[load-domains] Iniciando a carga das tabelas de domínio..."),
    ];
    for (const dataset of DOMAIN_DATASETS) {
      lines.push(
        ...rawDomainSql(dataset, grouped[dataset] ?? [], sourceFingerprint),
        "",
      );
    }
    lines.push(echo("[load-domains] Carga das tabelas de domínio concluída."));
    scripts["load-domains.sql"] = buildStepScript(
      "CNPJ DB Loader — etapa de domínios da importação direta PostgreSQL",
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
        `CNPJ DB Loader — etapa ${item.name} da importação direta PostgreSQL`,
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
      "CNPJ DB Loader — etapa de materialização da importação direta PostgreSQL",
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
      "CNPJ DB Loader — etapa de CNAEs secundários da importação direta PostgreSQL",
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
      "CNPJ DB Loader — etapa de índices da importação direta PostgreSQL",
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
      "CNPJ DB Loader — etapa de analyze da importação direta PostgreSQL",
      analyzeSql(selected),
      input,
      true,
    );
  }

  const orchestratorLines = [
    "-- Orquestrador de importação direta PostgreSQL do CNPJ DB Loader",
    "-- Gerado a partir dos arquivos sanitizados da Receita por cnpj-db-loader postgres generate-script.",
    "-- Execute com psql, por exemplo:",
    '--   psql -d "postgres://postgres:postgres@localhost:5432/cnpj" -f import-postgres-direct.sql',
    "",
    "\\set ON_ERROR_STOP on",
    echo(
      `Usando o encoding de origem ${input.sourceEncoding} nas operações de copy do psql...`,
    ),
    `set client_encoding to ${quoteSqlLiteral(input.sourceEncoding)};`,
    echo(
      `Iniciando a importação direta PostgreSQL do CNPJ DB Loader no modo de transação ${input.transactionMode}...`,
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
        `[orchestrator] Executando ${currentStep.name} (${currentStep.file})...`,
      ),
      `\\ir ${currentStep.file}`,
      echo(`[orchestrator] ${currentStep.name} concluído.`),
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
    echo("Importação híbrida PostgreSQL do CNPJ DB Loader concluída."),
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
    "-- Script de importação híbrida PostgreSQL do CNPJ DB Loader",
    "-- Gerado a partir dos arquivos CSV prontos para o PostgreSQL exportados por cnpj-db-loader postgres export-csv.",
    "-- Execute com psql, por exemplo:",
    '--   psql -d "postgres://postgres:postgres@localhost:5432/cnpj" -f import-postgres-direct.sql',
    "",
    "\\set ON_ERROR_STOP on",
    echo("Iniciando a importação híbrida PostgreSQL do CNPJ DB Loader..."),
    "",
    "begin;",
    "",
    "-- O schema final e os dados de seed continuam gerenciados por sql/schema.sql.",
    "-- Este script apenas reinicia as tabelas de staging e depois faz upsert dos dados finais.",
    "truncate table staging_empresas restart identity;",
    "truncate table staging_estabelecimentos restart identity;",
    "truncate table staging_socios restart identity;",
    "truncate table staging_simples restart identity;",
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
    echo("Atualizando as estatísticas do planejador..."),
    "analyze empresas;",
    "analyze estabelecimentos;",
    "analyze estabelecimento_cnaes_secundarios;",
    "analyze socios;",
    "analyze simples;",
    "analyze cnaes;",
    "analyze municipios;",
    "analyze paises;",
    "analyze naturezas_juridicas;",
    "analyze qualificacoes_socios;",
    "analyze motivos_situacao_cadastral;",
    "",
    "commit;",
    "",
    echo("Importação híbrida PostgreSQL do CNPJ DB Loader concluída."),
    "",
  ];
}
