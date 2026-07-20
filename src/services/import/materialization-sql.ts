import {
  companiesLayout,
  establishmentsLayout,
  partnersLayout,
  simplesLayout,
} from "../../dictionary/layouts/index.js";
import {
  COLUNA_CHAVE_DEDUPLICACAO_SOCIO,
  COLUNA_CNPJ_COMPLETO,
  COLUNA_CODIGO_CNAE,
  TABELA_EMPRESAS,
  TABELA_ESTABELECIMENTO_CNAES_SECUNDARIOS,
  TABELA_ESTABELECIMENTOS,
  TABELA_SIMPLES,
  TABELA_SOCIOS,
  TABELA_STAGING_EMPRESAS,
  TABELA_STAGING_ESTABELECIMENTOS,
  TABELA_STAGING_SIMPLES,
  TABELA_STAGING_SOCIOS,
} from "../schema/table-names.js";
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
    `      coalesce(${alias}.cnpj_basico, '') || '|' ||`,
    `      coalesce(${alias}.identificador_socio, '') || '|' ||`,
    `      coalesce(${alias}.nome_socio_razao_social, '') || '|' ||`,
    `      coalesce(${alias}.cnpj_cpf_socio, '') || '|' ||`,
    `      coalesce(${alias}.codigo_qualificacao_socio, '') || '|' ||`,
    `      coalesce((${alias}.data_entrada_sociedade - date '2000-01-01')::text, '') || '|' ||`,
    `      coalesce(${alias}.codigo_pais, '') || '|' ||`,
    `      coalesce(${alias}.cpf_representante_legal, '') || '|' ||`,
    `      coalesce(${alias}.nome_representante_legal, '') || '|' ||`,
    `      coalesce(${alias}.codigo_qualificacao_representante_legal, '') || '|' ||`,
    `      coalesce(${alias}.codigo_faixa_etaria, '')`,
    `    )`,
  ].join("\n");
}

function buildEstablishmentCnpjFullExpression(alias: string): string {
  return `${alias}.cnpj_basico || ${alias}.cnpj_ordem || ${alias}.cnpj_dv`;
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
      ")",
      "select",
      "  coalesce(max(staging_id), $1::bigint)::bigint as max_staging_id,",
      "  count(*)::bigint as source_rows,",
      "  count(*)::bigint as affected_rows",
      "from chunked;",
    ].join("\n"),
    values: [input.lastStagingId, input.chunkSize],
  };
}

function buildEstablishmentsChunkInsertSql(input: {
  insertColumns: readonly string[];
  selectColumns: readonly string[];
  conflictClause: string;
  lastStagingId: number;
  chunkSize: number;
  includeSecondaryCnaesTable: boolean;
}): MaterializationChunkQuery {
  const chunkSelectList = [
    "source.staging_id",
    ...input.selectColumns.map((column) => `source.${column}`),
    `${buildEstablishmentCnpjFullExpression("source")} as ${COLUNA_CNPJ_COMPLETO}`,
  ].join(",\n    ");
  const insertSelectList = input.insertColumns.join(", ");
  const secondaryCnaesCtes = input.includeSecondaryCnaesTable
    ? [
        ",",
        "deleted_secondary_cnaes as (",
        `  delete from ${TABELA_ESTABELECIMENTO_CNAES_SECUNDARIOS} target`,
        `  using (select distinct ${COLUNA_CNPJ_COMPLETO} from inserted_establishments) source_keys`,
        `  where target.${COLUNA_CNPJ_COMPLETO} = source_keys.${COLUNA_CNPJ_COMPLETO}`,
        "  returning 1",
        "),",
        "secondary_cnaes_source as (",
        "  select distinct",
        `    chunked.${COLUNA_CNPJ_COMPLETO},`,
        `    btrim(codigo_cnae) as ${COLUNA_CODIGO_CNAE}`,
        "  from chunked",
        "  inner join inserted_establishments inserted",
        `    on inserted.${COLUNA_CNPJ_COMPLETO} = chunked.${COLUNA_CNPJ_COMPLETO}`,
        "  cross join lateral unnest(string_to_array(chunked.cnae_fiscal_secundaria_raw, ',')) as codigo_cnae",
        "  where chunked.cnae_fiscal_secundaria_raw is not null",
        "    and chunked.cnae_fiscal_secundaria_raw <> ''",
        "    and btrim(codigo_cnae) <> ''",
        "),",
        "inserted_secondary_cnaes as (",
        `  insert into ${TABELA_ESTABELECIMENTO_CNAES_SECUNDARIOS} (${COLUNA_CNPJ_COMPLETO}, ${COLUNA_CODIGO_CNAE})`,
        `  select ${COLUNA_CNPJ_COMPLETO}, ${COLUNA_CODIGO_CNAE}`,
        "  from secondary_cnaes_source",
        `  on conflict (${COLUNA_CNPJ_COMPLETO}, ${COLUNA_CODIGO_CNAE}) do nothing`,
        "  returning 1",
        ")",
      ]
    : [];

  return {
    text: [
      "with chunked as (",
      `  select\n    ${chunkSelectList}`,
      `  from ${TABELA_STAGING_ESTABELECIMENTOS} source`,
      "  where source.staging_id > $1",
      "  order by source.staging_id asc",
      "  limit $2",
      "),",
      "inserted_establishments as (",
      `  insert into ${TABELA_ESTABELECIMENTOS} (${input.insertColumns.join(", ")})`,
      `  select ${insertSelectList}`,
      "  from chunked",
      ...(input.conflictClause ? [input.conflictClause] : []),
      `  returning ${COLUNA_CNPJ_COMPLETO}`,
      ")",
      ...secondaryCnaesCtes,
      "select",
      "  coalesce(max(staging_id), $1::bigint)::bigint as max_staging_id,",
      "  count(*)::bigint as source_rows,",
      "  count(*)::bigint as affected_rows",
      "from chunked;",
    ].join("\n"),
    values: [input.lastStagingId, input.chunkSize],
  };
}

function buildPartnersChunkInsertSql(input: {
  insertColumns: readonly string[];
  lastStagingId: number;
  chunkSize: number;
  includePartnerDedupeKeyInInsert: boolean;
  schemaCapabilities: ImportSchemaCapabilities;
}): MaterializationChunkQuery {
  const baseColumns = MATERIALIZATION_COLUMNS.partners;
  const chunkSelectList = [
    "source.staging_id",
    ...baseColumns.map((column) => `source.${column}`),
    ...(input.includePartnerDedupeKeyInInsert
      ? [
          `${buildPartnerDedupeExpression("source")} as ${COLUNA_CHAVE_DEDUPLICACAO_SOCIO}`,
        ]
      : []),
  ].join(",\n    ");
  const insertSelectList = input.insertColumns.join(", ");
  const conflictClause = getConflictClause(
    "partners",
    [...input.insertColumns],
    input.schemaCapabilities,
  );

  return {
    text: [
      "with chunked as (",
      `  select\n    ${chunkSelectList}`,
      `  from ${TABELA_STAGING_SOCIOS} source`,
      "  where source.staging_id > $1",
      "  order by source.staging_id asc",
      "  limit $2",
      "),",
      "deduped as (",
      "  select *",
      "  from (",
      "    select",
      "      chunked.*,",
      `      row_number() over (partition by ${COLUNA_CHAVE_DEDUPLICACAO_SOCIO} order by staging_id asc) as dedupe_rank`,
      "    from chunked",
      "  ) ranked",
      "  where dedupe_rank = 1",
      "),",
      "inserted as (",
      `  insert into ${TABELA_SOCIOS} (${input.insertColumns.join(", ")})`,
      `  select ${insertSelectList}`,
      "  from deduped",
      conflictClause,
      ")",
      "select",
      "  coalesce((select max(staging_id) from chunked), $1::bigint)::bigint as max_staging_id,",
      "  coalesce((select count(*) from chunked), 0)::bigint as source_rows,",
      "  coalesce((select count(*) from deduped), 0)::bigint as affected_rows;",
    ].join("\n"),
    values: [input.lastStagingId, input.chunkSize],
  };
}

export function buildMaterializationChunkQuery(input: {
  dataset: MaterializationDataset;
  schemaCapabilities: ImportSchemaCapabilities;
  lastStagingId: number;
  chunkSize: number;
  useConflictClause?: boolean;
}): MaterializationChunkQuery {
  const baseColumns = MATERIALIZATION_COLUMNS[input.dataset];
  const useConflictClause = input.useConflictClause ?? true;

  switch (input.dataset) {
    case "partners": {
      const insertColumns = input.schemaCapabilities
        .includePartnerDedupeKeyInInsert
        ? [...baseColumns, COLUNA_CHAVE_DEDUPLICACAO_SOCIO]
        : [...baseColumns];
      return buildPartnersChunkInsertSql({
        insertColumns,
        lastStagingId: input.lastStagingId,
        chunkSize: input.chunkSize,
        includePartnerDedupeKeyInInsert:
          input.schemaCapabilities.includePartnerDedupeKeyInInsert,
        schemaCapabilities: input.schemaCapabilities,
      });
    }
    case "companies":
      return buildChunkInsertSql({
        stagingTable: TABELA_STAGING_EMPRESAS,
        targetTable: TABELA_EMPRESAS,
        insertColumns: baseColumns,
        selectColumns: baseColumns,
        conflictClause: useConflictClause
          ? getConflictClause(
              "companies",
              [...baseColumns],
              input.schemaCapabilities,
            )
          : "",
        lastStagingId: input.lastStagingId,
        chunkSize: input.chunkSize,
      });
    case "establishments": {
      const insertColumns = input.schemaCapabilities
        .includeEstablishmentCnpjFullInInsert
        ? [...baseColumns, COLUNA_CNPJ_COMPLETO]
        : [...baseColumns];
      return buildEstablishmentsChunkInsertSql({
        insertColumns,
        selectColumns: baseColumns,
        conflictClause: useConflictClause
          ? getConflictClause(
              "establishments",
              [...insertColumns],
              input.schemaCapabilities,
            )
          : "",
        lastStagingId: input.lastStagingId,
        chunkSize: input.chunkSize,
        includeSecondaryCnaesTable:
          input.schemaCapabilities.includeEstablishmentSecondaryCnaesTable,
      });
    }
    case "simples_options":
      return buildChunkInsertSql({
        stagingTable: TABELA_STAGING_SIMPLES,
        targetTable: TABELA_SIMPLES,
        insertColumns: baseColumns,
        selectColumns: baseColumns,
        conflictClause: useConflictClause
          ? getConflictClause(
              "simples_options",
              [...baseColumns],
              input.schemaCapabilities,
            )
          : "",
        lastStagingId: input.lastStagingId,
        chunkSize: input.chunkSize,
      });
  }
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
