import { performance } from "node:perf_hooks";

import type { Client } from "pg";

import {
  COLUNA_CODIGO,
  COLUNA_DESCRICAO,
  NOMES_TABELAS_LOOKUP,
  TABELA_STAGING_EMPRESAS,
  TABELA_STAGING_ESTABELECIMENTOS,
  TABELA_STAGING_SOCIOS,
} from "../schema/table-names.js";
import { LOOKUP_PLACEHOLDER_LABEL, type LookupTableName } from "./types.js";
import type { MaterializationDataset } from "./materialization-sql.js";

type LookupReconciliationSource = {
  lookupTable: LookupTableName;
  sourceSql: string;
};

type LookupReconciliationResult = {
  lookupTable: LookupTableName;
  insertedCodes: number;
};

export type MaterializationLookupReconciliationSummary = {
  dataset: MaterializationDataset;
  results: LookupReconciliationResult[];
  totalInsertedCodes: number;
  durationMs: number;
};

type CountRow = {
  inserted_count: string;
};

const COMPANIES_LOOKUP_SOURCES: readonly LookupReconciliationSource[] = [
  {
    lookupTable: "legal_natures",
    sourceSql: `select source.codigo_natureza_juridica as code from ${TABELA_STAGING_EMPRESAS} source`,
  },
  {
    lookupTable: "partner_qualifications",
    sourceSql: `select source.codigo_qualificacao_responsavel as code from ${TABELA_STAGING_EMPRESAS} source`,
  },
  {
    lookupTable: "company_sizes",
    sourceSql: `select source.codigo_porte_empresa as code from ${TABELA_STAGING_EMPRESAS} source`,
  },
];

const ESTABLISHMENTS_LOOKUP_SOURCES: readonly LookupReconciliationSource[] = [
  {
    lookupTable: "branch_types",
    sourceSql: `select source.identificador_matriz_filial as code from ${TABELA_STAGING_ESTABELECIMENTOS} source`,
  },
  {
    lookupTable: "registration_statuses",
    sourceSql: `select source.situacao_cadastral as code from ${TABELA_STAGING_ESTABELECIMENTOS} source`,
  },
  {
    lookupTable: "reasons",
    sourceSql: `select source.motivo_situacao_cadastral as code from ${TABELA_STAGING_ESTABELECIMENTOS} source`,
  },
  {
    lookupTable: "countries",
    sourceSql: `select source.codigo_pais as code from ${TABELA_STAGING_ESTABELECIMENTOS} source`,
  },
  {
    lookupTable: "cnaes",
    sourceSql: `select source.cnae_fiscal_principal as code from ${TABELA_STAGING_ESTABELECIMENTOS} source`,
  },
  {
    lookupTable: "cities",
    sourceSql: `select source.codigo_municipio as code from ${TABELA_STAGING_ESTABELECIMENTOS} source`,
  },
];

const PARTNERS_LOOKUP_SOURCES: readonly LookupReconciliationSource[] = [
  {
    lookupTable: "partner_types",
    sourceSql: `select source.identificador_socio as code from ${TABELA_STAGING_SOCIOS} source`,
  },
  {
    lookupTable: "partner_qualifications",
    sourceSql: `select source.codigo_qualificacao_socio as code from ${TABELA_STAGING_SOCIOS} source`,
  },
  {
    lookupTable: "countries",
    sourceSql: `select source.codigo_pais as code from ${TABELA_STAGING_SOCIOS} source`,
  },
  {
    lookupTable: "partner_qualifications",
    sourceSql: `select source.codigo_qualificacao_representante_legal as code from ${TABELA_STAGING_SOCIOS} source`,
  },
  {
    lookupTable: "age_groups",
    sourceSql: `select source.codigo_faixa_etaria as code from ${TABELA_STAGING_SOCIOS} source`,
  },
];

const LOOKUP_SOURCES_BY_DATASET: Readonly<
  Record<MaterializationDataset, readonly LookupReconciliationSource[]>
> = {
  companies: COMPANIES_LOOKUP_SOURCES,
  establishments: ESTABLISHMENTS_LOOKUP_SOURCES,
  partners: PARTNERS_LOOKUP_SOURCES,
  simples_options: [],
};

export function getLookupReconciliationSources(
  dataset: MaterializationDataset,
): readonly LookupTableName[] {
  return LOOKUP_SOURCES_BY_DATASET[dataset].map((source) => source.lookupTable);
}

async function ensureLookupCodesFromSource(input: {
  client: Client;
  lookupTable: LookupTableName;
  sourceSql: string;
}): Promise<number> {
  const lookupTableName = NOMES_TABELAS_LOOKUP[input.lookupTable];
  const result = await input.client.query<CountRow>(
    [
      "with distinct_codes as (",
      "  select distinct trim(source_codes.code) as code",
      `  from (${input.sourceSql}) source_codes`,
      "  where trim(source_codes.code) <> ''",
      "),",
      "missing_codes as (",
      "  select distinct distinct_codes.code",
      "  from distinct_codes",
      `  left join ${lookupTableName} lookup_table on lookup_table.${COLUNA_CODIGO} = distinct_codes.code`,
      `  where lookup_table.${COLUNA_CODIGO} is null`,
      "),",
      "inserted as (",
      `  insert into ${lookupTableName} (${COLUNA_CODIGO}, ${COLUNA_DESCRICAO})`,
      "  select",
      "    missing_codes.code,",
      "    $1 || ' (' || missing_codes.code || ')'",
      "  from missing_codes",
      `  on conflict (${COLUNA_CODIGO}) do nothing`,
      `  returning ${COLUNA_CODIGO}`,
      ")",
      "select count(*)::bigint as inserted_count from inserted;",
    ].join("\n"),
    [LOOKUP_PLACEHOLDER_LABEL[input.lookupTable]],
  );

  return Number.parseInt(result.rows[0]?.inserted_count ?? "0", 10);
}

export async function reconcileMaterializationLookups(input: {
  client: Client;
  dataset: MaterializationDataset;
  onLookupStart?:
    | ((lookupTable: LookupTableName, index: number, total: number) => void)
    | undefined;
}): Promise<MaterializationLookupReconciliationSummary> {
  const startedAt = performance.now();
  const sources = LOOKUP_SOURCES_BY_DATASET[input.dataset];
  const aggregate = new Map<LookupTableName, number>();

  for (const [index, source] of sources.entries()) {
    input.onLookupStart?.(source.lookupTable, index + 1, sources.length);

    const insertedCodes = await ensureLookupCodesFromSource({
      client: input.client,
      lookupTable: source.lookupTable,
      sourceSql: source.sourceSql,
    });

    aggregate.set(
      source.lookupTable,
      (aggregate.get(source.lookupTable) ?? 0) + insertedCodes,
    );
  }

  const results = [...aggregate.entries()].map(
    ([lookupTable, insertedCodes]) => ({
      lookupTable,
      insertedCodes,
    }),
  );

  return {
    dataset: input.dataset,
    results,
    totalInsertedCodes: results.reduce(
      (sum, item) => sum + item.insertedCodes,
      0,
    ),
    durationMs: performance.now() - startedAt,
  };
}
