import { performance } from "node:perf_hooks";

import type { Client } from "pg";

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
    sourceSql:
      "select source.legal_nature_code as code from staging_companies source",
  },
  {
    lookupTable: "partner_qualifications",
    sourceSql:
      "select source.responsible_qualification_code as code from staging_companies source",
  },
  {
    lookupTable: "company_sizes",
    sourceSql:
      "select source.company_size_code as code from staging_companies source",
  },
];

const ESTABLISHMENTS_LOOKUP_SOURCES: readonly LookupReconciliationSource[] = [
  {
    lookupTable: "branch_types",
    sourceSql:
      "select source.branch_type_code as code from staging_establishments source",
  },
  {
    lookupTable: "registration_statuses",
    sourceSql:
      "select source.registration_status_code as code from staging_establishments source",
  },
  {
    lookupTable: "reasons",
    sourceSql:
      "select source.registration_status_reason_code as code from staging_establishments source",
  },
  {
    lookupTable: "countries",
    sourceSql:
      "select source.country_code as code from staging_establishments source",
  },
  {
    lookupTable: "cnaes",
    sourceSql:
      "select source.main_cnae_code as code from staging_establishments source",
  },
  {
    lookupTable: "cities",
    sourceSql:
      "select source.city_code as code from staging_establishments source",
  },
];

const PARTNERS_LOOKUP_SOURCES: readonly LookupReconciliationSource[] = [
  {
    lookupTable: "partner_types",
    sourceSql:
      "select source.partner_type_code as code from staging_partners source",
  },
  {
    lookupTable: "partner_qualifications",
    sourceSql:
      "select source.partner_qualification_code as code from staging_partners source",
  },
  {
    lookupTable: "countries",
    sourceSql:
      "select source.country_code as code from staging_partners source",
  },
  {
    lookupTable: "partner_qualifications",
    sourceSql:
      "select source.legal_representative_qualification_code as code from staging_partners source",
  },
  {
    lookupTable: "age_groups",
    sourceSql:
      "select source.age_group_code as code from staging_partners source",
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
      `  left join ${input.lookupTable} lookup_table on lookup_table.code = distinct_codes.code`,
      "  where lookup_table.code is null",
      "),",
      "inserted as (",
      `  insert into ${input.lookupTable} (code, description)`,
      "  select",
      "    missing_codes.code,",
      "    $1 || ' (' || missing_codes.code || ')'",
      "  from missing_codes",
      "  on conflict (code) do nothing",
      "  returning code",
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
