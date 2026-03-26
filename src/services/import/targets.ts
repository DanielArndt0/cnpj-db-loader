import type { ImportDatasetType, ImportWriteTarget } from "./types.js";

export const STAGED_IMPORT_DATASETS: ReadonlySet<ImportDatasetType> = new Set([
  "companies",
  "establishments",
  "partners",
  "simples_options",
]);

const STAGING_TABLE_BY_DATASET: Partial<Record<ImportDatasetType, string>> = {
  companies: "staging_companies",
  establishments: "staging_establishments",
  partners: "staging_partners",
  simples_options: "staging_simples_options",
};

export function usesStagingWriteTarget(dataset: ImportDatasetType): boolean {
  return STAGED_IMPORT_DATASETS.has(dataset);
}

export function resolveImportWriteTarget(
  dataset: ImportDatasetType,
): ImportWriteTarget {
  return usesStagingWriteTarget(dataset) ? "staging" : "final";
}

export function getTargetTableName(dataset: ImportDatasetType): string {
  return STAGING_TABLE_BY_DATASET[dataset] ?? dataset;
}

export function getSecondaryTargetTableName(
  dataset: ImportDatasetType,
): string | null {
  if (dataset !== "establishments") {
    return null;
  }

  return usesStagingWriteTarget(dataset)
    ? "staging_establishment_secondary_cnaes"
    : "establishment_secondary_cnaes";
}

export function collectRequiredStagingTables(
  datasets: readonly ImportDatasetType[],
): string[] {
  const tableNames = new Set<string>();

  for (const dataset of datasets) {
    if (!usesStagingWriteTarget(dataset)) {
      continue;
    }

    tableNames.add(getTargetTableName(dataset));
    const secondaryTableName = getSecondaryTargetTableName(dataset);
    if (secondaryTableName) {
      tableNames.add(secondaryTableName);
    }
  }

  return [...tableNames];
}
