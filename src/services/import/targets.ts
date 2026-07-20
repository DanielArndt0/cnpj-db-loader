import {
  NOMES_TABELAS_DATASET,
  NOMES_TABELAS_STAGING,
} from "../schema/table-names.js";
import type { ImportDatasetType, ImportWriteTarget } from "./types.js";

export const STAGED_IMPORT_DATASETS: ReadonlySet<ImportDatasetType> = new Set(
  Object.keys(NOMES_TABELAS_STAGING) as ImportDatasetType[],
);

export function usesStagingWriteTarget(dataset: ImportDatasetType): boolean {
  return STAGED_IMPORT_DATASETS.has(dataset);
}

export function resolveImportWriteTarget(
  dataset: ImportDatasetType,
): ImportWriteTarget {
  return usesStagingWriteTarget(dataset) ? "staging" : "final";
}

export function getTargetTableName(dataset: ImportDatasetType): string {
  return NOMES_TABELAS_STAGING[dataset] ?? NOMES_TABELAS_DATASET[dataset];
}

export function getSecondaryTargetTableName(
  dataset: ImportDatasetType,
): string | null {
  void dataset;
  return null;
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
  }

  return [...tableNames];
}

export function getFinalTargetTableName(dataset: ImportDatasetType): string {
  return NOMES_TABELAS_DATASET[dataset];
}
