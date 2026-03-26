import type {
  ImportDatasetPerformanceSummary,
  ImportDatasetPlan,
  ImportDatasetType,
  ImportPerformanceSummary,
} from "./types.js";

export type MutableDatasetPerformance = Omit<
  ImportDatasetPerformanceSummary,
  "rowsPerSecond" | "batchesPerMinute"
>;

export function calculateRowsPerSecond(
  rows: number,
  durationMs: number,
): number {
  if (rows <= 0 || durationMs <= 0) {
    return 0;
  }

  return rows / (durationMs / 1000);
}

export function calculateBatchesPerMinute(
  batches: number,
  durationMs: number,
): number {
  if (batches <= 0 || durationMs <= 0) {
    return 0;
  }

  return batches / (durationMs / 60000);
}

export function createDatasetPerformanceTracker(
  datasetPlan: ImportDatasetPlan,
  scanDurationMs: number,
): MutableDatasetPerformance {
  return {
    dataset: datasetPlan.dataset,
    files: datasetPlan.files.length,
    plannedRows: datasetPlan.totalRows,
    importedRows: 0,
    plannedBatches: datasetPlan.totalBatches,
    committedBatches: 0,
    resumedFiles: 0,
    skippedCompletedFiles: 0,
    retriedRows: 0,
    retriedBatches: 0,
    quarantinedRows: 0,
    scanDurationMs,
    importDurationMs: 0,
    insertDurationMs: 0,
    retryDurationMs: 0,
    quarantineDurationMs: 0,
  };
}

export function finalizeDatasetPerformance(
  tracker: MutableDatasetPerformance,
): ImportDatasetPerformanceSummary {
  return {
    ...tracker,
    rowsPerSecond: calculateRowsPerSecond(
      tracker.importedRows,
      tracker.importDurationMs,
    ),
    batchesPerMinute: calculateBatchesPerMinute(
      tracker.committedBatches,
      tracker.importDurationMs,
    ),
  };
}

export function buildImportPerformanceSummary(input: {
  planReused: boolean;
  totalDurationMs: number;
  scanDurationMs: number;
  executionDurationMs: number;
  lookupLoadDurationMs: number;
  executionRowsCommitted: number;
  executionBatchesCommitted: number;
  datasets: MutableDatasetPerformance[];
}): ImportPerformanceSummary {
  const finalizedDatasets = input.datasets.map(finalizeDatasetPerformance);

  return {
    planReused: input.planReused,
    totalDurationMs: input.totalDurationMs,
    scanDurationMs: input.scanDurationMs,
    executionDurationMs: input.executionDurationMs,
    lookupLoadDurationMs: input.lookupLoadDurationMs,
    insertDurationMs: finalizedDatasets.reduce(
      (sum, item) => sum + item.insertDurationMs,
      0,
    ),
    retryDurationMs: finalizedDatasets.reduce(
      (sum, item) => sum + item.retryDurationMs,
      0,
    ),
    quarantineDurationMs: finalizedDatasets.reduce(
      (sum, item) => sum + item.quarantineDurationMs,
      0,
    ),
    rowsPerSecond: calculateRowsPerSecond(
      input.executionRowsCommitted,
      input.executionDurationMs,
    ),
    batchesPerMinute: calculateBatchesPerMinute(
      input.executionBatchesCommitted,
      input.executionDurationMs,
    ),
    datasets: finalizedDatasets,
  };
}

export function buildImportWarnings(): string[] {
  return [
    "The importer uses exact file planning, checkpointed batch commits, and byte-offset resume. If a load unit fails, rerunning the same command resumes from the last committed checkpoint instead of restarting the full load.",
    "Import plans are persisted in the database and reused for the same validated input, source files, and batch size so resumed imports do not recount rows unnecessarily.",
    "Large datasets now land in lightweight staging tables through PostgreSQL COPY before final materialization. The final schema load is handled in a later phase of the pipeline.",
    "When a new import plan starts, the selected staging tables are truncated before loading so staged bulk loads stay clean and predictable. Resumed plans keep the staged rows that already match the saved checkpoints.",
    "Rows that fail parsing, normalization, COPY fallback, or row-level inserts are moved to import_quarantine and the import continues from the next row.",
    "The import summary includes baseline timing and throughput metrics for scan, execution, retry, and quarantine paths so future performance changes can be measured against a stable reference.",
    "The batch size now defines the staging load unit size. Increase --batch-size only after validating RAM usage and PostgreSQL stability in your environment.",
  ];
}

export function summarizeImportedDatasets(
  datasets: ImportDatasetPlan[],
): Array<{
  dataset: ImportDatasetType;
  files: number;
  rows: number;
}> {
  return datasets.map((datasetPlan) => ({
    dataset: datasetPlan.dataset,
    files: datasetPlan.files.length,
    rows: datasetPlan.totalRows,
  }));
}
