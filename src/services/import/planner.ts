import path from "node:path";

import type { Client } from "pg";

import { ValidationError } from "../../core/errors/index.js";
import type { FileInspection, InspectSummary } from "../inspect.service.js";
import { appendJsonLinesLog } from "../logging.service.js";
import {
  ensureImportPlanTables,
  readSavedImportPlan,
  saveImportPlan,
} from "./plan-store.js";
import {
  buildImportPlan,
  buildImportPlanFingerprint,
  collectImportSourceFiles,
} from "./planning.js";
import {
  IMPORT_ORDER,
  isImportDatasetType,
  type ImportDatasetPlan,
  type ImportDatasetType,
  type ImportProgressListener,
} from "./types.js";

export type ImportDatasetEntry = {
  dataset: ImportDatasetType;
  files: FileInspection[];
};

export type PreparedImportPlan = {
  planId: number | null;
  planReused: boolean;
  plan: {
    datasets: ImportDatasetPlan[];
    totalFiles: number;
    totalRows: number;
    totalBatches: number;
  };
  scanDurationMs: number;
  datasetScanDurationsMs: Partial<Record<ImportDatasetType, number>>;
  sourceFingerprint: string;
};

export function resolveRequestedDatasets(
  requestedDataset: string | undefined,
): ImportDatasetType[] {
  if (!requestedDataset) {
    return IMPORT_ORDER;
  }

  if (!isImportDatasetType(requestedDataset)) {
    throw new ValidationError(`Unsupported dataset type: ${requestedDataset}.`);
  }

  return IMPORT_ORDER.filter((dataset) => dataset === requestedDataset);
}

export function collectDatasetEntriesForImport(
  inspection: InspectSummary,
  selectedDatasets: ImportDatasetType[],
): ImportDatasetEntry[] {
  return selectedDatasets
    .map((dataset) => ({
      dataset,
      files: inspection.entries.filter(
        (entry) => entry.entryKind === "file" && entry.inferredType === dataset,
      ),
    }))
    .filter((entry) => entry.files.length > 0);
}

export async function prepareImportPlan(input: {
  client: Client;
  inputPath: string;
  validatedPath: string;
  batchSize: number;
  datasetEntries: ImportDatasetEntry[];
  targetDatabase: string;
  progressLogPath: string;
  onProgress?: ImportProgressListener;
}): Promise<PreparedImportPlan> {
  const {
    client,
    inputPath,
    validatedPath,
    batchSize,
    datasetEntries,
    targetDatabase,
    progressLogPath,
    onProgress,
  } = input;

  if (datasetEntries.length === 0) {
    throw new ValidationError(
      "No validated dataset files were found for the requested import.",
    );
  }

  await ensureImportPlanTables(client);

  const sourceFiles = await collectImportSourceFiles(
    validatedPath,
    datasetEntries,
  );
  const sourceFingerprint = buildImportPlanFingerprint(
    validatedPath,
    batchSize,
    sourceFiles,
  );
  const savedPlan = await readSavedImportPlan(client, sourceFingerprint);

  if (savedPlan) {
    const plan = {
      datasets: savedPlan.datasets,
      totalFiles: savedPlan.plan.totalFiles,
      totalRows: savedPlan.plan.totalRows,
      totalBatches: savedPlan.plan.totalBatches,
    };

    onProgress?.({
      kind: "plan_ready",
      totalDatasets: plan.datasets.length,
      totalFiles: plan.totalFiles,
      batchSize,
      totalRows: plan.totalRows,
      totalBatches: plan.totalBatches,
      targetDatabase,
      executionOrder: plan.datasets.map((item) => item.dataset),
      reused: true,
      planId: savedPlan.plan.id,
    });

    await appendJsonLinesLog(progressLogPath, {
      kind: "import_plan_reused",
      planId: savedPlan.plan.id,
      sourceFingerprint,
      inputPath: path.resolve(inputPath),
      validatedPath,
      targetDatabase,
      totalDatasets: plan.datasets.length,
      totalFiles: plan.totalFiles,
      totalRows: plan.totalRows,
      totalBatches: plan.totalBatches,
      batchSize,
      executionOrder: plan.datasets.map((item) => item.dataset),
      scanDurationMs: 0,
      timestamp: new Date().toISOString(),
    });

    return {
      planId: savedPlan.plan.id,
      planReused: true,
      plan,
      scanDurationMs: 0,
      datasetScanDurationsMs: {},
      sourceFingerprint,
    };
  }

  const builtPlan = await buildImportPlan(
    inputPath,
    validatedPath,
    sourceFiles,
    batchSize,
    onProgress,
    targetDatabase,
  );
  const plan = {
    datasets: builtPlan.datasets,
    totalFiles: builtPlan.totalFiles,
    totalRows: builtPlan.totalRows,
    totalBatches: builtPlan.totalBatches,
  };
  const persistedPlan = await saveImportPlan(client, {
    sourceFingerprint,
    inputPath: path.resolve(inputPath),
    validatedPath,
    batchSize,
    targetDatabase,
    datasets: plan.datasets,
    totalFiles: plan.totalFiles,
    totalRows: plan.totalRows,
    totalBatches: plan.totalBatches,
  });

  onProgress?.({
    kind: "plan_ready",
    totalDatasets: plan.datasets.length,
    totalFiles: plan.totalFiles,
    batchSize,
    totalRows: plan.totalRows,
    totalBatches: plan.totalBatches,
    targetDatabase,
    executionOrder: plan.datasets.map((item) => item.dataset),
    reused: false,
    planId: persistedPlan.id,
  });

  await appendJsonLinesLog(progressLogPath, {
    kind: "import_plan_ready",
    planId: persistedPlan.id,
    sourceFingerprint,
    inputPath: path.resolve(inputPath),
    validatedPath,
    targetDatabase,
    totalDatasets: plan.datasets.length,
    totalFiles: plan.totalFiles,
    totalRows: plan.totalRows,
    totalBatches: plan.totalBatches,
    batchSize,
    executionOrder: plan.datasets.map((item) => item.dataset),
    scanDurationMs: builtPlan.scanDurationMs,
    datasetScanDurationsMs: builtPlan.datasetScanDurationsMs,
    timestamp: new Date().toISOString(),
  });

  return {
    planId: persistedPlan.id,
    planReused: false,
    plan,
    scanDurationMs: builtPlan.scanDurationMs,
    datasetScanDurationsMs: builtPlan.datasetScanDurationsMs,
    sourceFingerprint,
  };
}
