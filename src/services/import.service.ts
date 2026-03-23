import path from "node:path";

import { Client } from "pg";

import { ValidationError } from "../core/errors/index.js";
import { resolveDbUrl } from "./db.service.js";
import { inspectFiles } from "./inspect.service.js";
import { appendJsonLinesLog, createJsonLinesLog } from "./logging.service.js";
import { validateInputDirectory } from "./validate.service.js";
import {
  ensureCheckpointTable,
  hydratePlanWithCheckpoints,
} from "./import/checkpoints.js";
import { importDatasetFile } from "./import/file-import.js";
import { loadLookupCaches } from "./import/lookups.js";
import { buildImportPlan } from "./import/planning.js";
import { ensureQuarantineTable } from "./import/quarantine.js";
import { detectImportSchemaCapabilities } from "./import/schema-capabilities.js";
import {
  IMPORT_ORDER,
  isImportDatasetType,
  maskDatabaseLabel,
  type ImportDatasetType,
  type ImportOptions,
  type ImportSummary,
} from "./import/types.js";

export type {
  ImportCheckpointRecord,
  ImportCheckpointStatus,
  ImportDatasetPlan,
  ImportDatasetType,
  ImportFilePlan,
  ImportOptions,
  ImportProgressEvent,
  ImportProgressListener,
  ImportSchemaCapabilities,
  ImportSummary,
} from "./import/types.js";

export async function importDataToDatabase(
  inputPath: string,
  options: ImportOptions = {},
): Promise<ImportSummary> {
  if (options.dataset && !isImportDatasetType(options.dataset)) {
    throw new ValidationError(`Unsupported dataset type: ${options.dataset}.`);
  }

  const validation = await validateInputDirectory(inputPath);
  if (!validation.ok) {
    throw new ValidationError(
      `The input directory is not ready for import. ${validation.errors.join(" ")}`,
    );
  }

  const validatedInspection = await inspectFiles(validation.validatedPath);
  const selectedDatasets = options.dataset
    ? IMPORT_ORDER.filter((dataset) => dataset === options.dataset)
    : IMPORT_ORDER;

  const datasetEntries = selectedDatasets.map((dataset) => ({
    dataset,
    files: validatedInspection.entries.filter(
      (entry) => entry.entryKind === "file" && entry.inferredType === dataset,
    ),
  }));

  const plannedEntries = datasetEntries.filter((item) => item.files.length > 0);

  if (plannedEntries.length === 0) {
    throw new ValidationError(
      "No validated dataset files were found for the requested import.",
    );
  }

  const dbUrl = await resolveDbUrl(options.dbUrl);
  const targetDatabase = maskDatabaseLabel(dbUrl);
  const batchSize = Math.max(1, options.batchSize ?? 500);
  const progressLogPath = await createJsonLinesLog("import-progress");

  const plan = await buildImportPlan(
    inputPath,
    validation.validatedPath,
    plannedEntries,
    batchSize,
    options.onProgress,
    targetDatabase,
  );

  await appendJsonLinesLog(progressLogPath, {
    kind: "import_plan_ready",
    inputPath: path.resolve(inputPath),
    validatedPath: validation.validatedPath,
    targetDatabase,
    totalDatasets: plan.datasets.length,
    totalFiles: plan.totalFiles,
    totalRows: plan.totalRows,
    totalBatches: plan.totalBatches,
    batchSize,
    executionOrder: plan.datasets.map((item) => item.dataset),
    timestamp: new Date().toISOString(),
  });

  const client = new Client({ connectionString: dbUrl });
  const datasetSummaries: Array<{
    dataset: ImportDatasetType;
    files: number;
    rows: number;
  }> = [];

  const counters = {
    committedRows: 0,
    committedBatches: 0,
    completedFiles: 0,
    secondaryCnaesRows: 0,
    quarantinedRows: 0,
    resumedFiles: 0,
    skippedCompletedFiles: 0,
  };

  try {
    await client.connect();
    await ensureCheckpointTable(client);
    await ensureQuarantineTable(client);
    const schemaCapabilities = await detectImportSchemaCapabilities(client);
    const checkpointTotals = await hydratePlanWithCheckpoints(
      client,
      plan.datasets,
      batchSize,
    );

    counters.committedRows = checkpointTotals.committedRows;
    counters.committedBatches = checkpointTotals.committedBatches;
    counters.completedFiles = checkpointTotals.completedFiles;
    counters.resumedFiles = checkpointTotals.resumedFiles;
    counters.skippedCompletedFiles = checkpointTotals.skippedCompletedFiles;

    options.onProgress?.({
      kind: "start",
      inputPath: path.resolve(inputPath),
      validatedPath: validation.validatedPath,
      totalDatasets: plan.datasets.length,
      totalFiles: plan.totalFiles,
      targetDatabase,
      totalRows: plan.totalRows,
      totalBatches: plan.totalBatches,
      committedRows: counters.committedRows,
      committedBatches: counters.committedBatches,
    });

    await appendJsonLinesLog(progressLogPath, {
      kind: "import_started",
      inputPath: path.resolve(inputPath),
      validatedPath: validation.validatedPath,
      targetDatabase,
      totalDatasets: plan.datasets.length,
      totalFiles: plan.totalFiles,
      totalRows: plan.totalRows,
      totalBatches: plan.totalBatches,
      batchSize,
      resumedFiles: counters.resumedFiles,
      skippedCompletedFiles: counters.skippedCompletedFiles,
      committedRows: counters.committedRows,
      committedBatches: counters.committedBatches,
      timestamp: new Date().toISOString(),
    });

    const lookupCache = await loadLookupCaches(client);

    let globalFileIndex = 0;
    for (const [datasetIndex, datasetPlan] of plan.datasets.entries()) {
      for (const filePlan of datasetPlan.files) {
        globalFileIndex += 1;
        await importDatasetFile(
          client,
          lookupCache,
          filePlan,
          schemaCapabilities,
          counters,
          {
            datasetIndex: datasetIndex + 1,
            totalDatasets: plan.datasets.length,
            totalFiles: plan.totalFiles,
            totalBatches: plan.totalBatches,
            fileIndex: globalFileIndex,
            onProgress: options.onProgress,
            progressLogPath,
            batchSize,
            verboseProgress: options.verboseProgress ?? false,
          },
        );
      }

      datasetSummaries.push({
        dataset: datasetPlan.dataset,
        files: datasetPlan.files.length,
        rows: datasetPlan.totalRows,
      });
    }
  } finally {
    await client.end().catch(() => undefined);
  }

  options.onProgress?.({
    kind: "finish",
    totalDatasets: plan.datasets.length,
    totalFiles: plan.totalFiles,
    completedFiles: counters.completedFiles,
    processedRows: counters.committedRows,
    totalRows: plan.totalRows,
    committedBatches: counters.committedBatches,
    totalBatches: plan.totalBatches,
    secondaryCnaesRows: counters.secondaryCnaesRows,
    quarantinedRows: counters.quarantinedRows,
  });

  await appendJsonLinesLog(progressLogPath, {
    kind: "import_finished",
    totalDatasets: plan.datasets.length,
    totalFiles: plan.totalFiles,
    completedFiles: counters.completedFiles,
    processedRows: counters.committedRows,
    totalRows: plan.totalRows,
    committedBatches: counters.committedBatches,
    totalBatches: plan.totalBatches,
    secondaryCnaesRows: counters.secondaryCnaesRows,
    quarantinedRows: counters.quarantinedRows,
    resumedFiles: counters.resumedFiles,
    skippedCompletedFiles: counters.skippedCompletedFiles,
    timestamp: new Date().toISOString(),
  });

  return {
    inputPath: path.resolve(inputPath),
    validatedPath: validation.validatedPath,
    targetDatabase,
    importedDatasets: datasetSummaries.map((item) => item.dataset),
    importedFiles: counters.completedFiles,
    processedRows: counters.committedRows,
    plannedRows: plan.totalRows,
    committedBatches: counters.committedBatches,
    plannedBatches: plan.totalBatches,
    secondaryCnaesRows: counters.secondaryCnaesRows,
    quarantinedRows: counters.quarantinedRows,
    resumedFiles: counters.resumedFiles,
    skippedCompletedFiles: counters.skippedCompletedFiles,
    datasetSummaries,
    warnings: [
      "The importer uses exact file planning, checkpointed batch commits, and byte-offset resume. If a batch fails, rerunning the same command resumes from the last committed checkpoint instead of restarting the full load.",
      "The importer remains idempotent for the current schema: rerunning the same validated files updates existing rows instead of duplicating them.",
      "Rows that fail validation or database constraints are moved to import_quarantine and the import continues from the next row.",
      "The default batch size is conservative to reduce RAM pressure during long PostgreSQL imports. Increase --batch-size only after validating RAM usage and PostgreSQL stability in your environment.",
    ],
    progressLogPath,
  };
}
