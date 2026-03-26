import path from "node:path";
import { performance } from "node:perf_hooks";

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
import {
  buildImportPlan,
  buildImportPlanFingerprint,
  collectImportSourceFiles,
} from "./import/planning.js";
import { ensureQuarantineTable } from "./import/quarantine.js";
import { detectImportSchemaCapabilities } from "./import/schema-capabilities.js";
import {
  ensureImportPlanTables,
  readSavedImportPlan,
  saveImportPlan,
  updateImportPlanStatus,
} from "./import/plan-store.js";
import {
  IMPORT_ORDER,
  isImportDatasetType,
  maskDatabaseLabel,
  type ImportDatasetPerformanceSummary,
  type ImportDatasetPlan,
  type ImportDatasetType,
  type ImportOptions,
  type ImportPerformanceSummary,
  type ImportSummary,
} from "./import/types.js";

type MutableDatasetPerformance = Omit<
  ImportDatasetPerformanceSummary,
  "rowsPerSecond" | "batchesPerMinute"
>;

export type {
  ImportCheckpointRecord,
  ImportCheckpointStatus,
  ImportDatasetPlan,
  ImportDatasetType,
  ImportFilePlan,
  ImportOptions,
  ImportPerformanceSummary,
  ImportProgressEvent,
  ImportProgressListener,
  ImportSchemaCapabilities,
  ImportSummary,
} from "./import/types.js";

function calculateRowsPerSecond(rows: number, durationMs: number): number {
  if (rows <= 0 || durationMs <= 0) {
    return 0;
  }

  return rows / (durationMs / 1000);
}

function calculateBatchesPerMinute(
  batches: number,
  durationMs: number,
): number {
  if (batches <= 0 || durationMs <= 0) {
    return 0;
  }

  return batches / (durationMs / 60000);
}

function createDatasetPerformanceTracker(
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

function finalizeDatasetPerformance(
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

export async function importDataToDatabase(
  inputPath: string,
  options: ImportOptions = {},
): Promise<ImportSummary> {
  const overallStartedAt = performance.now();

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
  let planId: number | null = null;
  let planReused = false;
  let plan: {
    datasets: ImportDatasetPlan[];
    totalFiles: number;
    totalRows: number;
    totalBatches: number;
  } | null = null;
  let scanDurationMs = 0;
  let datasetScanDurationsMs: Partial<Record<ImportDatasetType, number>> = {};

  const sourceFiles = await collectImportSourceFiles(
    validation.validatedPath,
    plannedEntries,
  );
  const sourceFingerprint = buildImportPlanFingerprint(
    validation.validatedPath,
    batchSize,
    sourceFiles,
  );

  const client = new Client({ connectionString: dbUrl });
  let clientConnected = false;
  const datasetSummaries: Array<{
    dataset: ImportDatasetType;
    files: number;
    rows: number;
  }> = [];
  const datasetPerformanceTrackers = new Map<
    ImportDatasetType,
    MutableDatasetPerformance
  >();

  const counters = {
    committedRows: 0,
    committedBatches: 0,
    completedFiles: 0,
    secondaryCnaesRows: 0,
    quarantinedRows: 0,
    resumedFiles: 0,
    skippedCompletedFiles: 0,
  };

  let lookupLoadDurationMs = 0;
  let checkpointBaselineRows = 0;
  let checkpointBaselineBatches = 0;

  try {
    await client.connect();
    clientConnected = true;
    await ensureCheckpointTable(client);
    await ensureQuarantineTable(client);
    await ensureImportPlanTables(client);

    const savedPlan = await readSavedImportPlan(client, sourceFingerprint);

    if (savedPlan) {
      planReused = true;
      planId = savedPlan.plan.id;
      plan = {
        datasets: savedPlan.datasets,
        totalFiles: savedPlan.plan.totalFiles,
        totalRows: savedPlan.plan.totalRows,
        totalBatches: savedPlan.plan.totalBatches,
      };

      options.onProgress?.({
        kind: "plan_ready",
        totalDatasets: plan.datasets.length,
        totalFiles: plan.totalFiles,
        batchSize,
        totalRows: plan.totalRows,
        totalBatches: plan.totalBatches,
        targetDatabase,
        executionOrder: plan.datasets.map((item) => item.dataset),
        reused: true,
        planId,
      });

      await appendJsonLinesLog(progressLogPath, {
        kind: "import_plan_reused",
        planId,
        sourceFingerprint,
        inputPath: path.resolve(inputPath),
        validatedPath: validation.validatedPath,
        targetDatabase,
        totalDatasets: plan.datasets.length,
        totalFiles: plan.totalFiles,
        totalRows: plan.totalRows,
        totalBatches: plan.totalBatches,
        batchSize,
        executionOrder: plan.datasets.map((item) => item.dataset),
        scanDurationMs,
        timestamp: new Date().toISOString(),
      });
    } else {
      const builtPlan = await buildImportPlan(
        inputPath,
        validation.validatedPath,
        sourceFiles,
        batchSize,
        options.onProgress,
        targetDatabase,
      );
      scanDurationMs = builtPlan.scanDurationMs;
      datasetScanDurationsMs = builtPlan.datasetScanDurationsMs;
      plan = {
        datasets: builtPlan.datasets,
        totalFiles: builtPlan.totalFiles,
        totalRows: builtPlan.totalRows,
        totalBatches: builtPlan.totalBatches,
      };
      const persistedPlan = await saveImportPlan(client, {
        sourceFingerprint,
        inputPath: path.resolve(inputPath),
        validatedPath: validation.validatedPath,
        batchSize,
        targetDatabase,
        datasets: plan.datasets,
        totalFiles: plan.totalFiles,
        totalRows: plan.totalRows,
        totalBatches: plan.totalBatches,
      });
      planId = persistedPlan.id;

      options.onProgress?.({
        kind: "plan_ready",
        totalDatasets: plan.datasets.length,
        totalFiles: plan.totalFiles,
        batchSize,
        totalRows: plan.totalRows,
        totalBatches: plan.totalBatches,
        targetDatabase,
        executionOrder: plan.datasets.map((item) => item.dataset),
        reused: false,
        planId,
      });

      await appendJsonLinesLog(progressLogPath, {
        kind: "import_plan_ready",
        planId,
        sourceFingerprint,
        inputPath: path.resolve(inputPath),
        validatedPath: validation.validatedPath,
        targetDatabase,
        totalDatasets: plan.datasets.length,
        totalFiles: plan.totalFiles,
        totalRows: plan.totalRows,
        totalBatches: plan.totalBatches,
        batchSize,
        executionOrder: plan.datasets.map((item) => item.dataset),
        scanDurationMs,
        datasetScanDurationsMs,
        timestamp: new Date().toISOString(),
      });
    }

    if (!plan) {
      throw new ValidationError(
        "Import plan was not available after import execution.",
      );
    }

    for (const datasetPlan of plan.datasets) {
      datasetPerformanceTrackers.set(
        datasetPlan.dataset,
        createDatasetPerformanceTracker(
          datasetPlan,
          datasetScanDurationsMs[datasetPlan.dataset] ?? 0,
        ),
      );
    }

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
    checkpointBaselineRows = checkpointTotals.committedRows;
    checkpointBaselineBatches = checkpointTotals.committedBatches;

    for (const datasetPlan of plan.datasets) {
      const tracker = datasetPerformanceTrackers.get(datasetPlan.dataset);
      if (!tracker) {
        continue;
      }

      for (const filePlan of datasetPlan.files) {
        if (
          filePlan.checkpoint?.status === "completed" &&
          filePlan.checkpoint.byteOffset >= filePlan.fileSize
        ) {
          tracker.skippedCompletedFiles += 1;
          continue;
        }

        if (
          filePlan.checkpoint &&
          (filePlan.checkpoint.byteOffset > 0 ||
            filePlan.checkpoint.rowsCommitted > 0)
        ) {
          tracker.resumedFiles += 1;
        }
      }
    }

    if (planId !== null) {
      await updateImportPlanStatus(client, planId, "in_progress");
    }

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
      planId,
      sourceFingerprint,
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

    const lookupStartedAt = performance.now();
    const lookupCache = await loadLookupCaches(client);
    lookupLoadDurationMs = performance.now() - lookupStartedAt;

    let globalFileIndex = 0;
    for (const [datasetIndex, datasetPlan] of plan.datasets.entries()) {
      const datasetTracker = datasetPerformanceTrackers.get(
        datasetPlan.dataset,
      );
      if (!datasetTracker) {
        continue;
      }

      await appendJsonLinesLog(progressLogPath, {
        kind: "dataset_started",
        dataset: datasetPlan.dataset,
        datasetIndex: datasetIndex + 1,
        totalDatasets: plan.datasets.length,
        plannedRows: datasetPlan.totalRows,
        plannedBatches: datasetPlan.totalBatches,
        resumedFiles: datasetTracker.resumedFiles,
        skippedCompletedFiles: datasetTracker.skippedCompletedFiles,
        timestamp: new Date().toISOString(),
      });

      for (const filePlan of datasetPlan.files) {
        globalFileIndex += 1;

        const rowsBefore = counters.committedRows;
        const batchesBefore = counters.committedBatches;
        const fileStartedAt = performance.now();
        const filePerformanceBefore = {
          insertDurationMs: datasetTracker.insertDurationMs,
          retryDurationMs: datasetTracker.retryDurationMs,
          quarantineDurationMs: datasetTracker.quarantineDurationMs,
          retriedRows: datasetTracker.retriedRows,
          retriedBatches: datasetTracker.retriedBatches,
          quarantinedRows: datasetTracker.quarantinedRows,
        };

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
            performance: datasetTracker,
          },
        );

        datasetTracker.importDurationMs += performance.now() - fileStartedAt;
        datasetTracker.importedRows += counters.committedRows - rowsBefore;
        datasetTracker.committedBatches +=
          counters.committedBatches - batchesBefore;

        await appendJsonLinesLog(progressLogPath, {
          kind: "file_metrics",
          dataset: datasetPlan.dataset,
          datasetIndex: datasetIndex + 1,
          filePath: filePlan.absolutePath,
          fileDisplayPath: filePlan.displayPath,
          fileIndex: globalFileIndex,
          importedRows: counters.committedRows - rowsBefore,
          committedBatches: counters.committedBatches - batchesBefore,
          insertDurationMs:
            datasetTracker.insertDurationMs -
            filePerformanceBefore.insertDurationMs,
          retryDurationMs:
            datasetTracker.retryDurationMs -
            filePerformanceBefore.retryDurationMs,
          quarantineDurationMs:
            datasetTracker.quarantineDurationMs -
            filePerformanceBefore.quarantineDurationMs,
          retriedRows:
            datasetTracker.retriedRows - filePerformanceBefore.retriedRows,
          retriedBatches:
            datasetTracker.retriedBatches -
            filePerformanceBefore.retriedBatches,
          quarantinedRows:
            datasetTracker.quarantinedRows -
            filePerformanceBefore.quarantinedRows,
          durationMs: performance.now() - fileStartedAt,
          timestamp: new Date().toISOString(),
        });
      }

      datasetSummaries.push({
        dataset: datasetPlan.dataset,
        files: datasetPlan.files.length,
        rows: datasetPlan.totalRows,
      });

      const finalizedDatasetPerformance =
        finalizeDatasetPerformance(datasetTracker);

      await appendJsonLinesLog(progressLogPath, {
        kind: "dataset_completed",
        dataset: datasetPlan.dataset,
        datasetIndex: datasetIndex + 1,
        totalDatasets: plan.datasets.length,
        metrics: finalizedDatasetPerformance,
        timestamp: new Date().toISOString(),
      });
    }

    if (planId !== null) {
      await updateImportPlanStatus(client, planId, "completed");
    }
  } catch (error) {
    if (clientConnected && planId !== null) {
      await updateImportPlanStatus(client, planId, "failed").catch(
        () => undefined,
      );
    }
    throw error;
  } finally {
    await client.end().catch(() => undefined);
  }

  if (!plan) {
    throw new ValidationError(
      "Import plan was not available after import execution.",
    );
  }

  const executionDurationMs =
    performance.now() - overallStartedAt - scanDurationMs;
  const datasetPerformance = plan.datasets
    .map((datasetPlan) => datasetPerformanceTrackers.get(datasetPlan.dataset))
    .filter((item): item is MutableDatasetPerformance => item !== undefined)
    .map(finalizeDatasetPerformance);
  const executionRowsCommitted = Math.max(
    0,
    counters.committedRows - checkpointBaselineRows,
  );
  const executionBatchesCommitted = Math.max(
    0,
    counters.committedBatches - checkpointBaselineBatches,
  );

  const performanceSummary: ImportPerformanceSummary = {
    planReused,
    totalDurationMs: performance.now() - overallStartedAt,
    scanDurationMs,
    executionDurationMs,
    lookupLoadDurationMs,
    insertDurationMs: datasetPerformance.reduce(
      (sum, item) => sum + item.insertDurationMs,
      0,
    ),
    retryDurationMs: datasetPerformance.reduce(
      (sum, item) => sum + item.retryDurationMs,
      0,
    ),
    quarantineDurationMs: datasetPerformance.reduce(
      (sum, item) => sum + item.quarantineDurationMs,
      0,
    ),
    rowsPerSecond: calculateRowsPerSecond(
      executionRowsCommitted,
      executionDurationMs,
    ),
    batchesPerMinute: calculateBatchesPerMinute(
      executionBatchesCommitted,
      executionDurationMs,
    ),
    datasets: datasetPerformance,
  };

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
    performance: performanceSummary,
    timestamp: new Date().toISOString(),
  });

  return {
    inputPath: path.resolve(inputPath),
    validatedPath: validation.validatedPath,
    targetDatabase,
    importPlanId: planId,
    reusedImportPlan: planReused,
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
    performance: performanceSummary,
    warnings: [
      "The importer uses exact file planning, checkpointed batch commits, and byte-offset resume. If a batch fails, rerunning the same command resumes from the last committed checkpoint instead of restarting the full load.",
      "Import plans are persisted in the database and reused for the same validated input, source files, and batch size so resumed imports do not recount rows unnecessarily.",
      "The importer remains idempotent for the current schema: rerunning the same validated files updates existing rows instead of duplicating them.",
      "Rows that fail validation or database constraints are moved to import_quarantine and the import continues from the next row.",
      "The import summary now includes baseline timing and throughput metrics for scan, execution, retry, and quarantine paths so future performance changes can be measured against a stable reference.",
      "The default batch size is conservative to reduce RAM pressure during long PostgreSQL imports. Increase --batch-size only after validating RAM usage and PostgreSQL stability in your environment.",
    ],
    progressLogPath,
  };
}
