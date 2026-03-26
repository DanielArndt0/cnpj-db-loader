import path from "node:path";
import { performance } from "node:perf_hooks";

import { Client } from "pg";

import { ValidationError } from "../../core/errors/index.js";
import { appendJsonLinesLog, createJsonLinesLog } from "../logging.service.js";
import {
  buildImportPerformanceSummary,
  buildImportWarnings,
  createDatasetPerformanceTracker,
  finalizeDatasetPerformance,
  summarizeImportedDatasets,
  type MutableDatasetPerformance,
} from "./finalizer.js";
import { importDatasetFile } from "./file-import.js";
import {
  ensureImportCheckpointSupport,
  hydrateImportPlanWithCheckpoints,
} from "./checkpoint-manager.js";
import {
  collectDatasetEntriesForImport,
  prepareImportPlan,
  resolveRequestedDatasets,
  type PreparedImportPlan,
} from "./planner.js";
import { ensureImportQuarantineSupport } from "./quarantine-writer.js";
import {
  ensureStagingSchemaSupport,
  resetStagingTablesForFreshPlan,
} from "./staging-schema.js";
import { detectImportSchemaCapabilities } from "./schema-capabilities.js";
import { updateImportPlanStatus } from "./plan-store.js";
import type { InspectSummary } from "../inspect.service.js";
import type {
  ImportDatasetType,
  ImportOptions,
  ImportPerformanceSummary,
  ImportSummary,
} from "./types.js";

export async function runImportPipeline(input: {
  inputPath: string;
  validatedPath: string;
  inspection: InspectSummary;
  dbUrl: string;
  options?: ImportOptions;
  targetDatabase: string;
}): Promise<ImportSummary> {
  const { inputPath, validatedPath, inspection, dbUrl, targetDatabase } = input;
  const options = input.options ?? {};
  const overallStartedAt = performance.now();
  const selectedDatasets = resolveRequestedDatasets(options.dataset);
  const datasetEntries = collectDatasetEntriesForImport(
    inspection,
    selectedDatasets,
  );
  const batchSize = Math.max(1, options.batchSize ?? 500);
  const progressLogPath = await createJsonLinesLog("import-progress");

  const client = new Client({ connectionString: dbUrl });
  let clientConnected = false;
  let planId: number | null = null;
  let planReused = false;
  let scanDurationMs = 0;
  let datasetScanDurationsMs: Partial<Record<ImportDatasetType, number>> = {};
  let sourceFingerprint = "";
  const lookupLoadDurationMs = 0;
  let checkpointBaselineRows = 0;
  let checkpointBaselineBatches = 0;

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

  let plan: PreparedImportPlan["plan"] | null = null;

  try {
    await client.connect();
    clientConnected = true;
    await ensureImportCheckpointSupport(client);
    await ensureImportQuarantineSupport(client);

    const preparedPlan = await prepareImportPlan({
      client,
      inputPath,
      validatedPath,
      batchSize,
      datasetEntries,
      targetDatabase,
      progressLogPath,
      ...(options.onProgress ? { onProgress: options.onProgress } : {}),
    });

    planId = preparedPlan.planId;
    planReused = preparedPlan.planReused;
    plan = preparedPlan.plan;
    scanDurationMs = preparedPlan.scanDurationMs;
    datasetScanDurationsMs = preparedPlan.datasetScanDurationsMs;
    sourceFingerprint = preparedPlan.sourceFingerprint;

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
    await ensureStagingSchemaSupport(
      client,
      plan.datasets.map((datasetPlan) => datasetPlan.dataset),
    );
    const checkpointTotals = await hydrateImportPlanWithCheckpoints(
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

    if (!planReused) {
      const resetTables = await resetStagingTablesForFreshPlan(
        client,
        plan.datasets.map((datasetPlan) => datasetPlan.dataset),
      );

      if (resetTables.length > 0) {
        await appendJsonLinesLog(progressLogPath, {
          kind: "staging_reset",
          tables: resetTables,
          timestamp: new Date().toISOString(),
        });
      }
    }

    if (planId !== null) {
      await updateImportPlanStatus(client, planId, "in_progress");
    }

    options.onProgress?.({
      kind: "start",
      inputPath: path.resolve(inputPath),
      validatedPath,
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
      validatedPath,
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

      await appendJsonLinesLog(progressLogPath, {
        kind: "dataset_completed",
        dataset: datasetPlan.dataset,
        datasetIndex: datasetIndex + 1,
        totalDatasets: plan.datasets.length,
        metrics: finalizeDatasetPerformance(datasetTracker),
        timestamp: new Date().toISOString(),
      });
    }

    if (planId !== null) {
      await updateImportPlanStatus(client, planId, "completed");
    }
  } catch (error) {
    await appendJsonLinesLog(progressLogPath, {
      kind: "import_failed",
      planId,
      sourceFingerprint,
      inputPath: path.resolve(inputPath),
      validatedPath,
      targetDatabase,
      error: error instanceof Error ? error.message : String(error),
      timestamp: new Date().toISOString(),
    }).catch(() => undefined);

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
    .filter((item): item is MutableDatasetPerformance => item !== undefined);
  const executionRowsCommitted = Math.max(
    0,
    counters.committedRows - checkpointBaselineRows,
  );
  const executionBatchesCommitted = Math.max(
    0,
    counters.committedBatches - checkpointBaselineBatches,
  );

  const performanceSummary: ImportPerformanceSummary =
    buildImportPerformanceSummary({
      planReused,
      totalDurationMs: performance.now() - overallStartedAt,
      scanDurationMs,
      executionDurationMs,
      lookupLoadDurationMs,
      executionRowsCommitted,
      executionBatchesCommitted,
      datasets: datasetPerformance,
    });

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

  const datasetSummaries = summarizeImportedDatasets(plan.datasets);

  return {
    inputPath: path.resolve(inputPath),
    validatedPath,
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
    warnings: buildImportWarnings(),
    progressLogPath,
  };
}
