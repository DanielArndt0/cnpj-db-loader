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
  ensureMaterializationCheckpointTable,
  resetMaterializationCheckpoints,
} from "./materialization-checkpoints.js";
import { materializeStagedDatasets } from "./materializer.js";
import {
  collectDatasetEntriesForImport,
  prepareImportPlan,
  resolveRequestedDatasets,
  type PreparedImportPlan,
} from "./planner.js";
import {
  updateImportPlanPhaseState,
  updateImportPlanStatus,
  readLatestImportPlanForValidatedPath,
} from "./plan-store.js";
import { ensureImportQuarantineSupport } from "./quarantine-writer.js";
import { detectImportSchemaCapabilities } from "./schema-capabilities.js";
import {
  ensureStagingSchemaSupport,
  resetStagingTablesForFreshPlan,
} from "./staging-schema.js";
import type { InspectSummary } from "../inspect.service.js";
import type {
  ImportDatasetPlan,
  ImportDatasetType,
  ImportOptions,
  ImportPerformanceSummary,
  ImportSummary,
} from "./types.js";

type DatasetTrackerMap = Map<ImportDatasetType, MutableDatasetPerformance>;

type SharedPipelineInput = {
  inputPath: string;
  validatedPath: string;
  inspection: InspectSummary;
  dbUrl: string;
  options?: ImportOptions;
  targetDatabase: string;
};

type PipelineCounters = {
  committedRows: number;
  committedBatches: number;
  completedFiles: number;
  secondaryCnaesRows: number;
  quarantinedRows: number;
  resumedFiles: number;
  skippedCompletedFiles: number;
};

type PreparedExecution = {
  client: Client;
  progressLogPath: string;
  planId: number | null;
  planReused: boolean;
  plan: PreparedImportPlan["plan"];
  datasetPerformanceTrackers: DatasetTrackerMap;
  scanDurationMs: number;
  sourceFingerprint: string;
  loadBatchSize: number;
  materializeBatchSize: number;
  counters: PipelineCounters;
  checkpointBaselineRows: number;
  checkpointBaselineBatches: number;
  schemaCapabilities: Awaited<
    ReturnType<typeof detectImportSchemaCapabilities>
  >;
};

function createCounters(): PipelineCounters {
  return {
    committedRows: 0,
    committedBatches: 0,
    completedFiles: 0,
    secondaryCnaesRows: 0,
    quarantinedRows: 0,
    resumedFiles: 0,
    skippedCompletedFiles: 0,
  };
}

function createDatasetTrackers(
  datasets: readonly ImportDatasetPlan[],
  scanDurations: Partial<Record<ImportDatasetType, number>>,
): DatasetTrackerMap {
  const trackers = new Map<ImportDatasetType, MutableDatasetPerformance>();

  for (const datasetPlan of datasets) {
    trackers.set(
      datasetPlan.dataset,
      createDatasetPerformanceTracker(
        datasetPlan,
        scanDurations[datasetPlan.dataset] ?? 0,
      ),
    );
  }

  return trackers;
}

function applyCheckpointMetricsToTrackers(
  datasets: readonly ImportDatasetPlan[],
  trackers: DatasetTrackerMap,
): void {
  for (const datasetPlan of datasets) {
    const tracker = trackers.get(datasetPlan.dataset);
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
}

function resolveLoadBatchSize(options: ImportOptions | undefined): number {
  return Math.max(1, options?.loadBatchSize ?? options?.batchSize ?? 500);
}

function resolveMaterializeBatchSize(
  options: ImportOptions | undefined,
): number {
  return Math.max(1, options?.materializeBatchSize ?? 50_000);
}

async function prepareExecutionForLoad(
  input: SharedPipelineInput,
): Promise<PreparedExecution> {
  const selectedDatasets = resolveRequestedDatasets(input.options?.dataset);
  const datasetEntries = collectDatasetEntriesForImport(
    input.inspection,
    selectedDatasets,
  );
  const loadBatchSize = resolveLoadBatchSize(input.options);
  const materializeBatchSize = resolveMaterializeBatchSize(input.options);
  const progressLogPath = await createJsonLinesLog("import-progress");
  const client = new Client({ connectionString: input.dbUrl });

  await client.connect();
  await ensureImportCheckpointSupport(client);
  await ensureMaterializationCheckpointTable(client);
  await ensureImportQuarantineSupport(client);

  const preparedPlan = await prepareImportPlan({
    client,
    inputPath: input.inputPath,
    validatedPath: input.validatedPath,
    batchSize: loadBatchSize,
    datasetEntries,
    targetDatabase: input.targetDatabase,
    progressLogPath,
    ...(input.options?.onProgress
      ? { onProgress: input.options.onProgress }
      : {}),
  });

  const datasetPerformanceTrackers = createDatasetTrackers(
    preparedPlan.plan.datasets,
    preparedPlan.datasetScanDurationsMs,
  );

  const schemaCapabilities = await detectImportSchemaCapabilities(client);
  await ensureStagingSchemaSupport(
    client,
    preparedPlan.plan.datasets.map((datasetPlan) => datasetPlan.dataset),
  );

  const checkpointTotals = await hydrateImportPlanWithCheckpoints(
    client,
    preparedPlan.plan.datasets,
    loadBatchSize,
  );

  const counters = createCounters();
  counters.committedRows = checkpointTotals.committedRows;
  counters.committedBatches = checkpointTotals.committedBatches;
  counters.completedFiles = checkpointTotals.completedFiles;
  counters.resumedFiles = checkpointTotals.resumedFiles;
  counters.skippedCompletedFiles = checkpointTotals.skippedCompletedFiles;

  applyCheckpointMetricsToTrackers(
    preparedPlan.plan.datasets,
    datasetPerformanceTrackers,
  );

  if (preparedPlan.planId !== null && !preparedPlan.planReused) {
    await resetMaterializationCheckpoints(client, preparedPlan.planId);
    await updateImportPlanPhaseState(client, {
      planId: preparedPlan.planId,
      loadStatus: "pending",
      materializationStatus: "pending",
      lastPhase: "planning",
      lastError: null,
    });
  }

  return {
    client,
    progressLogPath,
    planId: preparedPlan.planId,
    planReused: preparedPlan.planReused,
    plan: preparedPlan.plan,
    datasetPerformanceTrackers,
    scanDurationMs: preparedPlan.scanDurationMs,
    sourceFingerprint: preparedPlan.sourceFingerprint,
    loadBatchSize,
    materializeBatchSize,
    counters,
    checkpointBaselineRows: checkpointTotals.committedRows,
    checkpointBaselineBatches: checkpointTotals.committedBatches,
    schemaCapabilities,
  };
}

async function prepareExecutionForMaterialization(
  input: SharedPipelineInput,
): Promise<PreparedExecution> {
  const progressLogPath = await createJsonLinesLog("import-progress");
  const client = new Client({ connectionString: input.dbUrl });
  await client.connect();
  await ensureImportCheckpointSupport(client);
  await ensureMaterializationCheckpointTable(client);
  await ensureImportQuarantineSupport(client);

  const savedPlan = await readLatestImportPlanForValidatedPath(
    client,
    input.validatedPath,
    input.targetDatabase,
  );

  if (!savedPlan) {
    throw new ValidationError(
      'No saved import plan was found for this validated input path. Run "cnpj-db-loader import load" or "cnpj-db-loader import" first.',
    );
  }

  const requestedDatasets = resolveRequestedDatasets(input.options?.dataset);
  const requestedSet = new Set(requestedDatasets);
  const filteredDatasets = savedPlan.datasets.filter((datasetPlan) =>
    requestedSet.has(datasetPlan.dataset),
  );

  if (filteredDatasets.length === 0) {
    throw new ValidationError(
      "No datasets from the requested selection are available in the saved import plan.",
    );
  }

  const plan = {
    datasets: filteredDatasets,
    totalFiles: filteredDatasets.reduce(
      (sum, datasetPlan) => sum + datasetPlan.files.length,
      0,
    ),
    totalRows: filteredDatasets.reduce(
      (sum, datasetPlan) => sum + datasetPlan.totalRows,
      0,
    ),
    totalBatches: filteredDatasets.reduce(
      (sum, datasetPlan) => sum + datasetPlan.totalBatches,
      0,
    ),
  };

  const datasetPerformanceTrackers = createDatasetTrackers(plan.datasets, {});
  const schemaCapabilities = await detectImportSchemaCapabilities(client);
  await ensureStagingSchemaSupport(
    client,
    plan.datasets.map((datasetPlan) => datasetPlan.dataset),
  );

  const checkpointTotals = await hydrateImportPlanWithCheckpoints(
    client,
    plan.datasets,
    savedPlan.plan.batchSize,
  );

  const counters = createCounters();
  counters.committedRows = checkpointTotals.committedRows;
  counters.committedBatches = checkpointTotals.committedBatches;
  counters.completedFiles = checkpointTotals.completedFiles;
  counters.resumedFiles = checkpointTotals.resumedFiles;
  counters.skippedCompletedFiles = checkpointTotals.skippedCompletedFiles;
  applyCheckpointMetricsToTrackers(plan.datasets, datasetPerformanceTrackers);

  input.options?.onProgress?.({
    kind: "plan_ready",
    totalDatasets: plan.datasets.length,
    totalFiles: plan.totalFiles,
    batchSize: savedPlan.plan.batchSize,
    totalRows: plan.totalRows,
    totalBatches: plan.totalBatches,
    targetDatabase: input.targetDatabase,
    executionOrder: plan.datasets.map((item) => item.dataset),
    reused: true,
    planId: savedPlan.plan.id,
  });

  await appendJsonLinesLog(progressLogPath, {
    kind: "materialization_plan_reused",
    planId: savedPlan.plan.id,
    sourceFingerprint: savedPlan.plan.sourceFingerprint,
    inputPath: path.resolve(input.inputPath),
    validatedPath: input.validatedPath,
    targetDatabase: input.targetDatabase,
    totalDatasets: plan.datasets.length,
    totalFiles: plan.totalFiles,
    totalRows: plan.totalRows,
    totalBatches: plan.totalBatches,
    materializeBatchSize: resolveMaterializeBatchSize(input.options),
    executionOrder: plan.datasets.map((item) => item.dataset),
    timestamp: new Date().toISOString(),
  });

  return {
    client,
    progressLogPath,
    planId: savedPlan.plan.id,
    planReused: true,
    plan,
    datasetPerformanceTrackers,
    scanDurationMs: 0,
    sourceFingerprint: savedPlan.plan.sourceFingerprint,
    loadBatchSize: savedPlan.plan.batchSize,
    materializeBatchSize: resolveMaterializeBatchSize(input.options),
    counters,
    checkpointBaselineRows: checkpointTotals.committedRows,
    checkpointBaselineBatches: checkpointTotals.committedBatches,
    schemaCapabilities,
  };
}

async function runLoadStage(
  input: SharedPipelineInput,
  execution: PreparedExecution,
  stageLabel: "import" | "load",
): Promise<void> {
  if (!execution.planReused) {
    const resetTables = await resetStagingTablesForFreshPlan(
      execution.client,
      execution.plan.datasets.map((datasetPlan) => datasetPlan.dataset),
    );

    if (resetTables.length > 0) {
      await appendJsonLinesLog(execution.progressLogPath, {
        kind: "staging_reset",
        tables: resetTables,
        timestamp: new Date().toISOString(),
      });
    }
  }

  if (execution.planId !== null) {
    await updateImportPlanStatus(
      execution.client,
      execution.planId,
      "in_progress",
    );
    await updateImportPlanPhaseState(execution.client, {
      planId: execution.planId,
      loadStatus: "in_progress",
      lastPhase: "load",
      lastError: null,
    });
  }

  input.options?.onProgress?.({
    kind: "start",
    inputPath: path.resolve(input.inputPath),
    validatedPath: input.validatedPath,
    totalDatasets: execution.plan.datasets.length,
    totalFiles: execution.plan.totalFiles,
    targetDatabase: input.targetDatabase,
    totalRows: execution.plan.totalRows,
    totalBatches: execution.plan.totalBatches,
    committedRows: execution.counters.committedRows,
    committedBatches: execution.counters.committedBatches,
  });

  await appendJsonLinesLog(execution.progressLogPath, {
    kind: `${stageLabel}_started`,
    planId: execution.planId,
    sourceFingerprint: execution.sourceFingerprint,
    inputPath: path.resolve(input.inputPath),
    validatedPath: input.validatedPath,
    targetDatabase: input.targetDatabase,
    totalDatasets: execution.plan.datasets.length,
    totalFiles: execution.plan.totalFiles,
    totalRows: execution.plan.totalRows,
    totalBatches: execution.plan.totalBatches,
    loadBatchSize: execution.loadBatchSize,
    materializeBatchSize: execution.materializeBatchSize,
    resumedFiles: execution.counters.resumedFiles,
    skippedCompletedFiles: execution.counters.skippedCompletedFiles,
    committedRows: execution.counters.committedRows,
    committedBatches: execution.counters.committedBatches,
    timestamp: new Date().toISOString(),
  });

  let globalFileIndex = 0;
  for (const [datasetIndex, datasetPlan] of execution.plan.datasets.entries()) {
    const datasetTracker = execution.datasetPerformanceTrackers.get(
      datasetPlan.dataset,
    );
    if (!datasetTracker) {
      continue;
    }

    await appendJsonLinesLog(execution.progressLogPath, {
      kind: "dataset_started",
      dataset: datasetPlan.dataset,
      datasetIndex: datasetIndex + 1,
      totalDatasets: execution.plan.datasets.length,
      plannedRows: datasetPlan.totalRows,
      plannedBatches: datasetPlan.totalBatches,
      resumedFiles: datasetTracker.resumedFiles,
      skippedCompletedFiles: datasetTracker.skippedCompletedFiles,
      timestamp: new Date().toISOString(),
    });

    for (const filePlan of datasetPlan.files) {
      globalFileIndex += 1;

      const rowsBefore = execution.counters.committedRows;
      const batchesBefore = execution.counters.committedBatches;
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
        execution.client,
        filePlan,
        execution.schemaCapabilities,
        execution.counters,
        {
          datasetIndex: datasetIndex + 1,
          totalDatasets: execution.plan.datasets.length,
          totalFiles: execution.plan.totalFiles,
          totalBatches: execution.plan.totalBatches,
          fileIndex: globalFileIndex,
          onProgress: input.options?.onProgress,
          progressLogPath: execution.progressLogPath,
          batchSize: execution.loadBatchSize,
          verboseProgress: input.options?.verboseProgress ?? false,
          performance: datasetTracker,
        },
      );

      datasetTracker.importDurationMs += performance.now() - fileStartedAt;
      datasetTracker.importedRows +=
        execution.counters.committedRows - rowsBefore;
      datasetTracker.committedBatches +=
        execution.counters.committedBatches - batchesBefore;

      await appendJsonLinesLog(execution.progressLogPath, {
        kind: "file_metrics",
        dataset: datasetPlan.dataset,
        datasetIndex: datasetIndex + 1,
        filePath: filePlan.absolutePath,
        fileDisplayPath: filePlan.displayPath,
        fileIndex: globalFileIndex,
        importedRows: execution.counters.committedRows - rowsBefore,
        committedBatches: execution.counters.committedBatches - batchesBefore,
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
          datasetTracker.retriedBatches - filePerformanceBefore.retriedBatches,
        quarantinedRows:
          datasetTracker.quarantinedRows -
          filePerformanceBefore.quarantinedRows,
        durationMs: performance.now() - fileStartedAt,
        timestamp: new Date().toISOString(),
      });
    }

    await appendJsonLinesLog(execution.progressLogPath, {
      kind: "dataset_completed",
      dataset: datasetPlan.dataset,
      datasetIndex: datasetIndex + 1,
      totalDatasets: execution.plan.datasets.length,
      metrics: finalizeDatasetPerformance(datasetTracker),
      timestamp: new Date().toISOString(),
    });
  }

  if (execution.planId !== null) {
    await updateImportPlanPhaseState(execution.client, {
      planId: execution.planId,
      loadStatus: "completed",
      lastPhase: "load_completed",
      lastError: null,
    });
  }
}

async function runMaterializationStage(
  execution: PreparedExecution,
  onProgress: ImportOptions["onProgress"],
): Promise<void> {
  if (execution.planId === null) {
    throw new ValidationError(
      "The materialization stage requires a persisted import plan.",
    );
  }

  await updateImportPlanStatus(
    execution.client,
    execution.planId,
    "in_progress",
  );
  await updateImportPlanPhaseState(execution.client, {
    planId: execution.planId,
    materializationStatus: "in_progress",
    lastPhase: "materialization",
    lastError: null,
  });

  const materializationSummary = await materializeStagedDatasets({
    client: execution.client,
    planId: execution.planId,
    datasets: execution.plan.datasets.map((datasetPlan) => datasetPlan.dataset),
    schemaCapabilities: execution.schemaCapabilities,
    progressLogPath: execution.progressLogPath,
    datasetPerformanceTrackers: execution.datasetPerformanceTrackers,
    expectedRowsByDataset: new Map(
      execution.plan.datasets.map((datasetPlan) => [
        datasetPlan.dataset,
        datasetPlan.totalRows,
      ]),
    ),
    onProgress,
    completedFiles: execution.counters.completedFiles,
    totalFiles: execution.plan.totalFiles,
    processedRows: execution.counters.committedRows,
    totalRows: execution.plan.totalRows,
    committedBatches: execution.counters.committedBatches,
    totalBatches: execution.plan.totalBatches,
    chunkSize: execution.materializeBatchSize,
  });

  execution.counters.secondaryCnaesRows =
    materializationSummary.secondaryCnaesRows;

  await updateImportPlanPhaseState(execution.client, {
    planId: execution.planId,
    materializationStatus: "completed",
    lastPhase: "materialization_completed",
    lastError: null,
  });
}

async function buildSummary(
  input: SharedPipelineInput,
  execution: PreparedExecution,
  overallStartedAt: number,
  executionMode: "full" | "load" | "materialize",
): Promise<ImportSummary> {
  const executionDurationMs =
    performance.now() - overallStartedAt - execution.scanDurationMs;
  const datasetPerformance = execution.plan.datasets
    .map((datasetPlan) =>
      execution.datasetPerformanceTrackers.get(datasetPlan.dataset),
    )
    .filter((item): item is MutableDatasetPerformance => item !== undefined);
  const executionRowsCommitted = Math.max(
    0,
    execution.counters.committedRows - execution.checkpointBaselineRows,
  );
  const executionBatchesCommitted = Math.max(
    0,
    execution.counters.committedBatches - execution.checkpointBaselineBatches,
  );

  const performanceSummary: ImportPerformanceSummary =
    buildImportPerformanceSummary({
      planReused: execution.planReused,
      totalDurationMs: performance.now() - overallStartedAt,
      scanDurationMs: execution.scanDurationMs,
      executionDurationMs,
      lookupLoadDurationMs: 0,
      executionRowsCommitted,
      executionBatchesCommitted,
      datasets: datasetPerformance,
    });

  input.options?.onProgress?.({
    kind: "finish",
    totalDatasets: execution.plan.datasets.length,
    totalFiles: execution.plan.totalFiles,
    completedFiles: execution.counters.completedFiles,
    processedRows: execution.counters.committedRows,
    totalRows: execution.plan.totalRows,
    committedBatches: execution.counters.committedBatches,
    totalBatches: execution.plan.totalBatches,
    secondaryCnaesRows: execution.counters.secondaryCnaesRows,
    quarantinedRows: execution.counters.quarantinedRows,
  });

  await appendJsonLinesLog(execution.progressLogPath, {
    kind: "import_finished",
    totalDatasets: execution.plan.datasets.length,
    totalFiles: execution.plan.totalFiles,
    completedFiles: execution.counters.completedFiles,
    processedRows: execution.counters.committedRows,
    totalRows: execution.plan.totalRows,
    committedBatches: execution.counters.committedBatches,
    totalBatches: execution.plan.totalBatches,
    secondaryCnaesRows: execution.counters.secondaryCnaesRows,
    quarantinedRows: execution.counters.quarantinedRows,
    resumedFiles: execution.counters.resumedFiles,
    skippedCompletedFiles: execution.counters.skippedCompletedFiles,
    performance: performanceSummary,
    timestamp: new Date().toISOString(),
  });

  const datasetSummaries = summarizeImportedDatasets(execution.plan.datasets);

  return {
    executionMode,
    inputPath: path.resolve(input.inputPath),
    validatedPath: input.validatedPath,
    targetDatabase: input.targetDatabase,
    importPlanId: execution.planId,
    reusedImportPlan: execution.planReused,
    importedDatasets: datasetSummaries.map((item) => item.dataset),
    importedFiles: execution.counters.completedFiles,
    processedRows: execution.counters.committedRows,
    plannedRows: execution.plan.totalRows,
    committedBatches: execution.counters.committedBatches,
    plannedBatches: execution.plan.totalBatches,
    secondaryCnaesRows: execution.counters.secondaryCnaesRows,
    quarantinedRows: execution.counters.quarantinedRows,
    resumedFiles: execution.counters.resumedFiles,
    skippedCompletedFiles: execution.counters.skippedCompletedFiles,
    datasetSummaries,
    performance: performanceSummary,
    warnings: buildImportWarnings(),
    progressLogPath: execution.progressLogPath,
  };
}

async function finalizePlanCompletion(
  execution: PreparedExecution,
): Promise<void> {
  if (execution.planId === null) {
    return;
  }

  await updateImportPlanStatus(execution.client, execution.planId, "completed");
  await updateImportPlanPhaseState(execution.client, {
    planId: execution.planId,
    loadStatus: "completed",
    materializationStatus: "completed",
    lastPhase: "completed",
    lastError: null,
  });
}

async function closeClient(client: Client): Promise<void> {
  await client.end().catch(() => undefined);
}

export async function runImportPipeline(
  input: SharedPipelineInput,
): Promise<ImportSummary> {
  const overallStartedAt = performance.now();
  let execution: PreparedExecution | null = null;

  try {
    execution = await prepareExecutionForLoad(input);
    await runLoadStage(input, execution, "import");
    await runMaterializationStage(execution, input.options?.onProgress);
    await finalizePlanCompletion(execution);
    return await buildSummary(input, execution, overallStartedAt, "full");
  } catch (error) {
    if (execution && execution.planId !== null) {
      await updateImportPlanStatus(
        execution.client,
        execution.planId,
        "failed",
      ).catch(() => undefined);
      await updateImportPlanPhaseState(execution.client, {
        planId: execution.planId,
        lastError: error instanceof Error ? error.message : String(error),
      }).catch(() => undefined);
    }

    if (execution) {
      await appendJsonLinesLog(execution.progressLogPath, {
        kind: "import_failed",
        planId: execution.planId,
        sourceFingerprint: execution.sourceFingerprint,
        inputPath: path.resolve(input.inputPath),
        validatedPath: input.validatedPath,
        targetDatabase: input.targetDatabase,
        error: error instanceof Error ? error.message : String(error),
        timestamp: new Date().toISOString(),
      }).catch(() => undefined);
    }
    throw error;
  } finally {
    if (execution) {
      await closeClient(execution.client);
    }
  }
}

export async function runImportLoadPipeline(
  input: SharedPipelineInput,
): Promise<ImportSummary> {
  const overallStartedAt = performance.now();
  let execution: PreparedExecution | null = null;

  try {
    execution = await prepareExecutionForLoad(input);
    await runLoadStage(input, execution, "load");
    if (execution.planId !== null) {
      await updateImportPlanStatus(
        execution.client,
        execution.planId,
        "in_progress",
      );
    }
    return await buildSummary(input, execution, overallStartedAt, "load");
  } catch (error) {
    if (execution && execution.planId !== null) {
      await updateImportPlanStatus(
        execution.client,
        execution.planId,
        "failed",
      ).catch(() => undefined);
      await updateImportPlanPhaseState(execution.client, {
        planId: execution.planId,
        loadStatus: "failed",
        lastPhase: "load_failed",
        lastError: error instanceof Error ? error.message : String(error),
      }).catch(() => undefined);
    }
    throw error;
  } finally {
    if (execution) {
      await closeClient(execution.client);
    }
  }
}

export async function runImportMaterializationPipeline(
  input: SharedPipelineInput,
): Promise<ImportSummary> {
  const overallStartedAt = performance.now();
  let execution: PreparedExecution | null = null;

  try {
    execution = await prepareExecutionForMaterialization(input);
    await runMaterializationStage(execution, input.options?.onProgress);
    await finalizePlanCompletion(execution);
    return await buildSummary(
      input,
      execution,
      overallStartedAt,
      "materialize",
    );
  } catch (error) {
    if (execution && execution.planId !== null) {
      await updateImportPlanStatus(
        execution.client,
        execution.planId,
        "failed",
      ).catch(() => undefined);
      await updateImportPlanPhaseState(execution.client, {
        planId: execution.planId,
        materializationStatus: "failed",
        lastPhase: "materialization_failed",
        lastError: error instanceof Error ? error.message : String(error),
      }).catch(() => undefined);
    }
    throw error;
  } finally {
    if (execution) {
      await closeClient(execution.client);
    }
  }
}
