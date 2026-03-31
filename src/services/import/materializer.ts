import { performance } from "node:perf_hooks";

import type { Client } from "pg";

import { ValidationError } from "../../core/errors/index.js";
import { appendJsonLinesLog } from "../logging.service.js";
import {
  readMaterializationCheckpoint,
  writeMaterializationCheckpoint,
  type MaterializationCheckpointRecord,
} from "./materialization-checkpoints.js";
import {
  buildMaterializationChunkQuery,
  isMaterializationDataset,
  type MaterializationDataset,
} from "./materialization-sql.js";
import { reconcileMaterializationLookups } from "./materialization-lookups.js";
import { type MutableDatasetPerformance } from "./finalizer.js";
import { getFinalTargetTableName } from "./targets.js";
import type {
  ImportDatasetType,
  ImportProgressListener,
  ImportSchemaCapabilities,
} from "./types.js";

const MATERIALIZATION_ORDER: readonly MaterializationDataset[] = [
  "companies",
  "establishments",
  "simples_options",
  "partners",
];

const DEFAULT_MATERIALIZATION_CHUNK_SIZE = 50_000;
const MATERIALIZATION_HEARTBEAT_INTERVAL_MS = 15_000;
const EXACT_TARGET_COUNT_DATASETS: ReadonlySet<MaterializationDataset> =
  new Set(["companies", "establishments", "simples_options"]);

const STAGING_TABLE_BY_DATASET: Record<MaterializationDataset, string> = {
  companies: "staging_companies",
  establishments: "staging_establishments",
  simples_options: "staging_simples_options",
  partners: "staging_partners",
};

type ChunkRow = {
  max_staging_id: string;
  source_rows: string;
  affected_rows: string;
};

type CountRow = {
  total_count: string;
};

type MaxRow = {
  max_staging_id: string;
};

type StagingStateRow = {
  total_count: string;
  max_staging_id: string;
};

type QuarantineCountRow = {
  quarantined_rows: string;
};

type ValidatedCheckpoint = {
  checkpoint: MaterializationCheckpointRecord;
  adjusted: boolean;
  reason: string | null;
  stagingMaxId: number;
  targetRows: number | null;
};

export type MaterializationSummary = {
  datasets: Array<{
    dataset: MaterializationDataset;
    targetTable: string;
    affectedRows: number;
    sourceRows: number;
    chunksCompleted: number;
    durationMs: number;
  }>;
};

function resolveMaterializationDatasets(
  datasets: readonly ImportDatasetType[],
): MaterializationDataset[] {
  const requested = new Set(
    datasets.filter((dataset): dataset is MaterializationDataset =>
      isMaterializationDataset(dataset),
    ),
  );

  return MATERIALIZATION_ORDER.filter((dataset) => requested.has(dataset));
}

function emitMaterializationProgress(
  listener: ImportProgressListener | undefined,
  input: {
    datasets: readonly MaterializationDataset[];
    dataset: MaterializationDataset;
    datasetIndex: number;
    targetTable: string;
    stepLabel: string;
    completedDatasets: number;
    completedFiles: number;
    totalFiles: number;
    processedRows: number;
    totalRows: number;
    committedBatches: number;
    totalBatches: number;
    elapsedMs?: number;
  },
): void {
  listener?.({
    kind: "materialization_progress",
    dataset: input.dataset,
    datasetIndex: input.datasetIndex,
    totalDatasets: input.datasets.length,
    completedDatasets: input.completedDatasets,
    targetTable: input.targetTable,
    stepLabel: input.stepLabel,
    completedFiles: input.completedFiles,
    totalFiles: input.totalFiles,
    processedRows: input.processedRows,
    totalRows: input.totalRows,
    committedBatches: input.committedBatches,
    totalBatches: input.totalBatches,
    ...(input.elapsedMs === undefined ? {} : { elapsedMs: input.elapsedMs }),
  });
}

async function executeChunkQuery(
  client: Client,
  text: string,
  values: readonly unknown[],
): Promise<{ maxStagingId: number; sourceRows: number; affectedRows: number }> {
  const result = await client.query<ChunkRow>(text, [...values]);
  const row = result.rows[0];
  return {
    maxStagingId: row ? Number.parseInt(row.max_staging_id, 10) : 0,
    sourceRows: row ? Number.parseInt(row.source_rows, 10) : 0,
    affectedRows: row ? Number.parseInt(row.affected_rows, 10) : 0,
  };
}

async function readStagingMaxId(
  client: Client,
  stagingTable: string,
): Promise<number> {
  const result = await client.query<MaxRow>(
    `select coalesce(max(staging_id), 0)::bigint as max_staging_id from ${stagingTable}`,
  );

  return Number.parseInt(result.rows[0]?.max_staging_id ?? "0", 10);
}

async function readStagingState(
  client: Client,
  stagingTable: string,
): Promise<{ rowCount: number; maxStagingId: number }> {
  const result = await client.query<StagingStateRow>(
    `select count(*)::bigint as total_count, coalesce(max(staging_id), 0)::bigint as max_staging_id from ${stagingTable}`,
  );

  return {
    rowCount: Number.parseInt(result.rows[0]?.total_count ?? "0", 10),
    maxStagingId: Number.parseInt(result.rows[0]?.max_staging_id ?? "0", 10),
  };
}

async function readQuarantinedRowCountForPlan(input: {
  client: Client;
  planId: number;
  dataset: MaterializationDataset;
}): Promise<number> {
  const result = await input.client.query<QuarantineCountRow>(
    `select count(*)::bigint as quarantined_rows
       from (
         select distinct q.file_path, q.row_number
           from import_quarantine q
           inner join import_plan_files pf
             on pf.plan_id = $1
            and pf.dataset = q.dataset
            and pf.file_path = q.file_path
          where q.dataset = $2
            and q.row_number is not null
       ) quarantined`,
    [input.planId, input.dataset],
  );

  return Number.parseInt(result.rows[0]?.quarantined_rows ?? "0", 10);
}

function buildEmptyStagingMessage(
  stagingTable: string,
  dataset: MaterializationDataset,
  expectedRows: number | undefined,
): string {
  if (typeof expectedRows === "number" && expectedRows > 0) {
    return `The staging table ${stagingTable} is empty, but ${expectedRows} row(s) are expected for ${dataset} based on the saved load checkpoints. Run "cnpj-db-loader import load" again or clear stale checkpoint data before materializing.`;
  }

  return `The staging table ${stagingTable} is empty for ${dataset}. Load the dataset into staging before running materialization.`;
}

function buildStagingMismatchMessage(
  stagingTable: string,
  dataset: MaterializationDataset,
  expectedRows: number,
  actualRows: number,
): string {
  return `The staging table ${stagingTable} currently contains ${actualRows} row(s), but ${expectedRows} row(s) are expected for ${dataset} based on the saved load checkpoints. The saved staging state no longer matches the persisted load progress. Reload staging or clear the stale checkpoint data before retrying materialization.`;
}

async function validateStagingDatasetState(input: {
  client: Client;
  planId: number;
  dataset: MaterializationDataset;
  datasetIndex: number;
  totalDatasets: number;
  expectedRows: number | undefined;
  progressLogPath: string;
}): Promise<{ rowCount: number; maxStagingId: number }> {
  const stagingTable = STAGING_TABLE_BY_DATASET[input.dataset];
  const state = await readStagingState(input.client, stagingTable);
  const quarantinedRows = await readQuarantinedRowCountForPlan({
    client: input.client,
    planId: input.planId,
    dataset: input.dataset,
  });
  const effectiveExpectedRows =
    typeof input.expectedRows === "number"
      ? Math.max(0, input.expectedRows - quarantinedRows)
      : undefined;

  await appendJsonLinesLog(input.progressLogPath, {
    kind: "materialization_staging_validation_completed",
    dataset: input.dataset,
    datasetIndex: input.datasetIndex,
    totalDatasets: input.totalDatasets,
    stagingTable,
    expectedRows: input.expectedRows ?? null,
    quarantinedRows,
    effectiveExpectedRows: effectiveExpectedRows ?? null,
    actualRows: state.rowCount,
    maxStagingId: state.maxStagingId,
    timestamp: new Date().toISOString(),
  });

  if (state.rowCount === 0) {
    throw new ValidationError(
      buildEmptyStagingMessage(
        stagingTable,
        input.dataset,
        effectiveExpectedRows ?? input.expectedRows,
      ),
    );
  }

  if (
    typeof effectiveExpectedRows === "number" &&
    effectiveExpectedRows > 0 &&
    state.rowCount !== effectiveExpectedRows
  ) {
    throw new ValidationError(
      buildStagingMismatchMessage(
        stagingTable,
        input.dataset,
        effectiveExpectedRows,
        state.rowCount,
      ),
    );
  }

  return state;
}

async function readRowCount(
  client: Client,
  tableName: string,
): Promise<number> {
  const result = await client.query<CountRow>(
    `select count(*)::bigint as total_count from ${tableName}`,
  );

  return Number.parseInt(result.rows[0]?.total_count ?? "0", 10);
}

async function persistCheckpoint(
  client: Client,
  checkpoint: MaterializationCheckpointRecord,
): Promise<void> {
  await writeMaterializationCheckpoint(client, checkpoint);
}

function resetCheckpointForReplay(
  checkpoint: MaterializationCheckpointRecord,
): MaterializationCheckpointRecord {
  return {
    ...checkpoint,
    status: "pending",
    rowsMaterialized: 0,
    lastStagingId: 0,
    chunksCompleted: 0,
    lastError: null,
    startedAt: null,
    completedAt: null,
  };
}

async function validateDatasetCheckpoint(input: {
  client: Client;
  planId: number;
  dataset: MaterializationDataset;
  targetTable: string;
  expectedRows?: number;
  expectedStagedRows?: number;
}): Promise<ValidatedCheckpoint> {
  const stagingTable = STAGING_TABLE_BY_DATASET[input.dataset];
  let checkpoint = await readMaterializationCheckpoint(
    input.client,
    input.planId,
    input.dataset,
    input.targetTable,
  );
  const stagingMaxId = await readStagingMaxId(input.client, stagingTable);

  let adjusted = false;
  let reason: string | null = null;
  let targetRows: number | null = null;

  if (checkpoint.lastStagingId > stagingMaxId && stagingMaxId > 0) {
    checkpoint = resetCheckpointForReplay(checkpoint);
    adjusted = true;
    reason =
      "Checkpoint references a staging range beyond the current staging table. The dataset will be rematerialized from the beginning.";
  } else if (
    checkpoint.status === "completed" &&
    checkpoint.lastStagingId < stagingMaxId
  ) {
    checkpoint = {
      ...checkpoint,
      status: "in_progress",
      completedAt: null,
      lastError: null,
    };
    adjusted = true;
    reason =
      "Checkpoint was marked as completed before the current staging tail. The dataset will resume materialization from the saved staging cursor.";
  }

  if (
    EXACT_TARGET_COUNT_DATASETS.has(input.dataset) &&
    input.expectedRows !== undefined
  ) {
    targetRows = await readRowCount(input.client, input.targetTable);

    if (
      (checkpoint.status === "completed" ||
        checkpoint.lastStagingId >= stagingMaxId) &&
      targetRows < input.expectedRows
    ) {
      checkpoint = resetCheckpointForReplay(checkpoint);
      adjusted = true;
      reason = `The target table currently has ${targetRows} row(s), but ${input.expectedRows} row(s) are expected for ${input.dataset}. The dataset will be rematerialized from the beginning.`;
    }
  }

  if (adjusted) {
    await persistCheckpoint(input.client, checkpoint);
  }

  return {
    checkpoint,
    adjusted,
    reason,
    stagingMaxId,
    targetRows,
  };
}

async function materializeDatasetByChunks(input: {
  client: Client;
  planId: number;
  dataset: MaterializationDataset;
  datasetIndex: number;
  datasets: readonly MaterializationDataset[];
  targetTable: string;
  chunkSize: number;
  expectedRows?: number;
  expectedStagedRows?: number;
  schemaCapabilities: ImportSchemaCapabilities;
  progressLogPath: string;
  onProgress?: ImportProgressListener | undefined;
  completedFiles: number;
  totalFiles: number;
  processedRows: number;
  totalRows: number;
  committedBatches: number;
  totalBatches: number;
  completedDatasets: number;
}): Promise<{
  affectedRows: number;
  sourceRows: number;
  chunksCompleted: number;
  durationMs: number;
}> {
  const startedAt = performance.now();
  emitMaterializationProgress(input.onProgress, {
    datasets: input.datasets,
    dataset: input.dataset,
    datasetIndex: input.datasetIndex,
    targetTable: input.targetTable,
    stepLabel: "Validating staging state",
    completedDatasets: input.completedDatasets,
    completedFiles: input.completedFiles,
    totalFiles: input.totalFiles,
    processedRows: input.processedRows,
    totalRows: input.totalRows,
    committedBatches: input.committedBatches,
    totalBatches: input.totalBatches,
  });

  await appendJsonLinesLog(input.progressLogPath, {
    kind: "materialization_staging_validation_started",
    dataset: input.dataset,
    datasetIndex: input.datasetIndex,
    totalDatasets: input.datasets.length,
    targetTable: input.targetTable,
    expectedRows: input.expectedStagedRows ?? null,
    timestamp: new Date().toISOString(),
  });

  await validateStagingDatasetState({
    client: input.client,
    planId: input.planId,
    dataset: input.dataset,
    datasetIndex: input.datasetIndex,
    totalDatasets: input.datasets.length,
    expectedRows: input.expectedStagedRows,
    progressLogPath: input.progressLogPath,
  });

  const validated = await validateDatasetCheckpoint({
    client: input.client,
    planId: input.planId,
    dataset: input.dataset,
    targetTable: input.targetTable,
    ...(input.expectedRows === undefined
      ? {}
      : { expectedRows: input.expectedRows }),
  });
  let checkpoint = validated.checkpoint;

  if (validated.adjusted && validated.reason) {
    emitMaterializationProgress(input.onProgress, {
      datasets: input.datasets,
      dataset: input.dataset,
      datasetIndex: input.datasetIndex,
      targetTable: input.targetTable,
      stepLabel: "Checkpoint reconciled — resuming safely",
      completedDatasets: input.completedDatasets,
      completedFiles: input.completedFiles,
      totalFiles: input.totalFiles,
      processedRows: input.processedRows,
      totalRows: input.totalRows,
      committedBatches: input.committedBatches,
      totalBatches: input.totalBatches,
    });

    await appendJsonLinesLog(input.progressLogPath, {
      kind: "materialization_checkpoint_reconciled",
      dataset: input.dataset,
      datasetIndex: input.datasetIndex,
      totalDatasets: input.datasets.length,
      targetTable: input.targetTable,
      reason: validated.reason,
      stagingMaxId: validated.stagingMaxId,
      targetRows: validated.targetRows,
      expectedRows: input.expectedRows ?? null,
      timestamp: new Date().toISOString(),
    });
  }

  if (checkpoint.status === "completed") {
    emitMaterializationProgress(input.onProgress, {
      datasets: input.datasets,
      dataset: input.dataset,
      datasetIndex: input.datasetIndex,
      targetTable: input.targetTable,
      stepLabel: `Checkpoint complete — verified ${checkpoint.chunksCompleted} chunk(s)`,
      completedDatasets: input.completedDatasets,
      completedFiles: input.completedFiles,
      totalFiles: input.totalFiles,
      processedRows: input.processedRows,
      totalRows: input.totalRows,
      committedBatches: input.committedBatches,
      totalBatches: input.totalBatches,
    });

    await appendJsonLinesLog(input.progressLogPath, {
      kind: "materialization_dataset_skipped",
      dataset: input.dataset,
      datasetIndex: input.datasetIndex,
      totalDatasets: input.datasets.length,
      targetTable: input.targetTable,
      rowsMaterialized: checkpoint.rowsMaterialized,
      chunksCompleted: checkpoint.chunksCompleted,
      verified: true,
      timestamp: new Date().toISOString(),
    });

    return {
      affectedRows: checkpoint.rowsMaterialized,
      sourceRows: checkpoint.rowsMaterialized,
      chunksCompleted: checkpoint.chunksCompleted,
      durationMs: performance.now() - startedAt,
    };
  }

  checkpoint = {
    ...checkpoint,
    status: "in_progress",
    targetTable: input.targetTable,
    startedAt: checkpoint.startedAt ?? new Date(),
    completedAt: null,
    lastError: null,
  };
  await persistCheckpoint(input.client, checkpoint);

  let affectedRows = checkpoint.rowsMaterialized;
  let sourceRows = checkpoint.rowsMaterialized;
  let chunksCompleted = checkpoint.chunksCompleted;
  const targetHasRows =
    checkpoint.rowsMaterialized > 0 ||
    (await readRowCount(input.client, input.targetTable)) > 0;
  const useConflictClause = targetHasRows;

  const heartbeatTimer = setInterval(() => {
    const elapsedMs = performance.now() - startedAt;
    emitMaterializationProgress(input.onProgress, {
      datasets: input.datasets,
      dataset: input.dataset,
      datasetIndex: input.datasetIndex,
      targetTable: input.targetTable,
      stepLabel: `Chunk ${chunksCompleted + 1} | last staging id ${checkpoint.lastStagingId}`,
      completedDatasets: input.completedDatasets,
      completedFiles: input.completedFiles,
      totalFiles: input.totalFiles,
      processedRows: input.processedRows,
      totalRows: input.totalRows,
      committedBatches: input.committedBatches,
      totalBatches: input.totalBatches,
      elapsedMs,
    });

    void appendJsonLinesLog(input.progressLogPath, {
      kind: "materialization_dataset_heartbeat",
      dataset: input.dataset,
      datasetIndex: input.datasetIndex,
      totalDatasets: input.datasets.length,
      targetTable: input.targetTable,
      elapsedMs,
      chunksCompleted,
      lastStagingId: checkpoint.lastStagingId,
      rowsMaterialized: affectedRows,
      timestamp: new Date().toISOString(),
    }).catch(() => undefined);
  }, MATERIALIZATION_HEARTBEAT_INTERVAL_MS);

  try {
    while (true) {
      emitMaterializationProgress(input.onProgress, {
        datasets: input.datasets,
        dataset: input.dataset,
        datasetIndex: input.datasetIndex,
        targetTable: input.targetTable,
        stepLabel: `Chunk ${chunksCompleted + 1} | ${affectedRows} row(s) materialized`,
        completedDatasets: input.completedDatasets,
        completedFiles: input.completedFiles,
        totalFiles: input.totalFiles,
        processedRows: input.processedRows,
        totalRows: input.totalRows,
        committedBatches: input.committedBatches,
        totalBatches: input.totalBatches,
      });

      const query = buildMaterializationChunkQuery({
        dataset: input.dataset,
        schemaCapabilities: input.schemaCapabilities,
        lastStagingId: checkpoint.lastStagingId,
        chunkSize: input.chunkSize,
        useConflictClause,
      });
      const chunkStartedAt = performance.now();

      let chunkResult: {
        maxStagingId: number;
        sourceRows: number;
        affectedRows: number;
      };

      await input.client.query("begin");
      try {
        chunkResult = await executeChunkQuery(
          input.client,
          query.text,
          query.values,
        );

        if (chunkResult.sourceRows === 0) {
          checkpoint = {
            ...checkpoint,
            status: "completed",
            completedAt: new Date(),
            lastError: null,
          };
          await persistCheckpoint(input.client, checkpoint);
          await input.client.query("commit");
          break;
        }

        checkpoint = {
          ...checkpoint,
          lastStagingId: chunkResult.maxStagingId,
          rowsMaterialized:
            checkpoint.rowsMaterialized + chunkResult.affectedRows,
          chunksCompleted: checkpoint.chunksCompleted + 1,
          lastError: null,
        };
        await persistCheckpoint(input.client, checkpoint);
        await input.client.query("commit");
      } catch (error) {
        await input.client.query("rollback");
        const message = error instanceof Error ? error.message : String(error);
        checkpoint = {
          ...checkpoint,
          status: "failed",
          lastError: message,
        };
        await persistCheckpoint(input.client, checkpoint);
        throw error;
      }

      affectedRows = checkpoint.rowsMaterialized;
      sourceRows += chunkResult.sourceRows;
      chunksCompleted = checkpoint.chunksCompleted;

      await appendJsonLinesLog(input.progressLogPath, {
        kind: "materialization_chunk_completed",
        dataset: input.dataset,
        datasetIndex: input.datasetIndex,
        totalDatasets: input.datasets.length,
        targetTable: input.targetTable,
        chunkNumber: chunksCompleted,
        chunkSize: input.chunkSize,
        sourceRows: chunkResult.sourceRows,
        affectedRows: chunkResult.affectedRows,
        lastStagingId: checkpoint.lastStagingId,
        totalRowsMaterialized: affectedRows,
        durationMs: performance.now() - chunkStartedAt,
        timestamp: new Date().toISOString(),
      });
    }
  } finally {
    clearInterval(heartbeatTimer);
  }

  return {
    affectedRows,
    sourceRows,
    chunksCompleted,
    durationMs: performance.now() - startedAt,
  };
}

export async function materializeStagedDatasets(input: {
  client: Client;
  planId: number;
  datasets: readonly ImportDatasetType[];
  schemaCapabilities: ImportSchemaCapabilities;
  progressLogPath: string;
  datasetPerformanceTrackers: Map<ImportDatasetType, MutableDatasetPerformance>;
  expectedRowsByDataset: ReadonlyMap<ImportDatasetType, number>;
  expectedStagedRowsByDataset: ReadonlyMap<ImportDatasetType, number>;
  onProgress?: ImportProgressListener | undefined;
  completedFiles: number;
  totalFiles: number;
  processedRows: number;
  totalRows: number;
  committedBatches: number;
  totalBatches: number;
  chunkSize?: number;
}): Promise<MaterializationSummary> {
  const materializationDatasets = resolveMaterializationDatasets(
    input.datasets,
  );
  const chunkSize = Math.max(
    1,
    input.chunkSize ?? DEFAULT_MATERIALIZATION_CHUNK_SIZE,
  );

  const summary: MaterializationSummary = {
    datasets: [],
  };

  if (materializationDatasets.length === 0) {
    return summary;
  }

  input.onProgress?.({
    kind: "materialization_start",
    totalDatasets: materializationDatasets.length,
    datasets: [...materializationDatasets],
    completedFiles: input.completedFiles,
    totalFiles: input.totalFiles,
    processedRows: input.processedRows,
    totalRows: input.totalRows,
    committedBatches: input.committedBatches,
    totalBatches: input.totalBatches,
  });

  await appendJsonLinesLog(input.progressLogPath, {
    kind: "materialization_started",
    datasets: materializationDatasets,
    chunkSize,
    timestamp: new Date().toISOString(),
  });

  for (const [datasetIndex, dataset] of materializationDatasets.entries()) {
    const targetTable = getFinalTargetTableName(dataset);
    const datasetPosition = datasetIndex + 1;

    await appendJsonLinesLog(input.progressLogPath, {
      kind: "materialization_dataset_started",
      dataset,
      datasetIndex: datasetPosition,
      totalDatasets: materializationDatasets.length,
      targetTable,
      chunkSize,
      timestamp: new Date().toISOString(),
    });

    try {
      emitMaterializationProgress(input.onProgress, {
        datasets: materializationDatasets,
        dataset,
        datasetIndex: datasetPosition,
        targetTable,
        stepLabel: "Reconciling lookup dependencies",
        completedDatasets: summary.datasets.length,
        completedFiles: input.completedFiles,
        totalFiles: input.totalFiles,
        processedRows: input.processedRows,
        totalRows: input.totalRows,
        committedBatches: input.committedBatches,
        totalBatches: input.totalBatches,
      });

      await appendJsonLinesLog(input.progressLogPath, {
        kind: "materialization_lookup_reconciliation_started",
        dataset,
        datasetIndex: datasetPosition,
        totalDatasets: materializationDatasets.length,
        targetTable,
        timestamp: new Date().toISOString(),
      });

      const reconciliation = await reconcileMaterializationLookups({
        client: input.client,
        dataset,
      });

      await appendJsonLinesLog(input.progressLogPath, {
        kind: "materialization_lookup_reconciliation_completed",
        dataset,
        datasetIndex: datasetPosition,
        totalDatasets: materializationDatasets.length,
        targetTable,
        totalInsertedCodes: reconciliation.totalInsertedCodes,
        results: reconciliation.results.map((item) => ({
          lookupTable: item.lookupTable,
          insertedCodes: item.insertedCodes,
        })),
        durationMs: reconciliation.durationMs,
        timestamp: new Date().toISOString(),
      });

      const expectedRows = input.expectedRowsByDataset.get(dataset);
      const expectedStagedRows = input.expectedStagedRowsByDataset.get(dataset);
      const result = await materializeDatasetByChunks({
        client: input.client,
        planId: input.planId,
        dataset,
        datasetIndex: datasetPosition,
        datasets: materializationDatasets,
        targetTable,
        chunkSize,
        ...(expectedRows === undefined ? {} : { expectedRows }),
        ...(expectedStagedRows === undefined ? {} : { expectedStagedRows }),
        schemaCapabilities: input.schemaCapabilities,
        progressLogPath: input.progressLogPath,
        onProgress: input.onProgress,
        completedFiles: input.completedFiles,
        totalFiles: input.totalFiles,
        processedRows: input.processedRows,
        totalRows: input.totalRows,
        committedBatches: input.committedBatches,
        totalBatches: input.totalBatches,
        completedDatasets: summary.datasets.length,
      });

      const tracker = input.datasetPerformanceTrackers.get(dataset);
      if (tracker) {
        tracker.materializationDurationMs += result.durationMs;
      }

      summary.datasets.push({
        dataset,
        targetTable,
        affectedRows: result.affectedRows,
        sourceRows: result.sourceRows,
        chunksCompleted: result.chunksCompleted,
        durationMs: result.durationMs,
      });

      await appendJsonLinesLog(input.progressLogPath, {
        kind: "materialization_dataset_completed",
        dataset,
        datasetIndex: datasetPosition,
        totalDatasets: materializationDatasets.length,
        targetTable,
        affectedRows: result.affectedRows,
        sourceRows: result.sourceRows,
        chunksCompleted: result.chunksCompleted,
        durationMs: result.durationMs,
        timestamp: new Date().toISOString(),
      });
    } catch (error) {
      await appendJsonLinesLog(input.progressLogPath, {
        kind: "materialization_dataset_failed",
        dataset,
        datasetIndex: datasetPosition,
        totalDatasets: materializationDatasets.length,
        targetTable,
        error: error instanceof Error ? error.message : String(error),
        timestamp: new Date().toISOString(),
      });
      throw error;
    }
  }

  input.onProgress?.({
    kind: "materialization_finish",
    totalDatasets: materializationDatasets.length,
    completedDatasets: summary.datasets.length,
  });

  await appendJsonLinesLog(input.progressLogPath, {
    kind: "materialization_completed",
    datasets: summary.datasets.map((item) => ({
      dataset: item.dataset,
      targetTable: item.targetTable,
      affectedRows: item.affectedRows,
      sourceRows: item.sourceRows,
      chunksCompleted: item.chunksCompleted,
      durationMs: item.durationMs,
    })),
    timestamp: new Date().toISOString(),
  });

  return summary;
}
