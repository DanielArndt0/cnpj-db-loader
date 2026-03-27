import { performance } from "node:perf_hooks";

import type { Client } from "pg";

import { appendJsonLinesLog } from "../logging.service.js";
import {
  readMaterializationCheckpoint,
  writeMaterializationCheckpoint,
  type MaterializationCheckpointRecord,
} from "./materialization-checkpoints.js";
import {
  buildMaterializationChunkQuery,
  buildSecondaryCnaesMaterializationChunkQuery,
  isMaterializationDataset,
  type MaterializationDataset,
} from "./materialization-sql.js";
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

type ChunkRow = {
  max_staging_id: string;
  source_rows: string;
  affected_rows: string;
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
  secondaryCnaesRows: number;
  secondaryCnaesChunks: number;
  secondaryCnaesDurationMs: number;
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

async function persistCheckpoint(
  client: Client,
  checkpoint: MaterializationCheckpointRecord,
): Promise<void> {
  await writeMaterializationCheckpoint(client, checkpoint);
}

async function materializeDatasetByChunks(input: {
  client: Client;
  planId: number;
  dataset: MaterializationDataset;
  datasetIndex: number;
  datasets: readonly MaterializationDataset[];
  targetTable: string;
  chunkSize: number;
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
  let checkpoint = await readMaterializationCheckpoint(
    input.client,
    input.planId,
    input.dataset,
    input.targetTable,
  );

  if (checkpoint.status === "completed") {
    emitMaterializationProgress(input.onProgress, {
      datasets: input.datasets,
      dataset: input.dataset,
      datasetIndex: input.datasetIndex,
      targetTable: input.targetTable,
      stepLabel: `Already completed (${checkpoint.chunksCompleted} chunk(s))`,
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

async function materializeSecondaryCnaesByChunks(input: {
  client: Client;
  planId: number;
  chunkSize: number;
  progressLogPath: string;
}): Promise<{
  rows: number;
  chunksCompleted: number;
  durationMs: number;
}> {
  const checkpointDataset = "secondary_cnaes" as MaterializationDataset;
  const targetTable = "establishment_secondary_cnaes";
  const startedAt = performance.now();
  let checkpoint = await readMaterializationCheckpoint(
    input.client,
    input.planId,
    checkpointDataset,
    targetTable,
  );

  if (checkpoint.status === "completed") {
    return {
      rows: checkpoint.rowsMaterialized,
      chunksCompleted: checkpoint.chunksCompleted,
      durationMs: performance.now() - startedAt,
    };
  }

  checkpoint = {
    ...checkpoint,
    dataset: checkpointDataset,
    targetTable,
    status: "in_progress",
    startedAt: checkpoint.startedAt ?? new Date(),
    completedAt: null,
    lastError: null,
  };
  await persistCheckpoint(input.client, checkpoint);

  while (true) {
    const query = buildSecondaryCnaesMaterializationChunkQuery({
      lastStagingId: checkpoint.lastStagingId,
      chunkSize: input.chunkSize,
    });

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

    await appendJsonLinesLog(input.progressLogPath, {
      kind: "materialization_secondary_cnaes_chunk_completed",
      targetTable,
      chunkNumber: checkpoint.chunksCompleted,
      chunkSize: input.chunkSize,
      sourceRows: chunkResult.sourceRows,
      affectedRows: chunkResult.affectedRows,
      lastStagingId: checkpoint.lastStagingId,
      totalRowsMaterialized: checkpoint.rowsMaterialized,
      timestamp: new Date().toISOString(),
    });
  }

  return {
    rows: checkpoint.rowsMaterialized,
    chunksCompleted: checkpoint.chunksCompleted,
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
    secondaryCnaesRows: 0,
    secondaryCnaesChunks: 0,
    secondaryCnaesDurationMs: 0,
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
      const result = await materializeDatasetByChunks({
        client: input.client,
        planId: input.planId,
        dataset,
        datasetIndex: datasetPosition,
        datasets: materializationDatasets,
        targetTable,
        chunkSize,
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

  const secondaryStartedAt = performance.now();
  const secondaryResult = await materializeSecondaryCnaesByChunks({
    client: input.client,
    planId: input.planId,
    chunkSize,
    progressLogPath: input.progressLogPath,
  });
  summary.secondaryCnaesRows = secondaryResult.rows;
  summary.secondaryCnaesChunks = secondaryResult.chunksCompleted;
  summary.secondaryCnaesDurationMs = secondaryResult.durationMs;

  const establishmentsTracker =
    input.datasetPerformanceTrackers.get("establishments");
  if (establishmentsTracker) {
    establishmentsTracker.materializationDurationMs +=
      performance.now() - secondaryStartedAt;
  }

  input.onProgress?.({
    kind: "materialization_finish",
    totalDatasets: materializationDatasets.length,
    completedDatasets: summary.datasets.length,
    secondaryCnaesRows: summary.secondaryCnaesRows,
    secondaryCnaesDurationMs: summary.secondaryCnaesDurationMs,
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
    secondaryCnaesRows: summary.secondaryCnaesRows,
    secondaryCnaesChunks: summary.secondaryCnaesChunks,
    secondaryCnaesDurationMs: summary.secondaryCnaesDurationMs,
    timestamp: new Date().toISOString(),
  });

  return summary;
}
