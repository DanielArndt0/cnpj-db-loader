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

async function readStagingMaxId(
  client: Client,
  stagingTable: string,
): Promise<number> {
  const result = await client.query<MaxRow>(
    `select coalesce(max(staging_id), 0)::bigint as max_staging_id from ${stagingTable}`,
  );

  return Number.parseInt(result.rows[0]?.max_staging_id ?? "0", 10);
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

async function validateSecondaryCnaesCheckpoint(input: {
  client: Client;
  planId: number;
}): Promise<ValidatedCheckpoint> {
  const dataset = "secondary_cnaes" as MaterializationDataset;
  const targetTable = "establishment_secondary_cnaes";
  let checkpoint = await readMaterializationCheckpoint(
    input.client,
    input.planId,
    dataset,
    targetTable,
  );
  const stagingMaxId = await readStagingMaxId(
    input.client,
    STAGING_TABLE_BY_DATASET.establishments,
  );

  let adjusted = false;
  let reason: string | null = null;

  if (checkpoint.lastStagingId > stagingMaxId && stagingMaxId > 0) {
    checkpoint = resetCheckpointForReplay(checkpoint);
    adjusted = true;
    reason =
      "The saved secondary CNAE checkpoint no longer matches the current staging establishments table. Secondary CNAEs will be rematerialized from the beginning.";
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
      "Secondary CNAE materialization was previously marked as completed before the current staging tail. It will resume from the saved staging cursor.";
  }

  if (adjusted) {
    await persistCheckpoint(input.client, checkpoint);
  }

  return {
    checkpoint,
    adjusted,
    reason,
    stagingMaxId,
    targetRows: null,
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
  onProgress?: ImportProgressListener | undefined;
  completedDatasets: number;
  totalDatasets: number;
  completedFiles: number;
  totalFiles: number;
  processedRows: number;
  totalRows: number;
  committedBatches: number;
  totalBatches: number;
}): Promise<{
  rows: number;
  chunksCompleted: number;
  durationMs: number;
}> {
  const checkpointDataset = "secondary_cnaes" as MaterializationDataset;
  const targetTable = "establishment_secondary_cnaes";
  const startedAt = performance.now();
  const validated = await validateSecondaryCnaesCheckpoint({
    client: input.client,
    planId: input.planId,
  });
  let checkpoint = validated.checkpoint;

  if (validated.adjusted && validated.reason) {
    emitMaterializationProgress(input.onProgress, {
      datasets: ["establishments"],
      dataset: "establishments",
      datasetIndex: input.totalDatasets,
      targetTable,
      stepLabel: "Secondary CNAEs checkpoint reconciled",
      completedDatasets: input.completedDatasets,
      completedFiles: input.completedFiles,
      totalFiles: input.totalFiles,
      processedRows: input.processedRows,
      totalRows: input.totalRows,
      committedBatches: input.committedBatches,
      totalBatches: input.totalBatches,
    });

    await appendJsonLinesLog(input.progressLogPath, {
      kind: "materialization_secondary_cnaes_checkpoint_reconciled",
      targetTable,
      reason: validated.reason,
      stagingMaxId: validated.stagingMaxId,
      timestamp: new Date().toISOString(),
    });
  }

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
    emitMaterializationProgress(input.onProgress, {
      datasets: ["establishments"],
      dataset: "establishments",
      datasetIndex: input.totalDatasets,
      targetTable,
      stepLabel: `Secondary CNAEs chunk ${checkpoint.chunksCompleted + 1}`,
      completedDatasets: input.completedDatasets,
      completedFiles: input.completedFiles,
      totalFiles: input.totalFiles,
      processedRows: input.processedRows,
      totalRows: input.totalRows,
      committedBatches: input.committedBatches,
      totalBatches: input.totalBatches,
    });

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
  expectedRowsByDataset: ReadonlyMap<ImportDatasetType, number>;
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
      const result = await materializeDatasetByChunks({
        client: input.client,
        planId: input.planId,
        dataset,
        datasetIndex: datasetPosition,
        datasets: materializationDatasets,
        targetTable,
        chunkSize,
        ...(expectedRows === undefined ? {} : { expectedRows }),
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
    onProgress: input.onProgress,
    completedDatasets: summary.datasets.length,
    totalDatasets: materializationDatasets.length,
    completedFiles: input.completedFiles,
    totalFiles: input.totalFiles,
    processedRows: input.processedRows,
    totalRows: input.totalRows,
    committedBatches: input.committedBatches,
    totalBatches: input.totalBatches,
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
