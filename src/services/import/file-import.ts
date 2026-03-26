import { performance } from "node:perf_hooks";

import { Client } from "pg";

import { appendJsonLinesLog } from "../logging.service.js";
import {
  DATASET_LAYOUTS,
  type ImportCheckpointStatus,
  type ImportFilePlan,
  type ImportProgressListener,
  type ImportSchemaCapabilities,
  type LookupCacheMap,
} from "./types.js";
import {
  markImportCheckpointFailed,
  readImportCheckpoint,
  writeImportCheckpoint,
} from "./checkpoint-manager.js";
import { parseImportSourceLine } from "./parser.js";
import { normalizeImportRow } from "./normalizer.js";
import { readImportSourceLines } from "./source-reader.js";
import {
  writeImportBatchToTarget,
  writeImportRowToTarget,
} from "./staging-writer.js";
import { writeImportQuarantineRow } from "./quarantine-writer.js";
import { buildParsedPayload } from "./transform.js";
import { getInsertColumns } from "./sql.js";

export async function importDatasetFile(
  client: Client,
  lookupCache: LookupCacheMap,
  filePlan: ImportFilePlan,
  schemaCapabilities: ImportSchemaCapabilities,
  counters: {
    committedRows: number;
    committedBatches: number;
    completedFiles: number;
    secondaryCnaesRows: number;
    quarantinedRows: number;
  },
  progress: {
    datasetIndex: number;
    totalDatasets: number;
    totalFiles: number;
    totalBatches: number;
    fileIndex: number;
    onProgress: ImportProgressListener | undefined;
    progressLogPath: string;
    batchSize: number;
    verboseProgress: boolean;
    performance: {
      insertDurationMs: number;
      retryDurationMs: number;
      quarantineDurationMs: number;
      retriedRows: number;
      retriedBatches: number;
      quarantinedRows: number;
    };
  },
): Promise<number> {
  const dataset = filePlan.dataset;
  const filePath = filePlan.absolutePath;
  const layout = DATASET_LAYOUTS[dataset];
  const columns = getInsertColumns(dataset, schemaCapabilities);
  let checkpoint =
    filePlan.checkpoint ??
    (await readImportCheckpoint(
      client,
      dataset,
      filePath,
      filePlan.fileSize,
      filePlan.fileMtime,
    ));

  const emitProgress = (): void => {
    progress.onProgress?.({
      kind: "progress",
      dataset,
      datasetIndex: progress.datasetIndex,
      totalDatasets: progress.totalDatasets,
      currentFilePath: filePath,
      currentFileDisplayPath: filePlan.displayPath,
      fileIndex: progress.fileIndex,
      completedFiles: counters.completedFiles,
      totalFiles: progress.totalFiles,
      currentFileRowsCommitted: checkpoint.rowsCommitted,
      currentFileRowsTotal: filePlan.totalRows,
      committedRows: counters.committedRows,
      committedBatches: counters.committedBatches,
      totalBatches: progress.totalBatches,
      currentBatch:
        filePlan.totalBatches === 0
          ? 0
          : Math.min(
              filePlan.totalBatches,
              Math.max(
                1,
                Math.ceil(
                  Math.max(checkpoint.rowsCommitted, 1) / progress.batchSize,
                ),
              ),
            ),
      batchSize: progress.batchSize,
      checkpointOffset: checkpoint.byteOffset,
      currentFileSize: filePlan.fileSize,
      verboseProgress: progress.verboseProgress,
    });
  };

  if (
    checkpoint.status === "completed" &&
    checkpoint.byteOffset >= filePlan.fileSize
  ) {
    emitProgress();
    return checkpoint.rowsCommitted;
  }

  let fileRowsCommitted = checkpoint.rowsCommitted;
  let batchRows: ReturnType<typeof normalizeImportRow>[] = [];
  let batchLastOffset = checkpoint.byteOffset;

  const persistCheckpoint = async (
    status: ImportCheckpointStatus,
    byteOffset: number,
    rowsCommitted: number,
  ): Promise<void> => {
    checkpoint = {
      ...checkpoint,
      byteOffset,
      rowsCommitted,
      status,
      lastError: null,
    };
    await writeImportCheckpoint(client, checkpoint);
  };

  const runInTransaction = async (
    callback: () => Promise<void>,
  ): Promise<void> => {
    await client.query("begin");
    try {
      await callback();
      await client.query("commit");
    } catch (error) {
      await client.query("rollback");
      throw error;
    }
  };

  const quarantineRow = async (input: {
    rowNumber: number;
    checkpointOffset: number;
    rawLine: string;
    error: unknown;
    parsedPayload: Record<string, unknown> | null;
  }): Promise<void> => {
    const quarantineStartedAt = performance.now();

    await runInTransaction(async () => {
      await writeImportQuarantineRow(client, {
        dataset,
        filePath,
        rowNumber: input.rowNumber,
        checkpointOffset: input.checkpointOffset,
        rawLine: input.rawLine,
        error: input.error,
        parsedPayload: input.parsedPayload,
      });
      await persistCheckpoint(
        "in_progress",
        input.checkpointOffset,
        input.rowNumber,
      );
    });

    progress.performance.quarantineDurationMs +=
      performance.now() - quarantineStartedAt;
    progress.performance.quarantinedRows += 1;
    counters.quarantinedRows += 1;

    await appendJsonLinesLog(progress.progressLogPath, {
      kind: "row_quarantined",
      dataset,
      datasetIndex: progress.datasetIndex,
      filePath,
      fileDisplayPath: filePlan.displayPath,
      fileIndex: progress.fileIndex,
      rowNumber: input.rowNumber,
      checkpointOffset: input.checkpointOffset,
      error:
        input.error instanceof Error
          ? input.error.message
          : String(input.error),
      durationMs: performance.now() - quarantineStartedAt,
      timestamp: new Date().toISOString(),
    });
  };

  const commitBatch = async (markCompleted: boolean): Promise<void> => {
    if (batchRows.length === 0) {
      if (markCompleted) {
        await runInTransaction(async () => {
          await persistCheckpoint(
            "completed",
            filePlan.fileSize,
            fileRowsCommitted,
          );
        });
        emitProgress();
      }
      return;
    }

    const batchStartedAt = performance.now();

    try {
      const batchResult = await (async () => {
        let result:
          | Awaited<ReturnType<typeof writeImportBatchToTarget>>
          | undefined;

        await runInTransaction(async () => {
          result = await writeImportBatchToTarget({
            client,
            lookupCache,
            dataset,
            rows: batchRows,
            schemaCapabilities,
          });
          await persistCheckpoint(
            markCompleted ? "completed" : "in_progress",
            markCompleted ? filePlan.fileSize : batchLastOffset,
            fileRowsCommitted,
          );
        });

        return result;
      })();

      progress.performance.insertDurationMs +=
        performance.now() - batchStartedAt;
      counters.committedRows += batchRows.length;
      counters.committedBatches += 1;
      counters.secondaryCnaesRows += batchResult?.writtenSecondaryRows ?? 0;

      await appendJsonLinesLog(progress.progressLogPath, {
        kind: "batch_committed",
        dataset,
        datasetIndex: progress.datasetIndex,
        filePath,
        fileDisplayPath: filePlan.displayPath,
        fileIndex: progress.fileIndex,
        batchNumber: counters.committedBatches,
        batchSize: progress.batchSize,
        batchRows: batchRows.length,
        fileRowsCommitted,
        fileRowsTotal: filePlan.totalRows,
        totalRowsCommitted: counters.committedRows,
        totalBatchesCommitted: counters.committedBatches,
        totalBatchesPlanned: progress.totalBatches,
        checkpointOffset: checkpoint.byteOffset,
        fileSize: filePlan.fileSize,
        secondaryCnaesRows: batchResult?.writtenSecondaryRows ?? 0,
        durationMs: performance.now() - batchStartedAt,
        timestamp: new Date().toISOString(),
      });
    } catch (batchError) {
      progress.performance.insertDurationMs +=
        performance.now() - batchStartedAt;
      progress.performance.retriedBatches += 1;

      await appendJsonLinesLog(progress.progressLogPath, {
        kind: "batch_retry_fallback",
        dataset,
        datasetIndex: progress.datasetIndex,
        filePath,
        fileDisplayPath: filePlan.displayPath,
        fileIndex: progress.fileIndex,
        batchRows: batchRows.length,
        checkpointOffset: checkpoint.byteOffset,
        error:
          batchError instanceof Error ? batchError.message : String(batchError),
        timestamp: new Date().toISOString(),
      });

      for (const row of batchRows) {
        const retryStartedAt = performance.now();

        try {
          const rowResult = await (async () => {
            let result:
              | Awaited<ReturnType<typeof writeImportRowToTarget>>
              | undefined;

            await runInTransaction(async () => {
              result = await writeImportRowToTarget({
                client,
                lookupCache,
                dataset,
                row,
                schemaCapabilities,
              });
              await persistCheckpoint(
                "in_progress",
                row.nextOffset,
                row.sourceRowNumber,
              );
            });

            return result;
          })();

          progress.performance.retryDurationMs +=
            performance.now() - retryStartedAt;
          progress.performance.retriedRows += 1;
          counters.committedRows += 1;
          counters.secondaryCnaesRows += rowResult?.writtenSecondaryRows ?? 0;
        } catch (rowError) {
          progress.performance.retryDurationMs +=
            performance.now() - retryStartedAt;
          progress.performance.retriedRows += 1;
          await quarantineRow({
            rowNumber: row.sourceRowNumber,
            checkpointOffset: row.nextOffset,
            rawLine: row.rawLine,
            error: rowError,
            parsedPayload: buildParsedPayload(columns, row.values),
          });
        }
      }

      counters.committedBatches += 1;
      if (markCompleted) {
        await runInTransaction(async () => {
          await persistCheckpoint(
            "completed",
            filePlan.fileSize,
            fileRowsCommitted,
          );
        });
      }
    }

    batchRows = [];
    emitProgress();
  };

  emitProgress();

  try {
    for await (const sourceLine of readImportSourceLines(
      filePath,
      checkpoint.byteOffset,
    )) {
      if (sourceLine.rawLine.trim() === "") {
        checkpoint.byteOffset = sourceLine.nextOffset;
        continue;
      }

      try {
        const nextSourceRowNumber = fileRowsCommitted + 1;
        const parsedLine = parseImportSourceLine(sourceLine);
        const normalizedRow = normalizeImportRow({
          dataset,
          filePath,
          layout,
          parsedLine,
          schemaCapabilities,
          sourceRowNumber: nextSourceRowNumber,
        });

        batchRows.push(normalizedRow);
        fileRowsCommitted = nextSourceRowNumber;
        batchLastOffset = sourceLine.nextOffset;

        if (batchRows.length >= progress.batchSize) {
          await commitBatch(false);
        }
      } catch (rowError) {
        fileRowsCommitted += 1;
        batchLastOffset = sourceLine.nextOffset;
        await quarantineRow({
          rowNumber: fileRowsCommitted,
          checkpointOffset: sourceLine.nextOffset,
          rawLine: sourceLine.rawLine,
          error: rowError,
          parsedPayload: null,
        });
        emitProgress();
      }
    }

    if (batchRows.length > 0 || checkpoint.byteOffset < filePlan.fileSize) {
      await commitBatch(true);
    }

    counters.completedFiles += 1;
    emitProgress();

    return fileRowsCommitted;
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    await markImportCheckpointFailed(client, checkpoint, message);
    await appendJsonLinesLog(progress.progressLogPath, {
      kind: "file_failed",
      dataset,
      datasetIndex: progress.datasetIndex,
      filePath,
      fileDisplayPath: filePlan.displayPath,
      fileIndex: progress.fileIndex,
      fileRowsCommitted: checkpoint.rowsCommitted,
      fileRowsTotal: filePlan.totalRows,
      checkpointOffset: checkpoint.byteOffset,
      fileSize: filePlan.fileSize,
      error: message,
      timestamp: new Date().toISOString(),
    });
    throw error;
  }
}
