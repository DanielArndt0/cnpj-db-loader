import { performance } from "node:perf_hooks";

import { Client } from "pg";

import { appendJsonLinesLog } from "../logging.service.js";
import {
  DATASET_LAYOUTS,
  type BatchRow,
  type ImportCheckpointStatus,
  type ImportFilePlan,
  type ImportProgressListener,
  type ImportSchemaCapabilities,
  type LookupCacheMap,
} from "./types.js";
import { getInsertColumns, flushRows, flushSecondaryCnaes } from "./sql.js";
import {
  buildParsedPayload,
  extractSecondaryCnaes,
  normalizeFieldCount,
  parseDelimitedLine,
  transformRecord,
} from "./transform.js";
import { iterateFileLines } from "./planning.js";
import {
  markCheckpointFailed,
  readCheckpoint,
  writeCheckpoint,
} from "./checkpoints.js";
import { writeQuarantineRow } from "./quarantine.js";

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
    (await readCheckpoint(
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

  let lineNumber = 0;
  let fileRowsCommitted = checkpoint.rowsCommitted;
  let batchRows: BatchRow[] = [];
  let batchLastOffset = checkpoint.byteOffset;

  emitProgress();

  const commitBatch = async (markCompleted: boolean): Promise<void> => {
    const finalizeCheckpoint = async (
      status: ImportCheckpointStatus,
    ): Promise<void> => {
      await client.query("begin");
      try {
        checkpoint = {
          ...checkpoint,
          byteOffset: markCompleted ? filePlan.fileSize : batchLastOffset,
          rowsCommitted: fileRowsCommitted,
          status,
          lastError: null,
        };
        await writeCheckpoint(client, checkpoint);
        await client.query("commit");
      } catch (error) {
        await client.query("rollback");
        throw error;
      }
    };

    if (batchRows.length === 0) {
      if (markCompleted) {
        await finalizeCheckpoint("completed");
        emitProgress();
      }
      return;
    }

    const batchValues = batchRows.map((row) => row.values);
    const secondaryRows = batchRows.flatMap((row) => row.secondaryRows);
    const batchStartedAt = performance.now();

    await client.query("begin");
    try {
      await flushRows(
        client,
        lookupCache,
        dataset,
        batchValues,
        schemaCapabilities,
      );
      await flushSecondaryCnaes(client, secondaryRows);
      checkpoint = {
        ...checkpoint,
        byteOffset: markCompleted ? filePlan.fileSize : batchLastOffset,
        rowsCommitted: fileRowsCommitted,
        status: markCompleted ? "completed" : "in_progress",
        lastError: null,
      };
      await writeCheckpoint(client, checkpoint);
      await client.query("commit");
      progress.performance.insertDurationMs +=
        performance.now() - batchStartedAt;

      counters.committedRows += batchRows.length;
      counters.committedBatches += 1;
      counters.secondaryCnaesRows += secondaryRows.length;

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
        secondaryCnaesRows: secondaryRows.length,
        durationMs: performance.now() - batchStartedAt,
        timestamp: new Date().toISOString(),
      });
    } catch (batchError) {
      await client.query("rollback");
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
          await client.query("begin");
          await flushRows(
            client,
            lookupCache,
            dataset,
            [row.values],
            schemaCapabilities,
          );
          await flushSecondaryCnaes(client, row.secondaryRows);
          checkpoint = {
            ...checkpoint,
            byteOffset: row.nextOffset,
            rowsCommitted: row.sourceRowNumber,
            status: "in_progress",
            lastError: null,
          };
          await writeCheckpoint(client, checkpoint);
          await client.query("commit");
          progress.performance.retryDurationMs +=
            performance.now() - retryStartedAt;
          progress.performance.retriedRows += 1;
          counters.committedRows += 1;
          counters.secondaryCnaesRows += row.secondaryRows.length;
        } catch (rowError) {
          await client.query("rollback");
          progress.performance.retryDurationMs +=
            performance.now() - retryStartedAt;
          progress.performance.retriedRows += 1;
          await client.query("begin");
          const quarantineStartedAt = performance.now();
          try {
            await writeQuarantineRow(client, {
              dataset,
              filePath,
              rowNumber: row.sourceRowNumber,
              checkpointOffset: row.nextOffset,
              rawLine: row.rawLine,
              error: rowError,
              parsedPayload: buildParsedPayload(columns, row.values),
            });
            checkpoint = {
              ...checkpoint,
              byteOffset: row.nextOffset,
              rowsCommitted: row.sourceRowNumber,
              status: "in_progress",
              lastError: null,
            };
            await writeCheckpoint(client, checkpoint);
            await client.query("commit");
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
              rowNumber: row.sourceRowNumber,
              checkpointOffset: row.nextOffset,
              error:
                rowError instanceof Error ? rowError.message : String(rowError),
              durationMs: performance.now() - quarantineStartedAt,
              timestamp: new Date().toISOString(),
            });
          } catch (quarantineError) {
            await client.query("rollback");
            throw quarantineError;
          }
        }
      }

      counters.committedBatches += 1;
      if (markCompleted) {
        await finalizeCheckpoint("completed");
      }
    }

    batchRows = [];
    emitProgress();
  };

  try {
    for await (const item of iterateFileLines(
      filePath,
      checkpoint.byteOffset,
    )) {
      lineNumber += 1;

      if (item.line.trim() === "") {
        checkpoint.byteOffset = item.nextOffset;
        continue;
      }

      try {
        const parsedFields = normalizeFieldCount(
          parseDelimitedLine(item.line),
          layout.fields.length,
          filePath,
          lineNumber,
        );
        const record = transformRecord(
          dataset,
          layout,
          parsedFields,
          schemaCapabilities,
        );
        const nextSourceRowNumber = fileRowsCommitted + 1;
        batchRows.push({
          values: record,
          rawLine: item.line,
          nextOffset: item.nextOffset,
          sourceRowNumber: nextSourceRowNumber,
          secondaryRows:
            dataset === "establishments"
              ? extractSecondaryCnaes(record, columns)
              : [],
        });
        fileRowsCommitted = nextSourceRowNumber;
        batchLastOffset = item.nextOffset;

        if (batchRows.length >= progress.batchSize) {
          await commitBatch(false);
        }
      } catch (rowError) {
        fileRowsCommitted += 1;
        batchLastOffset = item.nextOffset;
        await client.query("begin");
        const quarantineStartedAt = performance.now();
        try {
          await writeQuarantineRow(client, {
            dataset,
            filePath,
            rowNumber: fileRowsCommitted,
            checkpointOffset: item.nextOffset,
            rawLine: item.line,
            error: rowError,
            parsedPayload: null,
          });
          checkpoint = {
            ...checkpoint,
            byteOffset: item.nextOffset,
            rowsCommitted: fileRowsCommitted,
            status: "in_progress",
            lastError: null,
          };
          await writeCheckpoint(client, checkpoint);
          await client.query("commit");
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
            rowNumber: fileRowsCommitted,
            checkpointOffset: item.nextOffset,
            error:
              rowError instanceof Error ? rowError.message : String(rowError),
            durationMs: performance.now() - quarantineStartedAt,
            timestamp: new Date().toISOString(),
          });
          emitProgress();
        } catch (quarantineError) {
          await client.query("rollback");
          throw quarantineError;
        }
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
    await markCheckpointFailed(client, checkpoint, message);
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
