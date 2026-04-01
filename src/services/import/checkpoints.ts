import { Client } from "pg";

import { ensureTableShape } from "./schema-validation.js";
import type {
  ImportCheckpointRecord,
  ImportCheckpointStatus,
  ImportDatasetPlan,
} from "./types.js";

export async function ensureCheckpointTable(client: Client): Promise<void> {
  await ensureTableShape(client, {
    tableName: "import_checkpoints",
    requiredColumns: [
      "dataset",
      "file_path",
      "file_size",
      "file_mtime",
      "byte_offset",
      "rows_committed",
      "status",
      "last_error",
      "updated_at",
    ],
    helpMessage:
      'The import checkpoint schema is required. Run "cnpj-db-loader schema generate --profile full" and apply the SQL before importing.',
  });
}

export async function readCheckpoint(
  client: Client,
  dataset: ImportCheckpointRecord["dataset"],
  filePath: string,
  fileSize: number,
  fileMtime: Date,
): Promise<ImportCheckpointRecord> {
  const existing = await client.query<{
    file_size: string;
    file_mtime: Date;
    byte_offset: string;
    rows_committed: string;
    status: ImportCheckpointStatus;
    last_error: string | null;
  }>(
    `select file_size, file_mtime, byte_offset, rows_committed, status, last_error
       from import_checkpoints
      where dataset = $1 and file_path = $2`,
    [dataset, filePath],
  );

  const baseRecord: ImportCheckpointRecord = {
    dataset,
    filePath,
    fileSize,
    fileMtime,
    byteOffset: 0,
    rowsCommitted: 0,
    status: "pending",
    lastError: null,
  };

  if (existing.rowCount === 0) {
    return baseRecord;
  }

  const row = existing.rows[0]!;
  const checkpoint: ImportCheckpointRecord = {
    dataset,
    filePath,
    fileSize: Number.parseInt(row.file_size, 10),
    fileMtime: new Date(row.file_mtime),
    byteOffset: Number.parseInt(row.byte_offset, 10),
    rowsCommitted: Number.parseInt(row.rows_committed, 10),
    status: row.status,
    lastError: row.last_error,
  };

  const sameMetadata =
    checkpoint.fileSize === fileSize &&
    checkpoint.fileMtime.getTime() === fileMtime.getTime();

  if (!sameMetadata) {
    return baseRecord;
  }

  return checkpoint;
}

export async function writeCheckpoint(
  client: Client,
  checkpoint: ImportCheckpointRecord,
): Promise<void> {
  await client.query(
    `insert into import_checkpoints (
        dataset,
        file_path,
        file_size,
        file_mtime,
        byte_offset,
        rows_committed,
        status,
        last_error,
        updated_at
      ) values ($1, $2, $3, $4, $5, $6, $7, $8, now())
      on conflict (dataset, file_path)
      do update set
        file_size = excluded.file_size,
        file_mtime = excluded.file_mtime,
        byte_offset = excluded.byte_offset,
        rows_committed = excluded.rows_committed,
        status = excluded.status,
        last_error = excluded.last_error,
        updated_at = now()`,
    [
      checkpoint.dataset,
      checkpoint.filePath,
      checkpoint.fileSize,
      checkpoint.fileMtime,
      checkpoint.byteOffset,
      checkpoint.rowsCommitted,
      checkpoint.status,
      checkpoint.lastError ?? null,
    ],
  );
}

export async function markCheckpointFailed(
  client: Client,
  checkpoint: ImportCheckpointRecord,
  errorMessage: string,
): Promise<void> {
  await writeCheckpoint(client, {
    ...checkpoint,
    status: "failed",
    lastError: errorMessage,
  });
}

export async function hydratePlanWithCheckpoints(
  client: Client,
  datasets: ImportDatasetPlan[],
  batchSize: number,
): Promise<{
  committedRows: number;
  committedBatches: number;
  completedFiles: number;
  resumedFiles: number;
  skippedCompletedFiles: number;
}> {
  let committedRows = 0;
  let committedBatches = 0;
  let completedFiles = 0;
  let resumedFiles = 0;
  let skippedCompletedFiles = 0;

  for (const datasetPlan of datasets) {
    for (const filePlan of datasetPlan.files) {
      const checkpoint = await readCheckpoint(
        client,
        datasetPlan.dataset,
        filePlan.absolutePath,
        filePlan.fileSize,
        filePlan.fileMtime,
      );
      filePlan.checkpoint = checkpoint;

      if (checkpoint.rowsCommitted > 0) {
        committedRows += checkpoint.rowsCommitted;
        committedBatches += Math.min(
          filePlan.totalBatches,
          Math.ceil(checkpoint.rowsCommitted / batchSize),
        );
      }

      if (
        checkpoint.status === "completed" &&
        checkpoint.byteOffset >= filePlan.fileSize
      ) {
        completedFiles += 1;
        skippedCompletedFiles += 1;
      } else if (checkpoint.byteOffset > 0 || checkpoint.rowsCommitted > 0) {
        resumedFiles += 1;
      }
    }
  }

  return {
    committedRows,
    committedBatches,
    completedFiles,
    resumedFiles,
    skippedCompletedFiles,
  };
}
