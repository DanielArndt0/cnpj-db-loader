import type { Client } from "pg";

import { ensureTableShape } from "./schema-validation.js";
import type { ImportPhaseStatus } from "./types.js";

export type MaterializationCheckpointRecord = {
  planId: number;
  dataset: string;
  targetTable: string;
  status: ImportPhaseStatus;
  rowsMaterialized: number;
  lastStagingId: number;
  chunksCompleted: number;
  lastError: string | null;
  startedAt: Date | null;
  completedAt: Date | null;
  updatedAt: Date;
  stagingRowCountVerified: number | null;
  stagingMaxStagingIdVerified: number | null;
  stagingValidatedAt: Date | null;
  lookupReconciliationStatus: ImportPhaseStatus;
  lookupReconciliationRowCountVerified: number | null;
  lookupReconciliationMaxStagingIdVerified: number | null;
  lookupReconciliationCompletedAt: Date | null;
  lastChunkFirstStagingId: number;
  lastChunkLastStagingId: number;
  lastChunkRows: number;
};

type MaterializationCheckpointRow = {
  plan_id: string;
  dataset: string;
  target_table: string;
  status: ImportPhaseStatus;
  rows_materialized: string;
  last_staging_id: string;
  chunks_completed: string;
  last_error: string | null;
  started_at: Date | null;
  completed_at: Date | null;
  updated_at: Date;
  staging_row_count_verified: string | null;
  staging_max_staging_id_verified: string | null;
  staging_validated_at: Date | null;
  lookup_reconciliation_status: ImportPhaseStatus | null;
  lookup_reconciliation_row_count_verified: string | null;
  lookup_reconciliation_max_staging_id_verified: string | null;
  lookup_reconciliation_completed_at: Date | null;
  last_chunk_first_staging_id: string | null;
  last_chunk_last_staging_id: string | null;
  last_chunk_rows: string | null;
};

function parseNullableInt(value: string | null): number | null {
  return value === null ? null : Number.parseInt(value, 10);
}

function mapCheckpointRow(
  row: MaterializationCheckpointRow,
): MaterializationCheckpointRecord {
  return {
    planId: Number.parseInt(row.plan_id, 10),
    dataset: row.dataset,
    targetTable: row.target_table,
    status: row.status,
    rowsMaterialized: Number.parseInt(row.rows_materialized, 10),
    lastStagingId: Number.parseInt(row.last_staging_id, 10),
    chunksCompleted: Number.parseInt(row.chunks_completed, 10),
    lastError: row.last_error,
    startedAt: row.started_at ? new Date(row.started_at) : null,
    completedAt: row.completed_at ? new Date(row.completed_at) : null,
    updatedAt: new Date(row.updated_at),
    stagingRowCountVerified: parseNullableInt(row.staging_row_count_verified),
    stagingMaxStagingIdVerified: parseNullableInt(
      row.staging_max_staging_id_verified,
    ),
    stagingValidatedAt: row.staging_validated_at
      ? new Date(row.staging_validated_at)
      : null,
    lookupReconciliationStatus: row.lookup_reconciliation_status ?? "pending",
    lookupReconciliationRowCountVerified: parseNullableInt(
      row.lookup_reconciliation_row_count_verified,
    ),
    lookupReconciliationMaxStagingIdVerified: parseNullableInt(
      row.lookup_reconciliation_max_staging_id_verified,
    ),
    lookupReconciliationCompletedAt: row.lookup_reconciliation_completed_at
      ? new Date(row.lookup_reconciliation_completed_at)
      : null,
    lastChunkFirstStagingId: Number.parseInt(
      row.last_chunk_first_staging_id ?? "0",
      10,
    ),
    lastChunkLastStagingId: Number.parseInt(
      row.last_chunk_last_staging_id ?? "0",
      10,
    ),
    lastChunkRows: Number.parseInt(row.last_chunk_rows ?? "0", 10),
  };
}

export async function ensureMaterializationCheckpointTable(
  client: Client,
): Promise<void> {
  await ensureTableShape(client, {
    tableName: "import_materialization_checkpoints",
    requiredColumns: [
      "plan_id",
      "dataset",
      "target_table",
      "status",
      "rows_materialized",
      "last_staging_id",
      "chunks_completed",
      "last_error",
      "started_at",
      "completed_at",
      "updated_at",
      "staging_row_count_verified",
      "staging_max_staging_id_verified",
      "staging_validated_at",
      "lookup_reconciliation_status",
      "lookup_reconciliation_row_count_verified",
      "lookup_reconciliation_max_staging_id_verified",
      "lookup_reconciliation_completed_at",
      "last_chunk_first_staging_id",
      "last_chunk_last_staging_id",
      "last_chunk_rows",
    ],
    helpMessage:
      'The materialization checkpoint schema is required. Run "cnpj-db-loader schema generate --profile full" and apply the SQL before importing.',
  });
}

export async function readMaterializationCheckpoint(
  client: Client,
  planId: number,
  dataset: string,
  targetTable: string,
): Promise<MaterializationCheckpointRecord> {
  const result = await client.query<MaterializationCheckpointRow>(
    `select
        plan_id,
        dataset,
        target_table,
        status,
        rows_materialized,
        last_staging_id,
        chunks_completed,
        last_error,
        started_at,
        completed_at,
        updated_at,
        staging_row_count_verified,
        staging_max_staging_id_verified,
        staging_validated_at,
        lookup_reconciliation_status,
        lookup_reconciliation_row_count_verified,
        lookup_reconciliation_max_staging_id_verified,
        lookup_reconciliation_completed_at,
        last_chunk_first_staging_id,
        last_chunk_last_staging_id,
        last_chunk_rows
      from import_materialization_checkpoints
      where plan_id = $1 and dataset = $2`,
    [planId, dataset],
  );

  if (result.rowCount === 0) {
    return {
      planId,
      dataset,
      targetTable,
      status: "pending",
      rowsMaterialized: 0,
      lastStagingId: 0,
      chunksCompleted: 0,
      lastError: null,
      startedAt: null,
      completedAt: null,
      updatedAt: new Date(),
      stagingRowCountVerified: null,
      stagingMaxStagingIdVerified: null,
      stagingValidatedAt: null,
      lookupReconciliationStatus: "pending",
      lookupReconciliationRowCountVerified: null,
      lookupReconciliationMaxStagingIdVerified: null,
      lookupReconciliationCompletedAt: null,
      lastChunkFirstStagingId: 0,
      lastChunkLastStagingId: 0,
      lastChunkRows: 0,
    };
  }

  return mapCheckpointRow(result.rows[0]!);
}

export async function writeMaterializationCheckpoint(
  client: Client,
  checkpoint: MaterializationCheckpointRecord,
): Promise<void> {
  await client.query(
    `insert into import_materialization_checkpoints (
        plan_id,
        dataset,
        target_table,
        status,
        rows_materialized,
        last_staging_id,
        chunks_completed,
        last_error,
        started_at,
        completed_at,
        updated_at,
        staging_row_count_verified,
        staging_max_staging_id_verified,
        staging_validated_at,
        lookup_reconciliation_status,
        lookup_reconciliation_row_count_verified,
        lookup_reconciliation_max_staging_id_verified,
        lookup_reconciliation_completed_at,
        last_chunk_first_staging_id,
        last_chunk_last_staging_id,
        last_chunk_rows
      ) values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, now(), $11, $12, $13, $14, $15, $16, $17, $18, $19, $20)
      on conflict (plan_id, dataset)
      do update set
        target_table = excluded.target_table,
        status = excluded.status,
        rows_materialized = excluded.rows_materialized,
        last_staging_id = excluded.last_staging_id,
        chunks_completed = excluded.chunks_completed,
        last_error = excluded.last_error,
        started_at = excluded.started_at,
        completed_at = excluded.completed_at,
        updated_at = now(),
        staging_row_count_verified = excluded.staging_row_count_verified,
        staging_max_staging_id_verified = excluded.staging_max_staging_id_verified,
        staging_validated_at = excluded.staging_validated_at,
        lookup_reconciliation_status = excluded.lookup_reconciliation_status,
        lookup_reconciliation_row_count_verified = excluded.lookup_reconciliation_row_count_verified,
        lookup_reconciliation_max_staging_id_verified = excluded.lookup_reconciliation_max_staging_id_verified,
        lookup_reconciliation_completed_at = excluded.lookup_reconciliation_completed_at,
        last_chunk_first_staging_id = excluded.last_chunk_first_staging_id,
        last_chunk_last_staging_id = excluded.last_chunk_last_staging_id,
        last_chunk_rows = excluded.last_chunk_rows`,
    [
      checkpoint.planId,
      checkpoint.dataset,
      checkpoint.targetTable,
      checkpoint.status,
      checkpoint.rowsMaterialized,
      checkpoint.lastStagingId,
      checkpoint.chunksCompleted,
      checkpoint.lastError,
      checkpoint.startedAt,
      checkpoint.completedAt,
      checkpoint.stagingRowCountVerified,
      checkpoint.stagingMaxStagingIdVerified,
      checkpoint.stagingValidatedAt,
      checkpoint.lookupReconciliationStatus,
      checkpoint.lookupReconciliationRowCountVerified,
      checkpoint.lookupReconciliationMaxStagingIdVerified,
      checkpoint.lookupReconciliationCompletedAt,
      checkpoint.lastChunkFirstStagingId,
      checkpoint.lastChunkLastStagingId,
      checkpoint.lastChunkRows,
    ],
  );
}

export async function writeMaterializationCheckpointProgress(
  client: Client,
  checkpoint: MaterializationCheckpointRecord,
): Promise<void> {
  await client.query(
    `update import_materialization_checkpoints
        set status = $3,
            rows_materialized = $4,
            last_staging_id = $5,
            chunks_completed = $6,
            last_error = $7,
            started_at = $8,
            completed_at = $9,
            updated_at = now(),
            last_chunk_first_staging_id = $10,
            last_chunk_last_staging_id = $11,
            last_chunk_rows = $12
      where plan_id = $1 and dataset = $2`,
    [
      checkpoint.planId,
      checkpoint.dataset,
      checkpoint.status,
      checkpoint.rowsMaterialized,
      checkpoint.lastStagingId,
      checkpoint.chunksCompleted,
      checkpoint.lastError,
      checkpoint.startedAt,
      checkpoint.completedAt,
      checkpoint.lastChunkFirstStagingId,
      checkpoint.lastChunkLastStagingId,
      checkpoint.lastChunkRows,
    ],
  );
}

export async function resetMaterializationCheckpoints(
  client: Client,
  planId: number,
): Promise<void> {
  await client.query(
    `delete from import_materialization_checkpoints where plan_id = $1`,
    [planId],
  );
}
