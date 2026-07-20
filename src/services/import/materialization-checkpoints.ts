import type { Client } from "pg";

import { TABELA_CHECKPOINTS_MATERIALIZACAO } from "../schema/table-names.js";
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
  plano_id: string;
  conjunto: string;
  tabela_destino: string;
  status: ImportPhaseStatus;
  linhas_materializadas: string;
  ultimo_staging_id: string;
  blocos_concluidos: string;
  ultimo_erro: string | null;
  iniciado_em: Date | null;
  concluido_em: Date | null;
  atualizado_em: Date;
  staging_linhas_verificado: string | null;
  staging_max_staging_id_verificado: string | null;
  staging_validado_em: Date | null;
  status_reconciliacao_dominio: ImportPhaseStatus | null;
  reconciliacao_dominio_linhas_verificado: string | null;
  reconciliacao_dominio_max_staging_id_verificado: string | null;
  reconciliacao_dominio_concluida_em: Date | null;
  ultimo_bloco_primeiro_staging_id: string | null;
  ultimo_bloco_ultimo_staging_id: string | null;
  ultimo_bloco_linhas: string | null;
};

function parseNullableInt(value: string | null): number | null {
  return value === null ? null : Number.parseInt(value, 10);
}

function mapCheckpointRow(
  row: MaterializationCheckpointRow,
): MaterializationCheckpointRecord {
  return {
    planId: Number.parseInt(row.plano_id, 10),
    dataset: row.conjunto,
    targetTable: row.tabela_destino,
    status: row.status,
    rowsMaterialized: Number.parseInt(row.linhas_materializadas, 10),
    lastStagingId: Number.parseInt(row.ultimo_staging_id, 10),
    chunksCompleted: Number.parseInt(row.blocos_concluidos, 10),
    lastError: row.ultimo_erro,
    startedAt: row.iniciado_em ? new Date(row.iniciado_em) : null,
    completedAt: row.concluido_em ? new Date(row.concluido_em) : null,
    updatedAt: new Date(row.atualizado_em),
    stagingRowCountVerified: parseNullableInt(row.staging_linhas_verificado),
    stagingMaxStagingIdVerified: parseNullableInt(
      row.staging_max_staging_id_verificado,
    ),
    stagingValidatedAt: row.staging_validado_em
      ? new Date(row.staging_validado_em)
      : null,
    lookupReconciliationStatus: row.status_reconciliacao_dominio ?? "pending",
    lookupReconciliationRowCountVerified: parseNullableInt(
      row.reconciliacao_dominio_linhas_verificado,
    ),
    lookupReconciliationMaxStagingIdVerified: parseNullableInt(
      row.reconciliacao_dominio_max_staging_id_verificado,
    ),
    lookupReconciliationCompletedAt: row.reconciliacao_dominio_concluida_em
      ? new Date(row.reconciliacao_dominio_concluida_em)
      : null,
    lastChunkFirstStagingId: Number.parseInt(
      row.ultimo_bloco_primeiro_staging_id ?? "0",
      10,
    ),
    lastChunkLastStagingId: Number.parseInt(
      row.ultimo_bloco_ultimo_staging_id ?? "0",
      10,
    ),
    lastChunkRows: Number.parseInt(row.ultimo_bloco_linhas ?? "0", 10),
  };
}

export async function ensureMaterializationCheckpointTable(
  client: Client,
): Promise<void> {
  await ensureTableShape(client, {
    tableName: TABELA_CHECKPOINTS_MATERIALIZACAO,
    requiredColumns: [
      "plano_id",
      "conjunto",
      "tabela_destino",
      "status",
      "linhas_materializadas",
      "ultimo_staging_id",
      "blocos_concluidos",
      "ultimo_erro",
      "iniciado_em",
      "concluido_em",
      "atualizado_em",
      "staging_linhas_verificado",
      "staging_max_staging_id_verificado",
      "staging_validado_em",
      "status_reconciliacao_dominio",
      "reconciliacao_dominio_linhas_verificado",
      "reconciliacao_dominio_max_staging_id_verificado",
      "reconciliacao_dominio_concluida_em",
      "ultimo_bloco_primeiro_staging_id",
      "ultimo_bloco_ultimo_staging_id",
      "ultimo_bloco_linhas",
    ],
    helpMessage:
      'O schema de checkpoint de materialização é obrigatório. Rode "cnpj-db-loader schema generate --profile full" e aplique o SQL antes de importar.',
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
        plano_id,
        conjunto,
        tabela_destino,
        status,
        linhas_materializadas,
        ultimo_staging_id,
        blocos_concluidos,
        ultimo_erro,
        iniciado_em,
        concluido_em,
        atualizado_em,
        staging_linhas_verificado,
        staging_max_staging_id_verificado,
        staging_validado_em,
        status_reconciliacao_dominio,
        reconciliacao_dominio_linhas_verificado,
        reconciliacao_dominio_max_staging_id_verificado,
        reconciliacao_dominio_concluida_em,
        ultimo_bloco_primeiro_staging_id,
        ultimo_bloco_ultimo_staging_id,
        ultimo_bloco_linhas
      from ${TABELA_CHECKPOINTS_MATERIALIZACAO}
      where plano_id = $1 and conjunto = $2`,
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
    `insert into ${TABELA_CHECKPOINTS_MATERIALIZACAO} (
        plano_id,
        conjunto,
        tabela_destino,
        status,
        linhas_materializadas,
        ultimo_staging_id,
        blocos_concluidos,
        ultimo_erro,
        iniciado_em,
        concluido_em,
        atualizado_em,
        staging_linhas_verificado,
        staging_max_staging_id_verificado,
        staging_validado_em,
        status_reconciliacao_dominio,
        reconciliacao_dominio_linhas_verificado,
        reconciliacao_dominio_max_staging_id_verificado,
        reconciliacao_dominio_concluida_em,
        ultimo_bloco_primeiro_staging_id,
        ultimo_bloco_ultimo_staging_id,
        ultimo_bloco_linhas
      ) values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, now(), $11, $12, $13, $14, $15, $16, $17, $18, $19, $20)
      on conflict (plano_id, conjunto)
      do update set
        tabela_destino = excluded.tabela_destino,
        status = excluded.status,
        linhas_materializadas = excluded.linhas_materializadas,
        ultimo_staging_id = excluded.ultimo_staging_id,
        blocos_concluidos = excluded.blocos_concluidos,
        ultimo_erro = excluded.ultimo_erro,
        iniciado_em = excluded.iniciado_em,
        concluido_em = excluded.concluido_em,
        atualizado_em = now(),
        staging_linhas_verificado = excluded.staging_linhas_verificado,
        staging_max_staging_id_verificado = excluded.staging_max_staging_id_verificado,
        staging_validado_em = excluded.staging_validado_em,
        status_reconciliacao_dominio = excluded.status_reconciliacao_dominio,
        reconciliacao_dominio_linhas_verificado = excluded.reconciliacao_dominio_linhas_verificado,
        reconciliacao_dominio_max_staging_id_verificado = excluded.reconciliacao_dominio_max_staging_id_verificado,
        reconciliacao_dominio_concluida_em = excluded.reconciliacao_dominio_concluida_em,
        ultimo_bloco_primeiro_staging_id = excluded.ultimo_bloco_primeiro_staging_id,
        ultimo_bloco_ultimo_staging_id = excluded.ultimo_bloco_ultimo_staging_id,
        ultimo_bloco_linhas = excluded.ultimo_bloco_linhas`,
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
    `update ${TABELA_CHECKPOINTS_MATERIALIZACAO}
        set status = $3,
            linhas_materializadas = $4,
            ultimo_staging_id = $5,
            blocos_concluidos = $6,
            ultimo_erro = $7,
            iniciado_em = $8,
            concluido_em = $9,
            atualizado_em = now(),
            ultimo_bloco_primeiro_staging_id = $10,
            ultimo_bloco_ultimo_staging_id = $11,
            ultimo_bloco_linhas = $12
      where plano_id = $1 and conjunto = $2`,
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
    `delete from ${TABELA_CHECKPOINTS_MATERIALIZACAO} where plano_id = $1`,
    [planId],
  );
}
