import { Client } from "pg";

import { TABELA_CHECKPOINTS_IMPORTACAO } from "../schema/table-names.js";
import { ensureTableShape } from "./schema-validation.js";
import type {
  ImportCheckpointRecord,
  ImportCheckpointStatus,
  ImportDatasetPlan,
} from "./types.js";

export async function ensureCheckpointTable(client: Client): Promise<void> {
  await ensureTableShape(client, {
    tableName: TABELA_CHECKPOINTS_IMPORTACAO,
    requiredColumns: [
      "conjunto",
      "caminho_arquivo",
      "tamanho_arquivo",
      "modificado_em",
      "deslocamento_bytes",
      "linhas_confirmadas",
      "status",
      "ultimo_erro",
      "atualizado_em",
    ],
    helpMessage:
      'O schema de checkpoint de importação é obrigatório. Rode "cnpj-db-loader schema generate --profile full" e aplique o SQL antes de importar.',
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
    tamanho_arquivo: string;
    modificado_em: Date;
    deslocamento_bytes: string;
    linhas_confirmadas: string;
    status: ImportCheckpointStatus;
    ultimo_erro: string | null;
  }>(
    `select tamanho_arquivo, modificado_em, deslocamento_bytes, linhas_confirmadas, status, ultimo_erro
       from ${TABELA_CHECKPOINTS_IMPORTACAO}
      where conjunto = $1 and caminho_arquivo = $2`,
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
    fileSize: Number.parseInt(row.tamanho_arquivo, 10),
    fileMtime: new Date(row.modificado_em),
    byteOffset: Number.parseInt(row.deslocamento_bytes, 10),
    rowsCommitted: Number.parseInt(row.linhas_confirmadas, 10),
    status: row.status,
    lastError: row.ultimo_erro,
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
    `insert into ${TABELA_CHECKPOINTS_IMPORTACAO} (
        conjunto,
        caminho_arquivo,
        tamanho_arquivo,
        modificado_em,
        deslocamento_bytes,
        linhas_confirmadas,
        status,
        ultimo_erro,
        atualizado_em
      ) values ($1, $2, $3, $4, $5, $6, $7, $8, now())
      on conflict (conjunto, caminho_arquivo)
      do update set
        tamanho_arquivo = excluded.tamanho_arquivo,
        modificado_em = excluded.modificado_em,
        deslocamento_bytes = excluded.deslocamento_bytes,
        linhas_confirmadas = excluded.linhas_confirmadas,
        status = excluded.status,
        ultimo_erro = excluded.ultimo_erro,
        atualizado_em = now()`,
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
