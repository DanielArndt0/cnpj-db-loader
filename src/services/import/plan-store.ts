import { Client } from "pg";

import {
  TABELA_ARQUIVOS_PLANO_IMPORTACAO,
  TABELA_PLANOS_IMPORTACAO,
} from "../schema/table-names.js";
import { ensureTableShape } from "./schema-validation.js";
import type {
  ImportDatasetPlan,
  ImportDatasetType,
  ImportPhaseStatus,
  ImportPlanRecord,
  ImportPlanStatus,
} from "./types.js";

const PLAN_SCHEMA_HELP_MESSAGE =
  'O schema de plano de importação é obrigatório. Rode "cnpj-db-loader schema generate --profile full" e aplique o SQL antes de importar.';

export async function ensureImportPlanTables(client: Client): Promise<void> {
  await ensureTableShape(client, {
    tableName: TABELA_PLANOS_IMPORTACAO,
    requiredColumns: [
      "impressao_digital_origem",
      "caminho_entrada",
      "caminho_validado",
      "tamanho_lote",
      "banco_destino",
      "total_conjuntos",
      "total_arquivos",
      "total_linhas",
      "total_lotes",
      "ordem_execucao",
      "status",
      "status_carga",
      "status_materializacao",
      "ultima_fase",
      "ultimo_erro",
      "criado_em",
      "atualizado_em",
      "ultimo_uso_em",
    ],
    helpMessage: PLAN_SCHEMA_HELP_MESSAGE,
  });

  await ensureTableShape(client, {
    tableName: TABELA_ARQUIVOS_PLANO_IMPORTACAO,
    requiredColumns: [
      "plano_id",
      "conjunto",
      "indice_conjunto",
      "indice_arquivo",
      "caminho_arquivo",
      "caminho_exibicao_arquivo",
      "tamanho_arquivo",
      "modificado_em",
      "total_linhas",
      "total_lotes",
    ],
    helpMessage: PLAN_SCHEMA_HELP_MESSAGE,
  });
}

type ImportPlanRow = {
  id: string;
  impressao_digital_origem: string;
  caminho_entrada: string;
  caminho_validado: string;
  tamanho_lote: string;
  banco_destino: string;
  total_conjuntos: string;
  total_arquivos: string;
  total_linhas: string;
  total_lotes: string;
  ordem_execucao: ImportDatasetType[];
  status: ImportPlanStatus;
  status_carga: ImportPhaseStatus;
  status_materializacao: ImportPhaseStatus;
  ultima_fase: string | null;
  ultimo_erro: string | null;
  criado_em: Date;
  atualizado_em: Date;
  ultimo_uso_em: Date;
};

type ImportPlanFileRow = {
  conjunto: ImportDatasetType;
  caminho_arquivo: string;
  caminho_exibicao_arquivo: string;
  tamanho_arquivo: string;
  modificado_em: Date;
  total_linhas: string;
  total_lotes: string;
  indice_conjunto: string;
  indice_arquivo: string;
};

function mapImportPlanRow(row: ImportPlanRow): ImportPlanRecord {
  return {
    id: Number.parseInt(row.id, 10),
    sourceFingerprint: row.impressao_digital_origem,
    inputPath: row.caminho_entrada,
    validatedPath: row.caminho_validado,
    batchSize: Number.parseInt(row.tamanho_lote, 10),
    targetDatabase: row.banco_destino,
    totalDatasets: Number.parseInt(row.total_conjuntos, 10),
    totalFiles: Number.parseInt(row.total_arquivos, 10),
    totalRows: Number.parseInt(row.total_linhas, 10),
    totalBatches: Number.parseInt(row.total_lotes, 10),
    executionOrder: row.ordem_execucao,
    status: row.status,
    loadStatus: row.status_carga,
    materializationStatus: row.status_materializacao,
    lastPhase: row.ultima_fase,
    lastError: row.ultimo_erro,
    createdAt: new Date(row.criado_em),
    updatedAt: new Date(row.atualizado_em),
    lastUsedAt: new Date(row.ultimo_uso_em),
  };
}

async function readPlanDatasets(
  client: Client,
  plan: ImportPlanRecord,
): Promise<ImportDatasetPlan[]> {
  const filesResult = await client.query<ImportPlanFileRow>(
    `select
        conjunto,
        caminho_arquivo,
        caminho_exibicao_arquivo,
        tamanho_arquivo,
        modificado_em,
        total_linhas,
        total_lotes,
        indice_conjunto,
        indice_arquivo
      from ${TABELA_ARQUIVOS_PLANO_IMPORTACAO}
      where plano_id = $1
      order by indice_conjunto asc, indice_arquivo asc`,
    [plan.id],
  );

  const grouped = new Map<ImportDatasetType, ImportDatasetPlan>();

  for (const row of filesResult.rows) {
    const current = grouped.get(row.conjunto) ?? {
      dataset: row.conjunto,
      files: [],
      totalRows: 0,
      totalBatches: 0,
    };

    const fileTotalRows = Number.parseInt(row.total_linhas, 10);
    const fileTotalBatches = Number.parseInt(row.total_lotes, 10);

    current.files.push({
      dataset: row.conjunto,
      absolutePath: row.caminho_arquivo,
      displayPath: row.caminho_exibicao_arquivo,
      fileSize: Number.parseInt(row.tamanho_arquivo, 10),
      fileMtime: new Date(row.modificado_em),
      totalRows: fileTotalRows,
      totalBatches: fileTotalBatches,
    });
    current.totalRows += fileTotalRows;
    current.totalBatches += fileTotalBatches;
    grouped.set(row.conjunto, current);
  }

  return plan.executionOrder
    .map((dataset) => grouped.get(dataset))
    .filter((item): item is ImportDatasetPlan => item !== undefined);
}

async function readPlanRecordByQuery(
  client: Client,
  query: string,
  values: readonly unknown[],
): Promise<{
  plan: ImportPlanRecord;
  datasets: ImportDatasetPlan[];
} | null> {
  const planResult = await client.query<ImportPlanRow>(query, [...values]);

  if (planResult.rowCount === 0) {
    return null;
  }

  const plan = mapImportPlanRow(planResult.rows[0]!);
  const datasets = await readPlanDatasets(client, plan);

  await client.query(
    `update ${TABELA_PLANOS_IMPORTACAO} set ultimo_uso_em = now(), atualizado_em = now() where id = $1`,
    [plan.id],
  );

  return { plan, datasets };
}

const IMPORT_PLAN_SELECT_COLUMNS = `
        id,
        impressao_digital_origem,
        caminho_entrada,
        caminho_validado,
        tamanho_lote,
        banco_destino,
        total_conjuntos,
        total_arquivos,
        total_linhas,
        total_lotes,
        ordem_execucao,
        status,
        status_carga,
        status_materializacao,
        ultima_fase,
        ultimo_erro,
        criado_em,
        atualizado_em,
        ultimo_uso_em`;

export async function readSavedImportPlan(
  client: Client,
  sourceFingerprint: string,
): Promise<{
  plan: ImportPlanRecord;
  datasets: ImportDatasetPlan[];
} | null> {
  return readPlanRecordByQuery(
    client,
    `select ${IMPORT_PLAN_SELECT_COLUMNS}
      from ${TABELA_PLANOS_IMPORTACAO}
      where impressao_digital_origem = $1`,
    [sourceFingerprint],
  );
}

export async function readLatestImportPlanForValidatedPath(
  client: Client,
  validatedPath: string,
  targetDatabase: string,
): Promise<{
  plan: ImportPlanRecord;
  datasets: ImportDatasetPlan[];
} | null> {
  return readPlanRecordByQuery(
    client,
    `select ${IMPORT_PLAN_SELECT_COLUMNS}
      from ${TABELA_PLANOS_IMPORTACAO}
      where caminho_validado = $1
        and banco_destino = $2
      order by ultimo_uso_em desc, atualizado_em desc, id desc
      limit 1`,
    [validatedPath, targetDatabase],
  );
}

export async function saveImportPlan(
  client: Client,
  input: {
    sourceFingerprint: string;
    inputPath: string;
    validatedPath: string;
    batchSize: number;
    targetDatabase: string;
    datasets: ImportDatasetPlan[];
    totalFiles: number;
    totalRows: number;
    totalBatches: number;
  },
): Promise<ImportPlanRecord> {
  await client.query("begin");
  try {
    const existing = await client.query<{ id: string }>(
      `select id from ${TABELA_PLANOS_IMPORTACAO} where impressao_digital_origem = $1`,
      [input.sourceFingerprint],
    );

    if ((existing.rowCount ?? 0) > 0) {
      await client.query(
        `delete from ${TABELA_ARQUIVOS_PLANO_IMPORTACAO} where plano_id = $1`,
        [existing.rows[0]!.id],
      );
    }

    const planResult = await client.query<ImportPlanRow>(
      `insert into ${TABELA_PLANOS_IMPORTACAO} (
          impressao_digital_origem,
          caminho_entrada,
          caminho_validado,
          tamanho_lote,
          banco_destino,
          total_conjuntos,
          total_arquivos,
          total_linhas,
          total_lotes,
          ordem_execucao,
          status,
          status_carga,
          status_materializacao,
          ultima_fase,
          ultimo_erro,
          criado_em,
          atualizado_em,
          ultimo_uso_em
        ) values (
          $1, $2, $3, $4, $5, $6, $7, $8, $9, $10::jsonb, 'planned', 'pending', 'pending', 'planning', null, now(), now(), now()
        )
        on conflict (impressao_digital_origem)
        do update set
          caminho_entrada = excluded.caminho_entrada,
          caminho_validado = excluded.caminho_validado,
          tamanho_lote = excluded.tamanho_lote,
          banco_destino = excluded.banco_destino,
          total_conjuntos = excluded.total_conjuntos,
          total_arquivos = excluded.total_arquivos,
          total_linhas = excluded.total_linhas,
          total_lotes = excluded.total_lotes,
          ordem_execucao = excluded.ordem_execucao,
          status = 'planned',
          status_carga = 'pending',
          status_materializacao = 'pending',
          ultima_fase = 'planning',
          ultimo_erro = null,
          atualizado_em = now(),
          ultimo_uso_em = now()
        returning ${IMPORT_PLAN_SELECT_COLUMNS}`,
      [
        input.sourceFingerprint,
        input.inputPath,
        input.validatedPath,
        input.batchSize,
        input.targetDatabase,
        input.datasets.length,
        input.totalFiles,
        input.totalRows,
        input.totalBatches,
        JSON.stringify(input.datasets.map((item) => item.dataset)),
      ],
    );

    const plan = mapImportPlanRow(planResult.rows[0]!);

    let fileIndex = 0;
    for (const [datasetIndex, datasetPlan] of input.datasets.entries()) {
      for (const filePlan of datasetPlan.files) {
        fileIndex += 1;
        await client.query(
          `insert into ${TABELA_ARQUIVOS_PLANO_IMPORTACAO} (
              plano_id,
              conjunto,
              indice_conjunto,
              indice_arquivo,
              caminho_arquivo,
              caminho_exibicao_arquivo,
              tamanho_arquivo,
              modificado_em,
              total_linhas,
              total_lotes
            ) values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            on conflict (plano_id, caminho_arquivo)
            do update set
              conjunto = excluded.conjunto,
              indice_conjunto = excluded.indice_conjunto,
              indice_arquivo = excluded.indice_arquivo,
              caminho_exibicao_arquivo = excluded.caminho_exibicao_arquivo,
              tamanho_arquivo = excluded.tamanho_arquivo,
              modificado_em = excluded.modificado_em,
              total_linhas = excluded.total_linhas,
              total_lotes = excluded.total_lotes`,
          [
            plan.id,
            datasetPlan.dataset,
            datasetIndex + 1,
            fileIndex,
            filePlan.absolutePath,
            filePlan.displayPath,
            filePlan.fileSize,
            filePlan.fileMtime,
            filePlan.totalRows,
            filePlan.totalBatches,
          ],
        );
      }
    }

    await client.query("commit");
    return plan;
  } catch (error) {
    await client.query("rollback");
    throw error;
  }
}

export async function updateImportPlanStatus(
  client: Client,
  planId: number,
  status: ImportPlanStatus,
): Promise<void> {
  await client.query(
    `update ${TABELA_PLANOS_IMPORTACAO}
        set status = $2,
            atualizado_em = now(),
            ultimo_uso_em = now()
      where id = $1`,
    [planId, status],
  );
}

export async function updateImportPlanPhaseState(
  client: Client,
  input: {
    planId: number;
    loadStatus?: ImportPhaseStatus;
    materializationStatus?: ImportPhaseStatus;
    lastPhase?: string | null;
    lastError?: string | null;
  },
): Promise<void> {
  const assignments: string[] = [
    "atualizado_em = now()",
    "ultimo_uso_em = now()",
  ];
  const values: unknown[] = [input.planId];
  let nextIndex = 2;

  if (input.loadStatus !== undefined) {
    assignments.push(`status_carga = $${nextIndex}`);
    values.push(input.loadStatus);
    nextIndex += 1;
  }

  if (input.materializationStatus !== undefined) {
    assignments.push(`status_materializacao = $${nextIndex}`);
    values.push(input.materializationStatus);
    nextIndex += 1;
  }

  if (input.lastPhase !== undefined) {
    assignments.push(`ultima_fase = $${nextIndex}`);
    values.push(input.lastPhase);
    nextIndex += 1;
  }

  if (input.lastError !== undefined) {
    assignments.push(`ultimo_erro = $${nextIndex}`);
    values.push(input.lastError);
    nextIndex += 1;
  }

  await client.query(
    `update ${TABELA_PLANOS_IMPORTACAO} set ${assignments.join(", ")} where id = $1`,
    values,
  );
}
