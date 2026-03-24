import { Client } from "pg";

import type {
  ImportDatasetPlan,
  ImportDatasetType,
  ImportPlanRecord,
  ImportPlanStatus,
} from "./types.js";

export async function ensureImportPlanTables(client: Client): Promise<void> {
  await client.query(`
    create table if not exists import_plans (
      id bigserial primary key,
      source_fingerprint text not null unique,
      input_path text not null,
      validated_path text not null,
      batch_size integer not null,
      target_database text not null,
      total_datasets integer not null,
      total_files integer not null,
      total_rows bigint not null,
      total_batches bigint not null,
      execution_order jsonb not null,
      status text not null default 'planned',
      created_at timestamptz not null default now(),
      updated_at timestamptz not null default now(),
      last_used_at timestamptz not null default now()
    )
  `);

  await client.query(`
    create table if not exists import_plan_files (
      id bigserial primary key,
      plan_id bigint not null references import_plans (id) on delete cascade,
      dataset text not null,
      dataset_index integer not null,
      file_index integer not null,
      file_path text not null,
      file_display_path text not null,
      file_size bigint not null,
      file_mtime timestamptz not null,
      total_rows bigint not null,
      total_batches bigint not null,
      unique (plan_id, file_path)
    )
  `);

  await client.query(
    `create index if not exists idx_import_plans_status on import_plans (status)`,
  );
  await client.query(
    `create index if not exists idx_import_plan_files_plan_id on import_plan_files (plan_id)`,
  );
  await client.query(
    `create index if not exists idx_import_plan_files_dataset on import_plan_files (dataset)`,
  );
}

type ImportPlanRow = {
  id: string;
  source_fingerprint: string;
  input_path: string;
  validated_path: string;
  batch_size: string;
  target_database: string;
  total_datasets: string;
  total_files: string;
  total_rows: string;
  total_batches: string;
  execution_order: ImportDatasetType[];
  status: ImportPlanStatus;
  created_at: Date;
  updated_at: Date;
  last_used_at: Date;
};

type ImportPlanFileRow = {
  dataset: ImportDatasetType;
  file_path: string;
  file_display_path: string;
  file_size: string;
  file_mtime: Date;
  total_rows: string;
  total_batches: string;
  dataset_index: string;
  file_index: string;
};

function mapImportPlanRow(row: ImportPlanRow): ImportPlanRecord {
  return {
    id: Number.parseInt(row.id, 10),
    sourceFingerprint: row.source_fingerprint,
    inputPath: row.input_path,
    validatedPath: row.validated_path,
    batchSize: Number.parseInt(row.batch_size, 10),
    targetDatabase: row.target_database,
    totalDatasets: Number.parseInt(row.total_datasets, 10),
    totalFiles: Number.parseInt(row.total_files, 10),
    totalRows: Number.parseInt(row.total_rows, 10),
    totalBatches: Number.parseInt(row.total_batches, 10),
    executionOrder: row.execution_order,
    status: row.status,
    createdAt: new Date(row.created_at),
    updatedAt: new Date(row.updated_at),
    lastUsedAt: new Date(row.last_used_at),
  };
}

export async function readSavedImportPlan(
  client: Client,
  sourceFingerprint: string,
): Promise<{
  plan: ImportPlanRecord;
  datasets: ImportDatasetPlan[];
} | null> {
  const planResult = await client.query<ImportPlanRow>(
    `select
        id,
        source_fingerprint,
        input_path,
        validated_path,
        batch_size,
        target_database,
        total_datasets,
        total_files,
        total_rows,
        total_batches,
        execution_order,
        status,
        created_at,
        updated_at,
        last_used_at
      from import_plans
      where source_fingerprint = $1`,
    [sourceFingerprint],
  );

  if (planResult.rowCount === 0) {
    return null;
  }

  const plan = mapImportPlanRow(planResult.rows[0]!);
  const filesResult = await client.query<ImportPlanFileRow>(
    `select
        dataset,
        file_path,
        file_display_path,
        file_size,
        file_mtime,
        total_rows,
        total_batches,
        dataset_index,
        file_index
      from import_plan_files
      where plan_id = $1
      order by dataset_index asc, file_index asc`,
    [plan.id],
  );

  const grouped = new Map<ImportDatasetType, ImportDatasetPlan>();

  for (const row of filesResult.rows) {
    const current = grouped.get(row.dataset) ?? {
      dataset: row.dataset,
      files: [],
      totalRows: 0,
      totalBatches: 0,
    };

    const fileTotalRows = Number.parseInt(row.total_rows, 10);
    const fileTotalBatches = Number.parseInt(row.total_batches, 10);

    current.files.push({
      dataset: row.dataset,
      absolutePath: row.file_path,
      displayPath: row.file_display_path,
      fileSize: Number.parseInt(row.file_size, 10),
      fileMtime: new Date(row.file_mtime),
      totalRows: fileTotalRows,
      totalBatches: fileTotalBatches,
    });
    current.totalRows += fileTotalRows;
    current.totalBatches += fileTotalBatches;
    grouped.set(row.dataset, current);
  }

  const datasets = plan.executionOrder
    .map((dataset) => grouped.get(dataset))
    .filter((item): item is ImportDatasetPlan => item !== undefined);

  await client.query(
    `update import_plans set last_used_at = now(), updated_at = now() where id = $1`,
    [plan.id],
  );

  return { plan, datasets };
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
      `select id from import_plans where source_fingerprint = $1`,
      [input.sourceFingerprint],
    );

    if ((existing.rowCount ?? 0) > 0) {
      await client.query(`delete from import_plan_files where plan_id = $1`, [
        existing.rows[0]!.id,
      ]);
    }

    const planResult = await client.query<ImportPlanRow>(
      `insert into import_plans (
          source_fingerprint,
          input_path,
          validated_path,
          batch_size,
          target_database,
          total_datasets,
          total_files,
          total_rows,
          total_batches,
          execution_order,
          status,
          created_at,
          updated_at,
          last_used_at
        ) values (
          $1, $2, $3, $4, $5, $6, $7, $8, $9, $10::jsonb, 'planned', now(), now(), now()
        )
        on conflict (source_fingerprint)
        do update set
          input_path = excluded.input_path,
          validated_path = excluded.validated_path,
          batch_size = excluded.batch_size,
          target_database = excluded.target_database,
          total_datasets = excluded.total_datasets,
          total_files = excluded.total_files,
          total_rows = excluded.total_rows,
          total_batches = excluded.total_batches,
          execution_order = excluded.execution_order,
          status = 'planned',
          updated_at = now(),
          last_used_at = now()
        returning
          id,
          source_fingerprint,
          input_path,
          validated_path,
          batch_size,
          target_database,
          total_datasets,
          total_files,
          total_rows,
          total_batches,
          execution_order,
          status,
          created_at,
          updated_at,
          last_used_at`,
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
          `insert into import_plan_files (
              plan_id,
              dataset,
              dataset_index,
              file_index,
              file_path,
              file_display_path,
              file_size,
              file_mtime,
              total_rows,
              total_batches
            ) values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            on conflict (plan_id, file_path)
            do update set
              dataset = excluded.dataset,
              dataset_index = excluded.dataset_index,
              file_index = excluded.file_index,
              file_display_path = excluded.file_display_path,
              file_size = excluded.file_size,
              file_mtime = excluded.file_mtime,
              total_rows = excluded.total_rows,
              total_batches = excluded.total_batches`,
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
    `update import_plans
        set status = $2,
            updated_at = now(),
            last_used_at = now()
      where id = $1`,
    [planId, status],
  );
}
