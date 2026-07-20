import type { Client } from "pg";

import { ValidationError } from "../../core/errors/index.js";
import type { ImportDatasetType } from "../import/types.js";
import { ensureCheckpointTable } from "../import/checkpoints.js";
import { ensureMaterializationCheckpointTable } from "../import/materialization-checkpoints.js";
import { ensureImportPlanTables } from "../import/plan-store.js";
import { resetStagingTablesForFreshPlan } from "../import/staging-schema.js";
import { isImportDatasetType, maskDatabaseLabel } from "../import/types.js";
import {
  TABELA_EMPRESAS,
  TABELA_ESTABELECIMENTO_CNAES_SECUNDARIOS,
  TABELA_ESTABELECIMENTOS,
  TABELA_SIMPLES,
  TABELA_SOCIOS,
} from "../schema/table-names.js";

export type DatabaseCleanupScope =
  | "staging"
  | "materialized"
  | "checkpoints"
  | "plans";

export type CheckpointCleanupPhase = "load" | "materialization" | "all";

export type DatabaseCleanupSummary = {
  scope: DatabaseCleanupScope;
  targetDatabase: string;
  dataset?: ImportDatasetType | undefined;
  phase?: CheckpointCleanupPhase | undefined;
  validatedPath?: string | undefined;
  planId?: number | undefined;
  truncatedTables: string[];
  deletedLoadCheckpoints: number;
  deletedMaterializationCheckpoints: number;
  deletedPlans: number;
  notes: string[];
};

const MATERIALIZED_DATASET_TABLES: Readonly<
  Partial<Record<ImportDatasetType, readonly string[]>>
> = {
  companies: [
    TABELA_SIMPLES,
    TABELA_SOCIOS,
    TABELA_ESTABELECIMENTO_CNAES_SECUNDARIOS,
    TABELA_ESTABELECIMENTOS,
    TABELA_EMPRESAS,
  ],
  establishments: [
    TABELA_ESTABELECIMENTO_CNAES_SECUNDARIOS,
    TABELA_ESTABELECIMENTOS,
  ],
  partners: [TABELA_SOCIOS],
  simples_options: [TABELA_SIMPLES],
};

function assertSupportedCleanupDataset(
  dataset: string | undefined,
  scopes: readonly DatabaseCleanupScope[],
): asserts dataset is ImportDatasetType | undefined {
  if (dataset === undefined) {
    return;
  }

  if (!isImportDatasetType(dataset)) {
    throw new ValidationError(`Tipo de dataset não suportado: ${dataset}.`);
  }

  if (scopes.includes("staging") || scopes.includes("materialized")) {
    const supported = Object.keys(
      MATERIALIZED_DATASET_TABLES,
    ) as ImportDatasetType[];
    if (!supported.includes(dataset)) {
      throw new ValidationError(
        `O dataset ${dataset} não é suportado para este escopo de limpeza. Datasets suportados: ${supported.join(", ")}.`,
      );
    }
  }
}

function createBaseSummary(
  input: Pick<
    DatabaseCleanupSummary,
    | "scope"
    | "targetDatabase"
    | "dataset"
    | "phase"
    | "validatedPath"
    | "planId"
  >,
): DatabaseCleanupSummary {
  return {
    ...input,
    truncatedTables: [],
    deletedLoadCheckpoints: 0,
    deletedMaterializationCheckpoints: 0,
    deletedPlans: 0,
    notes: [],
  };
}

function collectMaterializedTables(dataset?: ImportDatasetType): string[] {
  if (dataset) {
    return [...(MATERIALIZED_DATASET_TABLES[dataset] ?? [])];
  }

  const orderedTables = new Set<string>();
  for (const tableNames of Object.values(MATERIALIZED_DATASET_TABLES)) {
    for (const tableName of tableNames ?? []) {
      orderedTables.add(tableName);
    }
  }

  return [...orderedTables];
}

async function tableExists(
  client: Client,
  tableName: string,
): Promise<boolean> {
  const result = await client.query<{ exists: string | null }>(
    "select to_regclass(current_schema() || '.' || $1) as exists",
    [tableName],
  );

  return Boolean(result.rows[0]?.exists);
}

async function filterExistingTables(
  client: Client,
  tableNames: readonly string[],
): Promise<string[]> {
  const existingTables: string[] = [];

  for (const tableName of tableNames) {
    if (await tableExists(client, tableName)) {
      existingTables.push(tableName);
    }
  }

  return existingTables;
}

async function deleteLoadCheckpoints(
  client: Client,
  dataset?: ImportDatasetType,
): Promise<number> {
  await ensureCheckpointTable(client);

  const result = dataset
    ? await client.query(
        `delete from checkpoints_importacao where conjunto = $1`,
        [dataset],
      )
    : await client.query(`delete from checkpoints_importacao`);

  return result.rowCount ?? 0;
}

async function resolvePlanIdsForCleanup(
  client: Client,
  input: {
    targetDatabase: string;
    planId?: number | undefined;
    validatedPath?: string | undefined;
  },
): Promise<number[]> {
  await ensureImportPlanTables(client);

  if (input.planId !== undefined) {
    return [input.planId];
  }

  if (!input.validatedPath) {
    const result = await client.query<{ id: string }>(
      `select id from planos_importacao order by id asc`,
    );
    return result.rows.map((row) => Number.parseInt(row.id, 10));
  }

  const result = await client.query<{ id: string }>(
    `select id
       from planos_importacao
      where caminho_validado = $1
        and banco_destino = $2
      order by id asc`,
    [input.validatedPath, input.targetDatabase],
  );

  return result.rows.map((row) => Number.parseInt(row.id, 10));
}

async function deleteMaterializationCheckpoints(
  client: Client,
  input: {
    targetDatabase: string;
    planId?: number | undefined;
    validatedPath?: string | undefined;
    dataset?: ImportDatasetType | undefined;
  },
): Promise<number> {
  await ensureMaterializationCheckpointTable(client);
  const planIds = await resolvePlanIdsForCleanup(client, input);

  if (planIds.length === 0) {
    return 0;
  }

  if (input.dataset) {
    const result = await client.query(
      `delete from checkpoints_materializacao
        where plano_id = any($1::bigint[])
          and conjunto = $2`,
      [planIds, input.dataset],
    );
    return result.rowCount ?? 0;
  }

  const result = await client.query(
    `delete from checkpoints_materializacao
      where plano_id = any($1::bigint[])`,
    [planIds],
  );
  return result.rowCount ?? 0;
}

async function deleteImportPlans(
  client: Client,
  input: {
    targetDatabase: string;
    planId?: number | undefined;
    validatedPath?: string | undefined;
  },
): Promise<number> {
  await ensureImportPlanTables(client);

  if (input.planId !== undefined) {
    const result = await client.query(
      `delete from planos_importacao where id = $1`,
      [input.planId],
    );
    return result.rowCount ?? 0;
  }

  if (input.validatedPath) {
    const result = await client.query(
      `delete from planos_importacao
        where caminho_validado = $1
          and banco_destino = $2`,
      [input.validatedPath, input.targetDatabase],
    );
    return result.rowCount ?? 0;
  }

  const result = await client.query(`delete from planos_importacao`);
  return result.rowCount ?? 0;
}

export async function cleanupDatabaseStaging(
  client: Client,
  input: {
    dbUrl: string;
    dataset?: ImportDatasetType | undefined;
    validatedPath?: string | undefined;
  },
): Promise<DatabaseCleanupSummary> {
  assertSupportedCleanupDataset(input.dataset, ["staging"]);

  const targetDatabase = maskDatabaseLabel(input.dbUrl);
  const summary = createBaseSummary({
    scope: "staging",
    targetDatabase,
    dataset: input.dataset,
    validatedPath: input.validatedPath,
  });

  const datasets: ImportDatasetType[] = input.dataset
    ? [input.dataset]
    : ["companies", "establishments", "partners", "simples_options"];

  summary.truncatedTables = await resetStagingTablesForFreshPlan(
    client,
    datasets,
  );

  if (input.validatedPath) {
    summary.deletedMaterializationCheckpoints =
      await deleteMaterializationCheckpoints(client, {
        targetDatabase,
        validatedPath: input.validatedPath,
        dataset: input.dataset,
      });
    summary.notes.push(
      "Materialization checkpoints for the selected validated path were cleared so the next materialization can restart cleanly.",
    );
  }

  return summary;
}

export async function cleanupDatabaseMaterializedTables(
  client: Client,
  input: {
    dbUrl: string;
    dataset?: ImportDatasetType | undefined;
  },
): Promise<DatabaseCleanupSummary> {
  assertSupportedCleanupDataset(input.dataset, ["materialized"]);

  const targetDatabase = maskDatabaseLabel(input.dbUrl);
  const summary = createBaseSummary({
    scope: "materialized",
    targetDatabase,
    dataset: input.dataset,
  });

  const tableNames = await filterExistingTables(
    client,
    collectMaterializedTables(input.dataset),
  );
  if (tableNames.length > 0) {
    await client.query(`truncate ${tableNames.join(", ")}`);
  }
  summary.truncatedTables = tableNames;
  summary.notes.push(
    "Simplified final materialized tables were truncated in safe order for the current schema. Clear materialization checkpoints separately if you want the import plan to rebuild these tables from scratch.",
  );

  return summary;
}

export async function cleanupDatabaseCheckpoints(
  client: Client,
  input: {
    dbUrl: string;
    phase: CheckpointCleanupPhase;
    dataset?: ImportDatasetType | undefined;
    validatedPath?: string | undefined;
    planId?: number | undefined;
  },
): Promise<DatabaseCleanupSummary> {
  assertSupportedCleanupDataset(input.dataset, []);

  if (input.planId !== undefined && input.planId <= 0) {
    throw new ValidationError("O id do plano deve ser um inteiro positivo.");
  }

  const targetDatabase = maskDatabaseLabel(input.dbUrl);
  const summary = createBaseSummary({
    scope: "checkpoints",
    targetDatabase,
    dataset: input.dataset,
    phase: input.phase,
    validatedPath: input.validatedPath,
    planId: input.planId,
  });

  if (input.phase === "load" || input.phase === "all") {
    summary.deletedLoadCheckpoints = await deleteLoadCheckpoints(
      client,
      input.dataset,
    );
  }

  if (input.phase === "materialization" || input.phase === "all") {
    summary.deletedMaterializationCheckpoints =
      await deleteMaterializationCheckpoints(client, {
        targetDatabase,
        planId: input.planId,
        validatedPath: input.validatedPath,
        dataset: input.dataset,
      });
  }

  if (
    input.phase !== "load" &&
    input.planId === undefined &&
    input.validatedPath === undefined
  ) {
    summary.notes.push(
      "Without --plan-id or --validated-path, materialization checkpoint cleanup affects all saved import plans for this database.",
    );
  }

  return summary;
}

export async function cleanupDatabasePlans(
  client: Client,
  input: {
    dbUrl: string;
    validatedPath?: string | undefined;
    planId?: number | undefined;
  },
): Promise<DatabaseCleanupSummary> {
  if (input.planId !== undefined && input.planId <= 0) {
    throw new ValidationError("O id do plano deve ser um inteiro positivo.");
  }

  const targetDatabase = maskDatabaseLabel(input.dbUrl);
  const summary = createBaseSummary({
    scope: "plans",
    targetDatabase,
    validatedPath: input.validatedPath,
    planId: input.planId,
  });

  summary.deletedPlans = await deleteImportPlans(client, {
    targetDatabase,
    validatedPath: input.validatedPath,
    planId: input.planId,
  });
  summary.notes.push(
    "Deleting import plans also removes linked plan files and materialization checkpoints through database cascades.",
  );

  return summary;
}
