import type { Client } from "pg";

import { ValidationError } from "../../core/errors/index.js";
import type { ImportDatasetType } from "./types.js";
import { collectRequiredStagingTables } from "./targets.js";

const LEGACY_STAGING_TABLES = [
  "staging_establishment_secondary_cnaes",
] as const;

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

async function collectResettableStagingTables(
  client: Client,
  datasets: readonly ImportDatasetType[],
): Promise<string[]> {
  const tableNames = new Set(collectRequiredStagingTables(datasets));

  if (datasets.includes("establishments")) {
    for (const tableName of LEGACY_STAGING_TABLES) {
      if (await tableExists(client, tableName)) {
        tableNames.add(tableName);
      }
    }
  }

  return [...tableNames];
}

export async function ensureStagingSchemaSupport(
  client: Client,
  datasets: readonly ImportDatasetType[],
): Promise<void> {
  const requiredTables = collectRequiredStagingTables(datasets);

  if (requiredTables.length === 0) {
    return;
  }

  const missingTables: string[] = [];
  const invalidTables: string[] = [];

  for (const tableName of requiredTables) {
    if (!(await tableExists(client, tableName))) {
      missingTables.push(tableName);
      continue;
    }

    const stagingIdResult = await client.query<{ exists: boolean }>(
      `select exists (
         select 1
           from information_schema.columns
          where table_schema = current_schema()
            and table_name = $1
            and column_name = 'staging_id'
       ) as exists`,
      [tableName],
    );

    if (!stagingIdResult.rows[0]?.exists) {
      invalidTables.push(tableName);
    }
  }

  if (missingTables.length > 0) {
    throw new ValidationError(
      `The staging schema is required for the selected bulk-load datasets. Missing tables: ${missingTables.join(", ")}. Run "cnpj-db-loader schema generate --profile full" or "cnpj-db-loader schema generate --profile staging" and apply the SQL before importing.`,
    );
  }

  if (invalidTables.length > 0) {
    throw new ValidationError(
      `The staging schema is outdated for chunked materialization. Recreate or migrate these tables so they include the staging_id column: ${invalidTables.join(", ")}.`,
    );
  }
}

export async function resetStagingTablesForFreshPlan(
  client: Client,
  datasets: readonly ImportDatasetType[],
): Promise<string[]> {
  const tableNames = await collectResettableStagingTables(client, datasets);

  if (tableNames.length === 0) {
    return [];
  }

  await client.query(`truncate ${tableNames.join(", ")}`);
  return tableNames;
}
