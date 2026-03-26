import type { Client } from "pg";

import { ValidationError } from "../../core/errors/index.js";
import type { ImportDatasetType } from "./types.js";
import { collectRequiredStagingTables } from "./targets.js";

export async function ensureStagingSchemaSupport(
  client: Client,
  datasets: readonly ImportDatasetType[],
): Promise<void> {
  const requiredTables = collectRequiredStagingTables(datasets);

  if (requiredTables.length === 0) {
    return;
  }

  const missingTables: string[] = [];

  for (const tableName of requiredTables) {
    const result = await client.query<{ exists: string | null }>(
      "select to_regclass(current_schema() || '.' || $1) as exists",
      [tableName],
    );

    if (!result.rows[0]?.exists) {
      missingTables.push(tableName);
    }
  }

  if (missingTables.length > 0) {
    throw new ValidationError(
      `The staging schema is required for the selected bulk-load datasets. Missing tables: ${missingTables.join(", ")}. Run "cnpj-db-loader schema generate --profile full" or "cnpj-db-loader schema generate --profile staging" and apply the SQL before importing.`,
    );
  }
}

export async function resetStagingTablesForFreshPlan(
  client: Client,
  datasets: readonly ImportDatasetType[],
): Promise<string[]> {
  const tableNames = collectRequiredStagingTables(datasets);

  if (tableNames.length === 0) {
    return [];
  }

  await client.query(`truncate ${tableNames.join(", ")}`);
  return tableNames;
}
