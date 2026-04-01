import type { Client } from "pg";

import { ValidationError } from "../../core/errors/index.js";

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

async function readColumnNames(
  client: Client,
  tableName: string,
): Promise<Set<string>> {
  const result = await client.query<{ column_name: string }>(
    `select column_name
       from information_schema.columns
      where table_schema = current_schema()
        and table_name = $1`,
    [tableName],
  );

  return new Set(result.rows.map((row) => row.column_name));
}

export async function ensureTableShape(
  client: Client,
  input: {
    tableName: string;
    requiredColumns: readonly string[];
    helpMessage: string;
  },
): Promise<void> {
  const exists = await tableExists(client, input.tableName);

  if (!exists) {
    throw new ValidationError(
      `${input.helpMessage} Missing table: ${input.tableName}.`,
    );
  }

  const availableColumns = await readColumnNames(client, input.tableName);
  const missingColumns = input.requiredColumns.filter(
    (columnName) => !availableColumns.has(columnName),
  );

  if (missingColumns.length > 0) {
    throw new ValidationError(
      `${input.helpMessage} Table ${input.tableName} is missing required columns: ${missingColumns.join(", ")}.`,
    );
  }
}
