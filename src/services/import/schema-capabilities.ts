import { Client } from "pg";

import type { ImportSchemaCapabilities } from "./types.js";

type ColumnCapabilityRow = {
  table_name: string;
  column_name: string;
  is_generated: string;
};

function canInsertIntoColumn(
  rows: readonly ColumnCapabilityRow[],
  tableName: string,
  columnName: string,
): boolean {
  const row = rows.find(
    (item) => item.table_name === tableName && item.column_name === columnName,
  );

  if (!row) {
    return false;
  }

  return row.is_generated.toUpperCase() !== "ALWAYS";
}

export async function detectImportSchemaCapabilities(
  client: Client,
): Promise<ImportSchemaCapabilities> {
  const result = await client.query<ColumnCapabilityRow>(
    `select table_name, column_name, is_generated
       from information_schema.columns
      where table_schema = current_schema()
        and (
          (table_name = 'establishments' and column_name = 'cnpj_full') or
          (table_name = 'partners' and column_name = 'partner_dedupe_key')
        )`,
  );

  return {
    includeEstablishmentCnpjFullInInsert: canInsertIntoColumn(
      result.rows,
      "establishments",
      "cnpj_full",
    ),
    includePartnerDedupeKeyInInsert: canInsertIntoColumn(
      result.rows,
      "partners",
      "partner_dedupe_key",
    ),
  };
}
