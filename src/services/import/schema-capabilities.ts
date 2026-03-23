import { Client } from "pg";

import type { ImportSchemaCapabilities } from "./types.js";

export async function detectImportSchemaCapabilities(
  client: Client,
): Promise<ImportSchemaCapabilities> {
  const result = await client.query<{ is_generated: string }>(
    `select is_generated
       from information_schema.columns
      where table_schema = current_schema()
        and table_name = 'partners'
        and column_name = 'partner_dedupe_key'`,
  );

  const generated = result.rows[0]?.is_generated?.toUpperCase() === "ALWAYS";

  return {
    includePartnerDedupeKeyInInsert: !generated,
  };
}
