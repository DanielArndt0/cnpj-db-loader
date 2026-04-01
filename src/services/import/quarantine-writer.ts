import type { Client } from "pg";

import { ensureQuarantineTable, writeQuarantineRow } from "./quarantine.js";

export {
  ensureQuarantineTable as ensureImportQuarantineTable,
  writeQuarantineRow as writeImportQuarantineRow,
};

export async function ensureImportQuarantineSupport(
  client: Client,
): Promise<void> {
  await ensureQuarantineTable(client);
}
