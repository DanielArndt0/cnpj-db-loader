import type { Client } from "pg";

import { flushRows, flushSecondaryCnaes } from "./sql.js";
import type {
  BatchRow,
  ImportDatasetType,
  ImportSchemaCapabilities,
  LookupCacheMap,
} from "./types.js";

export type ImportBatchWriteInput = {
  client: Client;
  lookupCache: LookupCacheMap;
  dataset: ImportDatasetType;
  rows: BatchRow[];
  schemaCapabilities: ImportSchemaCapabilities;
};

export type ImportBatchWriteResult = {
  writtenRows: number;
  writtenSecondaryRows: number;
};

export async function writeImportBatchToTarget({
  client,
  lookupCache,
  dataset,
  rows,
  schemaCapabilities,
}: ImportBatchWriteInput): Promise<ImportBatchWriteResult> {
  if (rows.length === 0) {
    return {
      writtenRows: 0,
      writtenSecondaryRows: 0,
    };
  }

  const batchValues = rows.map((row) => row.values);
  const secondaryRows = rows.flatMap((row) => row.secondaryRows);

  await flushRows(
    client,
    lookupCache,
    dataset,
    batchValues,
    schemaCapabilities,
  );
  await flushSecondaryCnaes(client, secondaryRows);

  return {
    writtenRows: rows.length,
    writtenSecondaryRows: secondaryRows.length,
  };
}

export async function writeImportRowToTarget(
  input: Omit<ImportBatchWriteInput, "rows"> & { row: BatchRow },
): Promise<ImportBatchWriteResult> {
  return writeImportBatchToTarget({
    ...input,
    rows: [input.row],
  });
}
