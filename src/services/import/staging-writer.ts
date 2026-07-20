import type { Client } from "pg";

import {
  COLUNA_CNPJ_COMPLETO,
  COLUNA_CODIGO_CNAE,
} from "../schema/table-names.js";
import { copyRowsToTable } from "./copy-from.js";
import {
  buildInsertQuery,
  buildSecondaryInsertQuery,
  flushInsertQuery,
  getConflictClause,
  getInsertColumns,
} from "./sql.js";
import {
  getSecondaryTargetTableName,
  getTargetTableName,
  resolveImportWriteTarget,
  usesStagingWriteTarget,
} from "./targets.js";
import type {
  BatchRow,
  ImportDatasetType,
  ImportSchemaCapabilities,
} from "./types.js";

export type ImportBatchWriteInput = {
  client: Client;
  dataset: ImportDatasetType;
  rows: BatchRow[];
  schemaCapabilities: ImportSchemaCapabilities;
};

export type ImportBatchWriteResult = {
  writtenRows: number;
  writtenSecondaryRows: number;
  writeTarget: "final" | "staging";
  writeMode: "copy" | "insert";
  targetTable: string;
};

async function writeBatchToFinalTarget(
  client: Client,
  dataset: ImportDatasetType,
  rows: BatchRow[],
  schemaCapabilities: ImportSchemaCapabilities,
): Promise<ImportBatchWriteResult> {
  const targetTable = getTargetTableName(dataset);
  const columns = getInsertColumns(dataset, schemaCapabilities, "final");
  const batchValues = rows.map((row) => row.values);
  const secondaryRows = rows.flatMap((row) => row.secondaryRows);

  await flushInsertQuery(
    client,
    buildInsertQuery(
      targetTable,
      columns,
      batchValues,
      getConflictClause(dataset, columns),
    ),
  );

  const secondaryTargetTable = getSecondaryTargetTableName(dataset);
  if (secondaryTargetTable && secondaryRows.length > 0) {
    await flushInsertQuery(
      client,
      buildSecondaryInsertQuery(
        secondaryTargetTable,
        secondaryRows,
        `on conflict (${COLUNA_CNPJ_COMPLETO}, ${COLUNA_CODIGO_CNAE}) do nothing`,
      ),
    );
  }

  return {
    writtenRows: rows.length,
    writtenSecondaryRows: secondaryRows.length,
    writeTarget: "final",
    writeMode: "insert",
    targetTable,
  };
}

async function writeBatchToStagingTarget(
  client: Client,
  dataset: ImportDatasetType,
  rows: BatchRow[],
  schemaCapabilities: ImportSchemaCapabilities,
): Promise<ImportBatchWriteResult> {
  const targetTable = getTargetTableName(dataset);
  const columns = getInsertColumns(dataset, schemaCapabilities, "staging");
  const batchValues = rows.map((row) => row.values);
  const secondaryRows = rows.flatMap((row) => row.secondaryRows);

  await copyRowsToTable(client, targetTable, columns, batchValues);

  const secondaryTargetTable = getSecondaryTargetTableName(dataset);
  if (secondaryTargetTable && secondaryRows.length > 0) {
    await copyRowsToTable(
      client,
      secondaryTargetTable,
      [COLUNA_CNPJ_COMPLETO, COLUNA_CODIGO_CNAE],
      secondaryRows.map((row) => [row[0], row[1]]),
    );
  }

  return {
    writtenRows: rows.length,
    writtenSecondaryRows: secondaryRows.length,
    writeTarget: "staging",
    writeMode: "copy",
    targetTable,
  };
}

async function writeRowInsertFallback(
  client: Client,
  dataset: ImportDatasetType,
  row: BatchRow,
  schemaCapabilities: ImportSchemaCapabilities,
): Promise<ImportBatchWriteResult> {
  const writeTarget = resolveImportWriteTarget(dataset);
  const targetTable = getTargetTableName(dataset);
  const columns = getInsertColumns(dataset, schemaCapabilities, writeTarget);
  const conflictClause =
    writeTarget === "final" ? getConflictClause(dataset, columns) : "";

  await flushInsertQuery(
    client,
    buildInsertQuery(targetTable, columns, [row.values], conflictClause),
  );

  const secondaryTargetTable = getSecondaryTargetTableName(dataset);
  if (secondaryTargetTable && row.secondaryRows.length > 0) {
    await flushInsertQuery(
      client,
      buildSecondaryInsertQuery(
        secondaryTargetTable,
        row.secondaryRows,
        writeTarget === "final"
          ? `on conflict (${COLUNA_CNPJ_COMPLETO}, ${COLUNA_CODIGO_CNAE}) do nothing`
          : "",
      ),
    );
  }

  return {
    writtenRows: 1,
    writtenSecondaryRows: row.secondaryRows.length,
    writeTarget,
    writeMode: "insert",
    targetTable,
  };
}

export async function writeImportBatchToTarget({
  client,
  dataset,
  rows,
  schemaCapabilities,
}: ImportBatchWriteInput): Promise<ImportBatchWriteResult> {
  if (rows.length === 0) {
    return {
      writtenRows: 0,
      writtenSecondaryRows: 0,
      writeTarget: resolveImportWriteTarget(dataset),
      writeMode: usesStagingWriteTarget(dataset) ? "copy" : "insert",
      targetTable: getTargetTableName(dataset),
    };
  }

  if (usesStagingWriteTarget(dataset)) {
    return writeBatchToStagingTarget(client, dataset, rows, schemaCapabilities);
  }

  return writeBatchToFinalTarget(client, dataset, rows, schemaCapabilities);
}

export async function writeImportRowToTarget(
  input: Omit<ImportBatchWriteInput, "rows"> & { row: BatchRow },
): Promise<ImportBatchWriteResult> {
  return writeRowInsertFallback(
    input.client,
    input.dataset,
    input.row,
    input.schemaCapabilities,
  );
}
