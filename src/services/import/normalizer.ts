import type { TableLayout } from "../../dictionary/layouts/index.js";
import { getInsertColumns } from "./sql.js";
import { resolveImportWriteTarget } from "./targets.js";
import {
  extractSecondaryCnaes,
  normalizeFieldCount,
  transformRecord,
} from "./transform.js";
import type { ParsedImportSourceLine } from "./parser.js";
import type {
  BatchRow,
  ImportDatasetType,
  ImportSchemaCapabilities,
} from "./types.js";

export type NormalizeImportRowInput = {
  dataset: ImportDatasetType;
  filePath: string;
  layout: TableLayout;
  parsedLine: ParsedImportSourceLine;
  schemaCapabilities: ImportSchemaCapabilities;
  sourceRowNumber: number;
};

export function normalizeImportRow({
  dataset,
  filePath,
  layout,
  parsedLine,
  schemaCapabilities,
  sourceRowNumber,
}: NormalizeImportRowInput): BatchRow {
  const writeTarget = resolveImportWriteTarget(dataset);
  const columns = getInsertColumns(dataset, schemaCapabilities, writeTarget);
  const normalizedFields = normalizeFieldCount(
    parsedLine.fields,
    layout.fields.length,
    filePath,
    parsedLine.lineNumber,
  );
  const values = transformRecord(
    dataset,
    layout,
    normalizedFields,
    schemaCapabilities,
    writeTarget,
  );

  return {
    values,
    rawLine: parsedLine.rawLine,
    nextOffset: parsedLine.nextOffset,
    sourceRowNumber,
    secondaryRows:
      dataset === "establishments"
        ? extractSecondaryCnaes(values, columns)
        : [],
  };
}
