import { createReadStream } from "node:fs";
import { stat } from "node:fs/promises";
import path from "node:path";

import { Client } from "pg";

import { ValidationError } from "../core/errors/index.js";
import type { TableLayout } from "../dictionary/layouts/index.js";
import {
  citiesLayout,
  cnaesLayout,
  companiesLayout,
  countriesLayout,
  establishmentsLayout,
  legalNaturesLayout,
  partnerQualificationsLayout,
  partnersLayout,
  reasonsLayout,
  simplesLayout,
} from "../dictionary/layouts/index.js";
import { resolveDbUrl } from "./db.service.js";
import type { DatasetType, FileInspection } from "./inspect.service.js";
import { inspectFiles } from "./inspect.service.js";
import { validateInputDirectory } from "./validate.service.js";
import { appendJsonLinesLog, createJsonLinesLog } from "./logging.service.js";

type ImportDatasetType = Exclude<DatasetType, "zip-archive" | "unknown">;

type ImportCheckpointStatus =
  | "pending"
  | "in_progress"
  | "completed"
  | "failed";

type ImportCheckpointRecord = {
  dataset: ImportDatasetType;
  filePath: string;
  fileSize: number;
  fileMtime: Date;
  byteOffset: number;
  rowsCommitted: number;
  status: ImportCheckpointStatus;
  lastError?: string | null;
};

type ImportFilePlan = {
  dataset: ImportDatasetType;
  entry: FileInspection;
  absolutePath: string;
  displayPath: string;
  fileSize: number;
  fileMtime: Date;
  totalRows: number;
  totalBatches: number;
  checkpoint?: ImportCheckpointRecord;
};

type SanitizationAction = "remove_nul_bytes";

type ImportErrorCategory =
  | "invalid_utf8_sequence"
  | "not_null_violation"
  | "foreign_key_violation"
  | "invalid_field_count"
  | "parse_error"
  | "transform_error"
  | "database_error"
  | "unknown";

type QuarantineStage =
  | "parse"
  | "transform"
  | "row_insert"
  | "row_retry"
  | "batch_insert";

type ClassifiedImportError = {
  code: string;
  message: string;
  category: ImportErrorCategory;
  recoverable: boolean;
  canRetryLater: boolean;
};

type BatchRow = {
  values: unknown[];
  rawLine: string;
  nextOffset: number;
  sourceRowNumber: number;
  secondaryRows: Array<[string, string, number]>;
  sanitizationsApplied: SanitizationAction[];
};

type ImportDatasetPlan = {
  dataset: ImportDatasetType;
  files: ImportFilePlan[];
  totalRows: number;
  totalBatches: number;
};

type ImportSchemaCapabilities = {
  includePartnerDedupeKeyInInsert: boolean;
};

export type ImportProgressEvent =
  | {
      kind: "preparing_start";
      inputPath: string;
      validatedPath: string;
      totalDatasets: number;
      totalFiles: number;
      batchSize: number;
      targetDatabase: string;
    }
  | {
      kind: "preparing_progress";
      scannedFiles: number;
      totalFiles: number;
      countedRows: number;
      currentFileDisplayPath: string;
    }
  | {
      kind: "plan_ready";
      totalDatasets: number;
      totalFiles: number;
      batchSize: number;
      totalRows: number;
      totalBatches: number;
      targetDatabase: string;
      executionOrder: ImportDatasetType[];
    }
  | {
      kind: "start";
      inputPath: string;
      validatedPath: string;
      totalDatasets: number;
      totalFiles: number;
      targetDatabase: string;
      totalRows: number;
      totalBatches: number;
      committedRows: number;
      committedBatches: number;
    }
  | {
      kind: "progress";
      dataset: ImportDatasetType;
      datasetIndex: number;
      totalDatasets: number;
      currentFilePath: string;
      currentFileDisplayPath: string;
      fileIndex: number;
      completedFiles: number;
      totalFiles: number;
      currentFileRowsCommitted: number;
      currentFileRowsTotal: number;
      committedRows: number;
      committedBatches: number;
      totalBatches: number;
      currentBatch: number;
      batchSize: number;
      checkpointOffset: number;
      currentFileSize: number;
      verboseProgress: boolean;
    }
  | {
      kind: "finish";
      totalDatasets: number;
      totalFiles: number;
      completedFiles: number;
      processedRows: number;
      totalRows: number;
      committedBatches: number;
      totalBatches: number;
      secondaryCnaesRows: number;
      quarantinedRows: number;
      sanitizedRows: number;
    };

export type ImportProgressListener = (event: ImportProgressEvent) => void;

export type ImportOptions = {
  dbUrl?: string;
  dataset?: ImportDatasetType | undefined;
  batchSize?: number | undefined;
  verboseProgress?: boolean | undefined;
  onProgress?: ImportProgressListener | undefined;
};

export type ImportSummary = {
  inputPath: string;
  validatedPath: string;
  targetDatabase: string;
  importedDatasets: ImportDatasetType[];
  importedFiles: number;
  processedRows: number;
  plannedRows: number;
  committedBatches: number;
  plannedBatches: number;
  secondaryCnaesRows: number;
  quarantinedRows: number;
  sanitizedRows: number;
  resumedFiles: number;
  skippedCompletedFiles: number;
  datasetSummaries: Array<{
    dataset: ImportDatasetType;
    files: number;
    rows: number;
  }>;
  warnings: string[];
  progressLogPath: string;
};

const IMPORT_ORDER: ImportDatasetType[] = [
  "partner_qualifications",
  "legal_natures",
  "countries",
  "cities",
  "reasons",
  "cnaes",
  "companies",
  "establishments",
  "partners",
  "simples_options",
];

const DATASET_LAYOUTS: Record<ImportDatasetType, TableLayout> = {
  companies: companiesLayout,
  establishments: establishmentsLayout,
  partners: partnersLayout,
  simples_options: simplesLayout,
  countries: countriesLayout,
  cities: citiesLayout,
  partner_qualifications: partnerQualificationsLayout,
  legal_natures: legalNaturesLayout,
  cnaes: cnaesLayout,
  reasons: reasonsLayout,
};

type LookupTableName =
  | "partner_qualifications"
  | "legal_natures"
  | "company_sizes"
  | "branch_types"
  | "registration_statuses"
  | "reasons"
  | "countries"
  | "cnaes"
  | "cities"
  | "partner_types"
  | "age_groups";

type LookupCacheMap = Map<LookupTableName, Set<string>>;

const LOOKUP_TABLES: LookupTableName[] = [
  "partner_qualifications",
  "legal_natures",
  "company_sizes",
  "branch_types",
  "registration_statuses",
  "reasons",
  "countries",
  "cnaes",
  "cities",
  "partner_types",
  "age_groups",
];

const LOOKUP_PLACEHOLDER_LABEL: Record<LookupTableName, string> = {
  partner_qualifications: "Imported placeholder qualification",
  legal_natures: "Imported placeholder legal nature",
  company_sizes: "Imported placeholder company size",
  branch_types: "Imported placeholder branch type",
  registration_statuses: "Imported placeholder registration status",
  reasons: "Imported placeholder registration reason",
  countries: "Imported placeholder country",
  cnaes: "Imported placeholder CNAE",
  cities: "Imported placeholder city",
  partner_types: "Imported placeholder partner type",
  age_groups: "Imported placeholder age group",
};

function isImportDatasetType(value: string): value is ImportDatasetType {
  return IMPORT_ORDER.includes(value as ImportDatasetType);
}

function maskDatabaseLabel(url: string): string {
  try {
    const parsed = new URL(url);
    const databaseName = parsed.pathname.replace(/^\//, "") || "database";
    return `${parsed.hostname}/${databaseName}`;
  } catch {
    return "configured database";
  }
}

function getInsertColumns(
  dataset: ImportDatasetType,
  schemaCapabilities: ImportSchemaCapabilities,
): string[] {
  const columns = DATASET_LAYOUTS[dataset].fields.map(
    (field) => field.columnName,
  );

  if (
    dataset === "partners" &&
    schemaCapabilities.includePartnerDedupeKeyInInsert
  ) {
    return [...columns, "partner_dedupe_key"];
  }

  return columns;
}

function getConflictClause(
  dataset: ImportDatasetType,
  columns: string[],
): string {
  switch (dataset) {
    case "countries":
    case "cities":
    case "partner_qualifications":
    case "legal_natures":
    case "cnaes":
    case "reasons":
      return "on conflict (code) do update set description = excluded.description";
    case "companies": {
      const updateColumns = columns
        .filter((column) => column !== "cnpj_root")
        .map((column) => `${column} = excluded.${column}`)
        .concat(["updated_at = now()"])
        .join(", ");
      return `on conflict (cnpj_root) do update set ${updateColumns}`;
    }
    case "establishments": {
      const updateColumns = columns
        .filter(
          (column) =>
            !["cnpj_root", "cnpj_order", "cnpj_check_digits"].includes(column),
        )
        .map((column) => `${column} = excluded.${column}`)
        .concat(["updated_at = now()"])
        .join(", ");
      return `on conflict (cnpj_root, cnpj_order, cnpj_check_digits) do update set ${updateColumns}`;
    }
    case "simples_options": {
      const updateColumns = columns
        .filter((column) => column !== "cnpj_root")
        .map((column) => `${column} = excluded.${column}`)
        .concat(["updated_at = now()"])
        .join(", ");
      return `on conflict (cnpj_root) do update set ${updateColumns}`;
    }
    case "partners": {
      const updateColumns = columns
        .filter((column) => column !== "partner_dedupe_key")
        .map((column) => `${column} = excluded.${column}`)
        .concat(["updated_at = now()"])
        .join(", ");
      return `on conflict (partner_dedupe_key) do update set ${updateColumns}`;
    }
    default:
      return "";
  }
}

function buildInsertQuery(
  tableName: string,
  columns: string[],
  rows: unknown[][],
  conflictClause: string,
): { text: string; values: unknown[] } {
  const values: unknown[] = [];

  const valueGroups = rows.map((row, rowIndex) => {
    const placeholders = row.map((_, columnIndex) => {
      const placeholderIndex = rowIndex * columns.length + columnIndex + 1;
      values.push(row[columnIndex]);
      return `$${placeholderIndex}`;
    });

    return `(${placeholders.join(", ")})`;
  });

  const parts = [
    `insert into ${tableName} (${columns.join(", ")})`,
    `values ${valueGroups.join(", ")}`,
  ];

  if (conflictClause) {
    parts.push(conflictClause);
  }

  return {
    text: parts.join(" "),
    values,
  };
}

function buildSecondaryCnaesQuery(rows: Array<[string, string, number]>): {
  text: string;
  values: unknown[];
} {
  return buildInsertQuery(
    "establishment_secondary_cnaes",
    ["establishment_cnpj_full", "cnae_code", "source_order"],
    rows,
    "on conflict (establishment_cnpj_full, cnae_code) do update set source_order = excluded.source_order",
  );
}

function parseDelimitedLine(line: string): string[] {
  const fields: string[] = [];
  let current = "";
  let inQuotes = false;

  for (let index = 0; index < line.length; index += 1) {
    const char = line[index];
    const next = line[index + 1];

    if (char === '"') {
      if (inQuotes && next === '"') {
        current += '"';
        index += 1;
        continue;
      }

      inQuotes = !inQuotes;
      continue;
    }

    if (char === ";" && !inQuotes) {
      fields.push(current);
      current = "";
      continue;
    }

    current += char;
  }

  fields.push(current);
  return fields;
}

function normalizeFieldCount(
  fields: string[],
  expectedLength: number,
  filePath: string,
  lineNumber: number,
): string[] {
  const normalized = [...fields];

  while (
    normalized.length > expectedLength &&
    normalized[normalized.length - 1]?.trim() === ""
  ) {
    normalized.pop();
  }

  if (normalized.length < expectedLength) {
    while (normalized.length < expectedLength) {
      normalized.push("");
    }
  }

  if (normalized.length !== expectedLength) {
    throw new ValidationError(
      `Unexpected field count in ${filePath} at line ${lineNumber}. Expected ${expectedLength}, received ${normalized.length}.`,
    );
  }

  return normalized;
}

function toDatabaseValue(
  dataType: TableLayout["fields"][number]["dataType"],
  rawValue: string,
): unknown {
  const trimmed = rawValue.trim();

  if (trimmed === "") {
    return null;
  }

  switch (dataType) {
    case "integer":
      return /^-?\d+$/.test(trimmed) ? Number.parseInt(trimmed, 10) : null;
    case "numeric": {
      let normalized = trimmed;

      if (normalized.includes(",") && normalized.includes(".")) {
        normalized = normalized.replace(/\./g, "").replace(/,/g, ".");
      } else if (normalized.includes(",")) {
        normalized = normalized.replace(/,/g, ".");
      }

      return normalized;
    }
    case "date":
      if (!/^\d{8}$/.test(trimmed) || trimmed === "00000000") {
        return null;
      }

      return `${trimmed.slice(0, 4)}-${trimmed.slice(4, 6)}-${trimmed.slice(6, 8)}`;
    case "boolean": {
      const normalized = trimmed.toLowerCase();
      if (["1", "true", "t", "y", "yes", "s"].includes(normalized)) {
        return true;
      }
      if (["0", "false", "f", "n", "no"].includes(normalized)) {
        return false;
      }
      return null;
    }
    default:
      return trimmed;
  }
}

function normalizeCode(value: unknown, fallback: string): string {
  if (typeof value === "string" && value.trim() !== "") {
    return value.trim();
  }

  return fallback;
}

function sanitizeStringValue(value: string): {
  value: string;
  actions: SanitizationAction[];
} {
  const actions: SanitizationAction[] = [];
  let sanitized = value;

  if (sanitized.includes("\u0000")) {
    sanitized = sanitized.replace(/\u0000/g, "");
    actions.push("remove_nul_bytes");
  }

  return {
    value: sanitized,
    actions,
  };
}

function sanitizeRawLine(line: string): {
  line: string;
  actions: SanitizationAction[];
} {
  const sanitized = sanitizeStringValue(line);
  return {
    line: sanitized.value,
    actions: sanitized.actions,
  };
}

function sanitizeRowValues(values: unknown[]): {
  values: unknown[];
  actions: SanitizationAction[];
} {
  const actions = new Set<SanitizationAction>();
  const sanitizedValues = values.map((value) => {
    if (typeof value !== "string") {
      return value;
    }

    const sanitized = sanitizeStringValue(value);
    for (const action of sanitized.actions) {
      actions.add(action);
    }

    return sanitized.value;
  });

  return {
    values: sanitizedValues,
    actions: [...actions],
  };
}

function mergeSanitizationActions(
  ...actionGroups: SanitizationAction[][]
): SanitizationAction[] {
  return [...new Set(actionGroups.flat())];
}

function classifyImportError(error: unknown): ClassifiedImportError {
  const message = error instanceof Error ? error.message : String(error);
  const code =
    typeof error === "object" && error && "code" in error
      ? String((error as { code?: string }).code ?? "UNKNOWN")
      : "UNKNOWN";
  const normalizedMessage = message.toLowerCase();

  if (
    normalizedMessage.includes('invalid byte sequence for encoding "utf8"') ||
    normalizedMessage.includes("0x00") ||
    normalizedMessage.includes("null character")
  ) {
    return {
      code,
      message,
      category: "invalid_utf8_sequence",
      recoverable: true,
      canRetryLater: true,
    };
  }

  if (code === "23502" || normalizedMessage.includes("violates not-null constraint")) {
    return {
      code,
      message,
      category: "not_null_violation",
      recoverable: false,
      canRetryLater: false,
    };
  }

  if (code === "23503" || normalizedMessage.includes("violates foreign key constraint")) {
    return {
      code,
      message,
      category: "foreign_key_violation",
      recoverable: false,
      canRetryLater: true,
    };
  }

  if (error instanceof ValidationError && normalizedMessage.includes("unexpected field count")) {
    return {
      code,
      message,
      category: "invalid_field_count",
      recoverable: false,
      canRetryLater: false,
    };
  }

  if (error instanceof ValidationError) {
    return {
      code,
      message,
      category: "transform_error",
      recoverable: false,
      canRetryLater: true,
    };
  }

  return {
    code,
    message,
    category: code === "UNKNOWN" ? "unknown" : "database_error",
    recoverable: false,
    canRetryLater: true,
  };
}

function rebuildRecoveredBatchRow(
  dataset: ImportDatasetType,
  row: BatchRow,
  columns: string[],
): BatchRow | null {
  const sanitizedRawLine = sanitizeRawLine(row.rawLine);
  const sanitizedValues = sanitizeRowValues(row.values);
  const actions = mergeSanitizationActions(
    row.sanitizationsApplied,
    sanitizedRawLine.actions,
    sanitizedValues.actions,
  );

  if (actions.length === row.sanitizationsApplied.length) {
    return null;
  }

  const values = sanitizedValues.values;
  return {
    ...row,
    rawLine: sanitizedRawLine.line,
    values,
    secondaryRows:
      dataset === "establishments"
        ? extractSecondaryCnaes(values, columns)
        : row.secondaryRows,
    sanitizationsApplied: actions,
  };
}

function buildPartnerDedupeKey(
  recordByColumn: Record<string, unknown>,
): string {
  return [
    recordByColumn.cnpj_root,
    recordByColumn.partner_type_code,
    recordByColumn.partner_name,
    recordByColumn.partner_document,
    recordByColumn.partner_qualification_code,
    recordByColumn.entry_date,
    recordByColumn.country_code,
    recordByColumn.legal_representative_document,
    recordByColumn.legal_representative_name,
    recordByColumn.legal_representative_qualification_code,
    recordByColumn.age_group_code,
  ]
    .map((value) => (value == null ? "" : String(value).trim()))
    .join("|");
}

function transformRecord(
  dataset: ImportDatasetType,
  layout: TableLayout,
  rawFields: string[],
  schemaCapabilities: ImportSchemaCapabilities,
): unknown[] {
  const values = layout.fields.map((field, index) =>
    toDatabaseValue(field.dataType, rawFields[index] ?? ""),
  );

  const recordByColumn = Object.fromEntries(
    layout.fields.map((field, index) => [field.columnName, values[index]]),
  ) as Record<string, unknown>;

  if (dataset === "companies") {
    recordByColumn.company_size_code = normalizeCode(
      recordByColumn.company_size_code,
      "00",
    );
  }

  if (dataset === "establishments") {
    recordByColumn.branch_type_code = normalizeCode(
      recordByColumn.branch_type_code,
      "1",
    );
    recordByColumn.registration_status_code = normalizeCode(
      recordByColumn.registration_status_code,
      "01",
    );
  }

  if (
    dataset === "partners" &&
    schemaCapabilities.includePartnerDedupeKeyInInsert
  ) {
    return [...values, buildPartnerDedupeKey(recordByColumn)];
  }

  return layout.fields.map((field) => recordByColumn[field.columnName]);
}

function buildParsedPayload(
  columns: string[],
  values: unknown[],
): Record<string, unknown> {
  return Object.fromEntries(
    columns.map((column, index) => [column, values[index] ?? null]),
  );
}

function extractSecondaryCnaes(
  record: unknown[],
  columns: string[],
): Array<[string, string, number]> {
  const root = String(record[columns.indexOf("cnpj_root")] ?? "");
  const order = String(record[columns.indexOf("cnpj_order")] ?? "");
  const digits = String(record[columns.indexOf("cnpj_check_digits")] ?? "");
  const raw = record[columns.indexOf("secondary_cnaes_raw")];

  if (
    !root ||
    !order ||
    !digits ||
    typeof raw !== "string" ||
    raw.trim() === ""
  ) {
    return [];
  }

  const cnpjFull = `${root}${order}${digits}`;
  const seen = new Set<string>();
  const rows: Array<[string, string, number]> = [];

  raw
    .split(",")
    .map((value) => value.trim())
    .filter(Boolean)
    .forEach((cnaeCode, index) => {
      if (seen.has(cnaeCode)) {
        return;
      }

      seen.add(cnaeCode);
      rows.push([cnpjFull, cnaeCode, index + 1]);
    });

  return rows;
}

async function loadLookupCaches(client: Client): Promise<LookupCacheMap> {
  const cache: LookupCacheMap = new Map();

  for (const tableName of LOOKUP_TABLES) {
    const result = await client.query<{ code: string | null }>(
      `select code from ${tableName}`,
    );
    cache.set(
      tableName,
      new Set(
        result.rows
          .map((row) => row.code?.trim())
          .filter((value): value is string => Boolean(value)),
      ),
    );
  }

  return cache;
}

async function ensureLookupCodes(
  client: Client,
  cache: LookupCacheMap,
  tableName: LookupTableName,
  rawCodes: Array<unknown>,
): Promise<void> {
  const knownCodes = cache.get(tableName) ?? new Set<string>();
  cache.set(tableName, knownCodes);

  const missingCodes = [
    ...new Set(
      rawCodes
        .map((value) =>
          typeof value === "string" ? value.trim() : String(value ?? "").trim(),
        )
        .filter((value) => value !== "" && !knownCodes.has(value)),
    ),
  ];

  if (missingCodes.length === 0) {
    return;
  }

  const values: string[] = [];
  const placeholders: Array<string> = [];

  for (const [index, code] of missingCodes.entries()) {
    values.push(code, `${LOOKUP_PLACEHOLDER_LABEL[tableName]} (${code})`);
    const baseIndex = index * 2;
    placeholders.push(`($${baseIndex + 1}, $${baseIndex + 2})`);
  }

  await client.query(
    `insert into ${tableName} (code, description) values ${placeholders.join(", ")} on conflict (code) do nothing`,
    values,
  );

  for (const code of missingCodes) {
    knownCodes.add(code);
  }
}

async function ensureBatchForeignKeys(
  client: Client,
  cache: LookupCacheMap,
  dataset: ImportDatasetType,
  rows: unknown[][],
  columns: string[],
): Promise<void> {
  const columnValues = (columnName: string): Array<unknown> => {
    const columnIndex = columns.indexOf(columnName);
    if (columnIndex === -1) {
      return [];
    }

    return rows.map((row) => row[columnIndex]);
  };

  switch (dataset) {
    case "companies":
      await ensureLookupCodes(
        client,
        cache,
        "legal_natures",
        columnValues("legal_nature_code"),
      );
      await ensureLookupCodes(
        client,
        cache,
        "partner_qualifications",
        columnValues("responsible_qualification_code"),
      );
      await ensureLookupCodes(
        client,
        cache,
        "company_sizes",
        columnValues("company_size_code"),
      );
      break;
    case "establishments":
      await ensureLookupCodes(
        client,
        cache,
        "branch_types",
        columnValues("branch_type_code"),
      );
      await ensureLookupCodes(
        client,
        cache,
        "registration_statuses",
        columnValues("registration_status_code"),
      );
      await ensureLookupCodes(
        client,
        cache,
        "reasons",
        columnValues("registration_status_reason_code"),
      );
      await ensureLookupCodes(
        client,
        cache,
        "countries",
        columnValues("country_code"),
      );
      await ensureLookupCodes(
        client,
        cache,
        "cnaes",
        columnValues("main_cnae_code"),
      );
      await ensureLookupCodes(
        client,
        cache,
        "cities",
        columnValues("city_code"),
      );
      break;
    case "partners":
      await ensureLookupCodes(
        client,
        cache,
        "partner_types",
        columnValues("partner_type_code"),
      );
      await ensureLookupCodes(
        client,
        cache,
        "partner_qualifications",
        columnValues("partner_qualification_code"),
      );
      await ensureLookupCodes(
        client,
        cache,
        "countries",
        columnValues("country_code"),
      );
      await ensureLookupCodes(
        client,
        cache,
        "partner_qualifications",
        columnValues("legal_representative_qualification_code"),
      );
      await ensureLookupCodes(
        client,
        cache,
        "age_groups",
        columnValues("age_group_code"),
      );
      break;
    default:
      break;
  }
}

function resolveAbsolutePath(
  validatedPath: string,
  entry: FileInspection,
): string {
  return path.join(validatedPath, entry.relativePath);
}

function buildDisplayPath(filePath: string): string {
  return `${path.basename(path.dirname(filePath))} > ${path.basename(filePath)}`;
}

async function countFileRowsExact(filePath: string): Promise<number> {
  let count = 0;

  for await (const item of iterateFileLines(filePath, 0)) {
    if (item.line.trim() !== "") {
      count += 1;
    }
  }

  return count;
}

async function buildImportPlan(
  inputPath: string,
  validatedPath: string,
  datasetEntries: Array<{
    dataset: ImportDatasetType;
    files: FileInspection[];
  }>,
  batchSize: number,
  onProgress: ImportProgressListener | undefined,
  targetDatabase: string,
): Promise<{
  datasets: ImportDatasetPlan[];
  totalFiles: number;
  totalRows: number;
  totalBatches: number;
}> {
  const totalFiles = datasetEntries.reduce(
    (sum, item) => sum + item.files.length,
    0,
  );
  let scannedFiles = 0;
  let countedRows = 0;

  onProgress?.({
    kind: "preparing_start",
    inputPath: path.resolve(inputPath),
    validatedPath,
    totalDatasets: datasetEntries.length,
    totalFiles,
    batchSize,
    targetDatabase,
  });

  const datasets: ImportDatasetPlan[] = [];

  for (const item of datasetEntries) {
    const files: ImportFilePlan[] = [];
    let datasetRows = 0;
    let datasetBatches = 0;

    for (const entry of item.files.sort(sortEntries)) {
      const absolutePath = resolveAbsolutePath(validatedPath, entry);
      const fileStats = await stat(absolutePath);
      const totalRows = await countFileRowsExact(absolutePath);
      const totalBatches =
        totalRows === 0 ? 0 : Math.ceil(totalRows / batchSize);

      files.push({
        dataset: item.dataset,
        entry,
        absolutePath,
        displayPath: buildDisplayPath(absolutePath),
        fileSize: fileStats.size,
        fileMtime: fileStats.mtime,
        totalRows,
        totalBatches,
      });

      scannedFiles += 1;
      countedRows += totalRows;
      datasetRows += totalRows;
      datasetBatches += totalBatches;

      onProgress?.({
        kind: "preparing_progress",
        scannedFiles,
        totalFiles,
        countedRows,
        currentFileDisplayPath: buildDisplayPath(absolutePath),
      });
    }

    datasets.push({
      dataset: item.dataset,
      files,
      totalRows: datasetRows,
      totalBatches: datasetBatches,
    });
  }

  const totalRows = datasets.reduce((sum, item) => sum + item.totalRows, 0);
  const totalBatches = datasets.reduce(
    (sum, item) => sum + item.totalBatches,
    0,
  );

  onProgress?.({
    kind: "plan_ready",
    totalDatasets: datasets.length,
    totalFiles,
    batchSize,
    totalRows,
    totalBatches,
    targetDatabase,
    executionOrder: datasets.map((item) => item.dataset),
  });

  return {
    datasets,
    totalFiles,
    totalRows,
    totalBatches,
  };
}

function sortEntries(left: FileInspection, right: FileInspection): number {
  return left.relativePath.localeCompare(right.relativePath, undefined, {
    numeric: true,
    sensitivity: "base",
  });
}

async function flushRows(
  client: Client,
  lookupCache: LookupCacheMap,
  dataset: ImportDatasetType,
  rows: unknown[][],
  schemaCapabilities: ImportSchemaCapabilities,
): Promise<void> {
  if (rows.length === 0) {
    return;
  }

  const layout = DATASET_LAYOUTS[dataset];
  const columns = getInsertColumns(dataset, schemaCapabilities);
  await ensureBatchForeignKeys(client, lookupCache, dataset, rows, columns);
  const conflictClause = getConflictClause(dataset, columns);
  const query = buildInsertQuery(
    layout.tableName,
    columns,
    rows,
    conflictClause,
  );
  await client.query(query);
}

async function flushSecondaryCnaes(
  client: Client,
  rows: Array<[string, string, number]>,
): Promise<void> {
  if (rows.length === 0) {
    return;
  }

  const query = buildSecondaryCnaesQuery(rows);
  await client.query(query);
}

async function ensureCheckpointTable(client: Client): Promise<void> {
  await client.query(`
    create table if not exists import_checkpoints (
      id bigserial primary key,
      dataset text not null,
      file_path text not null,
      file_size bigint not null,
      file_mtime timestamptz not null,
      byte_offset bigint not null default 0,
      rows_committed bigint not null default 0,
      status text not null default 'pending',
      last_error text,
      updated_at timestamptz not null default now(),
      unique (dataset, file_path)
    )
  `);
  await client.query(
    `create index if not exists idx_import_checkpoints_status on import_checkpoints (status)`,
  );
  await client.query(
    `create index if not exists idx_import_checkpoints_dataset on import_checkpoints (dataset)`,
  );
}

async function ensureQuarantineTable(client: Client): Promise<void> {
  await client.query(`
    create table if not exists import_quarantine (
      id bigserial primary key,
      dataset text not null,
      file_path text not null,
      row_number bigint,
      checkpoint_offset bigint,
      error_code text,
      error_category text,
      error_stage text,
      error_message text not null,
      raw_line text not null,
      parsed_payload jsonb,
      sanitizations_applied jsonb,
      retry_count integer not null default 0,
      can_retry_later boolean not null default false,
      created_at timestamptz not null default now()
    )
  `);
  await client.query(
    `alter table import_quarantine add column if not exists error_category text`,
  );
  await client.query(
    `alter table import_quarantine add column if not exists error_stage text`,
  );
  await client.query(
    `alter table import_quarantine add column if not exists sanitizations_applied jsonb`,
  );
  await client.query(
    `alter table import_quarantine add column if not exists retry_count integer not null default 0`,
  );
  await client.query(
    `alter table import_quarantine add column if not exists can_retry_later boolean not null default false`,
  );
  await client.query(
    `create index if not exists idx_import_quarantine_dataset on import_quarantine (dataset)`,
  );
  await client.query(
    `create index if not exists idx_import_quarantine_file_path on import_quarantine (file_path)`,
  );
  await client.query(
    `create index if not exists idx_import_quarantine_error_category on import_quarantine (error_category)`,
  );
  await client.query(
    `create index if not exists idx_import_quarantine_can_retry_later on import_quarantine (can_retry_later)`,
  );
}

type QuarantineInput = {
  dataset: ImportDatasetType;
  filePath: string;
  rowNumber: number;
  checkpointOffset: number;
  rawLine: string;
  error: unknown;
  parsedPayload?: Record<string, unknown> | null;
  stage: QuarantineStage;
  retryCount?: number;
  sanitizationsApplied?: SanitizationAction[];
};

async function writeQuarantineRow(
  client: Client,
  input: QuarantineInput,
): Promise<void> {
  const classifiedError = classifyImportError(input.error);

  await client.query(
    `insert into import_quarantine (
       dataset,
       file_path,
       row_number,
       checkpoint_offset,
       error_code,
       error_category,
       error_stage,
       error_message,
       raw_line,
       parsed_payload,
       sanitizations_applied,
       retry_count,
       can_retry_later,
       created_at
     ) values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10::jsonb, $11::jsonb, $12, $13, now())`,
    [
      input.dataset,
      input.filePath,
      input.rowNumber,
      input.checkpointOffset,
      classifiedError.code,
      classifiedError.category,
      input.stage,
      classifiedError.message,
      input.rawLine,
      input.parsedPayload ? JSON.stringify(input.parsedPayload) : null,
      JSON.stringify(input.sanitizationsApplied ?? []),
      input.retryCount ?? 0,
      classifiedError.canRetryLater,
    ],
  );
}

async function detectImportSchemaCapabilities(
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

async function readCheckpoint(
  client: Client,
  dataset: ImportDatasetType,
  filePath: string,
  fileSize: number,
  fileMtime: Date,
): Promise<ImportCheckpointRecord> {
  const existing = await client.query<{
    dataset: string;
    file_path: string;
    file_size: string;
    file_mtime: Date;
    byte_offset: string;
    rows_committed: string;
    status: ImportCheckpointStatus;
    last_error: string | null;
  }>(
    `select dataset, file_path, file_size, file_mtime, byte_offset, rows_committed, status, last_error
       from import_checkpoints
      where dataset = $1 and file_path = $2`,
    [dataset, filePath],
  );

  const baseRecord: ImportCheckpointRecord = {
    dataset,
    filePath,
    fileSize,
    fileMtime,
    byteOffset: 0,
    rowsCommitted: 0,
    status: "pending",
    lastError: null,
  };

  if (existing.rowCount === 0) {
    return baseRecord;
  }

  const row = existing.rows[0]!;
  const checkpoint: ImportCheckpointRecord = {
    dataset,
    filePath,
    fileSize: Number.parseInt(row.file_size, 10),
    fileMtime: new Date(row.file_mtime),
    byteOffset: Number.parseInt(row.byte_offset, 10),
    rowsCommitted: Number.parseInt(row.rows_committed, 10),
    status: row.status,
    lastError: row.last_error,
  };

  const sameMetadata =
    checkpoint.fileSize === fileSize &&
    checkpoint.fileMtime.getTime() === fileMtime.getTime();

  if (!sameMetadata) {
    return baseRecord;
  }

  return checkpoint;
}

async function writeCheckpoint(
  client: Client,
  checkpoint: ImportCheckpointRecord,
): Promise<void> {
  await client.query(
    `insert into import_checkpoints (
        dataset,
        file_path,
        file_size,
        file_mtime,
        byte_offset,
        rows_committed,
        status,
        last_error,
        updated_at
      ) values ($1, $2, $3, $4, $5, $6, $7, $8, now())
      on conflict (dataset, file_path)
      do update set
        file_size = excluded.file_size,
        file_mtime = excluded.file_mtime,
        byte_offset = excluded.byte_offset,
        rows_committed = excluded.rows_committed,
        status = excluded.status,
        last_error = excluded.last_error,
        updated_at = now()`,
    [
      checkpoint.dataset,
      checkpoint.filePath,
      checkpoint.fileSize,
      checkpoint.fileMtime,
      checkpoint.byteOffset,
      checkpoint.rowsCommitted,
      checkpoint.status,
      checkpoint.lastError ?? null,
    ],
  );
}

async function markCheckpointFailed(
  client: Client,
  checkpoint: ImportCheckpointRecord,
  errorMessage: string,
): Promise<void> {
  await writeCheckpoint(client, {
    ...checkpoint,
    status: "failed",
    lastError: errorMessage,
  });
}

type IteratedLine = {
  line: string;
  nextOffset: number;
};

async function* iterateFileLines(
  filePath: string,
  startOffset = 0,
): AsyncGenerator<IteratedLine> {
  const stream = createReadStream(filePath, { start: startOffset });
  let buffered = Buffer.alloc(0);
  let offset = startOffset;

  try {
    for await (const chunk of stream) {
      const chunkBuffer = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
      buffered = buffered.length
        ? Buffer.concat([buffered, chunkBuffer])
        : chunkBuffer;

      while (true) {
        const newlineIndex = buffered.indexOf(0x0a);
        if (newlineIndex === -1) {
          break;
        }

        let lineBuffer = buffered.subarray(0, newlineIndex);
        const consumed = newlineIndex + 1;

        if (
          lineBuffer.length > 0 &&
          lineBuffer[lineBuffer.length - 1] === 0x0d
        ) {
          lineBuffer = lineBuffer.subarray(0, lineBuffer.length - 1);
        }

        offset += consumed;
        yield {
          line: lineBuffer.toString("utf8"),
          nextOffset: offset,
        };

        buffered = buffered.subarray(consumed);
      }
    }

    if (buffered.length > 0) {
      offset += buffered.length;
      yield {
        line: buffered.toString("utf8"),
        nextOffset: offset,
      };
    }
  } finally {
    stream.close();
  }
}

async function hydratePlanWithCheckpoints(
  client: Client,
  datasets: ImportDatasetPlan[],
  batchSize: number,
): Promise<{
  committedRows: number;
  committedBatches: number;
  completedFiles: number;
  resumedFiles: number;
  skippedCompletedFiles: number;
}> {
  let committedRows = 0;
  let committedBatches = 0;
  let completedFiles = 0;
  let resumedFiles = 0;
  let skippedCompletedFiles = 0;

  for (const datasetPlan of datasets) {
    for (const filePlan of datasetPlan.files) {
      const checkpoint = await readCheckpoint(
        client,
        datasetPlan.dataset,
        filePlan.absolutePath,
        filePlan.fileSize,
        filePlan.fileMtime,
      );
      filePlan.checkpoint = checkpoint;

      if (checkpoint.rowsCommitted > 0) {
        committedRows += checkpoint.rowsCommitted;
        committedBatches += Math.min(
          filePlan.totalBatches,
          Math.ceil(checkpoint.rowsCommitted / batchSize),
        );
      }

      if (
        checkpoint.status === "completed" &&
        checkpoint.byteOffset >= filePlan.fileSize
      ) {
        completedFiles += 1;
        skippedCompletedFiles += 1;
      } else if (checkpoint.byteOffset > 0 || checkpoint.rowsCommitted > 0) {
        resumedFiles += 1;
      }
    }
  }

  return {
    committedRows,
    committedBatches,
    completedFiles,
    resumedFiles,
    skippedCompletedFiles,
  };
}

async function importDatasetFile(
  client: Client,
  lookupCache: LookupCacheMap,
  filePlan: ImportFilePlan,
  schemaCapabilities: ImportSchemaCapabilities,
  counters: {
    committedRows: number;
    committedBatches: number;
    completedFiles: number;
    secondaryCnaesRows: number;
    quarantinedRows: number;
    sanitizedRows: number;
  },
  progress: {
    datasetIndex: number;
    totalDatasets: number;
    totalFiles: number;
    totalBatches: number;
    fileIndex: number;
    onProgress: ImportProgressListener | undefined;
    progressLogPath: string;
    batchSize: number;
    verboseProgress: boolean;
  },
): Promise<number> {
  const dataset = filePlan.dataset;
  const filePath = filePlan.absolutePath;
  const layout = DATASET_LAYOUTS[dataset];
  const columns = getInsertColumns(dataset, schemaCapabilities);
  let checkpoint =
    filePlan.checkpoint ??
    (await readCheckpoint(
      client,
      dataset,
      filePath,
      filePlan.fileSize,
      filePlan.fileMtime,
    ));

  const emitProgress = (): void => {
    progress.onProgress?.({
      kind: "progress",
      dataset,
      datasetIndex: progress.datasetIndex,
      totalDatasets: progress.totalDatasets,
      currentFilePath: filePath,
      currentFileDisplayPath: filePlan.displayPath,
      fileIndex: progress.fileIndex,
      completedFiles: counters.completedFiles,
      totalFiles: progress.totalFiles,
      currentFileRowsCommitted: checkpoint.rowsCommitted,
      currentFileRowsTotal: filePlan.totalRows,
      committedRows: counters.committedRows,
      committedBatches: counters.committedBatches,
      totalBatches: progress.totalBatches,
      currentBatch:
        filePlan.totalBatches === 0
          ? 0
          : Math.min(
              filePlan.totalBatches,
              Math.max(
                1,
                Math.ceil(
                  Math.max(checkpoint.rowsCommitted, 1) / progress.batchSize,
                ),
              ),
            ),
      batchSize: progress.batchSize,
      checkpointOffset: checkpoint.byteOffset,
      currentFileSize: filePlan.fileSize,
      verboseProgress: progress.verboseProgress,
    });
  };

  if (
    checkpoint.status === "completed" &&
    checkpoint.byteOffset >= filePlan.fileSize
  ) {
    emitProgress();
    return checkpoint.rowsCommitted;
  }

  let lineNumber = 0;
  let fileRowsCommitted = checkpoint.rowsCommitted;
  let batchRows: BatchRow[] = [];
  let batchLastOffset = checkpoint.byteOffset;

  emitProgress();

  const commitBatch = async (markCompleted: boolean): Promise<void> => {
    const finalizeCheckpoint = async (
      status: ImportCheckpointStatus,
    ): Promise<void> => {
      await client.query("begin");
      try {
        checkpoint = {
          ...checkpoint,
          byteOffset: markCompleted ? filePlan.fileSize : batchLastOffset,
          rowsCommitted: fileRowsCommitted,
          status,
          lastError: null,
        };
        await writeCheckpoint(client, checkpoint);
        await client.query("commit");
      } catch (error) {
        await client.query("rollback");
        throw error;
      }
    };

    if (batchRows.length === 0) {
      if (markCompleted) {
        await finalizeCheckpoint("completed");
        emitProgress();
      }
      return;
    }

    const batchValues = batchRows.map((row) => row.values);
    const secondaryRows = batchRows.flatMap((row) => row.secondaryRows);

    await client.query("begin");
    try {
      await flushRows(
        client,
        lookupCache,
        dataset,
        batchValues,
        schemaCapabilities,
      );
      await flushSecondaryCnaes(client, secondaryRows);
      checkpoint = {
        ...checkpoint,
        byteOffset: markCompleted ? filePlan.fileSize : batchLastOffset,
        rowsCommitted: fileRowsCommitted,
        status: markCompleted ? "completed" : "in_progress",
        lastError: null,
      };
      await writeCheckpoint(client, checkpoint);
      await client.query("commit");

      counters.committedRows += batchRows.length;
      counters.committedBatches += 1;
      counters.secondaryCnaesRows += secondaryRows.length;

      await appendJsonLinesLog(progress.progressLogPath, {
        kind: "batch_committed",
        dataset,
        datasetIndex: progress.datasetIndex,
        filePath,
        fileDisplayPath: filePlan.displayPath,
        fileIndex: progress.fileIndex,
        batchNumber: counters.committedBatches,
        batchSize: progress.batchSize,
        batchRows: batchRows.length,
        fileRowsCommitted,
        fileRowsTotal: filePlan.totalRows,
        totalRowsCommitted: counters.committedRows,
        totalBatchesCommitted: counters.committedBatches,
        totalBatchesPlanned: progress.totalBatches,
        checkpointOffset: checkpoint.byteOffset,
        fileSize: filePlan.fileSize,
        secondaryCnaesRows: secondaryRows.length,
        timestamp: new Date().toISOString(),
      });

      for (const row of batchRows.filter((item) => item.sanitizationsApplied.length > 0)) {
        counters.sanitizedRows += 1;
        await appendJsonLinesLog(progress.progressLogPath, {
          kind: "row_sanitized",
          dataset,
          datasetIndex: progress.datasetIndex,
          filePath,
          fileDisplayPath: filePlan.displayPath,
          fileIndex: progress.fileIndex,
          rowNumber: row.sourceRowNumber,
          checkpointOffset: row.nextOffset,
          sanitizationsApplied: row.sanitizationsApplied,
          timestamp: new Date().toISOString(),
        });
      }
    } catch {
      await client.query("rollback");

      for (const row of batchRows) {
        try {
          await client.query("begin");
          await flushRows(
            client,
            lookupCache,
            dataset,
            [row.values],
            schemaCapabilities,
          );
          await flushSecondaryCnaes(client, row.secondaryRows);
          checkpoint = {
            ...checkpoint,
            byteOffset: row.nextOffset,
            rowsCommitted: row.sourceRowNumber,
            status: "in_progress",
            lastError: null,
          };
          await writeCheckpoint(client, checkpoint);
          await client.query("commit");
          counters.committedRows += 1;
          counters.secondaryCnaesRows += row.secondaryRows.length;
        } catch (rowError) {
          await client.query("rollback");
          const classifiedError = classifyImportError(rowError);
          const recoveredRow = classifiedError.recoverable
            ? rebuildRecoveredBatchRow(dataset, row, columns)
            : null;

          if (recoveredRow) {
            try {
              await client.query("begin");
              await flushRows(
                client,
                lookupCache,
                dataset,
                [recoveredRow.values],
                schemaCapabilities,
              );
              await flushSecondaryCnaes(client, recoveredRow.secondaryRows);
              checkpoint = {
                ...checkpoint,
                byteOffset: recoveredRow.nextOffset,
                rowsCommitted: recoveredRow.sourceRowNumber,
                status: "in_progress",
                lastError: null,
              };
              await writeCheckpoint(client, checkpoint);
              await client.query("commit");
              counters.committedRows += 1;
              counters.secondaryCnaesRows += recoveredRow.secondaryRows.length;
              counters.sanitizedRows += 1;
              await appendJsonLinesLog(progress.progressLogPath, {
                kind: "row_sanitized",
                dataset,
                datasetIndex: progress.datasetIndex,
                filePath,
                fileDisplayPath: filePlan.displayPath,
                fileIndex: progress.fileIndex,
                rowNumber: recoveredRow.sourceRowNumber,
                checkpointOffset: recoveredRow.nextOffset,
                sanitizationsApplied: recoveredRow.sanitizationsApplied,
                timestamp: new Date().toISOString(),
              });
              continue;
            } catch (retryError) {
              await client.query("rollback");
              rowError = retryError;
            }
          }

          await client.query("begin");
          try {
            await writeQuarantineRow(client, {
              dataset,
              filePath,
              rowNumber: row.sourceRowNumber,
              checkpointOffset: row.nextOffset,
              rawLine: row.rawLine,
              error: rowError,
              parsedPayload: buildParsedPayload(columns, row.values),
              stage: recoveredRow ? "row_retry" : "row_insert",
              retryCount: recoveredRow ? 1 : 0,
              sanitizationsApplied: recoveredRow?.sanitizationsApplied ?? row.sanitizationsApplied,
            });
            checkpoint = {
              ...checkpoint,
              byteOffset: row.nextOffset,
              rowsCommitted: row.sourceRowNumber,
              status: "in_progress",
              lastError: null,
            };
            await writeCheckpoint(client, checkpoint);
            await client.query("commit");
            counters.quarantinedRows += 1;
            await appendJsonLinesLog(progress.progressLogPath, {
              kind: "row_quarantined",
              dataset,
              datasetIndex: progress.datasetIndex,
              filePath,
              fileDisplayPath: filePlan.displayPath,
              fileIndex: progress.fileIndex,
              rowNumber: row.sourceRowNumber,
              checkpointOffset: row.nextOffset,
              error: classifyImportError(rowError).message,
              errorCategory: classifyImportError(rowError).category,
              canRetryLater: classifyImportError(rowError).canRetryLater,
              timestamp: new Date().toISOString(),
            });
          } catch (quarantineError) {
            await client.query("rollback");
            throw quarantineError;
          }
        }
      }

      counters.committedBatches += 1;
      if (markCompleted) {
        await finalizeCheckpoint("completed");
      }
    }

    batchRows = [];
    emitProgress();
  };

  try {
    for await (const item of iterateFileLines(
      filePath,
      checkpoint.byteOffset,
    )) {
      lineNumber += 1;

      if (item.line.trim() === "") {
        checkpoint.byteOffset = item.nextOffset;
        continue;
      }

      try {
        const sanitizedLine = sanitizeRawLine(item.line);
        const parsedFields = normalizeFieldCount(
          parseDelimitedLine(sanitizedLine.line),
          layout.fields.length,
          filePath,
          lineNumber,
        );
        const record = transformRecord(
          dataset,
          layout,
          parsedFields,
          schemaCapabilities,
        );
        const nextSourceRowNumber = fileRowsCommitted + 1;
        batchRows.push({
          values: record,
          rawLine: item.line,
          nextOffset: item.nextOffset,
          sourceRowNumber: nextSourceRowNumber,
          secondaryRows:
            dataset === "establishments"
              ? extractSecondaryCnaes(record, columns)
              : [],
          sanitizationsApplied: sanitizedLine.actions,
        });
        fileRowsCommitted = nextSourceRowNumber;
        batchLastOffset = item.nextOffset;

        if (batchRows.length >= progress.batchSize) {
          await commitBatch(false);
        }
      } catch (rowError) {
        fileRowsCommitted += 1;
        batchLastOffset = item.nextOffset;
        const sanitizedLine = sanitizeRawLine(item.line);
        await client.query("begin");
        try {
          await writeQuarantineRow(client, {
            dataset,
            filePath,
            rowNumber: fileRowsCommitted,
            checkpointOffset: item.nextOffset,
            rawLine: item.line,
            error: rowError,
            parsedPayload: null,
            stage: "parse",
            retryCount: 0,
            sanitizationsApplied: sanitizedLine.actions,
          });
          checkpoint = {
            ...checkpoint,
            byteOffset: item.nextOffset,
            rowsCommitted: fileRowsCommitted,
            status: "in_progress",
            lastError: null,
          };
          await writeCheckpoint(client, checkpoint);
          await client.query("commit");
          counters.quarantinedRows += 1;
          await appendJsonLinesLog(progress.progressLogPath, {
            kind: "row_quarantined",
            dataset,
            datasetIndex: progress.datasetIndex,
            filePath,
            fileDisplayPath: filePlan.displayPath,
            fileIndex: progress.fileIndex,
            rowNumber: fileRowsCommitted,
            checkpointOffset: item.nextOffset,
            error: classifyImportError(rowError).message,
            errorCategory: classifyImportError(rowError).category,
            canRetryLater: classifyImportError(rowError).canRetryLater,
            timestamp: new Date().toISOString(),
          });
          emitProgress();
        } catch (quarantineError) {
          await client.query("rollback");
          throw quarantineError;
        }
      }
    }

    if (batchRows.length > 0 || checkpoint.byteOffset < filePlan.fileSize) {
      await commitBatch(true);
    }

    counters.completedFiles += 1;
    emitProgress();

    return fileRowsCommitted;
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    await markCheckpointFailed(client, checkpoint, message);
    await appendJsonLinesLog(progress.progressLogPath, {
      kind: "file_failed",
      dataset,
      datasetIndex: progress.datasetIndex,
      filePath,
      fileDisplayPath: filePlan.displayPath,
      fileIndex: progress.fileIndex,
      fileRowsCommitted: checkpoint.rowsCommitted,
      fileRowsTotal: filePlan.totalRows,
      checkpointOffset: checkpoint.byteOffset,
      fileSize: filePlan.fileSize,
      error: message,
      timestamp: new Date().toISOString(),
    });
    throw error;
  }
}

export async function importDataToDatabase(
  inputPath: string,
  options: ImportOptions = {},
): Promise<ImportSummary> {
  if (options.dataset && !isImportDatasetType(options.dataset)) {
    throw new ValidationError(`Unsupported dataset type: ${options.dataset}.`);
  }

  const validation = await validateInputDirectory(inputPath);
  if (!validation.ok) {
    throw new ValidationError(
      `The input directory is not ready for import. ${validation.errors.join(" ")}`,
    );
  }

  const validatedInspection = await inspectFiles(validation.validatedPath);
  const selectedDatasets = options.dataset
    ? IMPORT_ORDER.filter((dataset) => dataset === options.dataset)
    : IMPORT_ORDER;

  const datasetEntries = selectedDatasets.map((dataset) => ({
    dataset,
    files: validatedInspection.entries.filter(
      (entry) => entry.entryKind === "file" && entry.inferredType === dataset,
    ),
  }));

  const plannedEntries = datasetEntries.filter((item) => item.files.length > 0);

  if (plannedEntries.length === 0) {
    throw new ValidationError(
      "No validated dataset files were found for the requested import.",
    );
  }

  const dbUrl = await resolveDbUrl(options.dbUrl);
  const targetDatabase = maskDatabaseLabel(dbUrl);
  const batchSize = Math.max(1, options.batchSize ?? 500);
  const progressLogPath = await createJsonLinesLog("import-progress");

  const plan = await buildImportPlan(
    inputPath,
    validation.validatedPath,
    plannedEntries,
    batchSize,
    options.onProgress,
    targetDatabase,
  );

  await appendJsonLinesLog(progressLogPath, {
    kind: "import_plan_ready",
    inputPath: path.resolve(inputPath),
    validatedPath: validation.validatedPath,
    targetDatabase,
    totalDatasets: plan.datasets.length,
    totalFiles: plan.totalFiles,
    totalRows: plan.totalRows,
    totalBatches: plan.totalBatches,
    batchSize,
    executionOrder: plan.datasets.map((item) => item.dataset),
    timestamp: new Date().toISOString(),
  });

  const client = new Client({ connectionString: dbUrl });
  const datasetSummaries: Array<{
    dataset: ImportDatasetType;
    files: number;
    rows: number;
  }> = [];

  const counters = {
    committedRows: 0,
    committedBatches: 0,
    completedFiles: 0,
    secondaryCnaesRows: 0,
    quarantinedRows: 0,
    sanitizedRows: 0,
    resumedFiles: 0,
    skippedCompletedFiles: 0,
  };

  try {
    await client.connect();
    await ensureCheckpointTable(client);
    await ensureQuarantineTable(client);
    const schemaCapabilities = await detectImportSchemaCapabilities(client);
    const checkpointTotals = await hydratePlanWithCheckpoints(
      client,
      plan.datasets,
      batchSize,
    );

    counters.committedRows = checkpointTotals.committedRows;
    counters.committedBatches = checkpointTotals.committedBatches;
    counters.completedFiles = checkpointTotals.completedFiles;
    counters.resumedFiles = checkpointTotals.resumedFiles;
    counters.skippedCompletedFiles = checkpointTotals.skippedCompletedFiles;

    options.onProgress?.({
      kind: "start",
      inputPath: path.resolve(inputPath),
      validatedPath: validation.validatedPath,
      totalDatasets: plan.datasets.length,
      totalFiles: plan.totalFiles,
      targetDatabase,
      totalRows: plan.totalRows,
      totalBatches: plan.totalBatches,
      committedRows: counters.committedRows,
      committedBatches: counters.committedBatches,
    });

    await appendJsonLinesLog(progressLogPath, {
      kind: "import_started",
      inputPath: path.resolve(inputPath),
      validatedPath: validation.validatedPath,
      targetDatabase,
      totalDatasets: plan.datasets.length,
      totalFiles: plan.totalFiles,
      totalRows: plan.totalRows,
      totalBatches: plan.totalBatches,
      batchSize,
      resumedFiles: counters.resumedFiles,
      skippedCompletedFiles: counters.skippedCompletedFiles,
      committedRows: counters.committedRows,
      committedBatches: counters.committedBatches,
      timestamp: new Date().toISOString(),
    });

    const lookupCache = await loadLookupCaches(client);

    let globalFileIndex = 0;
    for (const [datasetIndex, datasetPlan] of plan.datasets.entries()) {
      for (const filePlan of datasetPlan.files) {
        globalFileIndex += 1;
        await importDatasetFile(
          client,
          lookupCache,
          filePlan,
          schemaCapabilities,
          counters,
          {
            datasetIndex: datasetIndex + 1,
            totalDatasets: plan.datasets.length,
            totalFiles: plan.totalFiles,
            totalBatches: plan.totalBatches,
            fileIndex: globalFileIndex,
            onProgress: options.onProgress,
            progressLogPath,
            batchSize,
            verboseProgress: options.verboseProgress ?? false,
          },
        );
      }

      datasetSummaries.push({
        dataset: datasetPlan.dataset,
        files: datasetPlan.files.length,
        rows: datasetPlan.totalRows,
      });
    }
  } finally {
    await client.end().catch(() => undefined);
  }

  options.onProgress?.({
    kind: "finish",
    totalDatasets: plan.datasets.length,
    totalFiles: plan.totalFiles,
    completedFiles: counters.completedFiles,
    processedRows: counters.committedRows,
    totalRows: plan.totalRows,
    committedBatches: counters.committedBatches,
    totalBatches: plan.totalBatches,
    secondaryCnaesRows: counters.secondaryCnaesRows,
    quarantinedRows: counters.quarantinedRows,
    sanitizedRows: counters.sanitizedRows,
  });

  await appendJsonLinesLog(progressLogPath, {
    kind: "import_finished",
    totalDatasets: plan.datasets.length,
    totalFiles: plan.totalFiles,
    completedFiles: counters.completedFiles,
    processedRows: counters.committedRows,
    totalRows: plan.totalRows,
    committedBatches: counters.committedBatches,
    totalBatches: plan.totalBatches,
    secondaryCnaesRows: counters.secondaryCnaesRows,
    quarantinedRows: counters.quarantinedRows,
    sanitizedRows: counters.sanitizedRows,
    resumedFiles: counters.resumedFiles,
    skippedCompletedFiles: counters.skippedCompletedFiles,
    timestamp: new Date().toISOString(),
  });

  return {
    inputPath: path.resolve(inputPath),
    validatedPath: validation.validatedPath,
    targetDatabase,
    importedDatasets: datasetSummaries.map((item) => item.dataset),
    importedFiles: counters.completedFiles,
    processedRows: counters.committedRows,
    plannedRows: plan.totalRows,
    committedBatches: counters.committedBatches,
    plannedBatches: plan.totalBatches,
    secondaryCnaesRows: counters.secondaryCnaesRows,
    quarantinedRows: counters.quarantinedRows,
    sanitizedRows: counters.sanitizedRows,
    resumedFiles: counters.resumedFiles,
    skippedCompletedFiles: counters.skippedCompletedFiles,
    datasetSummaries,
    warnings: [
      "The importer uses exact file planning, checkpointed batch commits, and byte-offset resume. If a batch fails, rerunning the same command resumes from the last committed checkpoint instead of restarting the full load.",
      "The importer remains idempotent for the current schema: rerunning the same validated files updates existing rows instead of duplicating them.",
      "Rows that fail validation or database constraints are retried with known sanitization rules first. Only rows that still fail are moved to import_quarantine.",
      "The default batch size is conservative to reduce RAM pressure during long PostgreSQL imports. Increase --batch-size only after validating RAM usage and PostgreSQL stability in your environment.",
    ],
    progressLogPath,
  };
}
