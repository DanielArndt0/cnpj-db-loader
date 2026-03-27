import { ValidationError } from "../../core/errors/index.js";
import type { TableLayout } from "../../dictionary/layouts/index.js";
import type {
  ImportDatasetType,
  ImportSchemaCapabilities,
  ImportWriteTarget,
} from "./types.js";

export type FieldValueParser = (rawValue: string) => unknown;

export type PartnerDedupeKeyIndices = {
  cnpjRoot: number;
  partnerTypeCode: number;
  partnerName: number;
  partnerDocument: number;
  partnerQualificationCode: number;
  entryDate: number;
  countryCode: number;
  legalRepresentativeDocument: number;
  legalRepresentativeName: number;
  legalRepresentativeQualificationCode: number;
  ageGroupCode: number;
};

export type SecondaryCnaesExtractionIndices = {
  cnpjRoot: number;
  cnpjOrder: number;
  cnpjCheckDigits: number;
  secondaryCnaesRaw: number;
};

export function parseDelimitedLine(line: string): string[] {
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

export function normalizeFieldCount(
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

export function createFieldValueParser(
  dataType: TableLayout["fields"][number]["dataType"],
): FieldValueParser {
  switch (dataType) {
    case "integer":
      return (rawValue) => {
        const trimmed = rawValue.trim();
        if (trimmed === "") {
          return null;
        }

        return /^-?\d+$/.test(trimmed) ? Number.parseInt(trimmed, 10) : null;
      };
    case "numeric":
      return (rawValue) => {
        const trimmed = rawValue.trim();
        if (trimmed === "") {
          return null;
        }

        if (trimmed.includes(",") && trimmed.includes(".")) {
          return trimmed.replace(/\./g, "").replace(/,/g, ".");
        }

        if (trimmed.includes(",")) {
          return trimmed.replace(/,/g, ".");
        }

        return trimmed;
      };
    case "date":
      return (rawValue) => {
        const trimmed = rawValue.trim();
        if (trimmed === "" || trimmed === "00000000") {
          return null;
        }

        if (!/^\d{8}$/.test(trimmed)) {
          return null;
        }

        return `${trimmed.slice(0, 4)}-${trimmed.slice(4, 6)}-${trimmed.slice(6, 8)}`;
      };
    case "boolean":
      return (rawValue) => {
        const trimmed = rawValue.trim();
        if (trimmed === "") {
          return null;
        }

        const normalized = trimmed.toLowerCase();
        if (["1", "true", "t", "y", "yes", "s"].includes(normalized)) {
          return true;
        }
        if (["0", "false", "f", "n", "no"].includes(normalized)) {
          return false;
        }
        return null;
      };
    default:
      return (rawValue) => {
        const trimmed = rawValue.trim();
        return trimmed === "" ? null : trimmed;
      };
  }
}

export function toDatabaseValue(
  dataType: TableLayout["fields"][number]["dataType"],
  rawValue: string,
): unknown {
  return createFieldValueParser(dataType)(rawValue);
}

function normalizeCode(value: unknown, fallback: string): string {
  if (typeof value === "string" && value.trim() !== "") {
    return value.trim();
  }

  return fallback;
}

export function createPartnerDedupeKeyBuilder(
  indices: PartnerDedupeKeyIndices,
): (record: readonly unknown[]) => string {
  const orderedIndices = [
    indices.cnpjRoot,
    indices.partnerTypeCode,
    indices.partnerName,
    indices.partnerDocument,
    indices.partnerQualificationCode,
    indices.entryDate,
    indices.countryCode,
    indices.legalRepresentativeDocument,
    indices.legalRepresentativeName,
    indices.legalRepresentativeQualificationCode,
    indices.ageGroupCode,
  ];

  return (record) =>
    orderedIndices
      .map((index) => {
        const value = record[index];
        return value == null ? "" : String(value).trim();
      })
      .join("|");
}

export function createSecondaryCnaesExtractor(
  indices: SecondaryCnaesExtractionIndices,
): (record: readonly unknown[]) => Array<[string, string, number]> {
  return (record) => {
    const root = String(record[indices.cnpjRoot] ?? "");
    const order = String(record[indices.cnpjOrder] ?? "");
    const digits = String(record[indices.cnpjCheckDigits] ?? "");
    const raw = record[indices.secondaryCnaesRaw];

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

export function transformRecord(
  dataset: ImportDatasetType,
  layout: TableLayout,
  rawFields: string[],
  schemaCapabilities: ImportSchemaCapabilities,
  writeTarget: ImportWriteTarget,
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
    writeTarget === "final" &&
    schemaCapabilities.includePartnerDedupeKeyInInsert
  ) {
    return [...values, buildPartnerDedupeKey(recordByColumn)];
  }

  return layout.fields.map((field) => recordByColumn[field.columnName]);
}

export function buildParsedPayload(
  columns: string[],
  values: unknown[],
): Record<string, unknown> {
  return Object.fromEntries(
    columns.map((column, index) => [column, values[index] ?? null]),
  );
}

export function extractSecondaryCnaes(
  record: unknown[],
  columns: string[],
): Array<[string, string, number]> {
  return createSecondaryCnaesExtractor({
    cnpjRoot: columns.indexOf("cnpj_root"),
    cnpjOrder: columns.indexOf("cnpj_order"),
    cnpjCheckDigits: columns.indexOf("cnpj_check_digits"),
    secondaryCnaesRaw: columns.indexOf("secondary_cnaes_raw"),
  })(record);
}
