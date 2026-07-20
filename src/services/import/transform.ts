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

export type EstablishmentCnpjFullIndices = {
  cnpjRoot: number;
  cnpjOrder: number;
  cnpjCheckDigits: number;
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

export function createEstablishmentCnpjFullBuilder(
  indices: EstablishmentCnpjFullIndices,
): (record: readonly unknown[]) => string {
  return (record) => {
    const root = String(record[indices.cnpjRoot] ?? "").trim();
    const order = String(record[indices.cnpjOrder] ?? "").trim();
    const digits = String(record[indices.cnpjCheckDigits] ?? "").trim();
    return `${root}${order}${digits}`;
  };
}

function buildPartnerDedupeKey(
  recordByColumn: Record<string, unknown>,
): string {
  return [
    recordByColumn.cnpj_basico,
    recordByColumn.identificador_socio,
    recordByColumn.nome_socio_razao_social,
    recordByColumn.cnpj_cpf_socio,
    recordByColumn.codigo_qualificacao_socio,
    recordByColumn.data_entrada_sociedade,
    recordByColumn.codigo_pais,
    recordByColumn.cpf_representante_legal,
    recordByColumn.nome_representante_legal,
    recordByColumn.codigo_qualificacao_representante_legal,
    recordByColumn.codigo_faixa_etaria,
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
    recordByColumn.codigo_porte_empresa = normalizeCode(
      recordByColumn.codigo_porte_empresa,
      "00",
    );
  }

  if (dataset === "establishments") {
    recordByColumn.identificador_matriz_filial = normalizeCode(
      recordByColumn.identificador_matriz_filial,
      "1",
    );
    recordByColumn.situacao_cadastral = normalizeCode(
      recordByColumn.situacao_cadastral,
      "01",
    );
  }

  const normalizedValues = layout.fields.map(
    (field) => recordByColumn[field.columnName],
  );

  if (writeTarget === "final") {
    if (
      dataset === "establishments" &&
      schemaCapabilities.includeEstablishmentCnpjFullInInsert
    ) {
      return [
        ...normalizedValues,
        `${recordByColumn.cnpj_basico ?? ""}${recordByColumn.cnpj_ordem ?? ""}${recordByColumn.cnpj_dv ?? ""}`,
      ];
    }

    if (
      dataset === "partners" &&
      schemaCapabilities.includePartnerDedupeKeyInInsert
    ) {
      return [...normalizedValues, buildPartnerDedupeKey(recordByColumn)];
    }
  }

  return normalizedValues;
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
  void record;
  void columns;
  return [];
}
