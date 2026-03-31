import { ValidationError } from "../../core/errors/index.js";
import type { TableLayout } from "../../dictionary/layouts/index.js";
import { getInsertColumns } from "./sql.js";
import { resolveImportWriteTarget } from "./targets.js";
import {
  createEstablishmentCnpjFullBuilder,
  createFieldValueParser,
  createPartnerDedupeKeyBuilder,
  normalizeFieldCount,
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

export type ImportRowNormalizer = {
  columns: string[];
  normalize: (
    parsedLine: ParsedImportSourceLine,
    sourceRowNumber: number,
  ) => BatchRow;
};

function validateRequiredColumns(
  requiredIndexes: readonly number[],
  values: readonly unknown[],
  layout: TableLayout,
): void {
  for (const index of requiredIndexes) {
    if (values[index] !== null) {
      continue;
    }

    throw new ValidationError(
      `Missing required value for ${layout.fields[index]?.columnName ?? "unknown column"}.`,
    );
  }
}

function resolveLayoutColumnIndex(
  layout: TableLayout,
  columnName: string,
): number {
  return layout.fields.findIndex((field) => field.columnName === columnName);
}

export function createImportRowNormalizer(input: {
  dataset: ImportDatasetType;
  filePath: string;
  layout: TableLayout;
  schemaCapabilities: ImportSchemaCapabilities;
}): ImportRowNormalizer {
  const writeTarget = resolveImportWriteTarget(input.dataset);
  const columns = getInsertColumns(
    input.dataset,
    input.schemaCapabilities,
    writeTarget,
  );
  const expectedLength = input.layout.fields.length;
  const requiredIndexes = input.layout.fields
    .map((field, index) => (field.nullable ? -1 : index))
    .filter((index) => index >= 0);
  const fieldParsers = input.layout.fields.map((field) =>
    createFieldValueParser(field.dataType),
  );
  const companySizeIndex =
    input.dataset === "companies"
      ? resolveLayoutColumnIndex(input.layout, "company_size_code")
      : -1;
  const branchTypeIndex =
    input.dataset === "establishments"
      ? resolveLayoutColumnIndex(input.layout, "branch_type_code")
      : -1;
  const registrationStatusIndex =
    input.dataset === "establishments"
      ? resolveLayoutColumnIndex(input.layout, "registration_status_code")
      : -1;
  const appendEstablishmentCnpjFull =
    input.dataset === "establishments" &&
    writeTarget === "final" &&
    input.schemaCapabilities.includeEstablishmentCnpjFullInInsert;
  const appendPartnerDedupeKey =
    input.dataset === "partners" &&
    writeTarget === "final" &&
    input.schemaCapabilities.includePartnerDedupeKeyInInsert;
  const buildEstablishmentCnpjFull = appendEstablishmentCnpjFull
    ? createEstablishmentCnpjFullBuilder({
        cnpjRoot: resolveLayoutColumnIndex(input.layout, "cnpj_root"),
        cnpjOrder: resolveLayoutColumnIndex(input.layout, "cnpj_order"),
        cnpjCheckDigits: resolveLayoutColumnIndex(
          input.layout,
          "cnpj_check_digits",
        ),
      })
    : null;
  const buildPartnerDedupeKey = appendPartnerDedupeKey
    ? createPartnerDedupeKeyBuilder({
        cnpjRoot: resolveLayoutColumnIndex(input.layout, "cnpj_root"),
        partnerTypeCode: resolveLayoutColumnIndex(
          input.layout,
          "partner_type_code",
        ),
        partnerName: resolveLayoutColumnIndex(input.layout, "partner_name"),
        partnerDocument: resolveLayoutColumnIndex(
          input.layout,
          "partner_document",
        ),
        partnerQualificationCode: resolveLayoutColumnIndex(
          input.layout,
          "partner_qualification_code",
        ),
        entryDate: resolveLayoutColumnIndex(input.layout, "entry_date"),
        countryCode: resolveLayoutColumnIndex(input.layout, "country_code"),
        legalRepresentativeDocument: resolveLayoutColumnIndex(
          input.layout,
          "legal_representative_document",
        ),
        legalRepresentativeName: resolveLayoutColumnIndex(
          input.layout,
          "legal_representative_name",
        ),
        legalRepresentativeQualificationCode: resolveLayoutColumnIndex(
          input.layout,
          "legal_representative_qualification_code",
        ),
        ageGroupCode: resolveLayoutColumnIndex(input.layout, "age_group_code"),
      })
    : null;

  return {
    columns,
    normalize(parsedLine, sourceRowNumber) {
      const normalizedFields = normalizeFieldCount(
        parsedLine.fields,
        expectedLength,
        input.filePath,
        parsedLine.lineNumber,
      );
      const values = new Array<unknown>(expectedLength);

      for (let index = 0; index < expectedLength; index += 1) {
        values[index] =
          fieldParsers[index]?.(normalizedFields[index] ?? "") ?? null;
      }

      if (companySizeIndex >= 0) {
        const currentValue = values[companySizeIndex];
        values[companySizeIndex] =
          typeof currentValue === "string" && currentValue.trim() !== ""
            ? currentValue.trim()
            : "00";
      }

      if (branchTypeIndex >= 0) {
        const currentValue = values[branchTypeIndex];
        values[branchTypeIndex] =
          typeof currentValue === "string" && currentValue.trim() !== ""
            ? currentValue.trim()
            : "1";
      }

      if (registrationStatusIndex >= 0) {
        const currentValue = values[registrationStatusIndex];
        values[registrationStatusIndex] =
          typeof currentValue === "string" && currentValue.trim() !== ""
            ? currentValue.trim()
            : "01";
      }

      validateRequiredColumns(requiredIndexes, values, input.layout);

      if (buildEstablishmentCnpjFull) {
        values.push(buildEstablishmentCnpjFull(values));
      }

      if (buildPartnerDedupeKey) {
        values.push(buildPartnerDedupeKey(values));
      }

      return {
        values,
        rawLine: parsedLine.rawLine,
        nextOffset: parsedLine.nextOffset,
        sourceRowNumber,
        secondaryRows: [],
      };
    },
  };
}

export function normalizeImportRow({
  dataset,
  filePath,
  layout,
  parsedLine,
  schemaCapabilities,
  sourceRowNumber,
}: NormalizeImportRowInput): BatchRow {
  return createImportRowNormalizer({
    dataset,
    filePath,
    layout,
    schemaCapabilities,
  }).normalize(parsedLine, sourceRowNumber);
}
