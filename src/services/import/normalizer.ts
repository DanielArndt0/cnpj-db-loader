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
      `Valor obrigatório ausente para ${layout.fields[index]?.columnName ?? "coluna desconhecida"}.`,
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
      ? resolveLayoutColumnIndex(input.layout, "codigo_porte_empresa")
      : -1;
  const branchTypeIndex =
    input.dataset === "establishments"
      ? resolveLayoutColumnIndex(input.layout, "identificador_matriz_filial")
      : -1;
  const registrationStatusIndex =
    input.dataset === "establishments"
      ? resolveLayoutColumnIndex(input.layout, "situacao_cadastral")
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
        cnpjRoot: resolveLayoutColumnIndex(input.layout, "cnpj_basico"),
        cnpjOrder: resolveLayoutColumnIndex(input.layout, "cnpj_ordem"),
        cnpjCheckDigits: resolveLayoutColumnIndex(input.layout, "cnpj_dv"),
      })
    : null;
  const buildPartnerDedupeKey = appendPartnerDedupeKey
    ? createPartnerDedupeKeyBuilder({
        cnpjRoot: resolveLayoutColumnIndex(input.layout, "cnpj_basico"),
        partnerTypeCode: resolveLayoutColumnIndex(
          input.layout,
          "identificador_socio",
        ),
        partnerName: resolveLayoutColumnIndex(
          input.layout,
          "nome_socio_razao_social",
        ),
        partnerDocument: resolveLayoutColumnIndex(
          input.layout,
          "cnpj_cpf_socio",
        ),
        partnerQualificationCode: resolveLayoutColumnIndex(
          input.layout,
          "codigo_qualificacao_socio",
        ),
        entryDate: resolveLayoutColumnIndex(
          input.layout,
          "data_entrada_sociedade",
        ),
        countryCode: resolveLayoutColumnIndex(input.layout, "codigo_pais"),
        legalRepresentativeDocument: resolveLayoutColumnIndex(
          input.layout,
          "cpf_representante_legal",
        ),
        legalRepresentativeName: resolveLayoutColumnIndex(
          input.layout,
          "nome_representante_legal",
        ),
        legalRepresentativeQualificationCode: resolveLayoutColumnIndex(
          input.layout,
          "codigo_qualificacao_representante_legal",
        ),
        ageGroupCode: resolveLayoutColumnIndex(
          input.layout,
          "codigo_faixa_etaria",
        ),
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
