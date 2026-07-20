import { Client } from "pg";

import {
  COLUNA_ATUALIZADO_EM,
  COLUNA_CHAVE_DEDUPLICACAO_SOCIO,
  COLUNA_CNPJ_BASICO,
  COLUNA_CNPJ_COMPLETO,
  COLUNA_CNPJ_DV,
  COLUNA_CNPJ_ORDEM,
  COLUNA_CODIGO,
  COLUNA_CODIGO_CNAE,
  COLUNA_DESCRICAO,
} from "../schema/table-names.js";
import { DATASET_LAYOUTS } from "./types.js";
import type {
  ImportDatasetType,
  ImportSchemaCapabilities,
  ImportWriteTarget,
} from "./types.js";

export function getInsertColumns(
  dataset: ImportDatasetType,
  schemaCapabilities: ImportSchemaCapabilities,
  writeTarget: ImportWriteTarget = "final",
): string[] {
  const columns = DATASET_LAYOUTS[dataset].fields.map(
    (field) => field.columnName,
  );

  if (writeTarget === "final") {
    if (
      dataset === "establishments" &&
      schemaCapabilities.includeEstablishmentCnpjFullInInsert
    ) {
      return [...columns, COLUNA_CNPJ_COMPLETO];
    }

    if (
      dataset === "partners" &&
      schemaCapabilities.includePartnerDedupeKeyInInsert
    ) {
      return [...columns, COLUNA_CHAVE_DEDUPLICACAO_SOCIO];
    }
  }

  return columns;
}

export function getConflictClause(
  dataset: ImportDatasetType,
  columns: string[],
  schemaCapabilities?: ImportSchemaCapabilities,
): string {
  switch (dataset) {
    case "countries":
    case "cities":
    case "partner_qualifications":
    case "legal_natures":
    case "cnaes":
    case "reasons":
      return `on conflict (${COLUNA_CODIGO}) do update set ${COLUNA_DESCRICAO} = excluded.${COLUNA_DESCRICAO}`;
    case "companies": {
      const updateColumns = columns
        .filter((column) => column !== COLUNA_CNPJ_BASICO)
        .map((column) => `${column} = excluded.${column}`)
        .concat([`${COLUNA_ATUALIZADO_EM} = now()`])
        .join(", ");
      return `on conflict (${COLUNA_CNPJ_BASICO}) do update set ${updateColumns}`;
    }
    case "establishments": {
      const updateColumns = columns
        .filter(
          (column) =>
            ![
              COLUNA_CNPJ_BASICO,
              COLUNA_CNPJ_ORDEM,
              COLUNA_CNPJ_DV,
              COLUNA_CNPJ_COMPLETO,
            ].includes(column),
        )
        .map((column) => `${column} = excluded.${column}`)
        .concat([`${COLUNA_ATUALIZADO_EM} = now()`])
        .join(", ");
      const conflictTarget =
        schemaCapabilities?.includeEstablishmentCnpjFullInInsert
          ? COLUNA_CNPJ_COMPLETO
          : `${COLUNA_CNPJ_BASICO}, ${COLUNA_CNPJ_ORDEM}, ${COLUNA_CNPJ_DV}`;
      return `on conflict (${conflictTarget}) do update set ${updateColumns}`;
    }
    case "simples_options": {
      const updateColumns = columns
        .filter((column) => column !== COLUNA_CNPJ_BASICO)
        .map((column) => `${column} = excluded.${column}`)
        .concat([`${COLUNA_ATUALIZADO_EM} = now()`])
        .join(", ");
      return `on conflict (${COLUNA_CNPJ_BASICO}) do update set ${updateColumns}`;
    }
    case "partners": {
      const updateColumns = columns
        .filter((column) => column !== COLUNA_CHAVE_DEDUPLICACAO_SOCIO)
        .map((column) => `${column} = excluded.${column}`)
        .concat([`${COLUNA_ATUALIZADO_EM} = now()`])
        .join(", ");
      return `on conflict (${COLUNA_CHAVE_DEDUPLICACAO_SOCIO}) do update set ${updateColumns}`;
    }
    default:
      return "";
  }
}

export function buildInsertQuery(
  tableName: string,
  columns: string[],
  rows: readonly unknown[][],
  conflictClause = "",
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

export function buildSecondaryInsertQuery(
  tableName: string,
  rows: ReadonlyArray<[string, string, number]>,
  conflictClause = "",
): {
  text: string;
  values: unknown[];
} {
  return buildInsertQuery(
    tableName,
    [COLUNA_CNPJ_COMPLETO, COLUNA_CODIGO_CNAE],
    rows.map((row) => [row[0], row[1]]),
    conflictClause,
  );
}

export async function flushInsertQuery(
  client: Client,
  query: { text: string; values: unknown[] },
): Promise<void> {
  await client.query(query);
}
