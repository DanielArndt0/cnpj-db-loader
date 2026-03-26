import { Client } from "pg";

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

  if (
    dataset === "partners" &&
    writeTarget === "final" &&
    schemaCapabilities.includePartnerDedupeKeyInInsert
  ) {
    return [...columns, "partner_dedupe_key"];
  }

  return columns;
}

export function getConflictClause(
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
    ["establishment_cnpj_full", "cnae_code", "source_order"],
    rows,
    conflictClause,
  );
}

export async function flushInsertQuery(
  client: Client,
  query: { text: string; values: unknown[] },
): Promise<void> {
  await client.query(query);
}
