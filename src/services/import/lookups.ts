import { Client } from "pg";

import type {
  ImportDatasetType,
  LookupCacheMap,
  LookupTableName,
} from "./types.js";
import { LOOKUP_PLACEHOLDER_LABEL, LOOKUP_TABLES } from "./types.js";

export async function loadLookupCaches(
  client: Client,
): Promise<LookupCacheMap> {
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
  const placeholders: string[] = [];

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

export async function ensureBatchForeignKeys(
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
