import { Client } from "pg";

import { NOMES_TABELAS_LOOKUP } from "../schema/table-names.js";
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

  for (const lookupKey of LOOKUP_TABLES) {
    const result = await client.query<{ codigo: string | null }>(
      `select codigo from ${NOMES_TABELAS_LOOKUP[lookupKey]}`,
    );
    cache.set(
      lookupKey,
      new Set(
        result.rows
          .map((row) => row.codigo?.trim())
          .filter((value): value is string => Boolean(value)),
      ),
    );
  }

  return cache;
}

async function ensureLookupCodes(
  client: Client,
  cache: LookupCacheMap,
  lookupKey: LookupTableName,
  rawCodes: Array<unknown>,
): Promise<void> {
  const knownCodes = cache.get(lookupKey) ?? new Set<string>();
  cache.set(lookupKey, knownCodes);

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
    values.push(code, `${LOOKUP_PLACEHOLDER_LABEL[lookupKey]} (${code})`);
    const baseIndex = index * 2;
    placeholders.push(`($${baseIndex + 1}, $${baseIndex + 2})`);
  }

  await client.query(
    `insert into ${NOMES_TABELAS_LOOKUP[lookupKey]} (codigo, descricao) values ${placeholders.join(", ")} on conflict (codigo) do nothing`,
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
        columnValues("codigo_natureza_juridica"),
      );
      await ensureLookupCodes(
        client,
        cache,
        "partner_qualifications",
        columnValues("codigo_qualificacao_responsavel"),
      );
      await ensureLookupCodes(
        client,
        cache,
        "company_sizes",
        columnValues("codigo_porte_empresa"),
      );
      break;
    case "establishments":
      await ensureLookupCodes(
        client,
        cache,
        "branch_types",
        columnValues("identificador_matriz_filial"),
      );
      await ensureLookupCodes(
        client,
        cache,
        "registration_statuses",
        columnValues("situacao_cadastral"),
      );
      await ensureLookupCodes(
        client,
        cache,
        "reasons",
        columnValues("motivo_situacao_cadastral"),
      );
      await ensureLookupCodes(
        client,
        cache,
        "countries",
        columnValues("codigo_pais"),
      );
      await ensureLookupCodes(
        client,
        cache,
        "cnaes",
        columnValues("cnae_fiscal_principal"),
      );
      await ensureLookupCodes(
        client,
        cache,
        "cities",
        columnValues("codigo_municipio"),
      );
      break;
    case "partners":
      await ensureLookupCodes(
        client,
        cache,
        "partner_types",
        columnValues("identificador_socio"),
      );
      await ensureLookupCodes(
        client,
        cache,
        "partner_qualifications",
        columnValues("codigo_qualificacao_socio"),
      );
      await ensureLookupCodes(
        client,
        cache,
        "countries",
        columnValues("codigo_pais"),
      );
      await ensureLookupCodes(
        client,
        cache,
        "partner_qualifications",
        columnValues("codigo_qualificacao_representante_legal"),
      );
      await ensureLookupCodes(
        client,
        cache,
        "age_groups",
        columnValues("codigo_faixa_etaria"),
      );
      break;
    default:
      break;
  }
}
