import { Client } from "pg";

import {
  COLUNA_CHAVE_DEDUPLICACAO_SOCIO,
  COLUNA_CNPJ_COMPLETO,
  COLUNA_CODIGO_CNAE,
  NOMES_TABELAS_LOOKUP,
  TABELA_ESTABELECIMENTO_CNAES_SECUNDARIOS,
  TABELA_ESTABELECIMENTOS,
  TABELA_EMPRESAS,
  TABELA_SOCIOS,
} from "../schema/table-names.js";
import type { ImportSchemaCapabilities } from "./types.js";

type ColumnCapabilityRow = {
  table_name: string;
  column_name: string;
  is_generated: string;
};

type LookupConstraintRow = {
  requires_lookup_reconciliation: boolean;
};

function canInsertIntoColumn(
  rows: readonly ColumnCapabilityRow[],
  tableName: string,
  columnName: string,
): boolean {
  const row = rows.find(
    (item) => item.table_name === tableName && item.column_name === columnName,
  );

  if (!row) {
    return false;
  }

  return row.is_generated.toUpperCase() !== "ALWAYS";
}

function hasRequiredColumns(
  rows: readonly ColumnCapabilityRow[],
  tableName: string,
  columnNames: readonly string[],
): boolean {
  const availableColumns = new Set(
    rows
      .filter((item) => item.table_name === tableName)
      .map((item) => item.column_name),
  );

  return columnNames.every((columnName) => availableColumns.has(columnName));
}

export async function detectImportSchemaCapabilities(
  client: Client,
): Promise<ImportSchemaCapabilities> {
  const lookupTableList = Object.values(NOMES_TABELAS_LOOKUP)
    .map((tableName) => `'${tableName}'`)
    .join(", ");

  const [columnResult, lookupConstraintResult] = await Promise.all([
    client.query<ColumnCapabilityRow>(
      `select table_name, column_name, is_generated
         from information_schema.columns
        where table_schema = current_schema()
          and (
            (table_name = '${TABELA_ESTABELECIMENTOS}' and column_name = '${COLUNA_CNPJ_COMPLETO}') or
            (table_name = '${TABELA_ESTABELECIMENTO_CNAES_SECUNDARIOS}' and column_name in ('${COLUNA_CNPJ_COMPLETO}', '${COLUNA_CODIGO_CNAE}')) or
            (table_name = '${TABELA_SOCIOS}' and column_name = '${COLUNA_CHAVE_DEDUPLICACAO_SOCIO}')
          )`,
    ),
    client.query<LookupConstraintRow>(
      `select exists (
         select 1
           from pg_constraint constraint_item
           inner join pg_class source_table on source_table.oid = constraint_item.conrelid
           inner join pg_namespace source_namespace on source_namespace.oid = source_table.relnamespace
           inner join pg_class target_table on target_table.oid = constraint_item.confrelid
          where constraint_item.contype = 'f'
            and source_namespace.nspname = current_schema()
            and source_table.relname in ('${TABELA_EMPRESAS}', '${TABELA_ESTABELECIMENTOS}', '${TABELA_SOCIOS}', '${TABELA_ESTABELECIMENTO_CNAES_SECUNDARIOS}')
            and target_table.relname in (${lookupTableList})
       ) as requires_lookup_reconciliation`,
    ),
  ]);

  return {
    includeEstablishmentCnpjFullInInsert: canInsertIntoColumn(
      columnResult.rows,
      TABELA_ESTABELECIMENTOS,
      COLUNA_CNPJ_COMPLETO,
    ),
    includeEstablishmentSecondaryCnaesTable: hasRequiredColumns(
      columnResult.rows,
      TABELA_ESTABELECIMENTO_CNAES_SECUNDARIOS,
      [COLUNA_CNPJ_COMPLETO, COLUNA_CODIGO_CNAE],
    ),
    includePartnerDedupeKeyInInsert: canInsertIntoColumn(
      columnResult.rows,
      TABELA_SOCIOS,
      COLUNA_CHAVE_DEDUPLICACAO_SOCIO,
    ),
    requiresLookupReconciliation:
      lookupConstraintResult.rows[0]?.requires_lookup_reconciliation ?? false,
  };
}
