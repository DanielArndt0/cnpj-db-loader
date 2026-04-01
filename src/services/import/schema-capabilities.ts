import { Client } from "pg";

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

export async function detectImportSchemaCapabilities(
  client: Client,
): Promise<ImportSchemaCapabilities> {
  const [columnResult, lookupConstraintResult] = await Promise.all([
    client.query<ColumnCapabilityRow>(
      `select table_name, column_name, is_generated
         from information_schema.columns
        where table_schema = current_schema()
          and (
            (table_name = 'establishments' and column_name = 'cnpj_full') or
            (table_name = 'partners' and column_name = 'partner_dedupe_key')
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
            and source_table.relname in ('companies', 'establishments', 'partners')
            and target_table.relname in (
              'countries',
              'cities',
              'partner_qualifications',
              'legal_natures',
              'cnaes',
              'reasons',
              'company_sizes',
              'branch_types',
              'registration_statuses',
              'partner_types',
              'age_groups'
            )
       ) as requires_lookup_reconciliation`,
    ),
  ]);

  return {
    includeEstablishmentCnpjFullInInsert: canInsertIntoColumn(
      columnResult.rows,
      "establishments",
      "cnpj_full",
    ),
    includePartnerDedupeKeyInInsert: canInsertIntoColumn(
      columnResult.rows,
      "partners",
      "partner_dedupe_key",
    ),
    requiresLookupReconciliation:
      lookupConstraintResult.rows[0]?.requires_lookup_reconciliation ?? false,
  };
}
