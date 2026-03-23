import type { Client } from "pg";

import { ValidationError } from "../../core/errors/index.js";
import type {
  QuarantineListFilters,
  QuarantineListRow,
  QuarantineListSummary,
  QuarantineRecord,
  QuarantineStatsCount,
  QuarantineStatsFilters,
  QuarantineStatsSummary,
} from "./types.js";

type WhereClause = {
  sql: string;
  values: unknown[];
};

function buildWhereClause(filters: QuarantineStatsFilters): WhereClause {
  const conditions: string[] = [];
  const values: unknown[] = [];

  const pushCondition = (condition: string, value: unknown): void => {
    values.push(value);
    conditions.push(condition.replace("$VALUE$", `$${values.length}`));
  };

  if (filters.dataset) {
    pushCondition("dataset = $VALUE$", filters.dataset);
  }

  if (filters.category) {
    pushCondition("coalesce(error_category, '') = $VALUE$", filters.category);
  }

  if (filters.stage) {
    pushCondition("coalesce(error_stage, '') = $VALUE$", filters.stage);
  }

  if (filters.retryable && filters.terminal) {
    throw new ValidationError(
      'Use either "--retryable" or "--terminal", but not both together.',
    );
  }

  if (filters.retryable) {
    conditions.push("can_retry_later = true");
  }

  if (filters.terminal) {
    conditions.push("can_retry_later = false");
  }

  return {
    sql: conditions.length > 0 ? `where ${conditions.join(" and ")}` : "",
    values,
  };
}

function mapCountRows(
  rows: Array<Record<string, unknown>>,
  keyName: string,
): QuarantineStatsCount[] {
  return rows.map((row) => ({
    key: String(row[keyName] ?? "unknown"),
    count: Number(row.count ?? 0),
  }));
}

export async function readQuarantineStats(
  client: Client,
  filters: QuarantineStatsFilters,
): Promise<QuarantineStatsSummary> {
  const where = buildWhereClause(filters);

  const totalRowsResult = await client.query(
    `select
       count(*)::bigint as total_rows,
       count(*) filter (where can_retry_later = true)::bigint as retryable_rows,
       count(*) filter (where can_retry_later = false)::bigint as terminal_rows
     from import_quarantine
     ${where.sql}`,
    where.values,
  );

  const rowsByDatasetResult = await client.query(
    `select dataset, count(*)::bigint as count
     from import_quarantine
     ${where.sql}
     group by dataset
     order by count desc, dataset asc`,
    where.values,
  );

  const rowsByCategoryResult = await client.query(
    `select coalesce(error_category, 'unknown') as error_category, count(*)::bigint as count
     from import_quarantine
     ${where.sql}
     group by coalesce(error_category, 'unknown')
     order by count desc, error_category asc`,
    where.values,
  );

  const rowsByStageResult = await client.query(
    `select coalesce(error_stage, 'unknown') as error_stage, count(*)::bigint as count
     from import_quarantine
     ${where.sql}
     group by coalesce(error_stage, 'unknown')
     order by count desc, error_stage asc`,
    where.values,
  );

  const totals = totalRowsResult.rows[0] ?? {};

  return {
    totalRows: Number(totals.total_rows ?? 0),
    retryableRows: Number(totals.retryable_rows ?? 0),
    terminalRows: Number(totals.terminal_rows ?? 0),
    rowsByDataset: mapCountRows(rowsByDatasetResult.rows, "dataset"),
    rowsByCategory: mapCountRows(rowsByCategoryResult.rows, "error_category"),
    rowsByStage: mapCountRows(rowsByStageResult.rows, "error_stage"),
    appliedFilters: filters,
  };
}

export async function readQuarantineList(
  client: Client,
  filters: QuarantineListFilters,
): Promise<QuarantineListSummary> {
  const where = buildWhereClause(filters);
  const conditions = where.sql ? [where.sql.replace(/^where\s+/i, "")] : [];
  const values = [...where.values];

  if (typeof filters.afterId === "number") {
    values.push(filters.afterId);
    conditions.push(`id > $${values.length}`);
  }

  values.push(filters.limit);

  const query = `select
      id,
      dataset,
      file_path,
      row_number,
      checkpoint_offset,
      error_code,
      error_category,
      error_stage,
      error_message,
      retry_count,
      can_retry_later,
      created_at
    from import_quarantine
    ${conditions.length > 0 ? `where ${conditions.join(" and ")}` : ""}
    order by id asc
    limit $${values.length}`;

  const result = await client.query(query, values);

  return {
    rows: result.rows.map(
      (row) =>
        ({
          id: Number(row.id),
          dataset: String(row.dataset),
          filePath: String(row.file_path),
          rowNumber: row.row_number === null ? null : Number(row.row_number),
          checkpointOffset:
            row.checkpoint_offset === null
              ? null
              : Number(row.checkpoint_offset),
          errorCode: row.error_code === null ? null : String(row.error_code),
          errorCategory:
            row.error_category === null ? null : String(row.error_category),
          errorStage: row.error_stage === null ? null : String(row.error_stage),
          errorMessage: String(row.error_message),
          retryCount: Number(row.retry_count ?? 0),
          canRetryLater: Boolean(row.can_retry_later),
          createdAt: new Date(row.created_at).toISOString(),
        }) satisfies QuarantineListRow,
    ),
    appliedFilters: filters,
  };
}

export async function readQuarantineRecordById(
  client: Client,
  id: number,
): Promise<QuarantineRecord | null> {
  const result = await client.query(
    `select
       id,
       dataset,
       file_path,
       row_number,
       checkpoint_offset,
       error_code,
       error_category,
       error_stage,
       error_message,
       raw_line,
       parsed_payload,
       sanitizations_applied,
       retry_count,
       can_retry_later,
       created_at
     from import_quarantine
     where id = $1`,
    [id],
  );

  const row = result.rows[0];
  if (!row) {
    return null;
  }

  return {
    id: Number(row.id),
    dataset: String(row.dataset),
    filePath: String(row.file_path),
    rowNumber: row.row_number === null ? null : Number(row.row_number),
    checkpointOffset:
      row.checkpoint_offset === null ? null : Number(row.checkpoint_offset),
    errorCode: row.error_code === null ? null : String(row.error_code),
    errorCategory:
      row.error_category === null ? null : String(row.error_category),
    errorStage: row.error_stage === null ? null : String(row.error_stage),
    errorMessage: String(row.error_message),
    rawLine: String(row.raw_line),
    parsedPayload:
      row.parsed_payload && typeof row.parsed_payload === "object"
        ? (row.parsed_payload as Record<string, unknown>)
        : null,
    sanitizationsApplied: Array.isArray(row.sanitizations_applied)
      ? (row.sanitizations_applied as unknown[])
      : [],
    retryCount: Number(row.retry_count ?? 0),
    canRetryLater: Boolean(row.can_retry_later),
    createdAt: new Date(row.created_at).toISOString(),
  };
}
