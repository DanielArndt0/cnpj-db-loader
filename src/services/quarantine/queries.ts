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
    pushCondition("conjunto = $VALUE$", filters.dataset);
  }

  if (filters.category) {
    pushCondition("coalesce(categoria_erro, '') = $VALUE$", filters.category);
  }

  if (filters.stage) {
    pushCondition("coalesce(etapa_erro, '') = $VALUE$", filters.stage);
  }

  if (filters.retryable && filters.terminal) {
    throw new ValidationError(
      'Use "--retryable" ou "--terminal", mas não os dois juntos.',
    );
  }

  if (filters.retryable) {
    conditions.push("pode_tentar_novamente = true");
  }

  if (filters.terminal) {
    conditions.push("pode_tentar_novamente = false");
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
       count(*) filter (where pode_tentar_novamente = true)::bigint as retryable_rows,
       count(*) filter (where pode_tentar_novamente = false)::bigint as terminal_rows
     from quarentena_importacao
     ${where.sql}`,
    where.values,
  );

  const rowsByDatasetResult = await client.query(
    `select conjunto as dataset, count(*)::bigint as count
     from quarentena_importacao
     ${where.sql}
     group by conjunto
     order by count desc, conjunto asc`,
    where.values,
  );

  const rowsByCategoryResult = await client.query(
    `select coalesce(categoria_erro, 'unknown') as error_category, count(*)::bigint as count
     from quarentena_importacao
     ${where.sql}
     group by coalesce(categoria_erro, 'unknown')
     order by count desc, error_category asc`,
    where.values,
  );

  const rowsByStageResult = await client.query(
    `select coalesce(etapa_erro, 'unknown') as error_stage, count(*)::bigint as count
     from quarentena_importacao
     ${where.sql}
     group by coalesce(etapa_erro, 'unknown')
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
      conjunto as dataset,
      caminho_arquivo as file_path,
      numero_linha as row_number,
      deslocamento_checkpoint as checkpoint_offset,
      codigo_erro as error_code,
      categoria_erro as error_category,
      etapa_erro as error_stage,
      mensagem_erro as error_message,
      total_tentativas as retry_count,
      pode_tentar_novamente as can_retry_later,
      criado_em as created_at
    from quarentena_importacao
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
       conjunto as dataset,
       caminho_arquivo as file_path,
       numero_linha as row_number,
       deslocamento_checkpoint as checkpoint_offset,
       codigo_erro as error_code,
       categoria_erro as error_category,
       etapa_erro as error_stage,
       mensagem_erro as error_message,
       linha_bruta as raw_line,
       payload_parseado as parsed_payload,
       sanitizacoes_aplicadas as sanitizations_applied,
       total_tentativas as retry_count,
       pode_tentar_novamente as can_retry_later,
       criado_em as created_at
     from quarentena_importacao
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
