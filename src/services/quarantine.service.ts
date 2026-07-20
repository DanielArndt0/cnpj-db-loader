import { Client } from "pg";

import { ServiceError, ValidationError } from "../core/errors/index.js";
import { resolveDatabaseUrl } from "./database.service.js";
import { ensureQuarantineTable } from "./import/quarantine.js";
import {
  readQuarantineList,
  readQuarantineRecordById,
  readQuarantineStats,
} from "./quarantine/queries.js";
import type {
  QuarantineListFilters,
  QuarantineListSummary,
  QuarantineRecord,
  QuarantineStatsFilters,
  QuarantineStatsSummary,
} from "./quarantine/types.js";

async function withQuarantineClient<T>(
  dbUrl: string | undefined,
  action: (client: Client) => Promise<T>,
): Promise<T> {
  const url = await resolveDatabaseUrl(dbUrl);
  const client = new Client({ connectionString: url });

  try {
    await client.connect();
    await ensureQuarantineTable(client);
    return await action(client);
  } catch (error) {
    if (error instanceof ValidationError) {
      throw error;
    }

    throw new ServiceError(
      "O comando de quarentena falhou ao consultar o PostgreSQL.",
      error,
    );
  } finally {
    await client.end().catch(() => undefined);
  }
}

export async function getQuarantineStats(
  filters: QuarantineStatsFilters & { dbUrl?: string },
): Promise<QuarantineStatsSummary> {
  return withQuarantineClient(filters.dbUrl, async (client) =>
    readQuarantineStats(client, filters),
  );
}

export async function listQuarantineRows(
  filters: QuarantineListFilters & { dbUrl?: string },
): Promise<QuarantineListSummary> {
  if (!Number.isInteger(filters.limit) || filters.limit <= 0) {
    throw new ValidationError(
      'A opção "--limit" deve ser um inteiro positivo.',
    );
  }

  if (
    typeof filters.afterId === "number" &&
    (!Number.isInteger(filters.afterId) || filters.afterId < 0)
  ) {
    throw new ValidationError(
      'A opção "--after-id" deve ser um inteiro não negativo.',
    );
  }

  return withQuarantineClient(filters.dbUrl, async (client) =>
    readQuarantineList(client, filters),
  );
}

export async function showQuarantineRow(
  id: number,
  options?: { dbUrl?: string },
): Promise<QuarantineRecord> {
  if (!Number.isInteger(id) || id <= 0) {
    throw new ValidationError(
      "O id da linha de quarentena deve ser um inteiro positivo.",
    );
  }

  const record = await withQuarantineClient(options?.dbUrl, async (client) =>
    readQuarantineRecordById(client, id),
  );

  if (!record) {
    throw new ValidationError(
      `Nenhuma linha de quarentena foi encontrada com o id ${id}.`,
    );
  }

  return record;
}

export type {
  QuarantineListFilters,
  QuarantineListSummary,
  QuarantineRecord,
  QuarantineStatsFilters,
  QuarantineStatsSummary,
} from "./quarantine/types.js";
