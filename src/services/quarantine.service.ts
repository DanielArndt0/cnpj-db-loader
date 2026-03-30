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
      "The quarantine command failed while querying PostgreSQL.",
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
      'The "--limit" option must be a positive integer.',
    );
  }

  if (
    typeof filters.afterId === "number" &&
    (!Number.isInteger(filters.afterId) || filters.afterId < 0)
  ) {
    throw new ValidationError(
      'The "--after-id" option must be a non-negative integer.',
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
      "The quarantine row id must be a positive integer.",
    );
  }

  const record = await withQuarantineClient(options?.dbUrl, async (client) =>
    readQuarantineRecordById(client, id),
  );

  if (!record) {
    throw new ValidationError(`No quarantine row was found with id ${id}.`);
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
