import { Client } from "pg";

import { ServiceError, ValidationError } from "../core/errors/index.js";
import { readDatabaseConfig } from "./config.service.js";
import {
  cleanupDatabaseCheckpoints,
  cleanupDatabaseMaterializedTables,
  cleanupDatabasePlans,
  cleanupDatabaseStaging,
  type CheckpointCleanupPhase,
  type DatabaseCleanupSummary,
} from "./database/cleanup.js";
import type { ImportDatasetType } from "./import/types.js";

export type { CheckpointCleanupPhase, DatabaseCleanupSummary };

export async function resolveDatabaseUrl(override?: string): Promise<string> {
  if (override) {
    return override;
  }

  const config = await readDatabaseConfig();

  if (!config.defaultDbUrl) {
    throw new ValidationError(
      'No database connection is configured. Use "cnpj-db-loader database config set <postgres-url>" or pass "--db-url".',
    );
  }

  return config.defaultDbUrl;
}

export async function testDatabaseConnection(url: string): Promise<void> {
  const client = new Client({ connectionString: url });

  try {
    await client.connect();
    await client.query("select 1");
  } catch (error) {
    throw new ServiceError("The PostgreSQL connection test failed.", error);
  } finally {
    await client.end().catch(() => undefined);
  }
}

async function withDatabaseClient<T>(
  dbUrl: string,
  operation: (client: Client) => Promise<T>,
): Promise<T> {
  const client = new Client({ connectionString: dbUrl });
  await client.connect();

  try {
    return await operation(client);
  } catch (error) {
    throw new ServiceError("The database maintenance operation failed.", error);
  } finally {
    await client.end().catch(() => undefined);
  }
}

export async function cleanupDatabaseStagingData(
  options: {
    dbUrl?: string;
    dataset?: ImportDatasetType | undefined;
    validatedPath?: string | undefined;
  } = {},
): Promise<DatabaseCleanupSummary> {
  const dbUrl = await resolveDatabaseUrl(options.dbUrl);
  return withDatabaseClient(dbUrl, async (client) =>
    cleanupDatabaseStaging(client, {
      dbUrl,
      dataset: options.dataset,
      validatedPath: options.validatedPath,
    }),
  );
}

export async function cleanupDatabaseMaterializedData(
  options: {
    dbUrl?: string;
    dataset?: ImportDatasetType | undefined;
  } = {},
): Promise<DatabaseCleanupSummary> {
  const dbUrl = await resolveDatabaseUrl(options.dbUrl);
  return withDatabaseClient(dbUrl, async (client) =>
    cleanupDatabaseMaterializedTables(client, {
      dbUrl,
      dataset: options.dataset,
    }),
  );
}

export async function cleanupDatabaseCheckpointsData(
  options: {
    dbUrl?: string;
    phase?: CheckpointCleanupPhase | undefined;
    dataset?: ImportDatasetType | undefined;
    validatedPath?: string | undefined;
    planId?: number | undefined;
  } = {},
): Promise<DatabaseCleanupSummary> {
  const phase = options.phase ?? "all";
  if (!["load", "materialization", "all"].includes(phase)) {
    throw new ValidationError(
      `Unsupported checkpoint cleanup phase: ${phase}. Use load, materialization, or all.`,
    );
  }

  const dbUrl = await resolveDatabaseUrl(options.dbUrl);
  return withDatabaseClient(dbUrl, async (client) =>
    cleanupDatabaseCheckpoints(client, {
      dbUrl,
      phase,
      dataset: options.dataset,
      validatedPath: options.validatedPath,
      planId: options.planId,
    }),
  );
}

export async function cleanupDatabasePlansData(
  options: {
    dbUrl?: string;
    validatedPath?: string | undefined;
    planId?: number | undefined;
  } = {},
): Promise<DatabaseCleanupSummary> {
  const dbUrl = await resolveDatabaseUrl(options.dbUrl);
  return withDatabaseClient(dbUrl, async (client) =>
    cleanupDatabasePlans(client, {
      dbUrl,
      validatedPath: options.validatedPath,
      planId: options.planId,
    }),
  );
}

export async function resolveDbUrl(override?: string): Promise<string> {
  return resolveDatabaseUrl(override);
}
