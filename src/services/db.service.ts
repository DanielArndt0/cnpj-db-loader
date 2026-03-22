import { Client } from "pg";

import { ServiceError, ValidationError } from "../core/errors/index.js";
import { readDatabaseConfig } from "./config.service.js";

export async function resolveDbUrl(override?: string): Promise<string> {
  if (override) {
    return override;
  }

  const config = await readDatabaseConfig();

  if (!config.defaultDbUrl) {
    throw new ValidationError(
      'No database connection is configured. Use "cnpj-db-loader db set <postgres-url>" or pass "--db-url".',
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
