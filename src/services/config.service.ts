import { ValidationError } from "../core/errors/index.js";
import type { DatabaseConfig } from "../core/types/index.js";
import { safeReadText, safeWriteText } from "../core/utils/index.js";
import { getConfigFilePath } from "../config/config-path.js";

export async function readDatabaseConfig(): Promise<DatabaseConfig> {
  const raw = await safeReadText(getConfigFilePath());
  if (!raw) {
    return {};
  }

  return JSON.parse(raw) as DatabaseConfig;
}

export async function writeDatabaseConfig(
  config: DatabaseConfig,
): Promise<void> {
  await safeWriteText(getConfigFilePath(), JSON.stringify(config, null, 2));
}

export function assertPostgresUrl(url: string): void {
  let parsed: URL;

  try {
    parsed = new URL(url);
  } catch {
    throw new ValidationError("The provided database URL is not a valid URL.", {
      url,
    });
  }

  if (!["postgres:", "postgresql:"].includes(parsed.protocol)) {
    throw new ValidationError(
      "The database URL must use the postgres or postgresql protocol.",
      { url },
    );
  }
}

export async function setDefaultDbUrl(url: string): Promise<void> {
  assertPostgresUrl(url);
  await writeDatabaseConfig({ defaultDbUrl: url });
}

export async function resetDefaultDbUrl(): Promise<void> {
  await writeDatabaseConfig({});
}
