import { ValidationError } from "../core/errors/index.js";
import type {
  DatabaseConfig,
  FederalRevenueConfig,
} from "../core/types/index.js";
import { safeReadText, safeWriteText } from "../core/utils/index.js";
import { getConfigFilePath } from "../config/config-path.js";
import {
  DEFAULT_FEDERAL_REVENUE_USER_AGENT,
  DEFAULT_FEDERAL_REVENUE_WEBDAV_URL,
} from "./federal-revenue/client.js";
import type { FederalRevenueClientOptions } from "./federal-revenue/types.js";

export type FederalRevenueConfigKey =
  | "share-token"
  | "webdav-url"
  | "user-agent";

export type FederalRevenueEffectiveConfig = {
  webdavUrl: string;
  userAgent: string;
  shareToken?: string | undefined;
  configured: {
    webdavUrl: boolean;
    userAgent: boolean;
    shareToken: boolean;
  };
};

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

function assertHttpUrl(url: string, label: string): void {
  let parsed: URL;

  try {
    parsed = new URL(url);
  } catch {
    throw new ValidationError(`${label} is not a valid URL.`, { url });
  }

  if (!["http:", "https:"].includes(parsed.protocol)) {
    throw new ValidationError(`${label} must use the http or https protocol.`, {
      url,
    });
  }
}

function assertNonEmpty(value: string, label: string): string {
  const trimmed = value.trim();
  if (!trimmed) {
    throw new ValidationError(`${label} cannot be empty.`);
  }

  return trimmed;
}

function normalizeFederalRevenueConfigKey(
  key: string,
): FederalRevenueConfigKey {
  const normalized = key.trim().toLowerCase();

  if (["share-token", "share_token", "token"].includes(normalized)) {
    return "share-token";
  }

  if (
    ["webdav-url", "webdav_url", "base-url", "base_url", "url"].includes(
      normalized,
    )
  ) {
    return "webdav-url";
  }

  if (["user-agent", "user_agent"].includes(normalized)) {
    return "user-agent";
  }

  throw new ValidationError(
    `Unknown Federal Revenue config key: ${key}. Expected share-token, webdav-url, or user-agent.`,
  );
}

function assignFederalRevenueConfigValue(
  config: FederalRevenueConfig,
  key: FederalRevenueConfigKey,
  value: string,
): FederalRevenueConfig {
  if (key === "share-token") {
    return {
      ...config,
      shareToken: assertNonEmpty(value, "Federal Revenue share token"),
    };
  }

  if (key === "webdav-url") {
    const webdavUrl = assertNonEmpty(value, "Federal Revenue WebDAV URL");
    assertHttpUrl(webdavUrl, "Federal Revenue WebDAV URL");
    return { ...config, webdavUrl };
  }

  return {
    ...config,
    userAgent: assertNonEmpty(value, "Federal Revenue user agent"),
  };
}

function deleteFederalRevenueConfigValue(
  config: FederalRevenueConfig,
  key: FederalRevenueConfigKey,
): FederalRevenueConfig {
  const nextConfig = { ...config };

  if (key === "share-token") {
    delete nextConfig.shareToken;
  }

  if (key === "webdav-url") {
    delete nextConfig.webdavUrl;
  }

  if (key === "user-agent") {
    delete nextConfig.userAgent;
  }

  return nextConfig;
}

function isFederalRevenueConfigEmpty(config: FederalRevenueConfig): boolean {
  return !config.shareToken && !config.webdavUrl && !config.userAgent;
}

export async function setDefaultDbUrl(url: string): Promise<void> {
  assertPostgresUrl(url);
  const currentConfig = await readDatabaseConfig();
  await writeDatabaseConfig({ ...currentConfig, defaultDbUrl: url });
}

export async function resetDefaultDbUrl(): Promise<void> {
  const currentConfig = await readDatabaseConfig();
  const nextConfig = { ...currentConfig };
  delete nextConfig.defaultDbUrl;
  await writeDatabaseConfig(nextConfig);
}

export async function setFederalRevenueConfigValue(
  key: string,
  value: string,
): Promise<FederalRevenueEffectiveConfig> {
  const normalizedKey = normalizeFederalRevenueConfigKey(key);
  const currentConfig = await readDatabaseConfig();
  const federalRevenueConfig = assignFederalRevenueConfigValue(
    currentConfig.federalRevenue ?? {},
    normalizedKey,
    value,
  );

  await writeDatabaseConfig({
    ...currentConfig,
    federalRevenue: federalRevenueConfig,
  });

  return getFederalRevenueEffectiveConfig(federalRevenueConfig);
}

export async function resetFederalRevenueConfig(
  key?: string | undefined,
): Promise<FederalRevenueEffectiveConfig> {
  const currentConfig = await readDatabaseConfig();

  if (!key) {
    const nextConfig = { ...currentConfig };
    delete nextConfig.federalRevenue;
    await writeDatabaseConfig(nextConfig);
    return getFederalRevenueEffectiveConfig({});
  }

  const normalizedKey = normalizeFederalRevenueConfigKey(key);
  const federalRevenueConfig = deleteFederalRevenueConfigValue(
    currentConfig.federalRevenue ?? {},
    normalizedKey,
  );
  const nextConfig = { ...currentConfig };

  if (isFederalRevenueConfigEmpty(federalRevenueConfig)) {
    delete nextConfig.federalRevenue;
  } else {
    nextConfig.federalRevenue = federalRevenueConfig;
  }

  await writeDatabaseConfig(nextConfig);
  return getFederalRevenueEffectiveConfig(federalRevenueConfig);
}

export function getFederalRevenueEffectiveConfig(
  config: FederalRevenueConfig = {},
): FederalRevenueEffectiveConfig {
  return {
    webdavUrl: config.webdavUrl ?? DEFAULT_FEDERAL_REVENUE_WEBDAV_URL,
    userAgent: config.userAgent ?? DEFAULT_FEDERAL_REVENUE_USER_AGENT,
    ...(config.shareToken ? { shareToken: config.shareToken } : {}),
    configured: {
      webdavUrl: Boolean(config.webdavUrl),
      userAgent: Boolean(config.userAgent),
      shareToken: Boolean(config.shareToken),
    },
  };
}

export async function readFederalRevenueEffectiveConfig(): Promise<FederalRevenueEffectiveConfig> {
  const currentConfig = await readDatabaseConfig();
  return getFederalRevenueEffectiveConfig(currentConfig.federalRevenue ?? {});
}

export async function resolveFederalRevenueClientOptions(
  overrides: FederalRevenueClientOptions = {},
): Promise<FederalRevenueClientOptions> {
  const currentConfig = await readDatabaseConfig();
  const effectiveConfig = getFederalRevenueEffectiveConfig(
    currentConfig.federalRevenue ?? {},
  );

  return {
    baseUrl: overrides.baseUrl ?? effectiveConfig.webdavUrl,
    shareToken: overrides.shareToken ?? effectiveConfig.shareToken,
    userAgent: overrides.userAgent ?? effectiveConfig.userAgent,
  };
}
