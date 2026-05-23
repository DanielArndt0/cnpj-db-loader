import { readFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

import type { AppConfig } from "../../core/types/index.js";

function getPackageVersion(): string {
  const currentDir = dirname(fileURLToPath(import.meta.url));
  const packageJsonPath = resolve(currentDir, "../../../package.json");

  const packageJson = JSON.parse(readFileSync(packageJsonPath, "utf-8")) as {
    version?: string;
  };

  return packageJson.version ?? "0.0.0";
}

export const APP_CONFIG: AppConfig = {
  appName: "cnpj-db-loader",
  version: getPackageVersion(),
  environment: (process.env.APP_ENV ??
    "development") as AppConfig["environment"],
  description:
    "Practical CLI for preparing Brazilian Federal Revenue CNPJ open data for PostgreSQL.",
};
