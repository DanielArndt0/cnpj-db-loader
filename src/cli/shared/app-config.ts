import { existsSync, readFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

import type { AppConfig } from "../../core/types/index.js";

function findPackageJsonPath(): string | null {
  let currentDir = dirname(fileURLToPath(import.meta.url));

  for (let depth = 0; depth < 6; depth += 1) {
    const candidatePath = resolve(currentDir, "package.json");
    if (existsSync(candidatePath)) {
      return candidatePath;
    }

    currentDir = resolve(currentDir, "..");
  }

  return null;
}

function getPackageVersion(): string {
  const packageJsonPath = findPackageJsonPath();
  if (!packageJsonPath) {
    return "0.0.0";
  }

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
    "CLI para inspecionar, extrair, validar e importar os conjuntos de dados públicos de CNPJ da Receita Federal.",
};
