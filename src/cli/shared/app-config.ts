import type { AppConfig } from "../../core/types/index.js";

export const APP_CONFIG: AppConfig = {
  appName: "cnpj-db-loader",
  version: "1.2.0",
  environment: (process.env.APP_ENV ??
    "development") as AppConfig["environment"],
  description:
    "CLI for inspecting, extracting, validating, and modeling Brazilian Federal Revenue CNPJ datasets for PostgreSQL.",
};
