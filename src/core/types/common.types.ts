export type AppEnvironment = "development" | "test" | "production";

export type AppConfig = {
  appName: string;
  version: string;
  environment: AppEnvironment;
  description: string;
};

export type DatasetBlock =
  | "companies"
  | "establishments"
  | "partners"
  | "simples_options"
  | "countries"
  | "cities"
  | "partner_qualifications"
  | "legal_natures"
  | "cnaes";

export type DatabaseConfig = {
  defaultDbUrl?: string;
};
