import { createControlSchemaParts } from "./control.js";
import { createDomainSchemaParts, createDomainSeedParts } from "./domain.js";
import { createIndexesSql } from "./indexes.js";
import { createOperationalSchemaParts } from "./operational.js";
import { createStagingSchemaParts } from "./staging.js";
import type { SchemaProfile } from "./types.js";

function createSchemaBody(profile: SchemaProfile): string[] {
  switch (profile) {
    case "staging":
      return createStagingSchemaParts();
    case "final":
      return [
        ...createDomainSchemaParts(),
        ...createOperationalSchemaParts(),
        ...createControlSchemaParts(),
        ...createDomainSeedParts(),
        createIndexesSql(),
      ];
    case "full":
    default:
      return [
        ...createDomainSchemaParts(),
        ...createOperationalSchemaParts(),
        ...createControlSchemaParts(),
        ...createStagingSchemaParts(),
        ...createDomainSeedParts(),
        createIndexesSql(),
      ];
  }
}

function createSchemaHeader(profile: SchemaProfile): string[] {
  return [
    "-- CNPJ DB Loader PostgreSQL schema",
    `-- Profile: ${profile}`,
    "-- Generated from the internal Receita Federal model.",
    "begin;",
  ];
}

export function generateSchemaParts(profile: SchemaProfile): string[] {
  return [
    ...createSchemaHeader(profile),
    ...createSchemaBody(profile),
    "commit;",
  ];
}
