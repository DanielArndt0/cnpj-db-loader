import { ValidationError } from "../../core/errors/index.js";

export const SCHEMA_PROFILES = ["full", "final", "staging"] as const;

export type SchemaProfile = (typeof SCHEMA_PROFILES)[number];

export type SchemaGenerationOptions = {
  profile?: SchemaProfile;
};

const SCHEMA_PROFILE_ALIASES: Record<string, SchemaProfile> = {
  all: "full",
  combined: "full",
  full: "full",
  final: "final",
  "final-load": "final",
  load: "final",
  minimal: "final",
  operational: "final",
  production: "final",
  stage: "staging",
  staging: "staging",
};

export function normalizeSchemaProfile(input?: string): SchemaProfile {
  const normalized = input?.trim().toLowerCase();

  if (!normalized) {
    return "full";
  }

  const profile = SCHEMA_PROFILE_ALIASES[normalized];
  if (profile) {
    return profile;
  }

  throw new ValidationError(
    `Invalid schema profile "${input}". Expected one of: full, final, or staging. Aliases such as final-load, load, operational, and stage are also accepted.`,
  );
}
