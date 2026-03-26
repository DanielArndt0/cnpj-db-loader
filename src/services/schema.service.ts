import { writeFile } from "node:fs/promises";

import { generateSchemaParts } from "./schema/builders.js";
import {
  normalizeSchemaProfile,
  type SchemaGenerationOptions,
  type SchemaProfile,
} from "./schema/types.js";

export function generateSchemaSql(options?: SchemaGenerationOptions): string {
  const profile = options?.profile ?? "full";
  return generateSchemaParts(profile).join("\n\n");
}

export async function writeSchemaFile(
  outFile: string,
  options?: SchemaGenerationOptions,
): Promise<void> {
  await writeFile(outFile, `${generateSchemaSql(options)}\n`, "utf8");
}

export function resolveSchemaProfile(profile?: string): SchemaProfile {
  return normalizeSchemaProfile(profile);
}

export type { SchemaGenerationOptions, SchemaProfile } from "./schema/types.js";
