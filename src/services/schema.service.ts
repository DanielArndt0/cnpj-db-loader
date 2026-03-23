import { writeFile } from "node:fs/promises";

import { generateSchemaParts } from "./schema/builders.js";

export function generateSchemaSql(): string {
  return generateSchemaParts().join("\n\n");
}

export async function writeSchemaFile(outFile: string): Promise<void> {
  await writeFile(outFile, `${generateSchemaSql()}\n`, "utf8");
}
