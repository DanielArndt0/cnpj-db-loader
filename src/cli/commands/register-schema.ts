import path from "node:path";

import type { Command } from "commander";

import { generateSchemaSql, writeSchemaFile } from "../../services/index.js";

function ensureSqlExtension(fileName: string): string {
  return fileName.toLowerCase().endsWith(".sql") ? fileName : `${fileName}.sql`;
}

function resolveSchemaOutputPath(name?: string, output?: string): string {
  const fileName = ensureSqlExtension(name?.trim() || "schema");
  return path.resolve(output ?? process.cwd(), fileName);
}

export function registerSchemaCommands(program: Command): void {
  const schema = program
    .command("schema")
    .description("Print or generate the PostgreSQL schema.");

  schema
    .command("print")
    .description("Print the generated SQL schema to stdout.")
    .action(() => {
      console.log(generateSchemaSql());
    });

  schema
    .command("generate")
    .option(
      "--name <name>",
      "Output file name without needing to type the .sql suffix.",
    )
    .option(
      "--output <path>",
      "Output directory. Defaults to the current working directory.",
    )
    .description("Generate the SQL schema file from the internal model.")
    .action(async (options: { name?: string; output?: string }) => {
      const targetPath = resolveSchemaOutputPath(options.name, options.output);
      await writeSchemaFile(targetPath);
      console.log(`Schema file written to ${targetPath}`);
    });
}
