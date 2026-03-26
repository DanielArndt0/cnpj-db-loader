import path from "node:path";

import type { Command } from "commander";

import {
  generateSchemaSql,
  resolveSchemaProfile,
  writeSchemaFile,
} from "../../services/index.js";
import type { SchemaProfile } from "../../services/index.js";

function ensureSqlExtension(fileName: string): string {
  return fileName.toLowerCase().endsWith(".sql") ? fileName : `${fileName}.sql`;
}

function getDefaultSchemaBaseName(profile: SchemaProfile): string {
  switch (profile) {
    case "staging":
      return "schema-staging";
    case "final":
      return "schema-final";
    default:
      return "schema";
  }
}

function resolveSchemaOutputPath(
  profile: SchemaProfile,
  name?: string,
  output?: string,
): string {
  const fileName = ensureSqlExtension(
    name?.trim() || getDefaultSchemaBaseName(profile),
  );
  return path.resolve(output ?? process.cwd(), fileName);
}

type SchemaCommandOptions = {
  name?: string;
  output?: string;
  profile?: string;
};

export function registerSchemaCommands(program: Command): void {
  const schema = program
    .command("schema")
    .description(
      "Print or generate PostgreSQL schemas for final, staging, or combined profiles.",
    );

  schema
    .command("print")
    .option(
      "--profile <profile>",
      "Schema profile to print: full, final, or staging. Defaults to full.",
    )
    .description("Print the generated SQL schema to stdout.")
    .action((options: SchemaCommandOptions) => {
      const profile = resolveSchemaProfile(options.profile);
      console.log(generateSchemaSql({ profile }));
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
    .option(
      "--profile <profile>",
      "Schema profile to generate: full, final, or staging. Defaults to full.",
    )
    .description("Generate the SQL schema file from the internal model.")
    .action(async (options: SchemaCommandOptions) => {
      const profile = resolveSchemaProfile(options.profile);
      const targetPath = resolveSchemaOutputPath(
        profile,
        options.name,
        options.output,
      );
      await writeSchemaFile(targetPath, { profile });
      console.log(`Schema file written to ${targetPath}`);
    });
}
