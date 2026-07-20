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
      "Imprime ou gera schemas PostgreSQL para os perfis de carga final simplificada, staging ou combinado.",
    );

  schema
    .command("print")
    .option(
      "--profile <profile>",
      "Perfil de schema a imprimir: full, final ou staging. O perfil final é simplificado para materialização rápida. Padrão: full.",
    )
    .description("Imprime o schema SQL gerado no stdout.")
    .action((options: SchemaCommandOptions) => {
      const profile = resolveSchemaProfile(options.profile);
      console.log(generateSchemaSql({ profile }));
    });

  schema
    .command("generate")
    .option(
      "--name <name>",
      "Nome do arquivo de saída, sem precisar digitar o sufixo .sql.",
    )
    .option(
      "--output <path>",
      "Diretório de saída. Padrão: o diretório de trabalho atual.",
    )
    .option(
      "--profile <profile>",
      "Perfil de schema a gerar: full, final ou staging. O perfil final é simplificado para materialização rápida. Padrão: full.",
    )
    .description("Gera o arquivo de schema SQL a partir do modelo interno.")
    .action(async (options: SchemaCommandOptions) => {
      const profile = resolveSchemaProfile(options.profile);
      const targetPath = resolveSchemaOutputPath(
        profile,
        options.name,
        options.output,
      );
      await writeSchemaFile(targetPath, { profile });
      console.log(`Arquivo de schema gravado em ${targetPath}`);
    });
}
