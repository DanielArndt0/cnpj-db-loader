import type { Command } from "commander";

import { confirm } from "../../core/prompts/confirm.js";
import type {
  PostgresCsvExportOptions,
  PostgresDirectScriptOptions,
} from "../../services/index.js";
import {
  exportPostgresCsvDataset,
  generatePostgresDirectScript,
  writeCommandLog,
} from "../../services/index.js";
import {
  createPostgresCsvExportProgressReporter,
  createPostgresDirectScriptProgressReporter,
  printPostgresCsvExportSummary,
  printPostgresDirectScriptSummary,
} from "../ui/output.js";

export function registerPostgresCommands(program: Command): void {
  const postgres = program
    .command("postgres")
    .description(
      "Utilitários orientados a PostgreSQL para carga híbrida e operações de banco.",
    );

  postgres
    .command("generate-script")
    .argument(
      "<input>",
      "Caminho do diretório de dataset sanitizado gerado por cnpj-db-loader sanitize.",
    )
    .option(
      "--output <path>",
      "Diretório de saída personalizado para o script psql gerado e o manifesto.",
    )
    .option(
      "--dataset <dataset>",
      "Gera um script apenas para um bloco de dataset, por exemplo establishments ou companies.",
    )
    .option(
      "--script-name <name>",
      "Nome do arquivo de script psql gerado. Padrão: import-postgres-direct.sql.",
    )
    .option(
      "--source-encoding <encoding>",
      "Client encoding do PostgreSQL usado ao ler os arquivos sanitizados da Receita. Padrão: UTF8.",
    )
    .option(
      "--transaction-mode <mode>",
      "Modo de transação dos scripts gerados: single, phase ou none. Padrão: single.",
    )
    .option(
      "--include <items>",
      "Etapas a incluir, separadas por vírgula: domains,companies,establishments,partners,simples,secondary-cnaes,indexes,analyze.",
    )
    .option("--skip-indexes", "Não gera a etapa de índices.")
    .option("--skip-analyze", "Não gera a etapa de analyze.")
    .option("-f, --force", "Pula a confirmação interativa.")
    .description(
      "Gera um script de importação psql direto que carrega os arquivos sanitizados da Receita sem reescrevê-los em novos arquivos CSV.",
    )
    .action(
      async (
        input: string,
        options: {
          output?: string;
          dataset?: string;
          scriptName?: string;
          sourceEncoding?: string;
          transactionMode?: string;
          include?: string;
          skipIndexes?: boolean;
          skipAnalyze?: boolean;
          force?: boolean;
        },
      ) => {
        if (!options.force) {
          const confirmed = await confirm(
            `Gerar um script de importação psql direto do PostgreSQL a partir de ${input}? Este comando não reescreve os arquivos de origem; cria apenas um script SQL e um manifesto.`,
          );
          if (!confirmed) {
            console.log("Geração do script direto do PostgreSQL cancelada.");
            return;
          }
        }

        const progress = createPostgresDirectScriptProgressReporter();
        const generateOptions: PostgresDirectScriptOptions = {
          onProgress: progress,
        };

        if (options.output) {
          generateOptions.outputPath = options.output;
        }

        if (options.dataset) {
          generateOptions.dataset =
            options.dataset as PostgresDirectScriptOptions["dataset"];
        }

        if (options.scriptName) {
          generateOptions.scriptName = options.scriptName;
        }

        if (options.sourceEncoding) {
          generateOptions.sourceEncoding = options.sourceEncoding;
        }

        if (options.transactionMode) {
          generateOptions.transactionMode =
            options.transactionMode as PostgresDirectScriptOptions["transactionMode"];
        }

        if (options.include) {
          generateOptions.include = options.include
            .split(",")
            .map((item) => item.trim())
            .filter(Boolean) as PostgresDirectScriptOptions["include"];
        }

        if (options.skipIndexes) {
          generateOptions.skipIndexes = true;
        }

        if (options.skipAnalyze) {
          generateOptions.skipAnalyze = true;
        }

        const summary = await generatePostgresDirectScript(
          input,
          generateOptions,
        );
        const logFilePath = await writeCommandLog(
          "postgres-generate-script",
          summary,
        );
        printPostgresDirectScriptSummary(summary, logFilePath);
      },
    );

  postgres
    .command("export-csv")
    .argument(
      "<input>",
      "Caminho do diretório de dataset sanitizado ou extraído e validado.",
    )
    .option(
      "--output <path>",
      "Diretório de saída personalizado para os arquivos CSV prontos para o PostgreSQL.",
    )
    .option(
      "--dataset <dataset>",
      "Exporta apenas um bloco de dataset, por exemplo establishments ou companies.",
    )
    .option(
      "--script-name <name>",
      "Nome do arquivo de script psql gerado. Padrão: import-postgres-direct.sql.",
    )
    .option("-f, --force", "Pula a confirmação interativa.")
    .description(
      "Converte os arquivos sanitizados da Receita em arquivos CSV reais prontos para o PostgreSQL e gera um script de importação psql direto.",
    )
    .action(
      async (
        input: string,
        options: {
          output?: string;
          dataset?: string;
          scriptName?: string;
          force?: boolean;
        },
      ) => {
        if (!options.force) {
          const confirmed = await confirm(
            `Exportar arquivos CSV prontos para o PostgreSQL a partir de ${input}? Este comando cria arquivos CSV normalizados e um script de importação psql gerado.`,
          );
          if (!confirmed) {
            console.log("Exportação de CSV para o PostgreSQL cancelada.");
            return;
          }
        }

        const progress = createPostgresCsvExportProgressReporter();
        const exportOptions: PostgresCsvExportOptions = {
          onProgress: progress,
        };

        if (options.output) {
          exportOptions.outputPath = options.output;
        }

        if (options.dataset) {
          exportOptions.dataset =
            options.dataset as PostgresCsvExportOptions["dataset"];
        }

        if (options.scriptName) {
          exportOptions.scriptName = options.scriptName;
        }

        const summary = await exportPostgresCsvDataset(input, exportOptions);
        const logFilePath = await writeCommandLog(
          "postgres-export-csv",
          summary,
        );
        printPostgresCsvExportSummary(summary, logFilePath);
      },
    );
}
