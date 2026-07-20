import type { Command } from "commander";

import {
  getQuarantineStats,
  listQuarantineRows,
  showQuarantineRow,
  writeCommandLog,
} from "../../services/index.js";
import {
  printQuarantineListSummary,
  printQuarantineRecord,
  printQuarantineStatsSummary,
} from "../ui/output.js";

export function registerQuarantineCommands(program: Command): void {
  const quarantine = program
    .command("quarantine")
    .description(
      "Inspeciona e analisa as linhas armazenadas em 'quarentena_importacao'.",
    );

  quarantine
    .command("stats")
    .option("--db-url <url>", "Sobrescreve a URL de conexão PostgreSQL padrão.")
    .option("--dataset <dataset>", "Filtra pelo nome do dataset.")
    .option("--category <category>", "Filtra pela categoria de erro.")
    .option("--stage <stage>", "Filtra pela etapa de erro.")
    .option("--retryable", "Mostra apenas linhas de quarentena reprocessáveis.")
    .option("--terminal", "Mostra apenas linhas de quarentena terminais.")
    .description("Mostra estatísticas agregadas de quarentena_importacao.")
    .action(
      async (options: {
        dbUrl?: string;
        dataset?: string;
        category?: string;
        stage?: string;
        retryable?: boolean;
        terminal?: boolean;
      }) => {
        const summary = await getQuarantineStats(options);
        const logFilePath = await writeCommandLog("quarantine-stats", summary);
        printQuarantineStatsSummary(summary, logFilePath);
      },
    );

  quarantine
    .command("list")
    .option("--db-url <url>", "Sobrescreve a URL de conexão PostgreSQL padrão.")
    .option("--dataset <dataset>", "Filtra pelo nome do dataset.")
    .option("--category <category>", "Filtra pela categoria de erro.")
    .option("--stage <stage>", "Filtra pela etapa de erro.")
    .option("--retryable", "Mostra apenas linhas de quarentena reprocessáveis.")
    .option("--terminal", "Mostra apenas linhas de quarentena terminais.")
    .option(
      "--limit <number>",
      "Limita o número de linhas retornadas. Padrão: 20.",
      (value) => Number.parseInt(value, 10),
      20,
    )
    .option(
      "--after-id <number>",
      "Retorna as linhas estritamente após o id de quarentena informado.",
      (value) => Number.parseInt(value, 10),
    )
    .description("Lista linhas de quarentena_importacao com filtros opcionais.")
    .action(
      async (options: {
        dbUrl?: string;
        dataset?: string;
        category?: string;
        stage?: string;
        retryable?: boolean;
        terminal?: boolean;
        limit: number;
        afterId?: number;
      }) => {
        const summary = await listQuarantineRows(options);
        const logFilePath = await writeCommandLog("quarantine-list", summary);
        printQuarantineListSummary(summary, logFilePath);
      },
    );

  quarantine
    .command("show")
    .argument("<id>", "Id da linha de quarentena a inspecionar.")
    .option("--db-url <url>", "Sobrescreve a URL de conexão PostgreSQL padrão.")
    .description("Mostra em detalhe uma linha em quarentena.")
    .action(async (id: string, options: { dbUrl?: string }) => {
      const record = await showQuarantineRow(Number.parseInt(id, 10), options);
      const logFilePath = await writeCommandLog("quarantine-show", record);
      printQuarantineRecord(record, logFilePath);
    });
}
