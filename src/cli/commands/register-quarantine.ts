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
    .description("Inspect and analyze rows stored in 'import_quarantine'.");

  quarantine
    .command("stats")
    .option("--db-url <url>", "Override the default PostgreSQL connection URL.")
    .option("--dataset <dataset>", "Filter by dataset name.")
    .option("--category <category>", "Filter by error category.")
    .option("--stage <stage>", "Filter by error stage.")
    .option("--retryable", "Show only retryable quarantine rows.")
    .option("--terminal", "Show only terminal quarantine rows.")
    .description("Show aggregate statistics for import_quarantine.")
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
    .option("--db-url <url>", "Override the default PostgreSQL connection URL.")
    .option("--dataset <dataset>", "Filter by dataset name.")
    .option("--category <category>", "Filter by error category.")
    .option("--stage <stage>", "Filter by error stage.")
    .option("--retryable", "Show only retryable quarantine rows.")
    .option("--terminal", "Show only terminal quarantine rows.")
    .option(
      "--limit <number>",
      "Limit the number of returned rows. Defaults to 20.",
      (value) => Number.parseInt(value, 10),
      20,
    )
    .option(
      "--after-id <number>",
      "Return rows strictly after the provided quarantine id.",
      (value) => Number.parseInt(value, 10),
    )
    .description("List rows from import_quarantine with optional filters.")
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
    .argument("<id>", "Quarantine row id to inspect.")
    .option("--db-url <url>", "Override the default PostgreSQL connection URL.")
    .description("Show one quarantined row in detail.")
    .action(async (id: string, options: { dbUrl?: string }) => {
      const record = await showQuarantineRow(Number.parseInt(id, 10), options);
      const logFilePath = await writeCommandLog("quarantine-show", record);
      printQuarantineRecord(record, logFilePath);
    });
}
