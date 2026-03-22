import type { Command } from "commander";

import { confirm } from "../../core/prompts/confirm.js";
import {
  readDatabaseConfig,
  resetDefaultDbUrl,
  setDefaultDbUrl,
  testDatabaseConnection,
  writeCommandLog,
} from "../../services/index.js";
import { printDbConfigSummary, printInfoWithLog } from "../ui/output.js";

export function registerDbCommands(program: Command): void {
  const db = program
    .command("db")
    .description("Manage PostgreSQL connection settings.");

  db.command("set")
    .argument("<url>", "PostgreSQL connection string to persist as default.")
    .description(
      "Persist the default PostgreSQL connection string for future commands.",
    )
    .action(async (url: string) => {
      await setDefaultDbUrl(url);
      const logFilePath = await writeCommandLog("db-set", {
        defaultDbUrl: url,
      });
      printInfoWithLog("DB", "Default database URL saved.", logFilePath);
    });

  db.command("show")
    .description("Show the currently persisted database configuration.")
    .action(async () => {
      const config = await readDatabaseConfig();
      const logFilePath = await writeCommandLog("db-show", config);
      printDbConfigSummary(config, logFilePath);
    });

  db.command("test")
    .option("--db-url <url>", "Override the default PostgreSQL connection URL.")
    .description("Test the PostgreSQL connection.")
    .action(async (options: { dbUrl?: string }) => {
      const config = await readDatabaseConfig();
      const url = options.dbUrl ?? config.defaultDbUrl;

      if (!url) {
        console.log("No database URL available for the connection test.");
        process.exitCode = 1;
        return;
      }

      await testDatabaseConnection(url);
      const logFilePath = await writeCommandLog("db-test", {
        testedUrl: url,
        ok: true,
      });
      printInfoWithLog("DB", "Database connection succeeded.", logFilePath);
    });

  db.command("reset")
    .option("-y, --yes", "Skip the confirmation prompt.")
    .description("Remove the persisted default database connection.")
    .action(async (options: { yes?: boolean }) => {
      if (!options.yes) {
        const confirmed = await confirm(
          "Remove the persisted default database configuration?",
        );
        if (!confirmed) {
          console.log("Database reset cancelled.");
          return;
        }
      }

      await resetDefaultDbUrl();
      const logFilePath = await writeCommandLog("db-reset", { ok: true });
      printInfoWithLog("DB", "Default database URL removed.", logFilePath);
    });
}
