import type { Command } from "commander";

import { confirm } from "../../core/prompts/confirm.js";
import type { ImportDatasetType } from "../../services/index.js";
import {
  cleanupDatabaseCheckpointsData,
  cleanupDatabaseMaterializedData,
  cleanupDatabasePlansData,
  cleanupDatabaseStagingData,
  readDatabaseConfig,
  resetDefaultDbUrl,
  setDefaultDbUrl,
  testDatabaseConnection,
  writeCommandLog,
} from "../../services/index.js";
import {
  printDatabaseCleanupSummary,
  printDatabaseConfigSummary,
  printInfoWithLog,
} from "../ui/output.js";

type DatabaseGlobalOptions = {
  dbUrl?: string;
  dataset?: string;
  validatedPath?: string;
  planId?: number;
  force?: boolean;
};

async function confirmDatabaseAction(
  message: string,
  force?: boolean,
): Promise<boolean> {
  if (force) {
    return true;
  }

  return confirm(message);
}

function resolveDataset(
  dataset: string | undefined,
): ImportDatasetType | undefined {
  return dataset as ImportDatasetType | undefined;
}

export function registerDatabaseCommands(program: Command): void {
  const database = program
    .command("database")
    .alias("db")
    .description(
      "Manage PostgreSQL connection settings and safe maintenance operations.",
    );

  const config = database
    .command("config")
    .description(
      "Read, persist, test, or reset the default PostgreSQL connection.",
    );

  config
    .command("set")
    .argument("<url>", "PostgreSQL connection string to persist as default.")
    .description(
      "Persist the default PostgreSQL connection string for future commands.",
    )
    .action(async (url: string) => {
      await setDefaultDbUrl(url);
      const logFilePath = await writeCommandLog("database-config-set", {
        defaultDbUrl: url,
      });
      printInfoWithLog("DATABASE", "Default database URL saved.", logFilePath);
    });

  config
    .command("show")
    .description("Show the currently persisted database configuration.")
    .action(async () => {
      const currentConfig = await readDatabaseConfig();
      const logFilePath = await writeCommandLog(
        "database-config-show",
        currentConfig,
      );
      printDatabaseConfigSummary(currentConfig, logFilePath);
    });

  config
    .command("test")
    .option("--db-url <url>", "Override the default PostgreSQL connection URL.")
    .description("Test the PostgreSQL connection.")
    .action(async (options: { dbUrl?: string }) => {
      const currentConfig = await readDatabaseConfig();
      const url = options.dbUrl ?? currentConfig.defaultDbUrl;

      if (!url) {
        console.log("No database URL available for the connection test.");
        process.exitCode = 1;
        return;
      }

      await testDatabaseConnection(url);
      const logFilePath = await writeCommandLog("database-config-test", {
        testedUrl: url,
        ok: true,
      });
      printInfoWithLog(
        "DATABASE",
        "Database connection succeeded.",
        logFilePath,
      );
    });

  config
    .command("reset")
    .option("-f, --force", "Skip the confirmation prompt.")
    .description("Remove the persisted default database connection.")
    .action(async (options: { force?: boolean }) => {
      const confirmed = await confirmDatabaseAction(
        "Remove the persisted default database configuration?",
        options.force,
      );
      if (!confirmed) {
        console.log("Database reset cancelled.");
        return;
      }

      await resetDefaultDbUrl();
      const logFilePath = await writeCommandLog("database-config-reset", {
        ok: true,
      });
      printInfoWithLog(
        "DATABASE",
        "Default database URL removed.",
        logFilePath,
      );
    });

  const cleanup = database
    .command("cleanup")
    .description(
      "Safely clear staging data, final materialized tables, checkpoints, or saved import plans.",
    );

  cleanup
    .command("staging")
    .option("--db-url <url>", "Override the default PostgreSQL connection URL.")
    .option(
      "--dataset <dataset>",
      "Restrict the cleanup to one staging dataset (companies, establishments, partners, simples_options).",
    )
    .option(
      "--validated-path <path>",
      "Also clear materialization checkpoints linked to the latest saved plan(s) for this validated path.",
    )
    .option("-f, --force", "Skip the confirmation prompt.")
    .description(
      "Truncate staging tables so a fresh bulk load can restart from a clean intermediate state.",
    )
    .action(async (options: DatabaseGlobalOptions) => {
      const confirmed = await confirmDatabaseAction(
        "Truncate staging tables now? This removes intermediate bulk-load data and may also clear materialization checkpoints when --validated-path is used.",
        options.force,
      );
      if (!confirmed) {
        console.log("Staging cleanup cancelled.");
        return;
      }

      const cleanupOptions: {
        dbUrl?: string;
        dataset?: ImportDatasetType | undefined;
        validatedPath?: string | undefined;
      } = {};
      if (options.dbUrl) {
        cleanupOptions.dbUrl = options.dbUrl;
      }
      if (options.dataset) {
        cleanupOptions.dataset = resolveDataset(options.dataset);
      }
      if (options.validatedPath) {
        cleanupOptions.validatedPath = options.validatedPath;
      }

      const summary = await cleanupDatabaseStagingData(cleanupOptions);
      const logFilePath = await writeCommandLog(
        "database-cleanup-staging",
        summary,
      );
      printDatabaseCleanupSummary(summary, logFilePath);
    });

  cleanup
    .command("materialized")
    .alias("final")
    .option("--db-url <url>", "Override the default PostgreSQL connection URL.")
    .option(
      "--dataset <dataset>",
      "Restrict the cleanup to one final materialized dataset (companies, establishments, partners, simples_options).",
    )
    .option("-f, --force", "Skip the confirmation prompt.")
    .description(
      "Truncate final relational tables populated by materialization in dependency-safe order.",
    )
    .action(async (options: DatabaseGlobalOptions) => {
      const confirmed = await confirmDatabaseAction(
        "Truncate final materialized tables now? This removes relational data already consolidated from staging.",
        options.force,
      );
      if (!confirmed) {
        console.log("Materialized-table cleanup cancelled.");
        return;
      }

      const cleanupOptions: {
        dbUrl?: string;
        dataset?: ImportDatasetType | undefined;
      } = {};
      if (options.dbUrl) {
        cleanupOptions.dbUrl = options.dbUrl;
      }
      if (options.dataset) {
        cleanupOptions.dataset = resolveDataset(options.dataset);
      }

      const summary = await cleanupDatabaseMaterializedData(cleanupOptions);
      const logFilePath = await writeCommandLog(
        "database-cleanup-materialized",
        summary,
      );
      printDatabaseCleanupSummary(summary, logFilePath);
    });

  cleanup
    .command("checkpoints")
    .option("--db-url <url>", "Override the default PostgreSQL connection URL.")
    .option(
      "--phase <phase>",
      "Choose which checkpoint family to clear: load, materialization, or all. Defaults to all.",
    )
    .option(
      "--dataset <dataset>",
      "Restrict the cleanup to one dataset. Load checkpoints accept any import dataset; materialization checkpoints accept staged datasets only.",
    )
    .option(
      "--validated-path <path>",
      "Limit materialization checkpoint cleanup to plan(s) associated with this validated path.",
    )
    .option(
      "--plan-id <id>",
      "Limit materialization checkpoint cleanup to a specific import plan id.",
      (value) => Number.parseInt(value, 10),
    )
    .option("-f, --force", "Skip the confirmation prompt.")
    .description(
      "Clear load checkpoints, materialization checkpoints, or both without truncating data tables.",
    )
    .action(
      async (
        options: DatabaseGlobalOptions & {
          phase?: "load" | "materialization" | "all";
        },
      ) => {
        const phase = options.phase ?? "all";
        const confirmed = await confirmDatabaseAction(
          `Clear ${phase} checkpoint data now? This affects saved resume state but does not truncate staging or final tables.`,
          options.force,
        );
        if (!confirmed) {
          console.log("Checkpoint cleanup cancelled.");
          return;
        }

        const cleanupOptions: {
          dbUrl?: string;
          phase?: "load" | "materialization" | "all";
          dataset?: ImportDatasetType | undefined;
          validatedPath?: string | undefined;
          planId?: number | undefined;
        } = { phase };
        if (options.dbUrl) {
          cleanupOptions.dbUrl = options.dbUrl;
        }
        if (options.dataset) {
          cleanupOptions.dataset = resolveDataset(options.dataset);
        }
        if (options.validatedPath) {
          cleanupOptions.validatedPath = options.validatedPath;
        }
        if (
          typeof options.planId === "number" &&
          !Number.isNaN(options.planId)
        ) {
          cleanupOptions.planId = options.planId;
        }

        const summary = await cleanupDatabaseCheckpointsData(cleanupOptions);
        const logFilePath = await writeCommandLog(
          "database-cleanup-checkpoints",
          summary,
        );
        printDatabaseCleanupSummary(summary, logFilePath);
      },
    );

  cleanup
    .command("plans")
    .option("--db-url <url>", "Override the default PostgreSQL connection URL.")
    .option(
      "--validated-path <path>",
      "Delete saved import plan(s) associated with this validated path for the selected database.",
    )
    .option(
      "--plan-id <id>",
      "Delete only one saved import plan by id.",
      (value) => Number.parseInt(value, 10),
    )
    .option("-f, --force", "Skip the confirmation prompt.")
    .description(
      "Delete saved import plans. Related plan files and materialization checkpoints are removed by database cascade.",
    )
    .action(async (options: DatabaseGlobalOptions) => {
      const confirmed = await confirmDatabaseAction(
        "Delete saved import plans now? This removes orchestration metadata and linked materialization checkpoints.",
        options.force,
      );
      if (!confirmed) {
        console.log("Plan cleanup cancelled.");
        return;
      }

      const cleanupOptions: {
        dbUrl?: string;
        validatedPath?: string | undefined;
        planId?: number | undefined;
      } = {};
      if (options.dbUrl) {
        cleanupOptions.dbUrl = options.dbUrl;
      }
      if (options.validatedPath) {
        cleanupOptions.validatedPath = options.validatedPath;
      }
      if (typeof options.planId === "number" && !Number.isNaN(options.planId)) {
        cleanupOptions.planId = options.planId;
      }

      const summary = await cleanupDatabasePlansData(cleanupOptions);
      const logFilePath = await writeCommandLog(
        "database-cleanup-plans",
        summary,
      );
      printDatabaseCleanupSummary(summary, logFilePath);
    });
}
