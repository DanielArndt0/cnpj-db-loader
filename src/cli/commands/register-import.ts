import type { Command } from "commander";

import { confirm } from "../../core/prompts/confirm.js";
import type { ImportOptions } from "../../services/index.js";
import { importDataToDatabase, writeCommandLog } from "../../services/index.js";
import {
  createImportProgressReporter,
  printImportSummary,
} from "../ui/output.js";

export function registerImportCommands(program: Command): void {
  program
    .command("import")
    .argument("<input>", "Path to the extracted or mixed input directory.")
    .option("--db-url <url>", "Override the default PostgreSQL connection URL.")
    .option(
      "--dataset <dataset>",
      "Import only one validated dataset block (for example: companies or cnaes).",
    )
    .option(
      "--batch-size <size>",
      "Maximum number of source rows to batch per insert query. Defaults to 500.",
      (value) => Number.parseInt(value, 10),
    )
    .option(
      "--verbose-progress",
      "Show checkpoint offset and batch details in the live import progress output.",
    )
    .option("-f, --force", "Skip the confirmation prompt.")
    .description(
      "Stream validated Receita Federal dataset files into the configured PostgreSQL database.",
    )
    .action(
      async (
        input: string,
        options: {
          dbUrl?: string;
          dataset?: string;
          batchSize?: number;
          verboseProgress?: boolean;
          force?: boolean;
        },
      ) => {
        if (!options.force) {
          const confirmed = await confirm(
            `Import dataset files from ${input} into PostgreSQL now? This command writes many records to the configured database.`,
          );
          if (!confirmed) {
            console.log("Import cancelled.");
            return;
          }
        }

        const progress = createImportProgressReporter();
        const importOptions: ImportOptions = {
          onProgress: progress,
        };

        if (options.dbUrl) {
          importOptions.dbUrl = options.dbUrl;
        }

        if (options.dataset) {
          importOptions.dataset = options.dataset as ImportOptions["dataset"];
        }

        if (
          typeof options.batchSize === "number" &&
          !Number.isNaN(options.batchSize)
        ) {
          importOptions.batchSize = options.batchSize;
        }

        if (options.verboseProgress) {
          importOptions.verboseProgress = true;
        }

        const summary = await importDataToDatabase(input, importOptions);
        const logFilePath = await writeCommandLog("import", summary);
        printImportSummary(summary, logFilePath);
      },
    );
}
