import type { Command } from "commander";

import { confirm } from "../../core/prompts/confirm.js";
import type { ImportOptions } from "../../services/index.js";
import {
  cleanupImportStaging,
  importDataToDatabase,
  loadImportDataToStaging,
  materializeImportedData,
  writeCommandLog,
} from "../../services/index.js";
import {
  createImportProgressReporter,
  printImportSummary,
  printInfoWithLog,
} from "../ui/output.js";

type SharedOptions = {
  dbUrl?: string;
  dataset?: string;
  loadBatchSize?: number;
  materializeBatchSize?: number;
  verboseProgress?: boolean;
  force?: boolean;
};

function applySharedImportOptions(
  options: SharedOptions,
  config: ImportOptions,
): ImportOptions {
  const nextOptions: ImportOptions = {
    ...config,
  };

  if (options.dbUrl) {
    nextOptions.dbUrl = options.dbUrl;
  }

  if (options.dataset) {
    nextOptions.dataset = options.dataset as ImportOptions["dataset"];
  }

  if (
    typeof options.loadBatchSize === "number" &&
    !Number.isNaN(options.loadBatchSize)
  ) {
    nextOptions.loadBatchSize = options.loadBatchSize;
    nextOptions.batchSize = options.loadBatchSize;
  }

  if (
    typeof options.materializeBatchSize === "number" &&
    !Number.isNaN(options.materializeBatchSize)
  ) {
    nextOptions.materializeBatchSize = options.materializeBatchSize;
  }

  if (options.verboseProgress) {
    nextOptions.verboseProgress = true;
  }

  return nextOptions;
}

async function confirmImportAction(
  message: string,
  force?: boolean,
): Promise<boolean> {
  if (force) {
    return true;
  }

  return confirm(message);
}

function registerSharedOptions(command: Command): Command {
  return command
    .option("--db-url <url>", "Override the default PostgreSQL connection URL.")
    .option(
      "--dataset <dataset>",
      "Process only one validated dataset block (for example: companies or cnaes).",
    )
    .option(
      "--load-batch-size <size>",
      "Maximum number of source rows per staging load unit. Defaults to 500.",
      (value) => Number.parseInt(value, 10),
    )
    .option(
      "--materialize-batch-size <size>",
      "Maximum number of staged rows per materialization chunk. Defaults to 50000.",
      (value) => Number.parseInt(value, 10),
    )
    .option(
      "--verbose-progress",
      "Show checkpoint offset and batch details in the live progress output.",
    )
    .option("-f, --force", "Skip the confirmation prompt.");
}

export function registerImportCommands(program: Command): void {
  const importCommand = registerSharedOptions(
    program
      .command("import")
      .argument("<input>", "Path to the extracted or mixed input directory.")
      .description(
        "Run the full import pipeline: prepare, load into staging/final targets, materialize staged datasets, and finalize the import plan.",
      ),
  );

  importCommand.action(async (input: string, options: SharedOptions) => {
    const confirmed = await confirmImportAction(
      `Run the full import pipeline for ${input}? This command loads staging tables and materializes the final relational tables.`,
      options.force,
    );
    if (!confirmed) {
      console.log("Import cancelled.");
      return;
    }

    const progress = createImportProgressReporter();
    const importOptions = applySharedImportOptions(options, {
      onProgress: progress,
    });
    const summary = await importDataToDatabase(input, importOptions);
    const logFilePath = await writeCommandLog("import", summary);
    printImportSummary(summary, logFilePath);
  });

  registerSharedOptions(
    importCommand
      .command("load")
      .argument("<input>", "Path to the extracted or mixed input directory.")
      .description(
        "Prepare the import plan and load validated files into staging or direct final targets without running final materialization.",
      ),
  ).action(async (input: string, options: SharedOptions) => {
    const confirmed = await confirmImportAction(
      `Load sanitized datasets from ${input} into staging/final targets now? This does not run final materialization.`,
      options.force,
    );
    if (!confirmed) {
      console.log("Load cancelled.");
      return;
    }

    const progress = createImportProgressReporter();
    const importOptions = applySharedImportOptions(options, {
      onProgress: progress,
    });
    const summary = await loadImportDataToStaging(input, importOptions);
    const logFilePath = await writeCommandLog("import-load", summary);
    printImportSummary(summary, logFilePath);
  });

  registerSharedOptions(
    importCommand
      .command("materialize")
      .argument("<input>", "Path to the extracted or mixed input directory.")
      .description(
        "Resume from the saved import plan and materialize staged datasets into the final relational tables.",
      ),
  ).action(async (input: string, options: SharedOptions) => {
    const confirmed = await confirmImportAction(
      `Materialize staged datasets for ${input} into the final relational tables now?`,
      options.force,
    );
    if (!confirmed) {
      console.log("Materialization cancelled.");
      return;
    }

    const progress = createImportProgressReporter();
    const importOptions = applySharedImportOptions(options, {
      onProgress: progress,
    });
    const summary = await materializeImportedData(input, importOptions);
    const logFilePath = await writeCommandLog("import-materialize", summary);
    printImportSummary(summary, logFilePath);
  });

  registerSharedOptions(
    importCommand
      .command("cleanup-staging")
      .description(
        "Truncate staging tables so a fresh bulk load can start from a clean intermediate state.",
      ),
  )
    .option(
      "--validated-path <path>",
      "Optionally clear saved materialization checkpoints for the latest plan of this validated path.",
    )
    .action(
      async (
        options: SharedOptions & {
          validatedPath?: string;
        },
      ) => {
        const confirmed = await confirmImportAction(
          "Truncate the configured staging tables now? This removes intermediate bulk-load data.",
          options.force,
        );
        if (!confirmed) {
          console.log("Cleanup cancelled.");
          return;
        }

        const cleanupOptions: {
          dbUrl?: string;
          dataset?: ImportOptions["dataset"];
          validatedPath?: string;
        } = {};

        if (options.dbUrl) {
          cleanupOptions.dbUrl = options.dbUrl;
        }
        if (options.dataset) {
          cleanupOptions.dataset = options.dataset as ImportOptions["dataset"];
        }
        if (options.validatedPath) {
          cleanupOptions.validatedPath = options.validatedPath;
        }

        const result = await cleanupImportStaging(cleanupOptions);
        const logFilePath = await writeCommandLog(
          "import-cleanup-staging",
          result,
        );
        printInfoWithLog(
          "IMPORT",
          `Truncated ${result.truncatedTables.length} staging table(s) on ${result.targetDatabase}.`,
          logFilePath,
        );
      },
    );
}
