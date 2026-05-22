import type { Command } from "commander";

import { confirm } from "../../core/prompts/confirm.js";
import type {
  FederalRevenueCheckOptions,
  FederalRevenueDownloadOptions,
  FederalRevenueSyncOptions,
  ImportOptions,
} from "../../services/index.js";
import {
  checkFederalRevenueDataset,
  downloadFederalRevenueDataset,
  syncFederalRevenueDataset,
  writeCommandLog,
} from "../../services/index.js";
import {
  createExtractionProgressReporter,
  createFederalRevenueDownloadProgressReporter,
  createImportProgressReporter,
  createSanitizeProgressReporter,
  printFederalRevenueCheckSummary,
  printFederalRevenueDownloadSummary,
  printFederalRevenueSyncSummary,
} from "../ui/output.js";

type FederalRevenueSharedOptions = {
  reference?: string;
  current?: boolean;
  baseUrl?: string;
  shareToken?: string;
};

type FederalRevenueDownloadCommandOptions = FederalRevenueSharedOptions & {
  output?: string;
  retries?: number;
  overwrite?: boolean;
  force?: boolean;
};

type FederalRevenueSyncCommandOptions = FederalRevenueDownloadCommandOptions & {
  extractOutput?: string;
  sanitizeOutput?: string;
  dbUrl?: string;
  dataset?: string;
  loadBatchSize?: number;
  materializeBatchSize?: number;
  verboseProgress?: boolean;
};

function applySharedOptions<T extends FederalRevenueCheckOptions>(
  options: FederalRevenueSharedOptions,
  target: T,
): T {
  if (options.reference) {
    target.reference = options.reference;
  }

  if (options.current) {
    target.current = true;
  }

  if (options.baseUrl) {
    target.baseUrl = options.baseUrl;
  }

  if (options.shareToken) {
    target.shareToken = options.shareToken;
  }

  return target;
}

function buildDownloadOptions(
  options: FederalRevenueDownloadCommandOptions,
): FederalRevenueDownloadOptions {
  const downloadOptions = applySharedOptions<FederalRevenueDownloadOptions>(
    options,
    {},
  );

  if (options.output) {
    downloadOptions.outputPath = options.output;
  }

  if (typeof options.retries === "number" && !Number.isNaN(options.retries)) {
    downloadOptions.retries = options.retries;
  }

  if (options.overwrite) {
    downloadOptions.overwrite = true;
  }

  return downloadOptions;
}

function buildImportOptions(
  options: FederalRevenueSyncCommandOptions,
): Omit<ImportOptions, "onProgress"> {
  const importOptions: Omit<ImportOptions, "onProgress"> = {};

  if (options.dbUrl) {
    importOptions.dbUrl = options.dbUrl;
  }

  if (options.dataset) {
    importOptions.dataset = options.dataset as ImportOptions["dataset"];
  }

  if (
    typeof options.loadBatchSize === "number" &&
    !Number.isNaN(options.loadBatchSize)
  ) {
    importOptions.loadBatchSize = options.loadBatchSize;
    importOptions.batchSize = options.loadBatchSize;
  }

  if (
    typeof options.materializeBatchSize === "number" &&
    !Number.isNaN(options.materializeBatchSize)
  ) {
    importOptions.materializeBatchSize = options.materializeBatchSize;
  }

  if (options.verboseProgress) {
    importOptions.verboseProgress = true;
  }

  return importOptions;
}

async function confirmFederalRevenueAction(
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
    .option(
      "--reference <yyyy-mm>",
      "Use an explicit monthly Federal Revenue reference, for example 2026-05.",
    )
    .option(
      "--current",
      "Use the current calendar month instead of the latest published reference.",
    )
    .option(
      "--base-url <url>",
      "Override the public Federal Revenue WebDAV base URL.",
    )
    .option(
      "--share-token <token>",
      "Override the public Federal Revenue share token.",
    );
}

function registerDownloadOptions(command: Command): Command {
  return registerSharedOptions(command)
    .option(
      "--output <path>",
      "Download root directory. A child folder named with the selected reference is created inside it.",
    )
    .option(
      "--retries <number>",
      "Retry attempts per file before marking it as failed. Defaults to 3.",
      (value) => Number.parseInt(value, 10),
    )
    .option(
      "--overwrite",
      "Download files again even when a completed local copy exists.",
    )
    .option("-f, --force", "Skip the confirmation prompt.");
}

export function registerFederalRevenueCommands(program: Command): void {
  const federalRevenue = program
    .command("federal-revenue")
    .alias("revenue")
    .description(
      "Check, download, and sync CNPJ monthly files from the Federal Revenue public share.",
    );

  registerSharedOptions(
    federalRevenue
      .command("check")
      .description(
        "Check the latest available Federal Revenue monthly CNPJ reference and list its ZIP files.",
      ),
  ).action(async (options: FederalRevenueSharedOptions) => {
    const summary = await checkFederalRevenueDataset(
      applySharedOptions<FederalRevenueCheckOptions>(options, {}),
    );
    const logFilePath = await writeCommandLog("federal-revenue-check", summary);
    printFederalRevenueCheckSummary(summary, logFilePath);
  });

  registerDownloadOptions(
    federalRevenue
      .command("download")
      .description(
        "Download the selected Federal Revenue monthly CNPJ ZIP files with safe .part files and retries.",
      ),
  ).action(async (options: FederalRevenueDownloadCommandOptions) => {
    const confirmed = await confirmFederalRevenueAction(
      "Download Federal Revenue CNPJ ZIP files now? Existing completed files are skipped unless --overwrite is used.",
      options.force,
    );
    if (!confirmed) {
      console.log("Federal Revenue download cancelled.");
      return;
    }

    const progress = createFederalRevenueDownloadProgressReporter();
    const summary = await downloadFederalRevenueDataset({
      ...buildDownloadOptions(options),
      onProgress: progress,
    });
    const logFilePath = await writeCommandLog(
      "federal-revenue-download",
      summary,
    );
    printFederalRevenueDownloadSummary(summary, logFilePath);

    if (summary.failedFiles > 0) {
      process.exitCode = 1;
    }
  });

  registerDownloadOptions(
    federalRevenue
      .command("sync")
      .option(
        "--extract-output <path>",
        "Custom extraction output directory. Defaults to <download-reference>/extracted.",
      )
      .option(
        "--sanitize-output <path>",
        "Custom sanitized output directory. Defaults to <download-reference>/sanitized.",
      )
      .option(
        "--db-url <url>",
        "Override the default PostgreSQL connection URL.",
      )
      .option(
        "--dataset <dataset>",
        "Process only one validated dataset block during import.",
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
        "Show checkpoint offset and batch details in the live import progress output.",
      )
      .description(
        "Download, extract, validate, sanitize, and import the selected Federal Revenue monthly CNPJ dataset.",
      ),
  ).action(async (options: FederalRevenueSyncCommandOptions) => {
    const confirmed = await confirmFederalRevenueAction(
      "Run the full Federal Revenue sync now? This downloads files, extracts archives, sanitizes the dataset, and imports it into PostgreSQL.",
      options.force,
    );
    if (!confirmed) {
      console.log("Federal Revenue sync cancelled.");
      return;
    }

    const downloadProgress = createFederalRevenueDownloadProgressReporter();
    const extractProgress = createExtractionProgressReporter();
    const sanitizeProgress = createSanitizeProgressReporter();
    const importProgress = createImportProgressReporter();

    const syncOptions: FederalRevenueSyncOptions = {
      ...buildDownloadOptions(options),
      onProgress: downloadProgress,
      onExtractProgress: extractProgress,
      onSanitizeProgress: sanitizeProgress,
      onImportProgress: importProgress,
      importOptions: buildImportOptions(options),
    };

    if (options.extractOutput) {
      syncOptions.extractOutputPath = options.extractOutput;
    }

    if (options.sanitizeOutput) {
      syncOptions.sanitizeOutputPath = options.sanitizeOutput;
    }

    const summary = await syncFederalRevenueDataset(syncOptions);
    const logFilePath = await writeCommandLog("federal-revenue-sync", summary);
    printFederalRevenueSyncSummary(summary, logFilePath);
  });
}
