import type { Command } from "commander";

import { ValidationError } from "../../core/errors/index.js";
import { confirm } from "../../core/prompts/confirm.js";
import type {
  FederalRevenueCheckOptions,
  FederalRevenueCleanOptions,
  FederalRevenueDownloadOptions,
  FederalRevenueStatusOptions,
  FederalRevenueSyncOptions,
  ImportOptions,
} from "../../services/index.js";
import {
  checkFederalRevenueDataset,
  cleanFederalRevenueDataset,
  downloadFederalRevenueDataset,
  getFederalRevenueStatus,
  retryFederalRevenueDataset,
  syncFederalRevenueDataset,
  writeCommandLog,
} from "../../services/index.js";
import {
  createExtractionProgressReporter,
  createFederalRevenueDownloadProgressReporter,
  createImportProgressReporter,
  createSanitizeProgressReporter,
  printFederalRevenueCheckSummary,
  printFederalRevenueCleanSummary,
  printFederalRevenueDownloadSummary,
  printFederalRevenueStatusSummary,
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

type FederalRevenueStatusCommandOptions = FederalRevenueSharedOptions & {
  output?: string;
};

type FederalRevenueRetryCommandOptions = FederalRevenueDownloadCommandOptions;

type FederalRevenueCleanCommandOptions = FederalRevenueSharedOptions & {
  output?: string;
  partials?: boolean;
  failed?: boolean;
  all?: boolean;
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
  forceLock?: boolean;
};

function mergeSharedOptions(
  referenceArgument: string | undefined,
  options: FederalRevenueSharedOptions,
): FederalRevenueSharedOptions {
  if (
    referenceArgument &&
    options.reference &&
    referenceArgument !== options.reference
  ) {
    throw new ValidationError(
      `Federal Revenue reference conflict: received ${referenceArgument} and ${options.reference}. Use only one reference value.`,
    );
  }

  const reference = options.reference ?? referenceArgument;

  if (reference && options.current) {
    throw new ValidationError(
      "Federal Revenue reference conflict: use either a reference or --current, not both.",
    );
  }

  return {
    ...options,
    ...(reference ? { reference } : {}),
  };
}

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

function buildStatusOptions(
  options: FederalRevenueStatusCommandOptions,
): FederalRevenueStatusOptions {
  const statusOptions = applySharedOptions<FederalRevenueStatusOptions>(
    options,
    {},
  );

  if (options.output) {
    statusOptions.outputPath = options.output;
  }

  return statusOptions;
}

function buildCleanOptions(
  options: FederalRevenueCleanCommandOptions,
): FederalRevenueCleanOptions {
  const cleanOptions = applySharedOptions<FederalRevenueCleanOptions>(
    options,
    {},
  );

  if (options.output) {
    cleanOptions.outputPath = options.output;
  }

  if (options.partials) {
    cleanOptions.partials = true;
  }

  if (options.failed) {
    cleanOptions.failed = true;
  }

  if (options.all) {
    cleanOptions.all = true;
  }

  return cleanOptions;
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

function registerStatusOptions(command: Command): Command {
  return registerSharedOptions(command).option(
    "--output <path>",
    "Download root directory where the selected reference folder is stored.",
  );
}

function registerCleanOptions(command: Command): Command {
  return registerStatusOptions(command)
    .option("--partials", "Remove only .part files for the selected reference.")
    .option(
      "--failed",
      "Remove failed and partial files tracked by the local manifest.",
    )
    .option(
      "--all",
      "Remove the entire local reference folder, including ZIP files and manifest state.",
    )
    .option("-f, --force", "Skip the confirmation prompt.");
}

export function registerFederalRevenueCommands(program: Command): void {
  const federalRevenue = program
    .command("federal-revenue")
    .alias("revenue")
    .description(
      "Check, download, sync, and maintain CNPJ monthly files from the Federal Revenue public share.",
    );

  registerSharedOptions(
    federalRevenue
      .command("check")
      .argument(
        "[reference]",
        "Optional monthly reference in YYYY-MM format. Same as --reference.",
      )
      .description(
        "Check the latest available Federal Revenue monthly CNPJ reference and list its ZIP files.",
      ),
  ).action(
    async (
      referenceArgument: string | undefined,
      options: FederalRevenueSharedOptions,
    ) => {
      const resolvedOptions = mergeSharedOptions(referenceArgument, options);
      const summary = await checkFederalRevenueDataset(
        applySharedOptions<FederalRevenueCheckOptions>(resolvedOptions, {}),
      );
      const logFilePath = await writeCommandLog(
        "federal-revenue-check",
        summary,
      );
      printFederalRevenueCheckSummary(summary, logFilePath);
    },
  );

  registerDownloadOptions(
    federalRevenue
      .command("download")
      .argument(
        "[reference]",
        "Optional monthly reference in YYYY-MM format. Same as --reference.",
      )
      .description(
        "Download the selected Federal Revenue monthly CNPJ ZIP files with safe .part files, manifest state, and retries.",
      ),
  ).action(
    async (
      referenceArgument: string | undefined,
      options: FederalRevenueDownloadCommandOptions,
    ) => {
      const resolvedOptions = mergeSharedOptions(referenceArgument, options);
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
        ...buildDownloadOptions({ ...options, ...resolvedOptions }),
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
    },
  );

  registerStatusOptions(
    federalRevenue
      .command("status")
      .argument(
        "[reference]",
        "Optional monthly reference in YYYY-MM format. Same as --reference.",
      )
      .description(
        "Read the local Federal Revenue manifest and report downloaded, failed, partial, and missing files.",
      ),
  ).action(
    async (
      referenceArgument: string | undefined,
      options: FederalRevenueStatusCommandOptions,
    ) => {
      const resolvedOptions = mergeSharedOptions(referenceArgument, options);
      const summary = await getFederalRevenueStatus(
        buildStatusOptions({ ...options, ...resolvedOptions }),
      );
      const logFilePath = await writeCommandLog(
        "federal-revenue-status",
        summary,
      );
      printFederalRevenueStatusSummary(summary, logFilePath);

      if (!summary.isComplete) {
        process.exitCode = 1;
      }
    },
  );

  registerDownloadOptions(
    federalRevenue
      .command("retry")
      .argument(
        "[reference]",
        "Optional monthly reference in YYYY-MM format. Same as --reference.",
      )
      .description(
        "Retry only incomplete Federal Revenue files tracked by the local manifest.",
      ),
  ).action(
    async (
      referenceArgument: string | undefined,
      options: FederalRevenueRetryCommandOptions,
    ) => {
      const resolvedOptions = mergeSharedOptions(referenceArgument, options);
      const confirmed = await confirmFederalRevenueAction(
        "Retry incomplete Federal Revenue files now? Completed files are kept.",
        options.force,
      );
      if (!confirmed) {
        console.log("Federal Revenue retry cancelled.");
        return;
      }

      const progress = createFederalRevenueDownloadProgressReporter();
      const summary = await retryFederalRevenueDataset({
        ...buildDownloadOptions({ ...options, ...resolvedOptions }),
        onProgress: progress,
      });
      const logFilePath = await writeCommandLog(
        "federal-revenue-retry",
        summary,
      );
      printFederalRevenueDownloadSummary(summary, logFilePath);

      if (
        summary.failedFiles > 0 ||
        summary.partialFiles > 0 ||
        summary.missingFiles > 0
      ) {
        process.exitCode = 1;
      }
    },
  );

  registerCleanOptions(
    federalRevenue
      .command("clean")
      .argument(
        "[reference]",
        "Optional monthly reference in YYYY-MM format. Same as --reference.",
      )
      .description(
        "Clean local Federal Revenue partial files, failed files, or an entire reference folder.",
      ),
  ).action(
    async (
      referenceArgument: string | undefined,
      options: FederalRevenueCleanCommandOptions,
    ) => {
      const resolvedOptions = mergeSharedOptions(referenceArgument, options);
      const actionLabel = options.all
        ? "remove the entire selected Federal Revenue reference folder"
        : options.failed
          ? "remove failed and partial Federal Revenue files"
          : "remove Federal Revenue .part files";
      const confirmed = await confirmFederalRevenueAction(
        `This will ${actionLabel}. Continue?`,
        options.force,
      );
      if (!confirmed) {
        console.log("Federal Revenue cleanup cancelled.");
        return;
      }

      const summary = await cleanFederalRevenueDataset(
        buildCleanOptions({ ...options, ...resolvedOptions }),
      );
      const logFilePath = await writeCommandLog(
        "federal-revenue-clean",
        summary,
      );
      printFederalRevenueCleanSummary(summary, logFilePath);
    },
  );

  registerDownloadOptions(
    federalRevenue
      .command("sync")
      .argument(
        "[reference]",
        "Optional monthly reference in YYYY-MM format. Same as --reference.",
      )
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
      .option(
        "--force-lock",
        "Remove an existing local sync lock before starting. Use only after confirming the previous process stopped.",
      )
      .description(
        "Download, extract, validate, sanitize, and import the selected Federal Revenue monthly CNPJ dataset.",
      ),
  ).action(
    async (
      referenceArgument: string | undefined,
      options: FederalRevenueSyncCommandOptions,
    ) => {
      const resolvedOptions = mergeSharedOptions(referenceArgument, options);
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

      const commandOptions = { ...options, ...resolvedOptions };
      const syncOptions: FederalRevenueSyncOptions = {
        ...buildDownloadOptions(commandOptions),
        onProgress: downloadProgress,
        onExtractProgress: extractProgress,
        onSanitizeProgress: sanitizeProgress,
        onImportProgress: importProgress,
        importOptions: buildImportOptions(commandOptions),
      };

      if (options.extractOutput) {
        syncOptions.extractOutputPath = options.extractOutput;
      }

      if (options.sanitizeOutput) {
        syncOptions.sanitizeOutputPath = options.sanitizeOutput;
      }

      if (options.forceLock) {
        syncOptions.forceLock = true;
      }

      const summary = await syncFederalRevenueDataset(syncOptions);
      const logFilePath = await writeCommandLog(
        "federal-revenue-sync",
        summary,
      );
      printFederalRevenueSyncSummary(summary, logFilePath);
    },
  );
}
