import { ValidationError } from "../../core/errors/index.js";
import { extractArchives } from "../extract.service.js";
import { importDataToDatabase } from "../import.service.js";
import { sanitizeInputDirectory } from "../sanitize.service.js";
import { validateInputDirectory } from "../validate.service.js";
import {
  checkFederalRevenueDataset,
  downloadFederalRevenueDataset,
} from "./download.js";
import { withFederalRevenueSyncLock } from "./lock.js";
import { buildFederalRevenueReferenceOutputPath } from "./manifest.js";
import type {
  FederalRevenueDownloadOptions,
  FederalRevenueSyncOptions,
  FederalRevenueSyncSummary,
} from "./types.js";

function buildLockedDownloadOptions(
  options: FederalRevenueSyncOptions,
  reference: string,
): FederalRevenueDownloadOptions {
  const { current, forceLock, ...downloadOptions } = options;
  void current;
  void forceLock;

  return {
    ...downloadOptions,
    reference,
    manifestCommand: "sync",
  };
}

async function runFederalRevenueSyncPipeline(
  options: FederalRevenueSyncOptions,
  reference: string,
): Promise<FederalRevenueSyncSummary> {
  const startedAt = new Date().toISOString();
  const download = await downloadFederalRevenueDataset(
    buildLockedDownloadOptions(options, reference),
  );

  if (download.failedFiles > 0) {
    throw new ValidationError(
      `Federal Revenue sync cannot continue because ${download.failedFiles} file(s) failed to download. Run federal-revenue retry ${download.reference} after fixing the cause.`,
      { reference: download.reference, outputPath: download.outputPath },
    );
  }

  if (download.partialFiles > 0 || download.missingFiles > 0) {
    throw new ValidationError(
      `Federal Revenue sync cannot continue because the local reference is incomplete. Partial files: ${download.partialFiles}. Missing files: ${download.missingFiles}.`,
      {
        reference: download.reference,
        outputPath: download.outputPath,
        partialFiles: download.partialFiles,
        missingFiles: download.missingFiles,
      },
    );
  }

  const extraction = await extractArchives(
    download.outputPath,
    options.extractOutputPath,
    options.onExtractProgress,
  );

  if (extraction.failedArchives.length > 0) {
    throw new ValidationError(
      `Federal Revenue sync cannot continue because ${extraction.failedArchives.length} archive(s) failed to extract.`,
      {
        reference: download.reference,
        failedArchives: extraction.failedArchives,
        outputPath: extraction.outputPath,
      },
    );
  }

  const validation = await validateInputDirectory(extraction.outputPath);
  if (!validation.ok) {
    throw new ValidationError(
      `Federal Revenue sync cannot continue because the extracted dataset is not valid. ${validation.errors.join(" ")}`,
      { reference: download.reference, errors: validation.errors },
    );
  }

  const sanitization = await sanitizeInputDirectory(extraction.outputPath, {
    ...options.sanitizeOptions,
    outputPath: options.sanitizeOutputPath,
    onProgress: options.onSanitizeProgress,
  });

  const importSummary = await importDataToDatabase(sanitization.outputPath, {
    ...options.importOptions,
    onProgress: options.onImportProgress,
  });

  return {
    reference: download.reference,
    download,
    extraction,
    validation,
    sanitization,
    import: importSummary,
    startedAt,
    finishedAt: new Date().toISOString(),
    warnings: [
      ...download.warnings,
      ...validation.warnings,
      ...sanitization.warnings,
      ...importSummary.warnings,
    ],
  };
}

export async function syncFederalRevenueDataset(
  options: FederalRevenueSyncOptions = {},
): Promise<FederalRevenueSyncSummary> {
  const check = await checkFederalRevenueDataset(options);
  const outputPath = buildFederalRevenueReferenceOutputPath(
    check.selectedReference,
    options.outputPath,
  );

  return withFederalRevenueSyncLock(
    {
      reference: check.selectedReference,
      outputPath,
      options: { forceLock: options.forceLock },
    },
    () => runFederalRevenueSyncPipeline(options, check.selectedReference),
  );
}
