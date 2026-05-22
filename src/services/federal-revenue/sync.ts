import { ValidationError } from "../../core/errors/index.js";
import { extractArchives } from "../extract.service.js";
import { importDataToDatabase } from "../import.service.js";
import { sanitizeInputDirectory } from "../sanitize.service.js";
import { validateInputDirectory } from "../validate.service.js";
import { downloadFederalRevenueDataset } from "./download.js";
import type {
  FederalRevenueSyncOptions,
  FederalRevenueSyncSummary,
} from "./types.js";

export async function syncFederalRevenueDataset(
  options: FederalRevenueSyncOptions = {},
): Promise<FederalRevenueSyncSummary> {
  const startedAt = new Date().toISOString();

  const download = await downloadFederalRevenueDataset({
    ...options,
    onProgress: options.onProgress,
  });

  if (download.failedFiles > 0) {
    throw new ValidationError(
      `Federal Revenue sync cannot continue because ${download.failedFiles} file(s) failed to download.`,
      { reference: download.reference, outputPath: download.outputPath },
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
