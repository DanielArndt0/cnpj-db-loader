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
      `O sync da Receita Federal não pode continuar porque ${download.failedFiles} arquivo(s) falharam no download. Execute rfb retry ${download.reference} após corrigir a causa.`,
      { reference: download.reference, outputPath: download.outputPath },
    );
  }

  if (download.partialFiles > 0 || download.missingFiles > 0) {
    throw new ValidationError(
      `O sync da Receita Federal não pode continuar porque a referência local está incompleta. Arquivos parciais: ${download.partialFiles}. Arquivos ausentes: ${download.missingFiles}.`,
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
      `O sync da Receita Federal não pode continuar porque ${extraction.failedArchives.length} arquivo(s) falharam na extração.`,
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
      `O sync da Receita Federal não pode continuar porque o dataset extraído não é válido. ${validation.errors.join(" ")}`,
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
