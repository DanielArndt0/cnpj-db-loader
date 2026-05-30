export type {
  SanitizeDatasetType,
  SanitizeOptions,
  SanitizePlan,
  SanitizeProgressEvent,
  SanitizeProgressListener,
  SanitizeSourceEncoding,
  SanitizeSummary,
} from "./sanitize/types.js";

import path from "node:path";

import { ValidationError } from "../core/errors/index.js";
import { inspectFiles } from "./inspect.service.js";
import { validateInputDirectory } from "./validate.service.js";
import { buildDisplayPath } from "./import/planning.js";
import type {
  SanitizeDatasetType,
  SanitizeFilePlan,
  SanitizeOptions,
  SanitizePlan,
  SanitizeSummary,
} from "./sanitize/types.js";
import {
  isRecognizedSanitizeEntry,
  isSanitizeDatasetType,
} from "./sanitize/types.js";
import { normalizeSanitizeSourceEncoding } from "./sanitize/encoding.js";
import { sanitizeDatasetFile } from "./sanitize/runner.js";

function defaultSanitizedOutputPath(validatedPath: string): string {
  const baseName = path.basename(validatedPath);
  if (baseName.toLowerCase() === "extracted") {
    return path.join(path.dirname(validatedPath), "sanitized");
  }

  return path.join(path.dirname(validatedPath), `${baseName}-sanitized`);
}

function inferNextStep(outputPath: string): string {
  return `cnpj-db-loader import ${outputPath.replace(/\\/g, "/")}`;
}

function buildSanitizePlan(
  validatedPath: string,
  outputPath: string,
  inspectedPath: Awaited<ReturnType<typeof inspectFiles>>,
  selectedDataset?: SanitizeDatasetType,
): SanitizePlan {
  const files = inspectedPath.entries
    .filter(isRecognizedSanitizeEntry)
    .filter(
      (entry) => !selectedDataset || entry.inferredType === selectedDataset,
    )
    .map<SanitizeFilePlan>((entry) => ({
      dataset: entry.inferredType,
      relativePath: entry.relativePath,
      absolutePath: path.join(validatedPath, entry.relativePath),
      outputPath: path.join(outputPath, entry.relativePath),
      displayPath: buildDisplayPath(
        path.join(validatedPath, entry.relativePath),
      ),
      fileSize: entry.size,
    }));

  return {
    validatedPath,
    outputPath,
    totalFiles: files.length,
    totalBytes: files.reduce((sum, item) => sum + item.fileSize, 0),
    datasets: [...new Set(files.map((item) => item.dataset))],
    files,
  };
}

export async function sanitizeInputDirectory(
  inputPath: string,
  options: SanitizeOptions = {},
): Promise<SanitizeSummary> {
  if (options.dataset && !isSanitizeDatasetType(options.dataset)) {
    throw new ValidationError(`Unsupported dataset type: ${options.dataset}.`);
  }

  const validation = await validateInputDirectory(inputPath);
  if (!validation.ok) {
    throw new ValidationError(
      `The input directory is not ready for sanitization. ${validation.errors.join(" ")}`,
    );
  }

  const validatedPath = validation.validatedPath;
  const outputPath = path.resolve(
    options.outputPath ?? defaultSanitizedOutputPath(validatedPath),
  );
  const inspectedValidatedPath = await inspectFiles(validatedPath);
  const plan = buildSanitizePlan(
    validatedPath,
    outputPath,
    inspectedValidatedPath,
    options.dataset,
  );

  if (plan.totalFiles === 0) {
    throw new ValidationError(
      "No recognized validated dataset files were found for sanitization.",
    );
  }

  const sourceEncoding = normalizeSanitizeSourceEncoding(
    options.sourceEncoding,
  );

  options.onProgress?.({
    kind: "start",
    validatedPath,
    outputPath,
    totalFiles: plan.totalFiles,
    totalBytes: plan.totalBytes,
    datasets: plan.datasets,
    sourceEncoding,
  });

  let processedFiles = 0;
  let processedRows = 0;
  let processedBytes = 0;
  let nulBytesRemoved = 0;
  let invalidBytesRemoved = 0;
  let controlCharsRemoved = 0;
  let replacementCharactersFound = 0;
  let replacementCharactersRemaining = 0;
  let changedFiles = 0;
  const fileSummaries: SanitizeSummary["files"] = [];

  for (const [index, filePlan] of plan.files.entries()) {
    const fileResult = await sanitizeDatasetFile(
      filePlan,
      (chunk) => {
        options.onProgress?.({
          kind: "progress",
          currentFileDisplayPath: filePlan.displayPath,
          fileIndex: index + 1,
          totalFiles: plan.totalFiles,
          bytesProcessed: processedBytes + chunk.fileBytesProcessed,
          totalBytes: plan.totalBytes,
          fileBytesProcessed: chunk.fileBytesProcessed,
          currentFileSize: chunk.currentFileSize,
          processedRows: processedRows + chunk.processedRows,
          nulBytesRemoved: nulBytesRemoved + chunk.nulBytesRemoved,
          invalidBytesRemoved: invalidBytesRemoved + chunk.invalidBytesRemoved,
          controlCharsRemoved: controlCharsRemoved + chunk.controlCharsRemoved,
          replacementCharactersFound:
            replacementCharactersFound + chunk.replacementCharactersFound,
          changedFiles,
        });
      },
      { sourceEncoding, strict: options.strict },
    );

    processedFiles += 1;
    processedRows += fileResult.lineCount;
    processedBytes += fileResult.totalBytesRead;
    nulBytesRemoved += fileResult.nulBytesRemoved;
    invalidBytesRemoved += fileResult.invalidBytesRemoved;
    controlCharsRemoved += fileResult.controlCharsRemoved;
    replacementCharactersFound += fileResult.replacementCharactersFound;
    replacementCharactersRemaining += fileResult.replacementCharactersRemaining;
    changedFiles += fileResult.changed ? 1 : 0;

    fileSummaries.push({
      dataset: filePlan.dataset,
      relativePath: filePlan.relativePath,
      outputPath: filePlan.outputPath,
      lineCount: fileResult.lineCount,
      changed: fileResult.changed,
      nulBytesRemoved: fileResult.nulBytesRemoved,
      invalidBytesRemoved: fileResult.invalidBytesRemoved,
      controlCharsRemoved: fileResult.controlCharsRemoved,
      replacementCharactersFound: fileResult.replacementCharactersFound,
      replacementCharactersRemaining: fileResult.replacementCharactersRemaining,
    });
  }

  options.onProgress?.({
    kind: "finish",
    totalFiles: plan.totalFiles,
    processedRows,
    nulBytesRemoved,
    invalidBytesRemoved,
    controlCharsRemoved,
    replacementCharactersFound,
    replacementCharactersRemaining,
    changedFiles,
    totalBytes: plan.totalBytes,
  });

  return {
    inputPath: path.resolve(inputPath),
    validatedPath,
    outputPath,
    totalFiles: plan.totalFiles,
    totalBytes: plan.totalBytes,
    processedFiles,
    processedRows,
    sourceEncoding,
    nulBytesRemoved,
    invalidBytesRemoved,
    controlCharsRemoved,
    replacementCharactersFound,
    replacementCharactersRemaining,
    changedFiles,
    unchangedFiles: plan.totalFiles - changedFiles,
    datasets: plan.datasets,
    files: fileSummaries,
    warnings: [
      "Sanitization normalizes Receita source files into UTF-8, removes NUL bytes at byte level and removes problematic control characters before PostgreSQL loading begins.",
      "Sanitized output is validated before replacing the destination file. Strict mode rejects Unicode replacement characters instead of silently hiding data loss.",
      "The PostgreSQL direct import path can use --source-encoding UTF8 when reading files generated by this sanitization command.",
      "The import command still keeps quarantine and row-level recovery for unexpected issues, but sanitizing first reduces the amount of slow fallback work during import.",
    ],
    nextStep: inferNextStep(outputPath),
  };
}
