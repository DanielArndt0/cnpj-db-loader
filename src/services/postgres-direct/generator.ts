import { mkdir, stat, writeFile } from "node:fs/promises";
import path from "node:path";

import { ValidationError } from "../../core/errors/index.js";
import { inspectFiles } from "../inspect.service.js";
import { validateInputDirectory } from "../validate.service.js";
import { buildDisplayPath, sortEntries } from "../import/planning.js";
import {
  IMPORT_ORDER,
  isImportDatasetType,
  type ImportDatasetType,
} from "../import/types.js";
import { generatePostgresSanitizedDirectImportScript } from "./script.js";
import type {
  PostgresDirectScriptDatasetSummary,
  PostgresDirectScriptOptions,
  PostgresDirectScriptSummary,
  PostgresDirectSourceFile,
} from "./types.js";

const DEFAULT_SOURCE_ENCODING = "WIN1252";

function defaultPostgresDirectOutputPath(inputPath: string): string {
  const baseName = path.basename(inputPath);
  if (baseName.toLowerCase() === "sanitized") {
    return path.join(path.dirname(inputPath), "postgres-direct");
  }

  return path.join(path.dirname(inputPath), `${baseName}-postgres-direct`);
}

function inferNextStep(scriptPath: string): string {
  return `psql "postgres://postgres:postgres@localhost:5432/cnpj" -f ${scriptPath.replace(/\\/g, "/")}`;
}

function normalizeSourceEncoding(value: string | undefined): string {
  const encoding = (value ?? DEFAULT_SOURCE_ENCODING).trim();

  if (!/^[A-Za-z0-9_-]+$/.test(encoding)) {
    throw new ValidationError(
      `Invalid source encoding: ${value}. Use a PostgreSQL client encoding name such as WIN1252 or UTF8.`,
    );
  }

  return encoding.toUpperCase();
}

export async function generatePostgresDirectScript(
  inputPath: string,
  options: PostgresDirectScriptOptions = {},
): Promise<PostgresDirectScriptSummary> {
  if (options.dataset && !isImportDatasetType(options.dataset)) {
    throw new ValidationError(`Unsupported dataset type: ${options.dataset}.`);
  }

  const validation = await validateInputDirectory(inputPath);
  if (!validation.ok && !options.dataset) {
    throw new ValidationError(
      `The input directory is not ready for PostgreSQL direct script generation. ${validation.errors.join(" ")}`,
    );
  }

  const validatedPath = validation.ok
    ? validation.validatedPath
    : path.resolve(inputPath);
  const outputPath = path.resolve(
    options.outputPath ?? defaultPostgresDirectOutputPath(validatedPath),
  );
  const sourceEncoding = normalizeSourceEncoding(options.sourceEncoding);
  const inspected = await inspectFiles(validatedPath);
  const recognizedFiles = inspected.entries
    .filter((entry) => entry.entryKind === "file")
    .flatMap((entry) => {
      if (!isImportDatasetType(entry.inferredType)) {
        return [];
      }

      if (options.dataset && entry.inferredType !== options.dataset) {
        return [];
      }

      return [{ ...entry, inferredType: entry.inferredType }];
    })
    .sort(sortEntries);

  if (recognizedFiles.length === 0) {
    throw new ValidationError(
      "No recognized dataset files were found for PostgreSQL direct script generation.",
    );
  }

  const datasets = [
    ...new Set(recognizedFiles.map((entry) => entry.inferredType)),
  ].sort(
    (left, right) => IMPORT_ORDER.indexOf(left) - IMPORT_ORDER.indexOf(right),
  );

  options.onProgress?.({
    kind: "start",
    inputPath: path.resolve(inputPath),
    validatedPath,
    outputPath,
    totalFiles: recognizedFiles.length,
    datasets,
    sourceEncoding,
  });

  await mkdir(outputPath, { recursive: true });

  const sourceFiles: PostgresDirectSourceFile[] = [];
  const summariesByDataset = new Map<
    ImportDatasetType,
    PostgresDirectScriptDatasetSummary
  >();

  for (const [index, entry] of recognizedFiles.entries()) {
    const dataset = entry.inferredType;
    const absolutePath = path.join(validatedPath, entry.relativePath);
    const fileStats = await stat(absolutePath);

    sourceFiles.push({
      dataset,
      absolutePath,
      relativePath: entry.relativePath,
      fileSize: fileStats.size,
    });

    const currentSummary = summariesByDataset.get(dataset) ?? {
      dataset,
      files: 0,
      totalBytes: 0,
      sourceFiles: [],
    };
    currentSummary.files += 1;
    currentSummary.totalBytes += fileStats.size;
    currentSummary.sourceFiles.push(absolutePath);
    summariesByDataset.set(dataset, currentSummary);

    options.onProgress?.({
      kind: "file_registered",
      dataset,
      fileIndex: index + 1,
      totalFiles: recognizedFiles.length,
      inputFile: buildDisplayPath(absolutePath),
      fileSize: fileStats.size,
    });
  }

  const scriptName = options.scriptName ?? "import-postgres-direct.sql";
  const scriptPath = path.join(outputPath, scriptName);
  const script = generatePostgresSanitizedDirectImportScript({
    files: sourceFiles,
    sourceEncoding,
  });
  await writeFile(scriptPath, script, "utf8");

  const manifestPath = path.join(outputPath, "manifest.json");
  const summaryDatasets = [...summariesByDataset.values()].sort(
    (left, right) =>
      IMPORT_ORDER.indexOf(left.dataset) - IMPORT_ORDER.indexOf(right.dataset),
  );
  const totalBytes = summaryDatasets.reduce(
    (sum, item) => sum + item.totalBytes,
    0,
  );

  const manifest = {
    generatedAt: new Date().toISOString(),
    mode: "direct-sanitized-script",
    inputPath: path.resolve(inputPath),
    validatedPath,
    outputPath,
    scriptPath,
    sourceEncoding,
    totalFiles: sourceFiles.length,
    totalBytes,
    datasets: summaryDatasets,
  };
  await writeFile(
    manifestPath,
    `${JSON.stringify(manifest, null, 2)}\n`,
    "utf8",
  );

  options.onProgress?.({
    kind: "finish",
    outputPath,
    scriptPath,
    totalFiles: sourceFiles.length,
    totalBytes,
  });

  return {
    inputPath: path.resolve(inputPath),
    validatedPath,
    outputPath,
    scriptPath,
    manifestPath,
    sourceEncoding,
    totalFiles: sourceFiles.length,
    totalBytes,
    datasets: summaryDatasets,
    warnings: [
      ...(validation.ok ? [] : validation.errors),
      "This script imports sanitized Receita files directly with psql \\copy. It avoids rewriting the full dataset into a second CSV tree.",
      "The generated script expects the database schema generated by cnpj-db-loader to be applied before execution.",
      "Use --source-encoding UTF8 only if your sanitized files are already UTF-8. The default WIN1252 matches the usual Receita file encoding.",
    ],
    nextStep: inferNextStep(scriptPath),
  };
}
