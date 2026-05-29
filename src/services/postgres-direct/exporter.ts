import { createWriteStream } from "node:fs";
import { mkdir, writeFile } from "node:fs/promises";
import path from "node:path";

import { ValidationError } from "../../core/errors/index.js";
import { inspectFiles } from "../inspect.service.js";
import { validateInputDirectory } from "../validate.service.js";
import { buildDisplayPath, sortEntries } from "../import/planning.js";
import { readImportSourceLines } from "../import/source-reader.js";
import { parseImportSourceLine } from "../import/parser.js";
import {
  DATASET_LAYOUTS,
  IMPORT_ORDER,
  isImportDatasetType,
  type ImportDatasetType,
  type ImportSchemaCapabilities,
} from "../import/types.js";
import { normalizeFieldCount, transformRecord } from "../import/transform.js";
import { formatCsvRow } from "./csv.js";
import { generatePostgresDirectImportScript } from "./script.js";
import type {
  PostgresCsvDatasetSummary,
  PostgresCsvExportOptions,
  PostgresCsvExportSummary,
  PostgresCsvFile,
} from "./types.js";

const POSTGRES_DIRECT_SCHEMA_CAPABILITIES: ImportSchemaCapabilities = {
  includeEstablishmentCnpjFullInInsert: true,
  includeEstablishmentSecondaryCnaesTable: true,
  includePartnerDedupeKeyInInsert: true,
  requiresLookupReconciliation: false,
};

function defaultPostgresCsvOutputPath(inputPath: string): string {
  const baseName = path.basename(inputPath);
  return path.join(path.dirname(inputPath), `${baseName}-postgres-csv`);
}

function normalizeOutputFileName(relativePath: string): string {
  const parsed = path.parse(relativePath);
  const baseName = parsed.name || parsed.base || "dataset";
  return path.join(parsed.dir, `${baseName}.csv`);
}

function resolveDatasetOutputPath(
  outputPath: string,
  dataset: ImportDatasetType,
  relativePath: string,
): string {
  return path.join(outputPath, dataset, normalizeOutputFileName(relativePath));
}

function inferNextStep(scriptPath: string): string {
  return `psql "postgres://postgres:postgres@localhost:5432/cnpj" -f ${scriptPath.replace(/\\/g, "/")}`;
}

async function writeCsvFile(input: {
  dataset: ImportDatasetType;
  inputFile: string;
  outputFile: string;
}): Promise<number> {
  const layout = DATASET_LAYOUTS[input.dataset];
  const columns = layout.fields.map((field) => field.columnName);

  await mkdir(path.dirname(input.outputFile), { recursive: true });

  const output = createWriteStream(input.outputFile, { encoding: "utf8" });
  let rows = 0;

  try {
    output.write(`${formatCsvRow(columns)}\n`);

    for await (const sourceLine of readImportSourceLines(input.inputFile)) {
      if (sourceLine.rawLine.trim() === "") {
        continue;
      }

      const parsed = parseImportSourceLine(sourceLine);
      const normalizedFields = normalizeFieldCount(
        parsed.fields,
        layout.fields.length,
        input.inputFile,
        parsed.lineNumber,
      );
      const values = transformRecord(
        input.dataset,
        layout,
        normalizedFields,
        POSTGRES_DIRECT_SCHEMA_CAPABILITIES,
        "staging",
      );

      output.write(`${formatCsvRow(values)}\n`);
      rows += 1;
    }
  } finally {
    output.end();
    await new Promise<void>((resolve, reject) => {
      output.on("finish", () => resolve());
      output.on("error", (error) => reject(error));
    });
  }

  return rows;
}

export async function exportPostgresCsvDataset(
  inputPath: string,
  options: PostgresCsvExportOptions = {},
): Promise<PostgresCsvExportSummary> {
  if (options.dataset && !isImportDatasetType(options.dataset)) {
    throw new ValidationError(`Unsupported dataset type: ${options.dataset}.`);
  }

  const validation = await validateInputDirectory(inputPath);
  if (!validation.ok) {
    throw new ValidationError(
      `The input directory is not ready for PostgreSQL CSV export. ${validation.errors.join(" ")}`,
    );
  }

  const validatedPath = validation.validatedPath;
  const outputPath = path.resolve(
    options.outputPath ?? defaultPostgresCsvOutputPath(validatedPath),
  );
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
      "No recognized dataset files were found for PostgreSQL CSV export.",
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
  });

  const exportedFiles: PostgresCsvFile[] = [];
  const summariesByDataset = new Map<
    ImportDatasetType,
    PostgresCsvDatasetSummary
  >();

  for (const [index, entry] of recognizedFiles.entries()) {
    const dataset = entry.inferredType;
    const inputFile = path.join(validatedPath, entry.relativePath);
    const outputFile = resolveDatasetOutputPath(
      outputPath,
      dataset,
      entry.relativePath,
    );

    options.onProgress?.({
      kind: "file_start",
      dataset,
      fileIndex: index + 1,
      totalFiles: recognizedFiles.length,
      inputFile: buildDisplayPath(inputFile),
      outputFile,
    });

    const rowCount = await writeCsvFile({ dataset, inputFile, outputFile });

    exportedFiles.push({
      dataset,
      absolutePath: outputFile,
      relativePath: path.relative(outputPath, outputFile),
      rowCount,
    });

    const currentSummary = summariesByDataset.get(dataset) ?? {
      dataset,
      files: 0,
      rows: 0,
      outputFiles: [],
    };
    currentSummary.files += 1;
    currentSummary.rows += rowCount;
    currentSummary.outputFiles.push(outputFile);
    summariesByDataset.set(dataset, currentSummary);

    options.onProgress?.({
      kind: "file_finish",
      dataset,
      fileIndex: index + 1,
      totalFiles: recognizedFiles.length,
      inputFile: buildDisplayPath(inputFile),
      outputFile,
      rows: rowCount,
    });
  }

  const scriptName = options.scriptName ?? "import-postgres-direct.sql";
  const scriptPath = path.join(outputPath, scriptName);
  const script = generatePostgresDirectImportScript({ files: exportedFiles });
  await writeFile(scriptPath, script, "utf8");

  const manifestPath = path.join(outputPath, "manifest.json");
  const summaryDatasets = [...summariesByDataset.values()].sort(
    (left, right) =>
      IMPORT_ORDER.indexOf(left.dataset) - IMPORT_ORDER.indexOf(right.dataset),
  );
  const totalRows = summaryDatasets.reduce((sum, item) => sum + item.rows, 0);

  const manifest = {
    generatedAt: new Date().toISOString(),
    inputPath: path.resolve(inputPath),
    validatedPath,
    outputPath,
    scriptPath,
    totalFiles: exportedFiles.length,
    totalRows,
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
    totalFiles: exportedFiles.length,
    totalRows,
  });

  return {
    inputPath: path.resolve(inputPath),
    validatedPath,
    outputPath,
    scriptPath,
    manifestPath,
    totalFiles: exportedFiles.length,
    totalRows,
    datasets: summaryDatasets,
    warnings: [
      "PostgreSQL-ready CSV export is intended for hybrid bulk imports after extraction, validation and sanitization.",
      "The generated SQL script resets staging tables and then upserts final tables. Review it before running against production databases.",
    ],
    nextStep: inferNextStep(scriptPath),
  };
}
