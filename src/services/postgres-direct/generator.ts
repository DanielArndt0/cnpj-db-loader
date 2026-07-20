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
import { generatePostgresDirectScriptFiles } from "./script.js";
import type {
  PostgresDirectIncludeTarget,
  PostgresDirectScriptDatasetSummary,
  PostgresDirectScriptOptions,
  PostgresDirectScriptSummary,
  PostgresDirectSourceFile,
  PostgresDirectTransactionMode,
} from "./types.js";

const DEFAULT_SOURCE_ENCODING = "UTF8";
const DEFAULT_TRANSACTION_MODE: PostgresDirectTransactionMode = "single";
const ALL_INCLUDE_TARGETS: PostgresDirectIncludeTarget[] = [
  "domains",
  "companies",
  "establishments",
  "partners",
  "simples",
  "secondary-cnaes",
  "indexes",
  "analyze",
];

const INCLUDE_TARGETS_BY_DATASET: Partial<
  Record<ImportDatasetType, PostgresDirectIncludeTarget>
> = {
  companies: "companies",
  establishments: "establishments",
  partners: "partners",
  simples_options: "simples",
  countries: "domains",
  cities: "domains",
  partner_qualifications: "domains",
  legal_natures: "domains",
  reasons: "domains",
  cnaes: "domains",
};

function defaultPostgresDirectOutputPath(inputPath: string): string {
  const baseName = path.basename(inputPath);
  if (baseName.toLowerCase() === "sanitized") {
    return path.join(path.dirname(inputPath), "postgres-direct");
  }

  return path.join(path.dirname(inputPath), `${baseName}-postgres-direct`);
}

function inferNextStep(scriptPath: string): string {
  return `psql -d "postgres://postgres:postgres@localhost:5432/cnpj" -f ${scriptPath.replace(/\\/g, "/")}`;
}

function normalizeSourceEncoding(value: string | undefined): string {
  const encoding = (value ?? DEFAULT_SOURCE_ENCODING).trim();

  if (!/^[A-Za-z0-9_-]+$/.test(encoding)) {
    throw new ValidationError(
      `Encoding de origem inválido: ${value}. Use um nome de client encoding do PostgreSQL, como UTF8, WIN1252 ou LATIN1.`,
    );
  }

  return encoding.toUpperCase();
}

function normalizeTransactionMode(
  value: PostgresDirectTransactionMode | undefined,
): PostgresDirectTransactionMode {
  const mode = value ?? DEFAULT_TRANSACTION_MODE;
  if (!["single", "phase", "none"].includes(mode)) {
    throw new ValidationError(
      `Modo de transação inválido: ${String(value)}. Use single, phase ou none.`,
    );
  }

  return mode;
}

function isIncludeTarget(value: string): value is PostgresDirectIncludeTarget {
  return (ALL_INCLUDE_TARGETS as string[]).includes(value);
}

function normalizeIncludeTargets(
  include: PostgresDirectIncludeTarget[] | undefined,
  dataset: ImportDatasetType | undefined,
): PostgresDirectIncludeTarget[] {
  if (include && include.length > 0) {
    const unique = [...new Set(include)];
    const invalid = unique.filter((item) => !isIncludeTarget(item));
    if (invalid.length > 0) {
      throw new ValidationError(
        `Alvo(s) de include inválido(s): ${invalid.join(", ")}. Use ${ALL_INCLUDE_TARGETS.join(", ")}.`,
      );
    }

    return unique;
  }

  if (dataset) {
    const target = INCLUDE_TARGETS_BY_DATASET[dataset];
    if (!target) {
      return [];
    }

    if (target === "establishments") {
      return ["establishments", "secondary-cnaes", "analyze"];
    }

    return [target, "analyze"];
  }

  return [...ALL_INCLUDE_TARGETS];
}

export async function generatePostgresDirectScript(
  inputPath: string,
  options: PostgresDirectScriptOptions = {},
): Promise<PostgresDirectScriptSummary> {
  if (options.dataset && !isImportDatasetType(options.dataset)) {
    throw new ValidationError(
      `Tipo de dataset não suportado: ${options.dataset}.`,
    );
  }

  const validation = await validateInputDirectory(inputPath);
  if (!validation.ok && !options.dataset) {
    throw new ValidationError(
      `O diretório de entrada não está pronto para a geração do script direto do PostgreSQL. ${validation.errors.join(" ")}`,
    );
  }

  const validatedPath = validation.ok
    ? validation.validatedPath
    : path.resolve(inputPath);
  const outputPath = path.resolve(
    options.outputPath ?? defaultPostgresDirectOutputPath(validatedPath),
  );
  const sourceEncoding = normalizeSourceEncoding(options.sourceEncoding);
  const transactionMode = normalizeTransactionMode(options.transactionMode);
  const include = normalizeIncludeTargets(options.include, options.dataset);
  const skipIndexes = options.skipIndexes ?? false;
  const skipAnalyze = options.skipAnalyze ?? false;
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
      "Nenhum arquivo de dataset reconhecido foi encontrado para a geração do script direto do PostgreSQL.",
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
    transactionMode,
    include,
    skipIndexes,
    skipAnalyze,
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
      fileMtime: fileStats.mtime.toISOString(),
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
  const generated = generatePostgresDirectScriptFiles({
    files: sourceFiles,
    validatedPath,
    sourceEncoding,
    transactionMode,
    include,
    skipIndexes,
    skipAnalyze,
  });

  const scriptFiles: string[] = [];
  for (const [fileName, script] of Object.entries(generated.scripts)) {
    const outputFileName =
      fileName === "import-postgres-direct.sql" ? scriptName : fileName;
    const outputFilePath = path.join(outputPath, outputFileName);
    await writeFile(outputFilePath, script, "utf8");
    scriptFiles.push(outputFilePath);
  }

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
    transactionMode,
    include,
    skipIndexes,
    skipAnalyze,
    inputPath: path.resolve(inputPath),
    validatedPath,
    outputPath,
    scriptPath,
    scriptFiles,
    sourceEncoding,
    totalFiles: sourceFiles.length,
    totalBytes,
    steps: generated.steps,
    sourceFingerprint: generated.sourceFingerprint,
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
    transactionMode,
    totalFiles: sourceFiles.length,
    totalBytes,
    datasets: summaryDatasets,
    scriptFiles,
    steps: generated.steps,
    warnings: [
      ...(validation.ok ? [] : validation.errors),
      "Este script importa os arquivos sanitizados da Receita diretamente com o \\copy do psql. Evita reescrever o dataset completo em uma segunda árvore de CSV.",
      "Os scripts gerados esperam que o schema do banco gerado pelo cnpj-db-loader seja aplicado antes da execução.",
      "O script direto do PostgreSQL agora usa UTF8 por padrão, porque o comando sanitize grava arquivos UTF-8 limpos.",
      "Use --source-encoding WIN1252 ou LATIN1 apenas ao gerar scripts para arquivos sanitizados legados produzidos por versões antigas do loader.",
      "A importação gerada agora é modular. Use import-postgres-direct.sql como orquestrador ou execute os scripts de cada fase manualmente.",
    ],
    nextStep: inferNextStep(scriptPath),
  };
}
