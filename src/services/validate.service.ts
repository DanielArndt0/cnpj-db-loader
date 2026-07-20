import path from "node:path";

import { isSupportedArchiveFileName } from "./extract/archive-path.js";
import type {
  DatasetType,
  FileInspection,
  InspectSummary,
} from "./inspect.service.js";
import { inspectFiles } from "./inspect.service.js";

const EXPECTED_DATASETS: DatasetType[] = [
  "companies",
  "establishments",
  "partners",
  "simples_options",
  "countries",
  "cities",
  "partner_qualifications",
  "legal_natures",
  "cnaes",
  "reasons",
];

export type ValidationSummary = {
  ok: boolean;
  errors: string[];
  warnings: string[];
  inspected: InspectSummary;
  validatedPath: string;
  presentDatasets: DatasetType[];
  missingDatasets: DatasetType[];
  nextStep?: string | undefined;
};

function isRecognizedDataset(
  entry: FileInspection,
): entry is FileInspection & { inferredType: DatasetType } {
  return (
    entry.inferredType !== "zip-archive" && entry.inferredType !== "unknown"
  );
}

function uniqueDatasets(entries: FileInspection[]): DatasetType[] {
  return [
    ...new Set(
      entries.filter(isRecognizedDataset).map((entry) => entry.inferredType),
    ),
  ].sort();
}

function summarizeUnknownEntries(unknownEntries: FileInspection[]): string[] {
  if (unknownEntries.length === 0) {
    return [];
  }

  const preview = unknownEntries.slice(0, 5).map((entry) => entry.relativePath);
  const warnings = [
    `Found ${unknownEntries.length} unrecognized file(s) inside the validated tree.`,
  ];

  for (const item of preview) {
    warnings.push(`Arquivo não reconhecido: ${item}`);
  }

  if (unknownEntries.length > preview.length) {
    warnings.push(
      `Arquivos não reconhecidos adicionais foram omitidos da saída do terminal. Verifique o arquivo de log para a lista completa.`,
    );
  }

  return warnings;
}

function selectEntriesForValidation(inspected: InspectSummary): {
  validatedPath: string;
  entries: FileInspection[];
} {
  if (inspected.detectedInputMode !== "mixed") {
    return {
      validatedPath: inspected.inputPath,
      entries: inspected.entries,
    };
  }

  const extractedPrefix = `extracted${path.sep}`;
  const extractedEntries = inspected.entries.filter(
    (entry) =>
      entry.relativePath === "extracted" ||
      entry.relativePath.startsWith(extractedPrefix),
  );

  if (extractedEntries.length > 0) {
    return {
      validatedPath: path.join(inspected.inputPath, "extracted"),
      entries: extractedEntries.map((entry) => ({
        ...entry,
        relativePath:
          entry.relativePath === "extracted"
            ? "."
            : entry.relativePath.slice(extractedPrefix.length),
      })),
    };
  }

  return {
    validatedPath: inspected.inputPath,
    entries: inspected.entries.filter(
      (entry) => !isSupportedArchiveFileName(entry.relativePath),
    ),
  };
}

function inferNextStep(summary: {
  ok: boolean;
  detectedInputMode: InspectSummary["detectedInputMode"];
  inputPath: string;
  validatedPath: string;
  missingDatasets: DatasetType[];
  zipArchivesFound: number;
  presentDatasets: DatasetType[];
}): string | undefined {
  const normalizedInputPath = summary.inputPath.replace(/\\/g, "/");

  if (summary.detectedInputMode === "zip-archives-only") {
    return `cnpj-db-loader extract ${normalizedInputPath}`;
  }

  if (
    !summary.ok &&
    summary.zipArchivesFound > 0 &&
    summary.presentDatasets.length === 0
  ) {
    return `cnpj-db-loader extract ${normalizedInputPath}`;
  }

  if (!summary.ok && summary.missingDatasets.length > 0) {
    return `Revise os arquivos extraídos e garanta que todos os blocos de dataset esperados estejam presentes.`;
  }

  if (summary.ok) {
    const normalizedValidatedPath = summary.validatedPath.replace(/\\/g, "/");
    const validatedBaseName = path
      .basename(summary.validatedPath)
      .toLowerCase();
    if (
      validatedBaseName === "sanitized" ||
      validatedBaseName.endsWith("-sanitized")
    ) {
      return `cnpj-db-loader database config show`;
    }

    return `cnpj-db-loader sanitize ${normalizedValidatedPath}`;
  }

  return undefined;
}

export async function validateInputDirectory(
  inputPath: string,
): Promise<ValidationSummary> {
  const inspected = await inspectFiles(inputPath);
  const errors: string[] = [];
  const warnings: string[] = [];

  if (inspected.totalEntries === 0) {
    errors.push(
      "No files or directories were found in the provided input path.",
    );
  }

  const selected = selectEntriesForValidation(inspected);
  const recognizedEntries = selected.entries.filter(isRecognizedDataset);
  const presentDatasets = uniqueDatasets(recognizedEntries);
  const missingDatasets = EXPECTED_DATASETS.filter(
    (dataset) => !presentDatasets.includes(dataset),
  );

  const unknownEntries = selected.entries.filter(
    (entry) => entry.entryKind === "file" && entry.inferredType === "unknown",
  );

  if (inspected.detectedInputMode === "zip-archives-only") {
    warnings.push(
      `Nenhuma árvore de dataset extraído foi encontrada em ${inspected.inputPath}. A extração é necessária antes que a validação possa checar a completude do dataset.`,
    );
  } else {
    if (presentDatasets.length === 0) {
      errors.push(
        `No recognized extracted dataset files were found in ${selected.validatedPath}.`,
      );
    }

    if (missingDatasets.length > 0) {
      errors.push(
        `The extracted dataset tree is incomplete. Missing dataset block(s): ${missingDatasets.join(", ")}.`,
      );
    }

    if (
      inspected.detectedInputMode === "mixed" &&
      presentDatasets.length > 0 &&
      missingDatasets.length === 0
    ) {
      warnings.push(
        `Uma árvore de dataset extraído válida foi encontrada em ${selected.validatedPath}. Também há arquivos ZIP no diretório pai, mas não é necessário rodar a extração novamente.`,
      );
    }
  }

  warnings.push(...summarizeUnknownEntries(unknownEntries));

  const nextStep = inferNextStep({
    ok: errors.length === 0,
    detectedInputMode: inspected.detectedInputMode,
    inputPath: inspected.inputPath,
    validatedPath: selected.validatedPath,
    missingDatasets,
    zipArchivesFound: inspected.zipArchivesFound,
    presentDatasets,
  });

  return {
    ok: errors.length === 0,
    errors,
    warnings,
    inspected,
    validatedPath: selected.validatedPath,
    presentDatasets,
    missingDatasets,
    nextStep,
  };
}
