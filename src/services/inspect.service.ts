import { readdir, stat } from "node:fs/promises";
import path from "node:path";

export type DatasetType =
  | "companies"
  | "establishments"
  | "partners"
  | "simples_options"
  | "countries"
  | "cities"
  | "partner_qualifications"
  | "legal_natures"
  | "cnaes"
  | "reasons"
  | "zip-archive"
  | "unknown";

export type InputDetectionMode =
  | "zip-archives-only"
  | "extracted-tree"
  | "mixed"
  | "empty";

export type FileInspection = {
  relativePath: string;
  entryName: string;
  entryKind: "file" | "directory";
  size: number;
  inferredType: DatasetType;
  requiresExtraction: boolean;
};

export type InspectSummary = {
  inputPath: string;
  detectedInputMode: InputDetectionMode;
  totalEntries: number;
  zipArchivesFound: number;
  extractedEntriesFound: number;
  recognizedByType: Record<string, number>;
  recognizedDatasets: Partial<Record<DatasetType, number>>;
  warnings: string[];
  nextStep?: string | undefined;
  entries: FileInspection[];
};

const DATASET_TYPES: DatasetType[] = [
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

const DATASET_ALIASES: Record<string, DatasetType> = {
  empresas: "companies",
  estabelecimentos: "establishments",
  socios: "partners",
  simples: "simples_options",
  paises: "countries",
  municipios: "cities",
  qualificacoes: "partner_qualifications",
  naturezas: "legal_natures",
  cnaes: "cnaes",
  motivos: "reasons",
  emprecsv: "companies",
  estabelece: "establishments",
  sociocsv: "partners",
  simplescsv: "simples_options",
  paiscsv: "countries",
  municcsv: "cities",
  qualscsv: "partner_qualifications",
  natjucsv: "legal_natures",
  cnaecsv: "cnaes",
  moticsv: "reasons",
};

function normalizeEntryName(name: string): string {
  return name
    .normalize("NFD")
    .replace(/[̀-ͯ]/g, "")
    .toLowerCase()
    .replace(/\.zip$/i, "")
    .replace(/[\\/]/g, "")
    .replace(/[\s._-]+/g, "")
    .replace(/\d+$/g, "");
}

function inferDatasetAlias(name: string): DatasetType | undefined {
  const normalized = normalizeEntryName(name);

  if (DATASET_ALIASES[normalized]) {
    return DATASET_ALIASES[normalized];
  }

  if (normalized.startsWith("empresas")) return "companies";
  if (normalized.startsWith("estabelecimentos")) return "establishments";
  if (normalized.startsWith("socios")) return "partners";
  if (normalized.startsWith("simples")) return "simples_options";
  if (normalized.startsWith("paises")) return "countries";
  if (normalized.startsWith("municipios")) return "cities";
  if (normalized.startsWith("qualificacoes")) return "partner_qualifications";
  if (normalized.startsWith("naturezas")) return "legal_natures";
  if (normalized.startsWith("cnaes")) return "cnaes";
  if (normalized.startsWith("motivos")) return "reasons";

  if (normalized.includes("emprecsv")) return "companies";
  if (normalized.includes("estabele")) return "establishments";
  if (normalized.includes("sociocsv")) return "partners";
  if (normalized.includes("simplescsv")) return "simples_options";
  if (normalized.includes("paiscsv")) return "countries";
  if (normalized.includes("municcsv")) return "cities";
  if (normalized.includes("qualscsv")) return "partner_qualifications";
  if (normalized.includes("natjucsv")) return "legal_natures";
  if (normalized.includes("cnaecsv")) return "cnaes";
  if (normalized.includes("moticsv")) return "reasons";

  return undefined;
}

function inferType(relativePath: string, name: string): DatasetType {
  if (name.toLowerCase().endsWith(".zip")) {
    return "zip-archive";
  }

  const alias = inferDatasetAlias(relativePath) ?? inferDatasetAlias(name);
  if (alias) {
    return alias;
  }

  return "unknown";
}

async function walk(
  rootPath: string,
  currentPath = rootPath,
): Promise<FileInspection[]> {
  const entries = await readdir(currentPath, { withFileTypes: true });
  const inspections: FileInspection[] = [];

  for (const entry of entries) {
    const fullPath = path.join(currentPath, entry.name);
    const entryStat = await stat(fullPath);
    const relativePath = path.relative(rootPath, fullPath) || entry.name;
    const inferredType = inferType(relativePath, entry.name);

    inspections.push({
      relativePath,
      entryName: entry.name,
      entryKind: entry.isDirectory() ? "directory" : "file",
      size: entryStat.size,
      inferredType,
      requiresExtraction:
        entry.isFile() && entry.name.toLowerCase().endsWith(".zip"),
    });

    if (entry.isDirectory()) {
      inspections.push(...(await walk(rootPath, fullPath)));
    }
  }

  return inspections;
}

function detectInputMode(
  entries: FileInspection[],
  zipArchivesFound: number,
  extractedEntriesFound: number,
): InputDetectionMode {
  if (entries.length === 0) {
    return "empty";
  }

  if (zipArchivesFound > 0 && extractedEntriesFound === 0) {
    return "zip-archives-only";
  }

  if (zipArchivesFound === 0 && extractedEntriesFound > 0) {
    return "extracted-tree";
  }

  return "mixed";
}

function inferNextStep(
  inputPath: string,
  mode: InputDetectionMode,
): string | undefined {
  const normalized = inputPath.replace(/\\/g, "/");

  if (mode === "zip-archives-only") {
    return `cnpj-db-loader extract ${normalized}`;
  }

  if (mode === "extracted-tree") {
    return `cnpj-db-loader validate ${normalized}`;
  }

  if (mode === "mixed") {
    return `cnpj-db-loader validate ${normalized}`;
  }

  return undefined;
}

export async function inspectFiles(inputPath: string): Promise<InspectSummary> {
  const resolvedInputPath = path.resolve(inputPath);
  const entries = await walk(resolvedInputPath);
  const zipArchivesFound = entries.filter((entry) =>
    entry.entryName.toLowerCase().endsWith(".zip"),
  ).length;
  const extractedEntriesFound = entries.filter(
    (entry) =>
      entry.inferredType !== "zip-archive" && entry.inferredType !== "unknown",
  ).length;

  const detectedInputMode = detectInputMode(
    entries,
    zipArchivesFound,
    extractedEntriesFound,
  );

  const recognizedByType = entries.reduce<Record<string, number>>(
    (accumulator, entry) => {
      accumulator[entry.inferredType] =
        (accumulator[entry.inferredType] ?? 0) + 1;
      return accumulator;
    },
    {},
  );

  const recognizedDatasets = DATASET_TYPES.reduce<
    Partial<Record<DatasetType, number>>
  >((accumulator, datasetType) => {
    const count = entries.filter(
      (entry) => entry.inferredType === datasetType,
    ).length;

    if (count > 0) {
      accumulator[datasetType] = count;
    }

    return accumulator;
  }, {});

  const warnings: string[] = [];

  if (detectedInputMode === "zip-archives-only") {
    for (const expectedType of DATASET_TYPES) {
      const expectedPrefixFound = entries.some(
        (entry) => inferDatasetAlias(entry.relativePath) === expectedType,
      );

      if (!expectedPrefixFound) {
        warnings.push(`Expected ZIP dataset block not found: ${expectedType}.`);
      }
    }

    if (zipArchivesFound > 0) {
      warnings.push(
        `Input contains ${zipArchivesFound} ZIP archive(s). Extract them before running validation on the dataset contents.`,
      );
    }
  } else {
    for (const expectedType of DATASET_TYPES) {
      if (!entries.some((entry) => entry.inferredType === expectedType)) {
        warnings.push(
          `Expected extracted dataset block not found: ${expectedType}.`,
        );
      }
    }

    if (detectedInputMode === "mixed") {
      warnings.push(
        "Input contains both ZIP archives and extracted content. Consider using a clean extracted directory.",
      );
    }
  }

  return {
    inputPath: resolvedInputPath,
    detectedInputMode,
    totalEntries: entries.length,
    zipArchivesFound,
    extractedEntriesFound,
    recognizedByType,
    recognizedDatasets,
    warnings,
    nextStep: inferNextStep(resolvedInputPath, detectedInputMode),
    entries,
  };
}
