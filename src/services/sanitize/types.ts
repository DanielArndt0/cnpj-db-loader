import type { DatasetType, FileInspection } from "../inspect.service.js";

export type SanitizeDatasetType = Exclude<
  DatasetType,
  "zip-archive" | "unknown"
>;

export type SanitizeFilePlan = {
  dataset: SanitizeDatasetType;
  relativePath: string;
  absolutePath: string;
  outputPath: string;
  displayPath: string;
  fileSize: number;
};

export type SanitizePlan = {
  validatedPath: string;
  outputPath: string;
  totalFiles: number;
  totalBytes: number;
  datasets: SanitizeDatasetType[];
  files: SanitizeFilePlan[];
};

export type SanitizedFileResult = {
  plan: SanitizeFilePlan;
  totalBytesRead: number;
  totalBytesWritten: number;
  nulBytesRemoved: number;
  lineCount: number;
  changed: boolean;
};

export type SanitizeSummary = {
  inputPath: string;
  validatedPath: string;
  outputPath: string;
  totalFiles: number;
  totalBytes: number;
  processedFiles: number;
  processedRows: number;
  nulBytesRemoved: number;
  changedFiles: number;
  unchangedFiles: number;
  datasets: SanitizeDatasetType[];
  files: Array<{
    dataset: SanitizeDatasetType;
    relativePath: string;
    outputPath: string;
    lineCount: number;
    changed: boolean;
    nulBytesRemoved: number;
  }>;
  warnings: string[];
  nextStep?: string | undefined;
};

export type SanitizeProgressEvent =
  | {
      kind: "start";
      validatedPath: string;
      outputPath: string;
      totalFiles: number;
      totalBytes: number;
      datasets: SanitizeDatasetType[];
    }
  | {
      kind: "progress";
      currentFileDisplayPath: string;
      fileIndex: number;
      totalFiles: number;
      bytesProcessed: number;
      totalBytes: number;
      fileBytesProcessed: number;
      currentFileSize: number;
      processedRows: number;
      nulBytesRemoved: number;
      changedFiles: number;
    }
  | {
      kind: "finish";
      totalFiles: number;
      processedRows: number;
      nulBytesRemoved: number;
      changedFiles: number;
      totalBytes: number;
    };

export type SanitizeProgressListener = (event: SanitizeProgressEvent) => void;

export type SanitizeOptions = {
  outputPath?: string | undefined;
  dataset?: SanitizeDatasetType | undefined;
  onProgress?: SanitizeProgressListener | undefined;
};

export function isSanitizeDatasetType(
  value: string,
): value is SanitizeDatasetType {
  return [
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
  ].includes(value);
}

export function isRecognizedSanitizeEntry(
  entry: FileInspection,
): entry is FileInspection & { inferredType: SanitizeDatasetType } {
  return (
    entry.entryKind === "file" &&
    entry.inferredType !== "zip-archive" &&
    entry.inferredType !== "unknown"
  );
}
