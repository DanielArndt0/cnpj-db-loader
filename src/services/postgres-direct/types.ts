import type { ImportDatasetType } from "../import/types.js";

export type PostgresCsvDatasetSummary = {
  dataset: ImportDatasetType;
  files: number;
  rows: number;
  outputFiles: string[];
};

export type PostgresCsvExportProgressEvent =
  | {
      kind: "start";
      inputPath: string;
      validatedPath: string;
      outputPath: string;
      totalFiles: number;
      datasets: ImportDatasetType[];
    }
  | {
      kind: "file_start";
      dataset: ImportDatasetType;
      fileIndex: number;
      totalFiles: number;
      inputFile: string;
      outputFile: string;
    }
  | {
      kind: "file_finish";
      dataset: ImportDatasetType;
      fileIndex: number;
      totalFiles: number;
      inputFile: string;
      outputFile: string;
      rows: number;
    }
  | {
      kind: "finish";
      outputPath: string;
      scriptPath: string;
      totalFiles: number;
      totalRows: number;
    };

export type PostgresCsvExportProgressListener = (
  event: PostgresCsvExportProgressEvent,
) => void;

export type PostgresCsvExportOptions = {
  outputPath?: string | undefined;
  dataset?: ImportDatasetType | undefined;
  scriptName?: string | undefined;
  onProgress?: PostgresCsvExportProgressListener | undefined;
};

export type PostgresCsvExportSummary = {
  inputPath: string;
  validatedPath: string;
  outputPath: string;
  scriptPath: string;
  manifestPath: string;
  totalFiles: number;
  totalRows: number;
  datasets: PostgresCsvDatasetSummary[];
  warnings: string[];
  nextStep?: string | undefined;
};

export type PostgresCsvFile = {
  dataset: ImportDatasetType;
  absolutePath: string;
  relativePath: string;
  rowCount: number;
};

export type PostgresDirectSourceFile = {
  dataset: ImportDatasetType;
  absolutePath: string;
  relativePath: string;
  fileSize: number;
};

export type PostgresDirectScriptDatasetSummary = {
  dataset: ImportDatasetType;
  files: number;
  totalBytes: number;
  sourceFiles: string[];
};

export type PostgresDirectTransactionMode = "single" | "phase" | "none";

export type PostgresDirectIncludeTarget =
  | "domains"
  | "companies"
  | "establishments"
  | "partners"
  | "simples"
  | "secondary-cnaes"
  | "indexes"
  | "analyze";

export type PostgresDirectScriptStep = {
  name: string;
  file: string;
  dependsOn: string[];
  included: boolean;
};

export type PostgresDirectScriptProgressEvent =
  | {
      kind: "start";
      inputPath: string;
      validatedPath: string;
      outputPath: string;
      totalFiles: number;
      datasets: ImportDatasetType[];
      sourceEncoding: string;
      transactionMode: PostgresDirectTransactionMode;
      include: PostgresDirectIncludeTarget[];
      skipIndexes: boolean;
      skipAnalyze: boolean;
    }
  | {
      kind: "file_registered";
      dataset: ImportDatasetType;
      fileIndex: number;
      totalFiles: number;
      inputFile: string;
      fileSize: number;
    }
  | {
      kind: "finish";
      outputPath: string;
      scriptPath: string;
      totalFiles: number;
      totalBytes: number;
    };

export type PostgresDirectScriptProgressListener = (
  event: PostgresDirectScriptProgressEvent,
) => void;

export type PostgresDirectScriptOptions = {
  outputPath?: string | undefined;
  dataset?: ImportDatasetType | undefined;
  scriptName?: string | undefined;
  sourceEncoding?: string | undefined;
  transactionMode?: PostgresDirectTransactionMode | undefined;
  include?: PostgresDirectIncludeTarget[] | undefined;
  skipIndexes?: boolean | undefined;
  skipAnalyze?: boolean | undefined;
  onProgress?: PostgresDirectScriptProgressListener | undefined;
};

export type PostgresDirectScriptSummary = {
  inputPath: string;
  validatedPath: string;
  outputPath: string;
  scriptPath: string;
  manifestPath: string;
  sourceEncoding: string;
  transactionMode: PostgresDirectTransactionMode;
  totalFiles: number;
  totalBytes: number;
  datasets: PostgresDirectScriptDatasetSummary[];
  scriptFiles: string[];
  steps: PostgresDirectScriptStep[];
  warnings: string[];
  nextStep?: string | undefined;
};
