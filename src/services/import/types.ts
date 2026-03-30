import type { TableLayout } from "../../dictionary/layouts/index.js";
import {
  citiesLayout,
  cnaesLayout,
  companiesLayout,
  countriesLayout,
  establishmentsLayout,
  legalNaturesLayout,
  partnerQualificationsLayout,
  partnersLayout,
  reasonsLayout,
  simplesLayout,
} from "../../dictionary/layouts/index.js";
import type { DatasetType } from "../inspect.service.js";

export type ImportDatasetType = Exclude<DatasetType, "zip-archive" | "unknown">;

export type ImportCheckpointStatus =
  | "pending"
  | "in_progress"
  | "completed"
  | "failed";

export type ImportCheckpointRecord = {
  dataset: ImportDatasetType;
  filePath: string;
  fileSize: number;
  fileMtime: Date;
  byteOffset: number;
  rowsCommitted: number;
  status: ImportCheckpointStatus;
  lastError?: string | null;
};

export type ImportFilePlan = {
  dataset: ImportDatasetType;
  absolutePath: string;
  displayPath: string;
  fileSize: number;
  fileMtime: Date;
  totalRows: number;
  totalBatches: number;
  checkpoint?: ImportCheckpointRecord;
};

export type ImportSourceFile = {
  dataset: ImportDatasetType;
  absolutePath: string;
  relativePath: string;
  displayPath: string;
  fileSize: number;
  fileMtime: Date;
};

export type BatchRow = {
  values: unknown[];
  rawLine: string;
  nextOffset: number;
  sourceRowNumber: number;
  secondaryRows: Array<[string, string, number]>;
};

export type ImportDatasetPlan = {
  dataset: ImportDatasetType;
  files: ImportFilePlan[];
  totalRows: number;
  totalBatches: number;
};

export type ImportPlanStatus =
  | "planned"
  | "in_progress"
  | "completed"
  | "failed"
  | "cancelled";

export type ImportPhaseStatus =
  | "pending"
  | "in_progress"
  | "completed"
  | "failed";

export type ImportPlanRecord = {
  id: number;
  sourceFingerprint: string;
  inputPath: string;
  validatedPath: string;
  batchSize: number;
  targetDatabase: string;
  totalDatasets: number;
  totalFiles: number;
  totalRows: number;
  totalBatches: number;
  executionOrder: ImportDatasetType[];
  status: ImportPlanStatus;
  loadStatus: ImportPhaseStatus;
  materializationStatus: ImportPhaseStatus;
  lastPhase: string | null;
  lastError: string | null;
  createdAt: Date;
  updatedAt: Date;
  lastUsedAt: Date;
};

export type ImportWriteTarget = "final" | "staging";

export type ImportSchemaCapabilities = {
  includePartnerDedupeKeyInInsert: boolean;
};

export type ImportDatasetPerformanceSummary = {
  dataset: ImportDatasetType;
  files: number;
  plannedRows: number;
  importedRows: number;
  plannedBatches: number;
  committedBatches: number;
  resumedFiles: number;
  skippedCompletedFiles: number;
  retriedRows: number;
  retriedBatches: number;
  quarantinedRows: number;
  scanDurationMs: number;
  importDurationMs: number;
  insertDurationMs: number;
  retryDurationMs: number;
  quarantineDurationMs: number;
  materializationDurationMs: number;
  rowsPerSecond: number;
  batchesPerMinute: number;
};

export type ImportPerformanceSummary = {
  planReused: boolean;
  totalDurationMs: number;
  scanDurationMs: number;
  executionDurationMs: number;
  lookupLoadDurationMs: number;
  insertDurationMs: number;
  retryDurationMs: number;
  quarantineDurationMs: number;
  materializationDurationMs: number;
  rowsPerSecond: number;
  batchesPerMinute: number;
  datasets: ImportDatasetPerformanceSummary[];
};

export type ImportProgressEvent =
  | {
      kind: "preparing_start";
      inputPath: string;
      validatedPath: string;
      totalDatasets: number;
      totalFiles: number;
      batchSize: number;
      loadBatchSize?: number;
      materializeBatchSize?: number;
      targetDatabase: string;
    }
  | {
      kind: "preparing_progress";
      scannedFiles: number;
      totalFiles: number;
      countedRows: number;
      currentFileDisplayPath: string;
    }
  | {
      kind: "plan_ready";
      totalDatasets: number;
      totalFiles: number;
      batchSize: number;
      loadBatchSize?: number;
      materializeBatchSize?: number;
      totalRows: number;
      totalBatches: number;
      targetDatabase: string;
      executionOrder: ImportDatasetType[];
      reused: boolean;
      planId: number | null;
    }
  | {
      kind: "start";
      inputPath: string;
      validatedPath: string;
      totalDatasets: number;
      totalFiles: number;
      targetDatabase: string;
      totalRows: number;
      totalBatches: number;
      committedRows: number;
      committedBatches: number;
    }
  | {
      kind: "progress";
      dataset: ImportDatasetType;
      datasetIndex: number;
      totalDatasets: number;
      currentFilePath: string;
      currentFileDisplayPath: string;
      fileIndex: number;
      completedFiles: number;
      totalFiles: number;
      currentFileRowsCommitted: number;
      currentFileRowsTotal: number;
      committedRows: number;
      committedBatches: number;
      totalBatches: number;
      currentBatch: number;
      batchSize: number;
      checkpointOffset: number;
      currentFileSize: number;
      verboseProgress: boolean;
    }
  | {
      kind: "materialization_start";
      totalDatasets: number;
      datasets: ImportDatasetType[];
      completedFiles: number;
      totalFiles: number;
      processedRows: number;
      totalRows: number;
      committedBatches: number;
      totalBatches: number;
    }
  | {
      kind: "materialization_progress";
      dataset: ImportDatasetType;
      datasetIndex: number;
      totalDatasets: number;
      completedDatasets: number;
      targetTable: string;
      stepLabel: string;
      completedFiles: number;
      totalFiles: number;
      processedRows: number;
      totalRows: number;
      committedBatches: number;
      totalBatches: number;
      elapsedMs?: number;
    }
  | {
      kind: "materialization_finish";
      totalDatasets: number;
      completedDatasets: number;
      secondaryCnaesRows: number;
      secondaryCnaesDurationMs: number;
    }
  | {
      kind: "finish";
      totalDatasets: number;
      totalFiles: number;
      completedFiles: number;
      processedRows: number;
      totalRows: number;
      committedBatches: number;
      totalBatches: number;
      secondaryCnaesRows: number;
      quarantinedRows: number;
    };

export type ImportProgressListener = (event: ImportProgressEvent) => void;

export type ImportOptions = {
  dbUrl?: string;
  dataset?: ImportDatasetType | undefined;
  batchSize?: number | undefined;
  loadBatchSize?: number | undefined;
  materializeBatchSize?: number | undefined;
  verboseProgress?: boolean | undefined;
  onProgress?: ImportProgressListener | undefined;
};

export type ImportExecutionMode = "full" | "load" | "materialize";

export type ImportSummary = {
  executionMode: ImportExecutionMode;
  inputPath: string;
  validatedPath: string;
  targetDatabase: string;
  importPlanId: number | null;
  reusedImportPlan: boolean;
  importedDatasets: ImportDatasetType[];
  importedFiles: number;
  processedRows: number;
  plannedRows: number;
  committedBatches: number;
  plannedBatches: number;
  secondaryCnaesRows: number;
  quarantinedRows: number;
  resumedFiles: number;
  skippedCompletedFiles: number;
  datasetSummaries: Array<{
    dataset: ImportDatasetType;
    files: number;
    rows: number;
  }>;
  performance: ImportPerformanceSummary;
  warnings: string[];
  progressLogPath: string;
};

export const IMPORT_ORDER: ImportDatasetType[] = [
  "partner_qualifications",
  "legal_natures",
  "countries",
  "cities",
  "reasons",
  "cnaes",
  "companies",
  "establishments",
  "partners",
  "simples_options",
];

export const DATASET_LAYOUTS: Record<ImportDatasetType, TableLayout> = {
  companies: companiesLayout,
  establishments: establishmentsLayout,
  partners: partnersLayout,
  simples_options: simplesLayout,
  countries: countriesLayout,
  cities: citiesLayout,
  partner_qualifications: partnerQualificationsLayout,
  legal_natures: legalNaturesLayout,
  cnaes: cnaesLayout,
  reasons: reasonsLayout,
};

export type LookupTableName =
  | "partner_qualifications"
  | "legal_natures"
  | "company_sizes"
  | "branch_types"
  | "registration_statuses"
  | "reasons"
  | "countries"
  | "cnaes"
  | "cities"
  | "partner_types"
  | "age_groups";

export type LookupCacheMap = Map<LookupTableName, Set<string>>;

export const LOOKUP_TABLES: LookupTableName[] = [
  "partner_qualifications",
  "legal_natures",
  "company_sizes",
  "branch_types",
  "registration_statuses",
  "reasons",
  "countries",
  "cnaes",
  "cities",
  "partner_types",
  "age_groups",
];

export const LOOKUP_PLACEHOLDER_LABEL: Record<LookupTableName, string> = {
  partner_qualifications: "Imported placeholder qualification",
  legal_natures: "Imported placeholder legal nature",
  company_sizes: "Imported placeholder company size",
  branch_types: "Imported placeholder branch type",
  registration_statuses: "Imported placeholder registration status",
  reasons: "Imported placeholder registration reason",
  countries: "Imported placeholder country",
  cnaes: "Imported placeholder CNAE",
  cities: "Imported placeholder city",
  partner_types: "Imported placeholder partner type",
  age_groups: "Imported placeholder age group",
};

export function isImportDatasetType(value: string): value is ImportDatasetType {
  return IMPORT_ORDER.includes(value as ImportDatasetType);
}

export function maskDatabaseLabel(url: string): string {
  try {
    const parsed = new URL(url);
    const databaseName = parsed.pathname.replace(/^\//, "") || "database";
    return `${parsed.hostname}/${databaseName}`;
  } catch {
    return "configured database";
  }
}
