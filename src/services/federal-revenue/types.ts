import type {
  ExtractionProgressListener,
  ExtractionSummary,
} from "../extract.service.js";
import type { ImportOptions, ImportSummary } from "../import.service.js";
import type { SanitizeOptions, SanitizeSummary } from "../sanitize.service.js";
import type { ValidationSummary } from "../validate.service.js";

export type FederalRevenueReferenceMode = "latest" | "current" | "explicit";

export type FederalRevenueClientOptions = {
  baseUrl?: string | undefined;
  shareToken?: string | undefined;
  userAgent?: string | undefined;
};

export type FederalRevenueReference = {
  reference: string;
  href: string;
};

export type FederalRevenueFile = {
  name: string;
  href: string;
  downloadUrl: string;
  sizeInBytes?: number | undefined;
  lastModified?: string | undefined;
  etag?: string | undefined;
};

export type FederalRevenueReferenceSelection = {
  mode: FederalRevenueReferenceMode;
  selectedReference: string;
  availableReferences: string[];
};

export type FederalRevenueCheckOptions = FederalRevenueClientOptions & {
  reference?: string | undefined;
  current?: boolean | undefined;
};

export type FederalRevenueCheckSummary = {
  selectedReference: string;
  selectionMode: FederalRevenueReferenceMode;
  availableReferences: string[];
  files: FederalRevenueFile[];
  totalFiles: number;
  totalBytes: number;
  remoteBaseUrl: string;
};

export type FederalRevenueDownloadStatus = "downloaded" | "skipped" | "failed";

export type FederalRevenueDownloadEntry = {
  fileName: string;
  filePath: string;
  status: FederalRevenueDownloadStatus;
  sizeInBytes?: number | undefined;
  remoteSizeInBytes?: number | undefined;
  errorMessage?: string | undefined;
};

export type FederalRevenueDownloadSummary = {
  reference: string;
  selectionMode: FederalRevenueReferenceMode;
  outputPath: string;
  remoteBaseUrl: string;
  filesFound: number;
  downloadedFiles: number;
  skippedFiles: number;
  failedFiles: number;
  totalBytes: number;
  downloadedBytes: number;
  entries: FederalRevenueDownloadEntry[];
  startedAt: string;
  finishedAt: string;
  warnings: string[];
  nextStep?: string | undefined;
};

export type FederalRevenueDownloadProgressEvent =
  | {
      kind: "start";
      reference: string;
      outputPath: string;
      totalFiles: number;
      totalBytes: number;
    }
  | {
      kind: "file-start";
      reference: string;
      fileName: string;
      fileIndex: number;
      totalFiles: number;
      completedFiles: number;
      downloadedBytes: number;
      totalBytes: number;
      fileSizeInBytes?: number | undefined;
    }
  | {
      kind: "file-complete";
      reference: string;
      fileName: string;
      fileIndex: number;
      totalFiles: number;
      completedFiles: number;
      downloadedBytes: number;
      totalBytes: number;
      fileSizeInBytes?: number | undefined;
    }
  | {
      kind: "file-skipped";
      reference: string;
      fileName: string;
      fileIndex: number;
      totalFiles: number;
      completedFiles: number;
      downloadedBytes: number;
      totalBytes: number;
      fileSizeInBytes?: number | undefined;
    }
  | {
      kind: "file-failed";
      reference: string;
      fileName: string;
      fileIndex: number;
      totalFiles: number;
      completedFiles: number;
      downloadedBytes: number;
      totalBytes: number;
      errorMessage: string;
      fileSizeInBytes?: number | undefined;
    }
  | {
      kind: "finish";
      reference: string;
      outputPath: string;
      totalFiles: number;
      downloadedFiles: number;
      skippedFiles: number;
      failedFiles: number;
      downloadedBytes: number;
      totalBytes: number;
    };

export type FederalRevenueDownloadProgressListener = (
  event: FederalRevenueDownloadProgressEvent,
) => void;

export type FederalRevenueDownloadOptions = FederalRevenueCheckOptions & {
  outputPath?: string | undefined;
  retries?: number | undefined;
  overwrite?: boolean | undefined;
  onProgress?: FederalRevenueDownloadProgressListener | undefined;
};

export type FederalRevenueSyncOptions = FederalRevenueDownloadOptions & {
  extractOutputPath?: string | undefined;
  sanitizeOutputPath?: string | undefined;
  sanitizeOptions?:
    | Omit<SanitizeOptions, "outputPath" | "onProgress">
    | undefined;
  importOptions?: Omit<ImportOptions, "onProgress"> | undefined;
  onExtractProgress?: ExtractionProgressListener | undefined;
  onSanitizeProgress?: SanitizeOptions["onProgress"] | undefined;
  onImportProgress?: ImportOptions["onProgress"] | undefined;
};

export type FederalRevenueSyncSummary = {
  reference: string;
  download: FederalRevenueDownloadSummary;
  extraction: ExtractionSummary;
  validation: ValidationSummary;
  sanitization: SanitizeSummary;
  import: ImportSummary;
  startedAt: string;
  finishedAt: string;
  warnings: string[];
};
