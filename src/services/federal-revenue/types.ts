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

export type FederalRevenueLocalFileStatus =
  | "downloaded"
  | "failed"
  | "partial"
  | "missing";

export type FederalRevenueManifestLastCommand = "download" | "sync" | "retry";

export type FederalRevenueManifestLastStatus =
  | "running"
  | "completed"
  | "failed";

export type FederalRevenueManifestFile = {
  fileName: string;
  filePath: string;
  partialFilePath: string;
  href: string;
  downloadUrl: string;
  status: FederalRevenueLocalFileStatus;
  updatedAt: string;
  remoteSizeInBytes?: number | undefined;
  localSizeInBytes?: number | undefined;
  lastModified?: string | undefined;
  etag?: string | undefined;
  downloadedAt?: string | undefined;
  errorMessage?: string | undefined;
};

export type FederalRevenueManifest = {
  version: 1;
  reference: string;
  remoteBaseUrl: string;
  outputPath: string;
  createdAt: string;
  updatedAt: string;
  lastCommand: FederalRevenueManifestLastCommand;
  lastStatus: FederalRevenueManifestLastStatus;
  files: FederalRevenueManifestFile[];
};

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
  manifestPath: string;
  remoteBaseUrl: string;
  filesFound: number;
  downloadedFiles: number;
  skippedFiles: number;
  failedFiles: number;
  partialFiles: number;
  missingFiles: number;
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
  incompleteOnly?: boolean | undefined;
  manifestCommand?: FederalRevenueManifestLastCommand | undefined;
  onProgress?: FederalRevenueDownloadProgressListener | undefined;
};

export type FederalRevenueSyncLockOptions = {
  forceLock?: boolean | undefined;
};

export type FederalRevenueSyncOptions = FederalRevenueDownloadOptions &
  FederalRevenueSyncLockOptions & {
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

export type FederalRevenueLocalStatusEntry = {
  fileName: string;
  filePath: string;
  partialFilePath: string;
  status: FederalRevenueLocalFileStatus;
  remoteSizeInBytes?: number | undefined;
  localSizeInBytes?: number | undefined;
  errorMessage?: string | undefined;
};

export type FederalRevenueStatusOptions = FederalRevenueCheckOptions & {
  outputPath?: string | undefined;
};

export type FederalRevenueStatusSummary = {
  reference: string;
  selectionMode: FederalRevenueReferenceMode;
  outputPath: string;
  manifestPath: string;
  manifestFound: boolean;
  filesFound: number;
  downloadedFiles: number;
  failedFiles: number;
  partialFiles: number;
  missingFiles: number;
  totalBytes: number;
  localBytes: number;
  isComplete: boolean;
  entries: FederalRevenueLocalStatusEntry[];
  warnings: string[];
  updatedAt?: string | undefined;
  lastCommand?: FederalRevenueManifestLastCommand | undefined;
  lastStatus?: FederalRevenueManifestLastStatus | undefined;
};

export type FederalRevenueRetryOptions = FederalRevenueDownloadOptions;

export type FederalRevenueCleanMode = "partials" | "failed" | "all";

export type FederalRevenueCleanOptions = FederalRevenueCheckOptions & {
  outputPath?: string | undefined;
  partials?: boolean | undefined;
  failed?: boolean | undefined;
  all?: boolean | undefined;
};

export type FederalRevenueCleanSummary = {
  reference: string;
  selectionMode: FederalRevenueReferenceMode;
  outputPath: string;
  manifestPath: string;
  mode: FederalRevenueCleanMode;
  removedFiles: number;
  removedBytes: number;
  removedPaths: string[];
  warnings: string[];
  startedAt: string;
  finishedAt: string;
};

export type FederalRevenueLockFile = {
  reference: string;
  outputPath: string;
  lockPath: string;
  pid: number;
  token: string;
  startedAt: string;
};
