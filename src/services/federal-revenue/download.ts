import { createWriteStream } from "node:fs";
import { mkdir, rename, stat, unlink } from "node:fs/promises";
import path from "node:path";
import { Readable } from "node:stream";
import type { ReadableStream as NodeReadableStream } from "node:stream/web";
import { pipeline } from "node:stream/promises";

import { ValidationError } from "../../core/errors/index.js";
import {
  buildFederalRevenueDownloadHeaders,
  listFederalRevenueFiles,
  resolveFederalRevenueReference,
} from "./client.js";
import type {
  FederalRevenueCheckOptions,
  FederalRevenueCheckSummary,
  FederalRevenueDownloadEntry,
  FederalRevenueDownloadOptions,
  FederalRevenueDownloadSummary,
  FederalRevenueFile,
} from "./types.js";

const DEFAULT_DOWNLOAD_ROOT = path.join(
  process.cwd(),
  "downloads",
  "federal-revenue",
);
const DEFAULT_DOWNLOAD_RETRIES = 3;

function resolveRetryCount(value: number | undefined): number {
  if (value === undefined || Number.isNaN(value)) {
    return DEFAULT_DOWNLOAD_RETRIES;
  }

  return Math.max(1, Math.floor(value));
}

function buildReferenceOutputPath(
  reference: string,
  outputPath?: string,
): string {
  return path.resolve(outputPath ?? DEFAULT_DOWNLOAD_ROOT, reference);
}

async function safeStat(filePath: string): Promise<{
  exists: boolean;
  size: number;
}> {
  try {
    const fileStat = await stat(filePath);
    return {
      exists: fileStat.isFile(),
      size: fileStat.size,
    };
  } catch {
    return {
      exists: false,
      size: 0,
    };
  }
}

async function safeUnlink(filePath: string): Promise<void> {
  try {
    await unlink(filePath);
  } catch {
    // Ignore cleanup errors. A later write attempt will surface any real issue.
  }
}

function isCompletedLocalFile(localSize: number, remoteSize?: number): boolean {
  if (remoteSize === undefined) {
    return localSize > 0;
  }

  return localSize === remoteSize;
}

async function downloadSingleFile(
  file: FederalRevenueFile,
  outputPath: string,
  options: FederalRevenueDownloadOptions,
): Promise<FederalRevenueDownloadEntry> {
  const filePath = path.join(outputPath, file.name);
  const partialFilePath = `${filePath}.part`;
  const localFile = await safeStat(filePath);

  if (
    !options.overwrite &&
    localFile.exists &&
    isCompletedLocalFile(localFile.size, file.sizeInBytes)
  ) {
    return {
      fileName: file.name,
      filePath,
      status: "skipped",
      sizeInBytes: localFile.size,
      remoteSizeInBytes: file.sizeInBytes,
    };
  }

  const retries = resolveRetryCount(options.retries);
  let lastError: unknown;

  for (let attempt = 1; attempt <= retries; attempt += 1) {
    await safeUnlink(partialFilePath);

    try {
      const response = await fetch(file.downloadUrl, {
        method: "GET",
        headers: buildFederalRevenueDownloadHeaders(options),
      });

      if (!response.ok) {
        throw new ValidationError(
          `Download failed with status ${response.status} ${response.statusText}.`,
          {
            fileName: file.name,
            status: response.status,
            statusText: response.statusText,
            attempt,
          },
        );
      }

      if (!response.body) {
        throw new ValidationError("Download response did not include a body.", {
          fileName: file.name,
          attempt,
        });
      }

      await pipeline(
        Readable.fromWeb(response.body as NodeReadableStream<Uint8Array>),
        createWriteStream(partialFilePath),
      );

      const downloadedFile = await safeStat(partialFilePath);
      if (
        file.sizeInBytes !== undefined &&
        downloadedFile.size !== file.sizeInBytes
      ) {
        throw new ValidationError(
          `Downloaded file size does not match the remote size for ${file.name}.`,
          {
            fileName: file.name,
            expectedSize: file.sizeInBytes,
            actualSize: downloadedFile.size,
            attempt,
          },
        );
      }

      await rename(partialFilePath, filePath);

      return {
        fileName: file.name,
        filePath,
        status: "downloaded",
        sizeInBytes: downloadedFile.size,
        remoteSizeInBytes: file.sizeInBytes,
      };
    } catch (error) {
      lastError = error;
    }
  }

  await safeUnlink(partialFilePath);

  return {
    fileName: file.name,
    filePath,
    status: "failed",
    remoteSizeInBytes: file.sizeInBytes,
    errorMessage:
      lastError instanceof Error ? lastError.message : String(lastError),
  };
}

export async function checkFederalRevenueDataset(
  options: FederalRevenueCheckOptions = {},
): Promise<FederalRevenueCheckSummary> {
  const selection = await resolveFederalRevenueReference(options);
  const { files, remoteBaseUrl } = await listFederalRevenueFiles(
    selection.selectedReference,
    options,
  );
  const totalBytes = files.reduce(
    (sum, file) => sum + (file.sizeInBytes ?? 0),
    0,
  );

  return {
    selectedReference: selection.selectedReference,
    selectionMode: selection.mode,
    availableReferences: selection.availableReferences,
    files,
    totalFiles: files.length,
    totalBytes,
    remoteBaseUrl,
  };
}

export async function downloadFederalRevenueDataset(
  options: FederalRevenueDownloadOptions = {},
): Promise<FederalRevenueDownloadSummary> {
  const startedAt = new Date().toISOString();
  const check = await checkFederalRevenueDataset(options);
  const outputPath = buildReferenceOutputPath(
    check.selectedReference,
    options.outputPath,
  );

  await mkdir(outputPath, { recursive: true });

  options.onProgress?.({
    kind: "start",
    reference: check.selectedReference,
    outputPath,
    totalFiles: check.totalFiles,
    totalBytes: check.totalBytes,
  });

  const entries: FederalRevenueDownloadEntry[] = [];
  let downloadedBytes = 0;
  let downloadedFiles = 0;
  let skippedFiles = 0;
  let failedFiles = 0;

  for (const [index, file] of check.files.entries()) {
    options.onProgress?.({
      kind: "file-start",
      reference: check.selectedReference,
      fileName: file.name,
      fileIndex: index + 1,
      totalFiles: check.totalFiles,
      completedFiles: entries.length,
      downloadedBytes,
      totalBytes: check.totalBytes,
      fileSizeInBytes: file.sizeInBytes,
    });

    const entry = await downloadSingleFile(file, outputPath, options);
    entries.push(entry);

    if (entry.status === "downloaded") {
      downloadedFiles += 1;
      downloadedBytes += entry.sizeInBytes ?? file.sizeInBytes ?? 0;
      options.onProgress?.({
        kind: "file-complete",
        reference: check.selectedReference,
        fileName: file.name,
        fileIndex: index + 1,
        totalFiles: check.totalFiles,
        completedFiles: entries.length,
        downloadedBytes,
        totalBytes: check.totalBytes,
        fileSizeInBytes: file.sizeInBytes,
      });
      continue;
    }

    if (entry.status === "skipped") {
      skippedFiles += 1;
      downloadedBytes += entry.sizeInBytes ?? file.sizeInBytes ?? 0;
      options.onProgress?.({
        kind: "file-skipped",
        reference: check.selectedReference,
        fileName: file.name,
        fileIndex: index + 1,
        totalFiles: check.totalFiles,
        completedFiles: entries.length,
        downloadedBytes,
        totalBytes: check.totalBytes,
        fileSizeInBytes: file.sizeInBytes,
      });
      continue;
    }

    failedFiles += 1;
    options.onProgress?.({
      kind: "file-failed",
      reference: check.selectedReference,
      fileName: file.name,
      fileIndex: index + 1,
      totalFiles: check.totalFiles,
      completedFiles: entries.length,
      downloadedBytes,
      totalBytes: check.totalBytes,
      errorMessage: entry.errorMessage ?? "Unknown download error",
      fileSizeInBytes: file.sizeInBytes,
    });
  }

  options.onProgress?.({
    kind: "finish",
    reference: check.selectedReference,
    outputPath,
    totalFiles: check.totalFiles,
    downloadedFiles,
    skippedFiles,
    failedFiles,
    downloadedBytes,
    totalBytes: check.totalBytes,
  });

  const warnings =
    failedFiles > 0
      ? [
          "Some Federal Revenue files could not be downloaded. Check the log file for details.",
        ]
      : [];

  return {
    reference: check.selectedReference,
    selectionMode: check.selectionMode,
    outputPath,
    remoteBaseUrl: check.remoteBaseUrl,
    filesFound: check.totalFiles,
    downloadedFiles,
    skippedFiles,
    failedFiles,
    totalBytes: check.totalBytes,
    downloadedBytes,
    entries,
    startedAt,
    finishedAt: new Date().toISOString(),
    warnings,
    nextStep: `cnpj-db-loader extract ${outputPath.replace(/\\/g, "/")}`,
  };
}
