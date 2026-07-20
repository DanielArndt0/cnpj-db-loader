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
import {
  buildFederalRevenueReferenceOutputPath,
  createFederalRevenueManifest,
  evaluateFederalRevenueManifestFiles,
  finalizeFederalRevenueManifest,
  getFederalRevenueManifestPath,
  readFederalRevenueManifest,
  updateFederalRevenueManifestFile,
} from "./manifest.js";
import type {
  FederalRevenueCheckOptions,
  FederalRevenueCheckSummary,
  FederalRevenueDownloadEntry,
  FederalRevenueDownloadOptions,
  FederalRevenueDownloadSummary,
  FederalRevenueFile,
  FederalRevenueLocalFileStatus,
} from "./types.js";

const DEFAULT_DOWNLOAD_RETRIES = 3;

function resolveRetryCount(value: number | undefined): number {
  if (value === undefined || Number.isNaN(value)) {
    return DEFAULT_DOWNLOAD_RETRIES;
  }

  return Math.max(1, Math.floor(value));
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

function toLocalStatus(
  entry: FederalRevenueDownloadEntry,
): FederalRevenueLocalFileStatus {
  if (entry.status === "failed") {
    return "failed";
  }

  return "downloaded";
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
          `O download da Receita Federal falhou para ${file.name}: HTTP ${response.status} ${response.statusText}.`,
          {
            fileName: file.name,
            status: response.status,
            statusText: response.statusText,
            attempt,
          },
        );
      }

      if (!response.body) {
        throw new ValidationError(
          `O download da Receita Federal falhou para ${file.name}: o corpo da resposta está vazio.`,
          {
            fileName: file.name,
            attempt,
          },
        );
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
          `O download da Receita Federal falhou para ${file.name}: o tamanho local não corresponde ao tamanho remoto.`,
          {
            fileName: file.name,
            expectedSize: file.sizeInBytes,
            actualSize: downloadedFile.size,
            attempt,
          },
        );
      }

      await safeUnlink(filePath);
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

  return {
    fileName: file.name,
    filePath,
    status: "failed",
    remoteSizeInBytes: file.sizeInBytes,
    errorMessage:
      lastError instanceof Error ? lastError.message : String(lastError),
  };
}

function shouldDownloadFile(
  file: FederalRevenueFile,
  incompleteFileNames: Set<string> | undefined,
): boolean {
  if (!incompleteFileNames) {
    return true;
  }

  return incompleteFileNames.has(file.name);
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
  const outputPath = buildFederalRevenueReferenceOutputPath(
    check.selectedReference,
    options.outputPath,
  );
  const manifestPath = getFederalRevenueManifestPath(outputPath);

  await mkdir(outputPath, { recursive: true });
  await createFederalRevenueManifest({
    reference: check.selectedReference,
    outputPath,
    remoteBaseUrl: check.remoteBaseUrl,
    files: check.files,
    lastCommand: options.manifestCommand ?? "download",
  });

  const manifest = await readFederalRevenueManifest(outputPath);
  const evaluatedFiles = manifest
    ? await evaluateFederalRevenueManifestFiles(manifest.files)
    : [];
  const incompleteFileNames = options.incompleteOnly
    ? new Set(
        evaluatedFiles
          .filter((entry) => entry.status !== "downloaded")
          .map((entry) => entry.fileName),
      )
    : undefined;
  const filesToProcess = check.files.filter((file) =>
    shouldDownloadFile(file, incompleteFileNames),
  );

  options.onProgress?.({
    kind: "start",
    reference: check.selectedReference,
    outputPath,
    totalFiles: filesToProcess.length,
    totalBytes: check.totalBytes,
  });

  const entries: FederalRevenueDownloadEntry[] = [];
  let downloadedBytes = 0;
  let downloadedFiles = 0;
  let skippedFiles = 0;
  let failedFiles = 0;

  for (const [index, file] of filesToProcess.entries()) {
    options.onProgress?.({
      kind: "file-start",
      reference: check.selectedReference,
      fileName: file.name,
      fileIndex: index + 1,
      totalFiles: filesToProcess.length,
      completedFiles: entries.length,
      downloadedBytes,
      totalBytes: check.totalBytes,
      fileSizeInBytes: file.sizeInBytes,
    });

    const entry = await downloadSingleFile(file, outputPath, options);
    entries.push(entry);

    await updateFederalRevenueManifestFile(outputPath, {
      fileName: entry.fileName,
      status: toLocalStatus(entry),
      localSizeInBytes: entry.sizeInBytes,
      errorMessage: entry.errorMessage,
      downloadedAt:
        entry.status === "downloaded" || entry.status === "skipped"
          ? new Date().toISOString()
          : undefined,
    });

    if (entry.status === "downloaded") {
      downloadedFiles += 1;
      downloadedBytes += entry.sizeInBytes ?? file.sizeInBytes ?? 0;
      options.onProgress?.({
        kind: "file-complete",
        reference: check.selectedReference,
        fileName: file.name,
        fileIndex: index + 1,
        totalFiles: filesToProcess.length,
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
        totalFiles: filesToProcess.length,
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
      totalFiles: filesToProcess.length,
      completedFiles: entries.length,
      downloadedBytes,
      totalBytes: check.totalBytes,
      errorMessage: entry.errorMessage ?? "Erro de download desconhecido",
      fileSizeInBytes: file.sizeInBytes,
    });
  }

  await finalizeFederalRevenueManifest(
    outputPath,
    failedFiles > 0 ? "failed" : "completed",
  );

  const finalManifest = await readFederalRevenueManifest(outputPath);
  const finalFiles = finalManifest
    ? await evaluateFederalRevenueManifestFiles(finalManifest.files)
    : [];
  const partialFiles = finalFiles.filter(
    (entry) => entry.status === "partial",
  ).length;
  const missingFiles = finalFiles.filter(
    (entry) => entry.status === "missing",
  ).length;

  options.onProgress?.({
    kind: "finish",
    reference: check.selectedReference,
    outputPath,
    totalFiles: filesToProcess.length,
    downloadedFiles,
    skippedFiles,
    failedFiles,
    downloadedBytes,
    totalBytes: check.totalBytes,
  });

  const warnings: string[] = [];

  if (options.incompleteOnly && filesToProcess.length === 0) {
    warnings.push(
      "Nenhum arquivo incompleto da Receita Federal foi encontrado para repetição.",
    );
  }

  if (failedFiles > 0) {
    warnings.push(
      "Alguns arquivos da Receita Federal não puderam ser baixados. Verifique o arquivo de log e use retry após corrigir a causa.",
    );
  }

  return {
    reference: check.selectedReference,
    selectionMode: check.selectionMode,
    outputPath,
    manifestPath,
    remoteBaseUrl: check.remoteBaseUrl,
    filesFound: check.totalFiles,
    downloadedFiles,
    skippedFiles,
    failedFiles,
    partialFiles,
    missingFiles,
    totalBytes: check.totalBytes,
    downloadedBytes,
    entries,
    startedAt,
    finishedAt: new Date().toISOString(),
    warnings,
    nextStep: `cnpj-db-loader extract ${outputPath.replace(/\\/g, "/")}`,
  };
}

export async function retryFederalRevenueDataset(
  options: FederalRevenueDownloadOptions = {},
): Promise<FederalRevenueDownloadSummary> {
  return downloadFederalRevenueDataset({
    ...options,
    incompleteOnly: true,
    manifestCommand: "retry",
  });
}
