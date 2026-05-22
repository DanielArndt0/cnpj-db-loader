import { mkdir, readFile, stat, writeFile } from "node:fs/promises";
import path from "node:path";

import { validateFederalRevenueReference } from "./client.js";
import type {
  FederalRevenueFile,
  FederalRevenueLocalFileStatus,
  FederalRevenueManifest,
  FederalRevenueManifestFile,
  FederalRevenueManifestLastCommand,
} from "./types.js";

export const FEDERAL_REVENUE_MANIFEST_VERSION = 1;
export const FEDERAL_REVENUE_CONTROL_DIR = ".cnpj-db-loader";
export const FEDERAL_REVENUE_CONTROL_SCOPE = "federal-revenue";
export const DEFAULT_FEDERAL_REVENUE_DOWNLOAD_ROOT = path.join(
  process.cwd(),
  "downloads",
  "federal-revenue",
);

export function buildFederalRevenueReferenceOutputPath(
  reference: string,
  outputPath?: string,
): string {
  validateFederalRevenueReference(reference);
  return path.resolve(
    outputPath ?? DEFAULT_FEDERAL_REVENUE_DOWNLOAD_ROOT,
    reference,
  );
}

export function getFederalRevenueControlDirectory(outputPath: string): string {
  return path.join(
    outputPath,
    FEDERAL_REVENUE_CONTROL_DIR,
    FEDERAL_REVENUE_CONTROL_SCOPE,
  );
}

export function getFederalRevenueManifestPath(outputPath: string): string {
  return path.join(
    getFederalRevenueControlDirectory(outputPath),
    "manifest.json",
  );
}

export function getFederalRevenueSyncLockPath(outputPath: string): string {
  return path.join(getFederalRevenueControlDirectory(outputPath), "sync.lock");
}

async function safeFileStat(filePath: string): Promise<{
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

function isCompleteSize(localSize: number, remoteSize?: number): boolean {
  if (remoteSize === undefined) {
    return localSize > 0;
  }

  return localSize === remoteSize;
}

function buildManifestFile(
  file: FederalRevenueFile,
  outputPath: string,
  previous?: FederalRevenueManifestFile,
): FederalRevenueManifestFile {
  const filePath = path.join(outputPath, file.name);
  const partialFilePath = `${filePath}.part`;
  const entry: FederalRevenueManifestFile = {
    fileName: file.name,
    filePath,
    partialFilePath,
    href: file.href,
    downloadUrl: file.downloadUrl,
    status: previous?.status ?? "missing",
    updatedAt: previous?.updatedAt ?? new Date().toISOString(),
  };

  if (file.sizeInBytes !== undefined) {
    entry.remoteSizeInBytes = file.sizeInBytes;
  }

  if (file.lastModified !== undefined) {
    entry.lastModified = file.lastModified;
  }

  if (file.etag !== undefined) {
    entry.etag = file.etag;
  }

  if (previous?.localSizeInBytes !== undefined) {
    entry.localSizeInBytes = previous.localSizeInBytes;
  }

  if (previous?.downloadedAt !== undefined) {
    entry.downloadedAt = previous.downloadedAt;
  }

  if (previous?.errorMessage !== undefined) {
    entry.errorMessage = previous.errorMessage;
  }

  return entry;
}

export async function readFederalRevenueManifest(
  outputPath: string,
): Promise<FederalRevenueManifest | undefined> {
  try {
    const manifestContent = await readFile(
      getFederalRevenueManifestPath(outputPath),
      "utf8",
    );
    return JSON.parse(manifestContent) as FederalRevenueManifest;
  } catch {
    return undefined;
  }
}

export async function writeFederalRevenueManifest(
  manifest: FederalRevenueManifest,
): Promise<void> {
  await mkdir(getFederalRevenueControlDirectory(manifest.outputPath), {
    recursive: true,
  });
  await writeFile(
    getFederalRevenueManifestPath(manifest.outputPath),
    `${JSON.stringify(manifest, null, 2)}\n`,
    "utf8",
  );
}

export async function createFederalRevenueManifest(input: {
  reference: string;
  outputPath: string;
  remoteBaseUrl: string;
  files: FederalRevenueFile[];
  lastCommand: FederalRevenueManifestLastCommand;
}): Promise<FederalRevenueManifest> {
  const existingManifest = await readFederalRevenueManifest(input.outputPath);
  const previousByName = new Map(
    existingManifest?.files.map((file) => [file.fileName, file]) ?? [],
  );
  const now = new Date().toISOString();

  const manifest: FederalRevenueManifest = {
    version: FEDERAL_REVENUE_MANIFEST_VERSION,
    reference: input.reference,
    remoteBaseUrl: input.remoteBaseUrl,
    outputPath: input.outputPath,
    createdAt: existingManifest?.createdAt ?? now,
    updatedAt: now,
    lastCommand: input.lastCommand,
    lastStatus: "running",
    files: input.files.map((file) =>
      buildManifestFile(file, input.outputPath, previousByName.get(file.name)),
    ),
  };

  await writeFederalRevenueManifest(manifest);
  return manifest;
}

export async function evaluateFederalRevenueManifestFile(
  entry: FederalRevenueManifestFile,
): Promise<FederalRevenueManifestFile> {
  const localFile = await safeFileStat(entry.filePath);
  const partialFile = await safeFileStat(entry.partialFilePath);
  const nextEntry: FederalRevenueManifestFile = {
    ...entry,
    updatedAt: new Date().toISOString(),
  };

  delete nextEntry.localSizeInBytes;

  if (
    localFile.exists &&
    isCompleteSize(localFile.size, entry.remoteSizeInBytes)
  ) {
    nextEntry.status = "downloaded";
    nextEntry.localSizeInBytes = localFile.size;
    delete nextEntry.errorMessage;
    return nextEntry;
  }

  if (partialFile.exists) {
    nextEntry.status = "partial";
    nextEntry.localSizeInBytes = partialFile.size;
    return nextEntry;
  }

  if (localFile.exists) {
    nextEntry.status = "partial";
    nextEntry.localSizeInBytes = localFile.size;
    nextEntry.errorMessage = `Local file size does not match the remote size. Expected ${entry.remoteSizeInBytes ?? "unknown"} byte(s), found ${localFile.size} byte(s).`;
    return nextEntry;
  }

  if (entry.status === "failed") {
    return nextEntry;
  }

  nextEntry.status = "missing";
  return nextEntry;
}

export async function evaluateFederalRevenueManifestFiles(
  entries: FederalRevenueManifestFile[],
): Promise<FederalRevenueManifestFile[]> {
  const evaluated: FederalRevenueManifestFile[] = [];

  for (const entry of entries) {
    evaluated.push(await evaluateFederalRevenueManifestFile(entry));
  }

  return evaluated;
}

export async function updateFederalRevenueManifestFile(
  outputPath: string,
  input: {
    fileName: string;
    status: FederalRevenueLocalFileStatus;
    localSizeInBytes?: number | undefined;
    errorMessage?: string | undefined;
    downloadedAt?: string | undefined;
  },
): Promise<void> {
  const manifest = await readFederalRevenueManifest(outputPath);
  if (!manifest) {
    return;
  }

  const updatedAt = new Date().toISOString();
  manifest.updatedAt = updatedAt;
  manifest.files = manifest.files.map((file) => {
    if (file.fileName !== input.fileName) {
      return file;
    }

    const updatedFile: FederalRevenueManifestFile = {
      ...file,
      status: input.status,
      updatedAt,
    };

    delete updatedFile.errorMessage;
    delete updatedFile.localSizeInBytes;
    delete updatedFile.downloadedAt;

    if (input.localSizeInBytes !== undefined) {
      updatedFile.localSizeInBytes = input.localSizeInBytes;
    }

    if (input.errorMessage !== undefined) {
      updatedFile.errorMessage = input.errorMessage;
    }

    if (input.downloadedAt !== undefined) {
      updatedFile.downloadedAt = input.downloadedAt;
    }

    return updatedFile;
  });

  await writeFederalRevenueManifest(manifest);
}

export async function finalizeFederalRevenueManifest(
  outputPath: string,
  lastStatus: FederalRevenueManifest["lastStatus"],
): Promise<void> {
  const manifest = await readFederalRevenueManifest(outputPath);
  if (!manifest) {
    return;
  }

  manifest.updatedAt = new Date().toISOString();
  manifest.lastStatus = lastStatus;
  manifest.files = await evaluateFederalRevenueManifestFiles(manifest.files);
  await writeFederalRevenueManifest(manifest);
}
