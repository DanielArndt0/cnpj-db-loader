import { readdir, rm, stat } from "node:fs/promises";
import path from "node:path";

import { ValidationError } from "../../core/errors/index.js";
import {
  getCurrentFederalRevenueReference,
  resolveFederalRevenueReference,
  validateFederalRevenueReference,
} from "./client.js";
import {
  buildFederalRevenueReferenceOutputPath,
  getFederalRevenueManifestPath,
  readFederalRevenueManifest,
  writeFederalRevenueManifest,
} from "./manifest.js";
import type {
  FederalRevenueCleanMode,
  FederalRevenueCleanOptions,
  FederalRevenueCleanSummary,
  FederalRevenueClientOptions,
  FederalRevenueManifestFile,
  FederalRevenueReferenceMode,
} from "./types.js";

async function safeStat(filePath: string): Promise<{
  exists: boolean;
  size: number;
  isDirectory: boolean;
}> {
  try {
    const fileStat = await stat(filePath);
    return {
      exists: true,
      size: fileStat.size,
      isDirectory: fileStat.isDirectory(),
    };
  } catch {
    return {
      exists: false,
      size: 0,
      isDirectory: false,
    };
  }
}

async function listPartialFiles(rootPath: string): Promise<string[]> {
  const rootStat = await safeStat(rootPath);
  if (!rootStat.exists || !rootStat.isDirectory) {
    return [];
  }

  const result: string[] = [];
  const entries = await readdir(rootPath, { withFileTypes: true });

  for (const entry of entries) {
    const entryPath = path.join(rootPath, entry.name);

    if (entry.isDirectory()) {
      result.push(...(await listPartialFiles(entryPath)));
      continue;
    }

    if (entry.isFile() && entry.name.endsWith(".part")) {
      result.push(entryPath);
    }
  }

  return result;
}

async function removeFileIfExists(filePath: string): Promise<{
  removed: boolean;
  size: number;
}> {
  const fileStat = await safeStat(filePath);
  if (!fileStat.exists || fileStat.isDirectory) {
    return { removed: false, size: 0 };
  }

  await rm(filePath, { force: true });
  return { removed: true, size: fileStat.size };
}

async function resolveCleanReference(
  options: FederalRevenueCleanOptions,
): Promise<{ reference: string; mode: FederalRevenueReferenceMode }> {
  if (options.reference) {
    validateFederalRevenueReference(options.reference);
    return { reference: options.reference, mode: "explicit" };
  }

  if (options.current) {
    return { reference: getCurrentFederalRevenueReference(), mode: "current" };
  }

  const selection = await resolveFederalRevenueReference(
    options as FederalRevenueClientOptions,
  );

  return { reference: selection.selectedReference, mode: selection.mode };
}

function resolveCleanMode(
  options: FederalRevenueCleanOptions,
): FederalRevenueCleanMode {
  const modes: FederalRevenueCleanMode[] = [];

  if (options.partials) {
    modes.push("partials");
  }

  if (options.failed) {
    modes.push("failed");
  }

  if (options.all) {
    modes.push("all");
  }

  if (modes.length !== 1) {
    throw new ValidationError(
      "Federal Revenue cleanup requires exactly one cleanup mode: --partials, --failed, or --all.",
    );
  }

  return modes[0]!;
}

function shouldCleanFailedEntry(file: FederalRevenueManifestFile): boolean {
  return file.status === "failed" || file.status === "partial";
}

export async function cleanFederalRevenueDataset(
  options: FederalRevenueCleanOptions = {},
): Promise<FederalRevenueCleanSummary> {
  const startedAt = new Date().toISOString();
  const cleanMode = resolveCleanMode(options);
  const selection = await resolveCleanReference(options);
  const outputPath = buildFederalRevenueReferenceOutputPath(
    selection.reference,
    options.outputPath,
  );
  const manifestPath = getFederalRevenueManifestPath(outputPath);
  const removedPaths: string[] = [];
  let removedBytes = 0;
  const warnings: string[] = [];

  if (cleanMode === "all") {
    const outputStat = await safeStat(outputPath);
    if (outputStat.exists) {
      await rm(outputPath, { recursive: true, force: true });
      removedPaths.push(outputPath);
      removedBytes += outputStat.size;
    } else {
      warnings.push(
        "The selected Federal Revenue reference folder does not exist locally.",
      );
    }

    return {
      reference: selection.reference,
      selectionMode: selection.mode,
      outputPath,
      manifestPath,
      mode: cleanMode,
      removedFiles: removedPaths.length,
      removedBytes,
      removedPaths,
      warnings,
      startedAt,
      finishedAt: new Date().toISOString(),
    };
  }

  if (cleanMode === "partials") {
    const partialFiles = await listPartialFiles(outputPath);

    for (const partialFile of partialFiles) {
      const result = await removeFileIfExists(partialFile);
      if (result.removed) {
        removedPaths.push(partialFile);
        removedBytes += result.size;
      }
    }

    if (partialFiles.length === 0) {
      warnings.push("No partial Federal Revenue download files were found.");
    }
  }

  if (cleanMode === "failed") {
    const manifest = await readFederalRevenueManifest(outputPath);

    if (!manifest) {
      warnings.push(
        "No local Federal Revenue manifest was found for this reference.",
      );
    } else {
      for (const file of manifest.files.filter(shouldCleanFailedEntry)) {
        for (const candidate of [file.filePath, file.partialFilePath]) {
          const result = await removeFileIfExists(candidate);
          if (result.removed) {
            removedPaths.push(candidate);
            removedBytes += result.size;
          }
        }
      }

      manifest.updatedAt = new Date().toISOString();
      manifest.files = manifest.files.map((file) => {
        if (!shouldCleanFailedEntry(file)) {
          return file;
        }

        const updatedFile: FederalRevenueManifestFile = {
          ...file,
          status: "missing",
          updatedAt: manifest.updatedAt,
        };
        delete updatedFile.localSizeInBytes;
        delete updatedFile.errorMessage;
        delete updatedFile.downloadedAt;
        return updatedFile;
      });
      await writeFederalRevenueManifest(manifest);
    }

    if (removedPaths.length === 0 && warnings.length === 0) {
      warnings.push("No failed or partial Federal Revenue files were found.");
    }
  }

  return {
    reference: selection.reference,
    selectionMode: selection.mode,
    outputPath,
    manifestPath,
    mode: cleanMode,
    removedFiles: removedPaths.length,
    removedBytes,
    removedPaths,
    warnings,
    startedAt,
    finishedAt: new Date().toISOString(),
  };
}
