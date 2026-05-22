import { ValidationError } from "../../core/errors/index.js";
import {
  getCurrentFederalRevenueReference,
  resolveFederalRevenueReference,
  validateFederalRevenueReference,
} from "./client.js";
import {
  buildFederalRevenueReferenceOutputPath,
  evaluateFederalRevenueManifestFiles,
  getFederalRevenueManifestPath,
  readFederalRevenueManifest,
} from "./manifest.js";
import type {
  FederalRevenueClientOptions,
  FederalRevenueLocalStatusEntry,
  FederalRevenueReferenceMode,
  FederalRevenueStatusOptions,
  FederalRevenueStatusSummary,
} from "./types.js";

function toStatusEntry(
  file: Awaited<ReturnType<typeof evaluateFederalRevenueManifestFiles>>[number],
): FederalRevenueLocalStatusEntry {
  const entry: FederalRevenueLocalStatusEntry = {
    fileName: file.fileName,
    filePath: file.filePath,
    partialFilePath: file.partialFilePath,
    status: file.status,
  };

  if (file.remoteSizeInBytes !== undefined) {
    entry.remoteSizeInBytes = file.remoteSizeInBytes;
  }

  if (file.localSizeInBytes !== undefined) {
    entry.localSizeInBytes = file.localSizeInBytes;
  }

  if (file.errorMessage !== undefined) {
    entry.errorMessage = file.errorMessage;
  }

  return entry;
}

async function resolveLocalReference(
  options: FederalRevenueStatusOptions,
): Promise<{
  reference: string;
  mode: FederalRevenueReferenceMode;
  availableReferences: string[];
}> {
  if (options.reference) {
    validateFederalRevenueReference(options.reference);
    return {
      reference: options.reference,
      mode: "explicit",
      availableReferences: [],
    };
  }

  if (options.current) {
    return {
      reference: getCurrentFederalRevenueReference(),
      mode: "current",
      availableReferences: [],
    };
  }

  const selection = await resolveFederalRevenueReference(
    options as FederalRevenueClientOptions,
  );

  return {
    reference: selection.selectedReference,
    mode: selection.mode,
    availableReferences: selection.availableReferences,
  };
}

export async function getFederalRevenueStatus(
  options: FederalRevenueStatusOptions = {},
): Promise<FederalRevenueStatusSummary> {
  const selection = await resolveLocalReference(options);
  const outputPath = buildFederalRevenueReferenceOutputPath(
    selection.reference,
    options.outputPath,
  );
  const manifestPath = getFederalRevenueManifestPath(outputPath);
  const manifest = await readFederalRevenueManifest(outputPath);

  if (!manifest) {
    return {
      reference: selection.reference,
      selectionMode: selection.mode,
      outputPath,
      manifestPath,
      manifestFound: false,
      filesFound: 0,
      downloadedFiles: 0,
      failedFiles: 0,
      partialFiles: 0,
      missingFiles: 0,
      totalBytes: 0,
      localBytes: 0,
      isComplete: false,
      entries: [],
      warnings: [
        "No local Federal Revenue manifest was found for this reference. Run download or sync first.",
      ],
    };
  }

  if (manifest.reference !== selection.reference) {
    throw new ValidationError(
      `Federal Revenue manifest mismatch: expected ${selection.reference}, found ${manifest.reference}.`,
      { outputPath, manifestPath },
    );
  }

  const evaluatedFiles = await evaluateFederalRevenueManifestFiles(
    manifest.files,
  );
  const entries = evaluatedFiles.map(toStatusEntry);
  const downloadedFiles = entries.filter(
    (entry) => entry.status === "downloaded",
  ).length;
  const failedFiles = entries.filter(
    (entry) => entry.status === "failed",
  ).length;
  const partialFiles = entries.filter(
    (entry) => entry.status === "partial",
  ).length;
  const missingFiles = entries.filter(
    (entry) => entry.status === "missing",
  ).length;
  const totalBytes = entries.reduce(
    (sum, entry) => sum + (entry.remoteSizeInBytes ?? 0),
    0,
  );
  const localBytes = entries.reduce(
    (sum, entry) => sum + (entry.localSizeInBytes ?? 0),
    0,
  );
  const warnings: string[] = [];

  if (failedFiles > 0 || partialFiles > 0 || missingFiles > 0) {
    warnings.push(
      "The local Federal Revenue reference is not complete. Use retry to resume incomplete or failed files.",
    );
  }

  return {
    reference: selection.reference,
    selectionMode: selection.mode,
    outputPath,
    manifestPath,
    manifestFound: true,
    filesFound: entries.length,
    downloadedFiles,
    failedFiles,
    partialFiles,
    missingFiles,
    totalBytes,
    localBytes,
    isComplete:
      entries.length > 0 &&
      failedFiles === 0 &&
      partialFiles === 0 &&
      missingFiles === 0,
    entries,
    warnings,
    updatedAt: manifest.updatedAt,
    lastCommand: manifest.lastCommand,
    lastStatus: manifest.lastStatus,
  };
}
