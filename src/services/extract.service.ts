import { mkdir, readdir, stat } from "node:fs/promises";
import path from "node:path";
import extract from "extract-zip";

import { detectOs, defaultExtractedOutputPath } from "../core/utils/index.js";

export type ExtractionEntry = {
  archivePath: string;
  archiveName: string;
  destinationPath: string;
  success: boolean;
  sizeInBytes: number;
  errorMessage?: string;
};

export type ExtractionSummary = {
  inputPath: string;
  outputPath: string;
  operatingSystem: string;
  zipFilesFound: number;
  extractedArchives: string[];
  skippedEntries: string[];
  failedArchives: string[];
  totalArchiveBytes: number;
  extractedArchiveBytes: number;
  entries: ExtractionEntry[];
};

export type ExtractionProgressEvent =
  | {
      kind: "start";
      totalArchives: number;
      totalBytes: number;
      inputPath: string;
      outputPath: string;
    }
  | {
      kind: "archive-start";
      currentArchiveName: string;
      currentArchivePath: string;
      archiveSizeInBytes: number;
      archiveIndex: number;
      totalArchives: number;
      completedArchives: number;
      extractedBytes: number;
      totalBytes: number;
    }
  | {
      kind: "archive-complete";
      currentArchiveName: string;
      currentArchivePath: string;
      archiveSizeInBytes: number;
      archiveIndex: number;
      totalArchives: number;
      completedArchives: number;
      extractedBytes: number;
      totalBytes: number;
    }
  | {
      kind: "archive-failed";
      currentArchiveName: string;
      currentArchivePath: string;
      archiveSizeInBytes: number;
      archiveIndex: number;
      totalArchives: number;
      completedArchives: number;
      extractedBytes: number;
      totalBytes: number;
      errorMessage: string;
    }
  | {
      kind: "finish";
      totalArchives: number;
      completedArchives: number;
      failedArchives: number;
      extractedBytes: number;
      totalBytes: number;
      outputPath: string;
    };

export type ExtractionProgressListener = (
  event: ExtractionProgressEvent,
) => void;

async function findZipFiles(rootPath: string): Promise<string[]> {
  const entries = await readdir(rootPath, { withFileTypes: true });
  const found: string[] = [];

  for (const entry of entries) {
    const fullPath = path.join(rootPath, entry.name);

    if (entry.isDirectory()) {
      if (entry.name.toLowerCase() === "extracted") {
        continue;
      }

      found.push(...(await findZipFiles(fullPath)));
      continue;
    }

    if (entry.isFile() && entry.name.toLowerCase().endsWith(".zip")) {
      found.push(fullPath);
    }
  }

  return found.sort((left, right) => left.localeCompare(right));
}

async function extractSingleArchive(
  zipPath: string,
  outputRootPath: string,
  archiveSizeInBytes: number,
): Promise<ExtractionEntry> {
  const archiveName = path.basename(zipPath);
  const folderName = path.basename(zipPath, path.extname(zipPath));
  const destinationPath = path.join(outputRootPath, folderName);

  try {
    await mkdir(destinationPath, { recursive: true });
    await extract(zipPath, { dir: destinationPath });

    return {
      archivePath: zipPath,
      archiveName,
      destinationPath,
      success: true,
      sizeInBytes: archiveSizeInBytes,
    };
  } catch (error) {
    return {
      archivePath: zipPath,
      archiveName,
      destinationPath,
      success: false,
      sizeInBytes: archiveSizeInBytes,
      errorMessage: error instanceof Error ? error.message : String(error),
    };
  }
}

export async function extractArchives(
  inputPath: string,
  outputPath?: string,
  onProgress?: ExtractionProgressListener,
): Promise<ExtractionSummary> {
  const resolvedInputPath = path.resolve(inputPath);
  const resolvedOutputPath = path.resolve(
    outputPath ?? defaultExtractedOutputPath(resolvedInputPath),
  );
  await mkdir(resolvedOutputPath, { recursive: true });

  const zipFiles = await findZipFiles(resolvedInputPath);
  const zipFileStats = await Promise.all(
    zipFiles.map(async (zipFile) => ({ zipFile, stat: await stat(zipFile) })),
  );
  const totalArchiveBytes = zipFileStats.reduce(
    (sum, item) => sum + item.stat.size,
    0,
  );

  onProgress?.({
    kind: "start",
    totalArchives: zipFiles.length,
    totalBytes: totalArchiveBytes,
    inputPath: resolvedInputPath,
    outputPath: resolvedOutputPath,
  });

  const extractedArchives: string[] = [];
  const failedArchives: string[] = [];
  const entries: ExtractionEntry[] = [];
  let extractedArchiveBytes = 0;
  let completedArchivesCount = 0;

  for (const [index, item] of zipFileStats.entries()) {
    const archiveName = path.basename(item.zipFile);

    onProgress?.({
      kind: "archive-start",
      currentArchiveName: archiveName,
      currentArchivePath: item.zipFile,
      archiveSizeInBytes: item.stat.size,
      archiveIndex: index + 1,
      totalArchives: zipFiles.length,
      completedArchives: completedArchivesCount,
      extractedBytes: extractedArchiveBytes,
      totalBytes: totalArchiveBytes,
    });

    const result = await extractSingleArchive(
      item.zipFile,
      resolvedOutputPath,
      item.stat.size,
    );
    entries.push(result);

    if (result.success) {
      extractedArchives.push(result.archiveName);
      extractedArchiveBytes += result.sizeInBytes;
      completedArchivesCount += 1;

      onProgress?.({
        kind: "archive-complete",
        currentArchiveName: result.archiveName,
        currentArchivePath: result.archivePath,
        archiveSizeInBytes: result.sizeInBytes,
        archiveIndex: index + 1,
        totalArchives: zipFiles.length,
        completedArchives: completedArchivesCount,
        extractedBytes: extractedArchiveBytes,
        totalBytes: totalArchiveBytes,
      });
    } else {
      failedArchives.push(result.archiveName);
      onProgress?.({
        kind: "archive-failed",
        currentArchiveName: result.archiveName,
        currentArchivePath: result.archivePath,
        archiveSizeInBytes: result.sizeInBytes,
        archiveIndex: index + 1,
        totalArchives: zipFiles.length,
        completedArchives: completedArchivesCount,
        extractedBytes: extractedArchiveBytes,
        totalBytes: totalArchiveBytes,
        errorMessage: result.errorMessage ?? "Unknown extraction error",
      });
    }
  }

  const skippedEntries: string[] = [];
  const outputEntries = await readdir(resolvedOutputPath);

  for (const entry of outputEntries) {
    const fullPath = path.join(resolvedOutputPath, entry);
    const entryStat = await stat(fullPath);
    if (!entryStat.isFile() && !entryStat.isDirectory()) {
      skippedEntries.push(entry);
    }
  }

  onProgress?.({
    kind: "finish",
    totalArchives: zipFiles.length,
    completedArchives: completedArchivesCount,
    failedArchives: failedArchives.length,
    extractedBytes: extractedArchiveBytes,
    totalBytes: totalArchiveBytes,
    outputPath: resolvedOutputPath,
  });

  return {
    inputPath: resolvedInputPath,
    outputPath: resolvedOutputPath,
    operatingSystem: detectOs(),
    zipFilesFound: zipFiles.length,
    extractedArchives,
    skippedEntries,
    failedArchives,
    totalArchiveBytes,
    extractedArchiveBytes,
    entries,
  };
}
