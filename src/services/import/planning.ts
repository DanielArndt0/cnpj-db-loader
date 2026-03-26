import { createHash } from "node:crypto";
import { createReadStream } from "node:fs";
import { stat } from "node:fs/promises";
import { performance } from "node:perf_hooks";
import path from "node:path";

import type { FileInspection } from "../inspect.service.js";
import type {
  ImportDatasetPlan,
  ImportProgressListener,
  ImportSourceFile,
} from "./types.js";
import type { ImportDatasetType } from "./types.js";

export type IteratedLine = {
  line: string;
  nextOffset: number;
};

export type ImportPlanBuildResult = {
  datasets: ImportDatasetPlan[];
  totalFiles: number;
  totalRows: number;
  totalBatches: number;
  scanDurationMs: number;
  datasetScanDurationsMs: Partial<Record<ImportDatasetType, number>>;
};

export function resolveAbsolutePath(
  validatedPath: string,
  entry: FileInspection,
): string {
  return path.join(validatedPath, entry.relativePath);
}

export function buildDisplayPath(filePath: string): string {
  return `${path.basename(path.dirname(filePath))} > ${path.basename(filePath)}`;
}

export async function* iterateFileLines(
  filePath: string,
  startOffset = 0,
): AsyncGenerator<IteratedLine> {
  const stream = createReadStream(filePath, { start: startOffset });
  let buffered = Buffer.alloc(0);
  let offset = startOffset;

  try {
    for await (const chunk of stream) {
      const chunkBuffer = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
      buffered = buffered.length
        ? Buffer.concat([buffered, chunkBuffer])
        : chunkBuffer;

      while (true) {
        const newlineIndex = buffered.indexOf(0x0a);
        if (newlineIndex === -1) {
          break;
        }

        let lineBuffer = buffered.subarray(0, newlineIndex);
        const consumed = newlineIndex + 1;

        if (
          lineBuffer.length > 0 &&
          lineBuffer[lineBuffer.length - 1] === 0x0d
        ) {
          lineBuffer = lineBuffer.subarray(0, lineBuffer.length - 1);
        }

        offset += consumed;
        yield {
          line: lineBuffer.toString("utf8"),
          nextOffset: offset,
        };

        buffered = buffered.subarray(consumed);
      }
    }

    if (buffered.length > 0) {
      offset += buffered.length;
      yield {
        line: buffered.toString("utf8"),
        nextOffset: offset,
      };
    }
  } finally {
    stream.close();
  }
}

export async function countFileRowsExact(filePath: string): Promise<number> {
  let count = 0;

  for await (const item of iterateFileLines(filePath, 0)) {
    if (item.line.trim() !== "") {
      count += 1;
    }
  }

  return count;
}

export function sortEntries(
  left: FileInspection,
  right: FileInspection,
): number {
  return left.relativePath.localeCompare(right.relativePath, undefined, {
    numeric: true,
    sensitivity: "base",
  });
}

export async function collectImportSourceFiles(
  validatedPath: string,
  datasetEntries: Array<{
    dataset: ImportDatasetType;
    files: FileInspection[];
  }>,
): Promise<
  Array<{
    dataset: ImportDatasetType;
    files: ImportSourceFile[];
  }>
> {
  const collected: Array<{
    dataset: ImportDatasetType;
    files: ImportSourceFile[];
  }> = [];

  for (const item of datasetEntries) {
    const files: ImportSourceFile[] = [];

    for (const entry of item.files.sort(sortEntries)) {
      const absolutePath = resolveAbsolutePath(validatedPath, entry);
      const fileStats = await stat(absolutePath);
      files.push({
        dataset: item.dataset,
        absolutePath,
        relativePath: entry.relativePath,
        displayPath: buildDisplayPath(absolutePath),
        fileSize: fileStats.size,
        fileMtime: fileStats.mtime,
      });
    }

    collected.push({
      dataset: item.dataset,
      files,
    });
  }

  return collected;
}

export function buildImportPlanFingerprint(
  validatedPath: string,
  batchSize: number,
  datasetEntries: Array<{
    dataset: ImportDatasetType;
    files: ImportSourceFile[];
  }>,
): string {
  const hash = createHash("sha256");
  hash.update(
    JSON.stringify({
      validatedPath: path.resolve(validatedPath),
      batchSize,
      datasets: datasetEntries.map((item) => ({
        dataset: item.dataset,
        files: item.files.map((file) => ({
          relativePath: file.relativePath,
          fileSize: file.fileSize,
          fileMtime: file.fileMtime.toISOString(),
        })),
      })),
    }),
  );
  return hash.digest("hex");
}

export async function buildImportPlan(
  inputPath: string,
  validatedPath: string,
  datasetEntries: Array<{
    dataset: ImportDatasetType;
    files: ImportSourceFile[];
  }>,
  batchSize: number,
  onProgress: ImportProgressListener | undefined,
  targetDatabase: string,
): Promise<ImportPlanBuildResult> {
  const totalFiles = datasetEntries.reduce(
    (sum, item) => sum + item.files.length,
    0,
  );
  let scannedFiles = 0;
  let countedRows = 0;
  const planBuildStartedAt = performance.now();

  onProgress?.({
    kind: "preparing_start",
    inputPath: path.resolve(inputPath),
    validatedPath,
    totalDatasets: datasetEntries.length,
    totalFiles,
    batchSize,
    targetDatabase,
  });

  const datasets: ImportDatasetPlan[] = [];
  const datasetScanDurationsMs: Partial<Record<ImportDatasetType, number>> = {};

  for (const item of datasetEntries) {
    const files = [];
    let datasetRows = 0;
    let datasetBatches = 0;
    const datasetScanStartedAt = performance.now();

    for (const sourceFile of item.files) {
      const totalRows = await countFileRowsExact(sourceFile.absolutePath);
      const totalBatches =
        totalRows === 0 ? 0 : Math.ceil(totalRows / batchSize);

      files.push({
        dataset: item.dataset,
        absolutePath: sourceFile.absolutePath,
        displayPath: sourceFile.displayPath,
        fileSize: sourceFile.fileSize,
        fileMtime: sourceFile.fileMtime,
        totalRows,
        totalBatches,
      });

      scannedFiles += 1;
      countedRows += totalRows;
      datasetRows += totalRows;
      datasetBatches += totalBatches;

      onProgress?.({
        kind: "preparing_progress",
        scannedFiles,
        totalFiles,
        countedRows,
        currentFileDisplayPath: sourceFile.displayPath,
      });
    }

    datasetScanDurationsMs[item.dataset] =
      performance.now() - datasetScanStartedAt;

    datasets.push({
      dataset: item.dataset,
      files,
      totalRows: datasetRows,
      totalBatches: datasetBatches,
    });
  }

  const totalRows = datasets.reduce((sum, item) => sum + item.totalRows, 0);
  const totalBatches = datasets.reduce(
    (sum, item) => sum + item.totalBatches,
    0,
  );

  return {
    datasets,
    totalFiles,
    totalRows,
    totalBatches,
    scanDurationMs: performance.now() - planBuildStartedAt,
    datasetScanDurationsMs,
  };
}
