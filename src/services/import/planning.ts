import { createReadStream } from "node:fs";
import { stat } from "node:fs/promises";
import path from "node:path";

import type { FileInspection } from "../inspect.service.js";
import type {
  ImportDatasetPlan,
  ImportFilePlan,
  ImportProgressListener,
} from "./types.js";
import type { ImportDatasetType } from "./types.js";

export type IteratedLine = {
  line: string;
  nextOffset: number;
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

export async function buildImportPlan(
  inputPath: string,
  validatedPath: string,
  datasetEntries: Array<{
    dataset: ImportDatasetType;
    files: FileInspection[];
  }>,
  batchSize: number,
  onProgress: ImportProgressListener | undefined,
  targetDatabase: string,
): Promise<{
  datasets: ImportDatasetPlan[];
  totalFiles: number;
  totalRows: number;
  totalBatches: number;
}> {
  const totalFiles = datasetEntries.reduce(
    (sum, item) => sum + item.files.length,
    0,
  );
  let scannedFiles = 0;
  let countedRows = 0;

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

  for (const item of datasetEntries) {
    const files: ImportFilePlan[] = [];
    let datasetRows = 0;
    let datasetBatches = 0;

    for (const entry of item.files.sort(sortEntries)) {
      const absolutePath = resolveAbsolutePath(validatedPath, entry);
      const fileStats = await stat(absolutePath);
      const totalRows = await countFileRowsExact(absolutePath);
      const totalBatches =
        totalRows === 0 ? 0 : Math.ceil(totalRows / batchSize);

      files.push({
        dataset: item.dataset,
        entry,
        absolutePath,
        displayPath: buildDisplayPath(absolutePath),
        fileSize: fileStats.size,
        fileMtime: fileStats.mtime,
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
        currentFileDisplayPath: buildDisplayPath(absolutePath),
      });
    }

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

  onProgress?.({
    kind: "plan_ready",
    totalDatasets: datasets.length,
    totalFiles,
    batchSize,
    totalRows,
    totalBatches,
    targetDatabase,
    executionOrder: datasets.map((item) => item.dataset),
  });

  return { datasets, totalFiles, totalRows, totalBatches };
}
