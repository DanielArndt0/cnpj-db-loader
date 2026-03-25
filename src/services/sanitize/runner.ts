import { createReadStream, createWriteStream } from "node:fs";
import { mkdir } from "node:fs/promises";
import path from "node:path";

import type { SanitizeFilePlan, SanitizedFileResult } from "./types.js";

function stripNulBytes(chunk: Buffer): { buffer: Buffer; removed: number } {
  let removed = 0;

  for (let index = 0; index < chunk.length; index += 1) {
    if (chunk[index] === 0x00) {
      removed += 1;
    }
  }

  if (removed === 0) {
    return { buffer: chunk, removed: 0 };
  }

  const sanitized = Buffer.allocUnsafe(chunk.length - removed);
  let outputIndex = 0;

  for (let index = 0; index < chunk.length; index += 1) {
    const value = chunk[index]!;
    if (value !== 0x00) {
      sanitized[outputIndex] = value;
      outputIndex += 1;
    }
  }

  return { buffer: sanitized, removed };
}

export async function sanitizeDatasetFile(
  plan: SanitizeFilePlan,
  onChunk?: (update: {
    bytesProcessed: number;
    fileBytesProcessed: number;
    currentFileSize: number;
    processedRows: number;
    nulBytesRemoved: number;
  }) => void,
): Promise<SanitizedFileResult> {
  await mkdir(path.dirname(plan.outputPath), { recursive: true });

  const input = createReadStream(plan.absolutePath);
  const output = createWriteStream(plan.outputPath);

  let totalBytesRead = 0;
  let totalBytesWritten = 0;
  let nulBytesRemoved = 0;
  let lineCount = 0;
  let sawAnyByte = false;
  let lastByteWasNewline = false;

  try {
    for await (const chunk of input) {
      const chunkBuffer = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
      totalBytesRead += chunkBuffer.length;

      const { buffer, removed } = stripNulBytes(chunkBuffer);
      nulBytesRemoved += removed;
      sawAnyByte = sawAnyByte || buffer.length > 0;

      for (let index = 0; index < buffer.length; index += 1) {
        if (buffer[index] === 0x0a) {
          lineCount += 1;
        }
      }

      if (buffer.length > 0) {
        lastByteWasNewline = buffer[buffer.length - 1] === 0x0a;
      }

      totalBytesWritten += buffer.length;
      output.write(buffer);

      onChunk?.({
        bytesProcessed: chunkBuffer.length,
        fileBytesProcessed: totalBytesRead,
        currentFileSize: plan.fileSize,
        processedRows: lineCount,
        nulBytesRemoved,
      });
    }

    if (sawAnyByte && !lastByteWasNewline) {
      lineCount += 1;
    }
  } finally {
    input.close();
    output.end();
    await new Promise<void>((resolve) => output.on("finish", () => resolve()));
  }

  return {
    plan,
    totalBytesRead,
    totalBytesWritten,
    nulBytesRemoved,
    lineCount,
    changed: nulBytesRemoved > 0 || totalBytesRead !== totalBytesWritten,
  };
}
