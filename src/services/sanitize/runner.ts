import { createReadStream, createWriteStream } from "node:fs";
import { mkdir } from "node:fs/promises";
import path from "node:path";

import {
  normalizeSanitizeSourceEncoding,
  SanitizeEncodingNormalizer,
} from "./encoding.js";
import type { SanitizeFilePlan, SanitizedFileResult } from "./types.js";

async function writeUtf8(
  output: ReturnType<typeof createWriteStream>,
  value: string,
): Promise<void> {
  if (value.length === 0) {
    return;
  }

  if (!output.write(value, "utf8")) {
    await new Promise<void>((resolve, reject) => {
      output.once("drain", resolve);
      output.once("error", reject);
    });
  }
}

function countNewlines(value: string): number {
  let count = 0;

  for (let index = 0; index < value.length; index += 1) {
    if (value[index] === "\n") {
      count += 1;
    }
  }

  return count;
}

export async function sanitizeDatasetFile(
  plan: SanitizeFilePlan,
  onChunk?: (update: {
    bytesProcessed: number;
    fileBytesProcessed: number;
    currentFileSize: number;
    processedRows: number;
    nulBytesRemoved: number;
    invalidBytesRemoved: number;
    controlCharsRemoved: number;
  }) => void,
  options: { sourceEncoding?: string | undefined } = {},
): Promise<SanitizedFileResult> {
  await mkdir(path.dirname(plan.outputPath), { recursive: true });

  const sourceEncoding = normalizeSanitizeSourceEncoding(
    options.sourceEncoding,
  );
  const normalizer = new SanitizeEncodingNormalizer(sourceEncoding);
  const input = createReadStream(plan.absolutePath);
  const output = createWriteStream(plan.outputPath, { encoding: "utf8" });

  let totalBytesRead = 0;
  let totalBytesWritten = 0;
  let nulBytesRemoved = 0;
  let invalidBytesRemoved = 0;
  let controlCharsRemoved = 0;
  let lineCount = 0;
  let sawAnyCharacter = false;
  let lastCharacterWasNewline = false;

  const processText = async (text: string): Promise<void> => {
    if (text.length === 0) {
      return;
    }

    sawAnyCharacter = true;
    lineCount += countNewlines(text);
    lastCharacterWasNewline = text.endsWith("\n");
    totalBytesWritten += Buffer.byteLength(text, "utf8");
    await writeUtf8(output, text);
  };

  try {
    for await (const chunk of input) {
      const chunkBuffer = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
      totalBytesRead += chunkBuffer.length;

      const normalized = normalizer.normalizeChunk(chunkBuffer);
      nulBytesRemoved += normalized.nulBytesRemoved;
      invalidBytesRemoved += normalized.invalidBytesRemoved;
      controlCharsRemoved += normalized.controlCharsRemoved;
      await processText(normalized.text);

      onChunk?.({
        bytesProcessed: chunkBuffer.length,
        fileBytesProcessed: totalBytesRead,
        currentFileSize: plan.fileSize,
        processedRows: lineCount,
        nulBytesRemoved,
        invalidBytesRemoved,
        controlCharsRemoved,
      });
    }

    const flushed = normalizer.flush();
    nulBytesRemoved += flushed.nulBytesRemoved;
    invalidBytesRemoved += flushed.invalidBytesRemoved;
    controlCharsRemoved += flushed.controlCharsRemoved;
    await processText(flushed.text);

    if (sawAnyCharacter && !lastCharacterWasNewline) {
      lineCount += 1;
    }
  } finally {
    input.close();
    output.end();
    await new Promise<void>((resolve, reject) => {
      output.on("finish", () => resolve());
      output.on("error", (error) => reject(error));
    });
  }

  return {
    plan,
    totalBytesRead,
    totalBytesWritten,
    sourceEncoding,
    nulBytesRemoved,
    invalidBytesRemoved,
    controlCharsRemoved,
    lineCount,
    changed:
      nulBytesRemoved > 0 ||
      invalidBytesRemoved > 0 ||
      controlCharsRemoved > 0 ||
      totalBytesRead !== totalBytesWritten,
  };
}
